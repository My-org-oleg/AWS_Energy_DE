"""Shared session-scoped fixtures: a dedicated test database plus the fixture data.

The suite used to read `DATABASE_URL` and rely on the dev database already
holding `raw.*` populated from the real 81,655-row GPKGs, with the private
`data/boundaries/*.gpkg` alongside it. Two problems with that: a fresh
checkout failed in ways that looked like product bugs, and a test run mutated
whatever the developer had in their dev database.

So the suite is now hermetic. It runs against `TEST_DATABASE_URL`, refuses to
run if that resolves to the same database as `DATABASE_URL`, and rebuilds the
pipeline schemas from the small committed fixtures in `tests/fixtures/`
(regenerate with `python scripts/make_test_fixtures.py`). Nothing here reads
`data/`.

Tests that need no database (`tests/test_viz_*.py`) never connect, so they keep
running without one — with or without `TEST_DATABASE_URL` set.
"""

import os
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy import text

from etl.config import SOURCE_NAMES
from etl.transform import transform_sources

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent
FIXTURE_DIR = TESTS_DIR / "fixtures"
BOUNDARY_MANIFEST = FIXTURE_DIR / "boundaries" / "manifest.txt"
SOURCE_FIXTURES = FIXTURE_DIR / "sources"
CLEAN_SLATE_SQL = REPO_ROOT / "scripts" / "drop_all_pipeline_data.sql"

# PostgreSQL SQLSTATE for "the database does not exist".


def _database_of(url: str) -> str | None:
    """The database a URL points at, without connecting to it.

    The guard has to run before the test database exists — creating it is the
    fixture's next step, and a first run is the common case — so asking the
    server is not an option. `make_url` still normalises the driver spelling, so
    `postgresql://host/db` and `postgresql+psycopg2://host/db` compare equal.

    Trade-off: this compares database *names*, so two URLs naming the same
    database on different servers are treated as a collision. That errs towards
    refusing to run, which is the safe direction for a guard whose whole job is
    to protect dev data.
    """
    return sa.make_url(url).database


def _clean_slate_sql() -> str:
    """`drop_all_pipeline_data.sql` without its psql meta-commands.

    Reusing the script keeps "what an empty database means" defined in one
    place for the dev host and for the tests, instead of a second copy of the
    dependency-ordered drop logic drifting here.
    """
    body = "\n".join(
        line
        for line in CLEAN_SLATE_SQL.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("\\")
    )
    # Guard against the script being emptied or gutted: it selects its targets
    # from an array of schema names, so if that array stops covering a schema
    # the pipeline owns, "clean slate" quietly stops being one.
    from etl.config import (
        CORE_SCHEMA,
        MARTS_SCHEMA,
        RAW_SCHEMA,
        SERVICE_SCHEMA,
        STAGING_SCHEMA,
    )

    missing = [
        schema
        for schema in (
            RAW_SCHEMA,
            STAGING_SCHEMA,
            CORE_SCHEMA,
            SERVICE_SCHEMA,
            MARTS_SCHEMA,
        )
        if f"'{schema}'" not in body
    ]
    assert not missing, f"{CLEAN_SLATE_SQL} does not clean {missing}"
    return body


# The dev URL as configured before this session redirects anything. Captured
# here because the redirect below overwrites DATABASE_URL, and the safety guard
# in `_test_database` still needs to know what the developer pointed at.
DEV_DATABASE_URL = os.environ.get("DATABASE_URL")

# A deliberately unreachable database name, used when TEST_DATABASE_URL is not
# set. Pointing DATABASE_URL here means *no* test can reach the dev database
# even by accident — not just the ones that request `_test_database`. An earlier
# conditional redirect left tests that build their own engine free to write to
# dev. The name is self-documenting so any connection attempt says
# `database "no_test_database_url_set" does not exist`.
UNSET_TEST_DATABASE_URL = "postgresql+psycopg2://localhost/no_test_database_url_set"

# Redirect the session at import time, not from a fixture. Seven test modules
# build `ENGINE = create_engine(os.environ["DATABASE_URL"])` while being
# imported, and pytest imports test modules *after* conftest but *before* any
# fixture runs — so an override inside a fixture would arrive too late and
# those modules would keep pointing at the dev database. etl.config is already
# imported above (and has loaded .env), so this assignment wins.
#
# This is unconditional on purpose. When TEST_DATABASE_URL is missing we still
# overwrite DATABASE_URL, so the dev database is structurally out of reach and
# the dev-mutation guard below cannot be side-stepped by a test that forgets to
# request the fixture. The cost is that a bare `pytest` on a fresh checkout
# fails with our own message instead of a KeyError during collection, which is
# the better failure: it says what to set. Tests that need no database
# (tests/test_viz_*.py) still run, because they never connect.
os.environ["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL") or UNSET_TEST_DATABASE_URL


def _database_exists(admin: sa.Engine, name: str) -> bool:
    with admin.connect() as conn:
        return bool(
            conn.execute(
                sa.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": name},
            ).scalar()
        )


def _ensure_database_exists(url: str) -> None:
    """Create the test database if it is missing.

    Connects to the server's `postgres` maintenance database, because
    `CREATE DATABASE` cannot run inside the database it is creating. Existence
    is checked rather than inferred from an error, so re-running the suite
    against an existing database is a no-op. A missing role or an unreachable
    server surfaces as the original error, with a hint, instead of being
    swallowed.
    """
    target = sa.make_url(url)
    admin = sa.create_engine(target.set(database="postgres"))
    try:
        if _database_exists(admin, target.database):
            return
        quoted = admin.dialect.identifier_preparer.quote(target.database)
        try:
            with admin.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as conn:
                conn.execute(sa.text(f"CREATE DATABASE {quoted}"))
        except sa.exc.DatabaseError as exc:
            # Two cases land here and neither is a bug in the suite: the role
            # lacks CREATEDB, or a parallel run created the database in between
            # the check and the CREATE. Re-check before deciding.
            if not _database_exists(admin, target.database):
                pytest.fail(
                    f"Could not create the test database {target.database!r} on "
                    f"{target.host}:{target.port or 5432}. The role in "
                    f"TEST_DATABASE_URL needs the CREATEDB privilege — an empty "
                    f"database name is enough, the suite creates it. Original "
                    f"error: {exc}"
                )
    except sa.exc.OperationalError as exc:
        pytest.fail(
            f"Could not reach {target.host}:{target.port or 5432} to provision "
            f"the test database {target.database!r}. Original error: {exc}"
        )
    finally:
        admin.dispose()


@pytest.fixture(scope="session")
def _test_database():
    """Point the session at a dedicated, empty test database.

    Fails rather than skips when `TEST_DATABASE_URL` is missing or collides
    with `DATABASE_URL`: a database test that quietly ran against nothing is a
    false green, which is worse than a loud failure. Nothing is dropped until
    both checks have passed, so a misconfigured run cannot reach dev data.

    Otherwise provisions the database if it is missing, adds PostGIS, and drops
    the pipeline schemas, so a freshly created empty database is enough.
    """
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is not set. The suite needs its own database so "
            "it cannot clobber dev data (issue #12); point it at a scratch "
            "PostGIS database, e.g. "
            "postgresql+psycopg2://etl:etl@localhost:5432/energy_de_test — see "
            "TEST_DATABASE_URL in .env.example, and note the port must match "
            "the server in DATABASE_URL. The database is created for you if "
            "the role has CREATEDB."
        )

    dev_url = DEV_DATABASE_URL
    target = _database_of(url)
    if dev_url and target is not None and target == _database_of(dev_url):
        pytest.fail(
            f"TEST_DATABASE_URL and DATABASE_URL both point at database "
            f"{target!r}. Refusing to run the suite against the dev database; "
            f"point TEST_DATABASE_URL at a separate scratch database."
        )

    _ensure_database_exists(url)

    engine = sa.create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))
            # The clean-slate script is one multi-statement DO block, which
            # SQLAlchemy's parameter-binding wrappers reject, so it goes
            # straight through the DBAPI cursor.
            cursor = connection.connection.cursor()
            try:
                cursor.execute(_clean_slate_sql())
            finally:
                cursor.close()
    finally:
        engine.dispose()
    return _database_of(url)


@pytest.fixture(scope="session")
def _boundary_fixtures(_test_database):
    """Load the small boundary fixtures, so the spatial join resolves for real.

    The transform joins boundary levels 1/2/3 and fails when
    `service.boundaries` is missing, so the Source fixtures are useless
    without this. The fixture points sit inside the fixture polygons, giving
    deterministic state/region/district values.
    """
    from etl.extract import extract_boundaries

    report = extract_boundaries(BOUNDARY_MANIFEST)
    assert report.passed, report.errors
    return report


@pytest.fixture(scope="session")
def _staged_sources(_boundary_fixtures):
    """Extract the six Source fixtures into staging once per session.

    Extraction goes through the real content-routed path, so the fixture files
    are validated, cast and versioned exactly as a published snapshot would
    be. Sharing the staged result is safe because every test that mutates
    staging restores it (or re-transforms the affected source in a `finally`).
    """
    from etl.extract import extract_source

    for source in SOURCE_NAMES:
        path = SOURCE_FIXTURES / f"{source}.gpkg"
        assert path.exists(), (
            f"missing fixture {path}; regenerate with "
            f"`python scripts/make_test_fixtures.py`"
        )
        report = extract_source(path)
        assert report.passed, f"{source}: {report.errors}"

    for source in SOURCE_NAMES:
        report = transform_sources(source)
        assert report.passed, report.errors


def drop_core_tables(engine) -> None:
    """Drop both Core kinds and their per-kind property tables.

    Shared by the integration modules that load Core from the fixtures
    (`tests/test_marts.py`, `tests/test_viz_timescope.py`), so the drop list
    lives in one place.  Dropping a Core table cascades to the marts that
    read it, which is why the marts are dropped first in those modules.
    """
    from etl.config import CORE_SCHEMA

    with engine.begin() as conn:
        for table, links, props in (
            ("generators", "generator_units_properties", "generator_properties"),
            ("storages", "storage_units_properties", "storage_properties"),
        ):
            for name in (links, props):
                conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.{name} CASCADE"))
            conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.{table} CASCADE"))
