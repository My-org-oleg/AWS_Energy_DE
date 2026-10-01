"""Integration tests for the marts materialized views (issue #9).

Run against the live dev PostGIS (`DATABASE_URL`).  The module-scoped fixture
loads both core kinds fresh, builds the three stored pivots, and refreshes
them; read-only tests query that shared state.  Tests that mutate core or the
views delete their probe rows and re-refresh before returning, so every later
test sees marts reconciled to core.
"""

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

from etl.config import CORE_SCHEMA, MARTS_SCHEMA, OUTSIDE_STATE
from etl.load import load_generators, load_storages
from etl.marts import _ACTIVE, _mart_definitions, build_marts, verify_marts

from conftest import drop_core_tables

ENGINE = create_engine(os.environ["DATABASE_URL"])

MART_NAMES = tuple(_mart_definitions(CORE_SCHEMA))


def _scalar(sql: str):
    with ENGINE.connect() as conn:
        return conn.execute(text(sql)).scalar()


def _drop_marts():
    with ENGINE.begin() as conn:
        for name in MART_NAMES:
            conn.execute(
                text(f"DROP MATERIALIZED VIEW IF EXISTS {MARTS_SCHEMA}.{name} CASCADE")
            )


@pytest.fixture(scope="module")
def _loaded_core():
    """Load both core kinds fresh before the marts are built."""
    _drop_marts()
    drop_core_tables(ENGINE)
    gen_report = load_generators()
    sto_report = load_storages()
    assert gen_report.passed, gen_report.errors
    assert sto_report.passed, sto_report.errors
    yield {"generators": gen_report, "storages": sto_report}
    _drop_marts()
    drop_core_tables(ENGINE)


@pytest.fixture(scope="module")
def _marts(_loaded_core):
    """Build and refresh the marts views once per module."""
    report = build_marts()
    assert report.passed, report.errors
    yield report
    _drop_marts()


@pytest.fixture(scope="module", autouse=True)
def _staging_ready(_staged_sources):
    """Staging is transformed once per session; nothing to do here."""


# ------------------------------------------------------------------ #
#  Table shape                                                         #
# ------------------------------------------------------------------ #


class TestTableShape:
    def test_marts_schema_exists(self, _loaded_core, _marts):
        n = _scalar(
            "SELECT COUNT(*) FROM information_schema.schemata "
            f"WHERE schema_name = '{MARTS_SCHEMA}'"
        )
        assert n == 1

    def test_three_materialized_views_exist(self, _loaded_core, _marts):
        with ENGINE.connect() as conn:
            views = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT matviewname FROM pg_matviews "
                        f"WHERE schemaname = '{MARTS_SCHEMA}'"
                    )
                ).fetchall()
            }
        assert views == set(MART_NAMES)

    def test_installation_counts_columns(self, _loaded_core, _marts):
        cols = self._column_names("installation_counts")
        assert {"state", "energy_source", "installation_count"} <= cols, (
            f"Missing columns: "
            f"{({'state', 'energy_source', 'installation_count'} - cols)}"
        )

    def test_generation_capacity_columns(self, _loaded_core, _marts):
        cols = self._column_names("generation_capacity")
        assert {"state", "energy_source", "generation_capacity"} <= cols, (
            f"Missing columns: "
            f"{({'state', 'energy_source', 'generation_capacity'} - cols)}"
        )

    def test_storage_capacity_columns(self, _loaded_core, _marts):
        cols = self._column_names("storage_capacity")
        assert {"state", "source_type", "storage_capacity"} <= cols, (
            f"Missing columns: "
            f"{({'state', 'source_type', 'storage_capacity'} - cols)}"
        )

    def _column_names(self, view: str) -> set[str]:
        with ENGINE.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {MARTS_SCHEMA}.{view} LIMIT 0"))
            return set(result.keys())


# ------------------------------------------------------------------ #
#  Shared helpers for the shared mart state                            #
# ------------------------------------------------------------------ #

# The active-unit rule written out again, on purpose. The reconciliation tests
# below compare the stored pivots against an aggregation computed from core
# with *this* text rather than with `etl.marts._ACTIVE`, so a rule that drifts
# in the pipeline cannot quietly redefine what the stored numbers are supposed
# to equal. Keep it in step with `etl/marts.py` by hand.
ACTIVE = (
    "commissioning_date <= CURRENT_DATE "
    "AND (decommissioning_date IS NULL OR decommissioning_date >= CURRENT_DATE)"
)


def _cells(sql: str) -> dict[tuple[str, str], float]:
    """Read an aggregate query into {(state, pivot): value} cells."""
    with ENGINE.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {(r[0], r[1]): float(r[2] or 0) for r in rows}


def _mart_cells(view: str, pivot: str, value: str) -> dict[tuple[str, str], float]:
    return _cells(f"SELECT state, {pivot}, {value} FROM {MARTS_SCHEMA}.{view}")


def _assert_cells_equal(
    expected_sql: str, view: str, pivot: str, value: str
) -> None:
    """Reconcile a stored pivot against a core aggregation (independent SQL)."""
    expected = _cells(expected_sql)
    stored = _mart_cells(view, pivot, value)
    assert set(stored) == set(expected), (
        f"{view} cell keys differ: only-in-mart="
        f"{set(stored) - set(expected)}, only-in-core={set(expected) - set(stored)}"
    )
    for key in expected:
        assert abs(stored[key] - expected[key]) < 0.01, (
            f"{view}[{key}]: stored {stored[key]} != core {expected[key]}"
        )


def _null_states(view: str) -> int:
    return int(_scalar(f"SELECT COUNT(*) FROM {MARTS_SCHEMA}.{view} WHERE state IS NULL"))


def _view_definition(view: str) -> str:
    """The SQL the database actually holds for a pivot, as Postgres prints it."""
    return str(
        _scalar(f"SELECT pg_get_viewdef('{MARTS_SCHEMA}.{view}'::regclass, true)")
    )


def _insert_generator(
    *,
    state: str | None,
    decommissioning_date=None,
    capacity: float = 123.5,
    commissioning_date: str = "2010-01-01",
) -> str:
    """Insert a distinctive probe row into core.generators; return its id."""
    ref_id = f"test_marts_probe_{uuid.uuid4().hex[:8]}"
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                f"INSERT INTO {CORE_SCHEMA}.generators "
                f"(energy_source, installed_capacity, commissioning_date, "
                f"decommissioning_date, longitude, latitude, geo_accuracy, "
                f"reference_id, reference_date, secondary_attributes, "
                f"country_iso, state, region, district, collision) "
                f"VALUES ('wind', :cap, :commissioning_date, :decommissioning_date, "
                f"10.0, 50.0, 1, :ref, '2020-01-01 00:00:00', NULL, "
                f"'DEU', :state, NULL, NULL, false)"
            ),
            {
                "cap": capacity,
                "commissioning_date": commissioning_date,
                "decommissioning_date": decommissioning_date,
                "ref": ref_id,
                "state": state,
            },
        )
    return ref_id


def _insert_storage(
    *, state: str | None, source_type: str = "Battery", capacity: float = 50.0
) -> str:
    """Insert a distinctive probe row into core.storages; return its id."""
    ref_id = f"test_marts_probe_{uuid.uuid4().hex[:8]}"
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                f"INSERT INTO {CORE_SCHEMA}.storages "
                f"(energy_source, storage_type, storage_capacity, installed_capacity, "
                f"commissioning_date, decommissioning_date, longitude, latitude, "
                f"geo_accuracy, reference_id, reference_date, secondary_attributes, "
                f"country_iso, state, region, district, collision) "
                f"VALUES ('storage', :source_type, :cap, NULL, '2010-01-01', NULL, "
                f"10.0, 50.0, 1, :ref, '2020-01-01 00:00:00', NULL, "
                f"'DEU', :state, NULL, NULL, false)"
            ),
            {
                "source_type": source_type,
                "cap": capacity,
                "ref": ref_id,
                "state": state,
            },
        )
    return ref_id


def _delete_probe(table: str, ref_id: str) -> None:
    with ENGINE.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {CORE_SCHEMA}.{table} WHERE reference_id = :ref"),
            {"ref": ref_id},
        )


def _set_decommissioning(table: str, ref_id: str, date: str | None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                f"UPDATE {CORE_SCHEMA}.{table} "
                f"SET decommissioning_date = :date WHERE reference_id = :ref"
            ),
            {"date": date, "ref": ref_id},
        )


def _today() -> str:
    """The database's `CURRENT_DATE`, so a test never races a local clock.

    Read through the connection rather than computed in Python: the pivots
    evaluate `CURRENT_DATE` server-side, and a test that disagreed with the
    server by a day would be testing the wrong boundary.
    """
    return str(_scalar("SELECT CURRENT_DATE"))


# ------------------------------------------------------------------ #
#  Build report contract                                              #
# ------------------------------------------------------------------ #


class TestBuildReport:
    def test_created_all_three_on_fresh_build(self, _loaded_core, _marts):
        assert _marts.created == list(MART_NAMES)

    def test_refreshed_all_three(self, _loaded_core, _marts):
        assert _marts.refreshed == list(MART_NAMES)

    def test_refresh_times_recorded_for_all(self, _loaded_core, _marts):
        assert set(_marts.refresh_times) == set(MART_NAMES)
        assert all(t > 0 for t in _marts.refresh_times.values())

    def test_verified_passes(self, _loaded_core, _marts):
        assert _marts.verified
        assert _marts.errors == []

    def test_build_is_idempotent(self, _loaded_core, _marts):
        """A second build creates nothing and still passes verification."""
        again = build_marts()
        assert again.created == []
        assert again.refreshed == list(MART_NAMES)
        assert again.verified
        assert again.errors == []
        assert again.recreated == []

    def test_a_stale_definition_is_recreated_not_just_refreshed(
        self, _loaded_core, _marts
    ):
        """Editing the predicate has to reach a database that already has the views.

        A materialized view is created once and refreshed thereafter, so a change
        to the pivot SQL would otherwise be refreshed into a view still carrying
        the old definition — and then verification, which compares against the new
        SQL, would report drift on every build forever with nothing to fix it.
        Recreating on a definition change is what makes the next edit to
        `_ACTIVE` a deploy rather than a manual drop.
        """
        stale = _mart_definitions(CORE_SCHEMA)["installation_counts"]
        stale_sql = stale.select_sql.replace(
            f"({_ACTIVE})",
            "(decommissioning_date IS NULL OR decommissioning_date > CURRENT_DATE)",
        )
        assert stale_sql != stale.select_sql, "the stale predicate must differ"
        with ENGINE.begin() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW {MARTS_SCHEMA}.installation_counts"))
            conn.execute(
                text(
                    f"CREATE MATERIALIZED VIEW {MARTS_SCHEMA}.installation_counts "
                    f"AS {stale_sql}"
                )
            )
            conn.execute(
                text(f"DELETE FROM {MARTS_SCHEMA}.definition_fingerprints")
            )

        report = build_marts()

        assert "installation_counts" in report.recreated
        assert report.created == []
        assert report.verified, report.errors
        # And the stored view now carries the rule the code has, not the one
        # the drop replaced.
        assert "commissioning_date <= CURRENT_DATE" in _view_definition(
            "installation_counts"
        )

    def test_a_view_with_no_recorded_fingerprint_counts_as_stale(
        self, _loaded_core, _marts
    ):
        """The database that predates the fingerprint table is the one to repair.

        Every deployment built before this table existed has views whose
        definition nobody has checked. Treating "no record" as "unknown, leave
        it" would record the current fingerprint over an unverified definition,
        and the build after that would find it matching and never look again —
        which is the case that actually needed fixing.
        """
        with ENGINE.begin() as conn:
            conn.execute(text(f"DELETE FROM {MARTS_SCHEMA}.definition_fingerprints"))

        report = build_marts()

        assert sorted(report.recreated) == sorted(MART_NAMES)
        assert report.created == []
        assert report.verified, report.errors
        # And it is now recorded, so the next build is quiet again.
        assert build_marts().recreated == []

    def test_a_fingerprint_that_has_not_changed_does_not_recreate(
        self, _loaded_core, _marts
    ):
        assert build_marts().recreated == []


# ------------------------------------------------------------------ #
#  Content reconciliation — pivots equal core active-unit aggregates  #
# ------------------------------------------------------------------ #


class TestContent:
    def test_installation_counts_match_core(self, _loaded_core, _marts):
        expected = f"""
            SELECT COALESCE(state, '{OUTSIDE_STATE}') AS state, energy_source,
                   COUNT(*) AS installation_count
            FROM (
                SELECT state, energy_source FROM {CORE_SCHEMA}.generators
                WHERE {ACTIVE}
                UNION ALL
                SELECT state, energy_source FROM {CORE_SCHEMA}.storages
                WHERE {ACTIVE}
            ) active_units
            GROUP BY state, energy_source
        """
        _assert_cells_equal(expected, "installation_counts", "energy_source", "installation_count")

    def test_generation_capacity_matches_core(self, _loaded_core, _marts):
        expected = f"""
            SELECT COALESCE(state, '{OUTSIDE_STATE}') AS state, energy_source,
                   SUM(installed_capacity) AS generation_capacity
            FROM {CORE_SCHEMA}.generators
            WHERE {ACTIVE}
            GROUP BY state, energy_source
        """
        _assert_cells_equal(expected, "generation_capacity", "energy_source", "generation_capacity")

    def test_storage_capacity_matches_core(self, _loaded_core, _marts):
        expected = f"""
            SELECT COALESCE(state, '{OUTSIDE_STATE}') AS state,
                   storage_type AS source_type,
                   SUM(storage_capacity) AS storage_capacity
            FROM {CORE_SCHEMA}.storages
            WHERE {ACTIVE}
            GROUP BY state, storage_type
        """
        _assert_cells_equal(expected, "storage_capacity", "source_type", "storage_capacity")

    def test_storage_capacity_keyed_by_source_type(self, _loaded_core, _marts):
        """The storage pivot keys on source_type, not energy_source."""
        with ENGINE.connect() as conn:
            keys = {
                row[0]
                for row in conn.execute(
                    text(
                        f"SELECT DISTINCT source_type FROM {MARTS_SCHEMA}.storage_capacity "
                        "ORDER BY 1"
                    )
                ).fetchall()
            }
        assert keys == {"Battery", "Pumped storage", "Hydrogen storage"}


# ------------------------------------------------------------------ #
#  Active units only + decommissioned exclusion                        #
# ------------------------------------------------------------------ #


class TestActiveOnly:
    def test_decommissioned_generator_excluded(self, _loaded_core, _marts):
        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        caps = _mart_cells("generation_capacity", "energy_source", "generation_capacity")
        key = ("Bayern", "wind")
        base_count, base_cap = counts[key], caps[key]

        ref_id = _insert_generator(state="Bayern")
        build_marts()
        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        caps = _mart_cells("generation_capacity", "energy_source", "generation_capacity")
        assert counts[key] == base_count + 1
        assert abs(caps[key] - (base_cap + 123.5)) < 0.01

        _set_decommissioning("generators", ref_id, "2000-01-01")
        build_marts()
        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        caps = _mart_cells("generation_capacity", "energy_source", "generation_capacity")
        assert counts[key] == base_count
        assert abs(caps[key] - base_cap) < 0.01

        _delete_probe("generators", ref_id)
        build_marts()

    def test_decommissioned_storage_excluded(self, _loaded_core, _marts):
        cells = _mart_cells("storage_capacity", "source_type", "storage_capacity")
        key = ("Bayern", "Battery")
        base = cells[key]

        ref_id = _insert_storage(state="Bayern")
        build_marts()
        cells = _mart_cells("storage_capacity", "source_type", "storage_capacity")
        assert abs(cells[key] - (base + 50.0)) < 0.01

        _set_decommissioning("storages", ref_id, "2000-01-01")
        build_marts()
        cells = _mart_cells("storage_capacity", "source_type", "storage_capacity")
        assert abs(cells[key] - base) < 0.01

        _delete_probe("storages", ref_id)
        build_marts()

    def test_generator_commissioned_in_the_future_is_excluded(
        self, _loaded_core, _marts
    ):
        """A unit published before it starts running is not installed yet.

        The source can legitimately carry a commissioning date ahead of today —
        a planned plant is data, not an error, and it stays in Core. What it
        must not do is appear in today's installation count, which is a
        statement about what exists now.
        """
        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        caps = _mart_cells("generation_capacity", "energy_source", "generation_capacity")
        key = ("Bayern", "wind")
        base_count, base_cap = counts[key], caps[key]

        ref_id = _insert_generator(
            state="Bayern", commissioning_date="2999-01-01", capacity=500.0
        )
        build_marts()

        assert _mart_cells("installation_counts", "energy_source", "installation_count")[
            key
        ] == base_count
        assert abs(
            _mart_cells("generation_capacity", "energy_source", "generation_capacity")[
                key
            ]
            - base_cap
        ) < 0.01

        _delete_probe("generators", ref_id)
        build_marts()

    def test_generator_decommissioned_today_is_still_active(self, _loaded_core, _marts):
        """"Not decommissioned before the interval end" includes the end.

        This is the one day the two layers are most likely to disagree: the
        visualization reads `>=` for the same unit. A strict bound here would
        drop it from the marts while the map still shows it.
        """
        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        key = ("Bayern", "wind")
        base_count = counts[key]

        ref_id = _insert_generator(state="Bayern", capacity=7.0)
        _set_decommissioning("generators", ref_id, _today())
        build_marts()

        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        assert counts[key] == base_count + 1

        _delete_probe("generators", ref_id)
        build_marts()


# ------------------------------------------------------------------ #
#  NULL handling — all-NULL capacity groups report 0, never NULL      #
# ------------------------------------------------------------------ #


class TestNullValues:
    def test_null_generator_capacity_group_reports_zero(self, _loaded_core, _marts):
        state = "Marts_Null_Cap"
        ref_id = _insert_generator(state=state, capacity=None)
        build_marts()
        try:
            with ENGINE.connect() as conn:
                value = conn.execute(
                    text(
                        f"SELECT generation_capacity FROM {MARTS_SCHEMA}.generation_capacity "
                        "WHERE state = :state AND energy_source = 'wind'"
                    ),
                    {"state": state},
                ).scalar()
            assert value is not None, "expected a row for the all-NULL capacity group"
            assert value == 0, f"expected 0, got {value!r}"
        finally:
            _delete_probe("generators", ref_id)
            build_marts()

    def test_null_storage_capacity_group_reports_zero(self, _loaded_core, _marts):
        state = "Marts_Null_Cap"
        ref_id = _insert_storage(state=state, capacity=None)
        build_marts()
        try:
            with ENGINE.connect() as conn:
                value = conn.execute(
                    text(
                        f"SELECT storage_capacity FROM {MARTS_SCHEMA}.storage_capacity "
                        "WHERE state = :state AND source_type = 'Battery'"
                    ),
                    {"state": state},
                ).scalar()
            assert value is not None, "expected a row for the all-NULL capacity group"
            assert value == 0, f"expected 0, got {value!r}"
        finally:
            _delete_probe("storages", ref_id)
            build_marts()


class TestOutsideBucket:
    def test_state_null_units_reported_as_outside(self, _loaded_core, _marts):
        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        caps = _mart_cells("generation_capacity", "energy_source", "generation_capacity")
        out_key = (OUTSIDE_STATE, "wind")
        base_count, base_cap = counts[out_key], caps[out_key]

        ref_id = _insert_generator(state=None)
        build_marts()

        for view in MART_NAMES:
            assert _null_states(view) == 0, f"{view} has a NULL-state row"

        counts = _mart_cells("installation_counts", "energy_source", "installation_count")
        caps = _mart_cells("generation_capacity", "energy_source", "generation_capacity")
        assert counts[out_key] == base_count + 1
        assert abs(caps[out_key] - (base_cap + 123.5)) < 0.01

        _delete_probe("generators", ref_id)
        build_marts()

    def test_outside_cells_match_state_null_core_rows(self, _loaded_core, _marts):
        expected = f"""
            SELECT '{OUTSIDE_STATE}' AS state, energy_source, COUNT(*) AS installation_count
            FROM {CORE_SCHEMA}.generators
            WHERE ({ACTIVE}) AND state IS NULL
            GROUP BY energy_source
            UNION ALL
            SELECT '{OUTSIDE_STATE}' AS state, energy_source, COUNT(*) AS installation_count
            FROM {CORE_SCHEMA}.storages
            WHERE ({ACTIVE}) AND state IS NULL
            GROUP BY energy_source
        """
        stored = {
            key: v
            for key, v in _mart_cells(
                "installation_counts", "energy_source", "installation_count"
            ).items()
            if key[0] == OUTSIDE_STATE
        }
        core = _cells(expected)
        assert stored == core


# ------------------------------------------------------------------ #
#  Verification — fail loudly on drift                                 #
# ------------------------------------------------------------------ #


class TestVerification:
    def test_verify_passes_when_fresh(self, _loaded_core, _marts):
        assert verify_marts(ENGINE) == []

    def test_verify_fails_on_unrefreshed_core_change(self, _loaded_core, _marts):
        gen_id = _insert_generator(state="Bayern")
        sto_id = _insert_storage(state="Bayern")
        try:
            errors = verify_marts(ENGINE)
        finally:
            _delete_probe("generators", gen_id)
            _delete_probe("storages", sto_id)
            build_marts()

        assert errors, "verify_marts must fail loudly after an unrefreshed core change"
        assert any("installation_counts" in e for e in errors)
        assert any("generation_capacity" in e for e in errors)
        assert any("storage_capacity" in e for e in errors)


# ------------------------------------------------------------------ #
#  Core precondition — must run last (drops core tables)              #
# ------------------------------------------------------------------ #


class TestCorePrecondition:
    def test_missing_core_tables_fails_cleanly(self, _loaded_core):
        """A build without the core tables fails gracefully, not with a raw DDL error."""
        drop_core_tables(ENGINE)
        report = build_marts()
        assert not report.passed
        assert report.created == []
        assert report.refreshed == []
        assert any(
            "core.generators" in e and "core.storages" in e for e in report.errors
        ), f"expected a clear core-tables-missing error, got {report.errors}"
        # _loaded_core teardown will drop whatever remains


class TestCurrentDatePivots:
    """The marts stay current-date pivots (issue #9).

    They are the stored, spec-literal shape and cannot answer an arbitrary
    interval, so the visualization timescope reads Core instead.  These tests
    pin that boundary: the active-unit rule the pivots are built from is
    anchored to ``CURRENT_DATE`` and takes no interval parameters, so it cannot
    be mistaken for a substitute for a historical Core query.
    """

    def test_the_active_rule_is_anchored_to_the_current_date(self):
        assert "CURRENT_DATE" in _ACTIVE

    def test_the_active_rule_takes_no_interval_parameters(self):
        for definition in _mart_definitions(CORE_SCHEMA).values():
            assert ":from" not in definition.select_sql
            assert ":to" not in definition.select_sql

    def test_every_pivot_filters_on_the_same_active_rule(self):
        for definition in _mart_definitions(CORE_SCHEMA).values():
            assert _ACTIVE in definition.select_sql

    def test_the_active_rule_has_an_upper_commissioning_bound(self):
        """A unit that is not commissioned yet is not Active.

        "Active" is the interval rule with its end pinned to today, and that
        rule has two bounds: commissioned by the end of the interval, and not
        decommissioned before it. The second was here alone, which counted a
        generator that starts running next year among the units installed today
        — in the marts and in the visualization, which would then disagree
        about the same unit on the same day.
        """
        assert "commissioning_date <= CURRENT_DATE" in _ACTIVE

    def test_the_decommissioning_bound_is_inclusive(self):
        """A unit decommissioned on the report date still counts for that date.

        "Has not ended before the interval end" is `>=`, not `>`. The
        visualization already reads that way, so a strict bound here would put
        the two layers a day apart on the one day a unit is switched off.
        """
        assert "decommissioning_date >= CURRENT_DATE" in _ACTIVE
