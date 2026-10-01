"""Marts layer: three stored wide pivots at state grain (issue #9).

The marts are Postgres `MATERIALIZED VIEW`s (ADR 0003) computed from the
active units in core — `marts.installation_counts` (state × energy_source,
counting active units), `marts.generation_capacity` (state × energy_source,
summing active generator installed_capacity) and `marts.storage_capacity`
(state × source_type, summing active storage storage_capacity).  They are
built and refreshed on demand by `python -m etl marts`; after refresh the
stored pivots are verified against a fresh aggregation of core and any drift
is reported as a failure.  A state-null unit (a load-stage collision that
still reaches core) is reported under the OUTSIDE_STATE bucket rather than a
NULL key.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import get_engine
from etl.config import CORE_SCHEMA, MARTS_SCHEMA, OUTSIDE_STATE
from etl.db_utils import _ensure_schema, _table_exists
from etl.reports import MartsReport
from etl.verify import _verify_marts

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MartDefinition:
    """One stored-pivot definition: key column, value column, and SQL.

    ``select_sql`` must produce exactly the columns ``(state, pivot, value)``
    under those final aliases.  It is used both to create the materialized
    view and as the live expected aggregation for verification, so the check
    is always "does the stored pivot reconcile to current core?".  The shared
    ACTIVE_UNITS predicate keeps the three pivots consistent (ADR 0003).
    """

    pivot: str
    value: str
    select_sql: str


# "Active" is the interval rule with its end pinned to today (CONTEXT.md), so
# both of its bounds are here: commissioned by today, and not decommissioned
# before today. A generator starting next year is not installed today, and a
# unit switched off today has not ended before today — hence `>=` on the second
# bound, which is what "not decommissioned before" means at an inclusive end.
#
# This is the visualization's timescope rule (`viz.data.ACTIVE_UNIT_PREDICATE`)
# with its interval collapsed to today, but not a verbatim copy of it: that
# predicate spans `commissioning_date BETWEEN :from AND :to`, so evaluating it
# at a single day would mean commissioned *exactly* today. The marts ask the
# different and intended question — commissioned at any point up to today — and
# the two agree about a unit commissioned in the past, which is every real one.
#
# A NULL `commissioning_date` excludes a unit, because `NULL <= CURRENT_DATE` is
# not true. That is deliberate: a unit with no commissioning date has not been
# shown to be installed, and the visualization excludes it for the same reason.
# The old one-sided predicate counted such units, so this is a real narrowing
# for any row missing the date.
_ACTIVE = (
    "commissioning_date <= CURRENT_DATE "
    "AND (decommissioning_date IS NULL OR decommissioning_date >= CURRENT_DATE)"
)

def _mart_definitions(core_schema: str) -> dict[str, _MartDefinition]:
    return {
        "installation_counts": _MartDefinition(
            pivot="energy_source",
            value="installation_count",
            select_sql=f"""
                SELECT COALESCE(state, '{OUTSIDE_STATE}') AS state,
                       energy_source, COUNT(*) AS installation_count
                FROM (
                    SELECT state, energy_source FROM {core_schema}.generators
                    WHERE ({_ACTIVE})
                    UNION ALL
                    SELECT state, energy_source FROM {core_schema}.storages
                    WHERE ({_ACTIVE})
                ) active_units
                GROUP BY COALESCE(state, '{OUTSIDE_STATE}'), energy_source
            """,
        ),
        "generation_capacity": _MartDefinition(
            pivot="energy_source",
            value="generation_capacity",
            select_sql=f"""
                SELECT COALESCE(state, '{OUTSIDE_STATE}') AS state,
                       energy_source,
                       ROUND(COALESCE(SUM(installed_capacity), 0)::numeric, 6)
                           AS generation_capacity
                FROM {core_schema}.generators
                WHERE ({_ACTIVE})
                GROUP BY COALESCE(state, '{OUTSIDE_STATE}'), energy_source
            """,
        ),
        "storage_capacity": _MartDefinition(
            pivot="source_type",
            value="storage_capacity",
            select_sql=f"""
                SELECT COALESCE(state, '{OUTSIDE_STATE}') AS state,
                       storage_type AS source_type,
                       ROUND(COALESCE(SUM(storage_capacity), 0)::numeric, 6)
                           AS storage_capacity
                FROM {core_schema}.storages
                WHERE ({_ACTIVE})
                GROUP BY COALESCE(state, '{OUTSIDE_STATE}'), storage_type
            """,
        ),
    }



def build_marts(engine: Engine | None = None) -> MartsReport:
    """Create, refresh, and verify the marts materialized views.

    Ensures the marts schema and the three stored pivots exist, recreates any
    whose definition no longer matches the SQL here, refreshes all three from
    core, and verifies the stored pivots reconcile to the live active-unit
    aggregates.  A failed creation, refresh, or verification is captured in the
    report's errors; ``python -m etl marts`` exits non-zero when it happens
    (fail loudly).
    """
    report = MartsReport()
    start = time.perf_counter()
    try:
        engine = engine or get_engine()
        missing = [
            t
            for t in ("generators", "storages")
            if not _table_exists(engine, t, CORE_SCHEMA)
        ]
        if missing:
            report.errors.append(
                "core tables missing: "
                + ", ".join(f"{CORE_SCHEMA}.{t}" for t in missing)
                + "; run 'python -m etl load' first"
            )
        else:
            definitions = _mart_definitions(CORE_SCHEMA)
            report.created, report.recreated = _create_marts(engine, definitions)
            report.refresh_times = _refresh_marts(engine, definitions)
            report.refreshed = list(definitions)
            report.errors = _verify_marts(engine, definitions)
            report.verified = not report.errors
    except Exception as e:
        report.errors.append(f"Marts failed: {e}")
        log.exception("Marts failed")
    report.total_time = time.perf_counter() - start
    if report.errors:
        for err in report.errors:
            log.error("Marts failed: %s", err)
    else:
        log.info(
            "Marts built (%s new, %s recreated, refreshed %s, %.3fs)",
            ", ".join(report.created) or "none",
            ", ".join(report.recreated) or "none",
            ", ".join(report.refreshed) or "none",
            report.total_time,
        )
    return report


def verify_marts(engine: Engine) -> list[str]:
    """Reconcile the stored marts to live core; return the drift errors."""
    return _verify_marts(engine, _mart_definitions(CORE_SCHEMA))


def _fingerprint(sql: str) -> str:
    """A stable digest of one pivot's SQL, insensitive to how it is laid out.

    Whitespace is collapsed first so that reformatting the query is not read as
    a change of meaning — only an edit to the query itself is. The digest is of
    *this* SQL rather than of what `pg_get_viewdef` returns, because Postgres
    prints its own normalised, re-quoted version of whatever it was given and
    the two never match as text.
    """
    normalised = " ".join(sql.split())
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


def _ensure_fingerprints(engine: Engine) -> None:
    """The record of which SQL each stored pivot was last created from.

    One row per pivot, in the marts schema beside the views it describes rather
    than in `service`. ADR 0007 puts *pipeline* operational metadata in `service`
    — what has been loaded, what has been processed — and this is none of that: it
    describes a view in this schema and is meaningless without the view, so it is
    dropped and recreated with it and `drop_all_pipeline_data.sql` clears it along
    with everything else. The blocked-startup alert claims in
    `service.bootstrap_alerts` are the opposite case and do live in `service`.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {MARTS_SCHEMA}.definition_fingerprints (
                    view_name TEXT PRIMARY KEY,
                    source_fingerprint TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )


def _present_matviews(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        return {
            name
            for (name,) in conn.execute(
                text(
                    "SELECT matviewname FROM pg_matviews WHERE schemaname = :schema"
                ),
                {"schema": MARTS_SCHEMA},
            ).all()
        }


def _create_mart(engine: Engine, name: str, definition: _MartDefinition) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE MATERIALIZED VIEW {MARTS_SCHEMA}.{name} "
                f"AS {definition.select_sql}"
            )
        )


def _create_marts(
    engine: Engine,
    definitions: dict[str, _MartDefinition],
) -> tuple[list[str], list[str]]:
    """Create the missing pivots and replace the stale ones.

    Returns `(created, recreated)`: the pivots that did not exist, and the ones
    that did exist but no longer hold the SQL written here.

    A materialized view is created once and refreshed thereafter, so a change to
    the SQL above does not reach a database that already has the view — it goes
    on executing the old active-unit rule forever, and verification, which
    compares the stored cells against *this* SQL, reports drift on every build
    with nothing anyone can do about it. Comparing a fingerprint of the source
    SQL against the one recorded at creation is what makes the next edit to that
    rule a deploy rather than a manual `DROP MATERIALIZED VIEW` somebody has to
    remember to run.

    A view whose fingerprint is absent counts as stale, not as unknown. That is
    the case for every database built before this table existed, and it is
    precisely the one that has to be repaired: leaving it alone would record the
    current fingerprint over a definition nobody had checked, and the next build
    would then find it matching and never look again. Replacing a pivot is safe
    because a pivot holds nothing of its own — it is derived from core on every
    refresh — and these three names are the pipeline's own.
    """
    _ensure_schema(engine, MARTS_SCHEMA)
    _ensure_fingerprints(engine)
    with engine.connect() as conn:
        recorded = {
            view_name: source_fingerprint
            for view_name, source_fingerprint in conn.execute(
                text(
                    f"SELECT view_name, source_fingerprint "
                    f"FROM {MARTS_SCHEMA}.definition_fingerprints"
                )
            ).all()
        }
    present = _present_matviews(engine)
    fingerprints = {
        name: _fingerprint(definition.select_sql)
        for name, definition in definitions.items()
    }
    created: list[str] = []
    recreated: list[str] = []
    for name, definition in definitions.items():
        if name in present:
            if recorded.get(name) == fingerprints[name]:
                continue
            _replace_mart(engine, name, definition)
            recreated.append(name)
            log.info(
                "Recreated materialized view %s.%s: its definition no longer "
                "matches the pivot SQL",
                MARTS_SCHEMA,
                name,
            )
        else:
            created.append(name)
            log.info("Created materialized view %s.%s", MARTS_SCHEMA, name)
            _create_mart(engine, name, definition)
    _record_fingerprints(engine, fingerprints)
    return created, recreated


def _replace_mart(engine: Engine, name: str, definition: _MartDefinition) -> None:
    """Drop and recreate one pivot in a single transaction.

    The drop and the create have to be the same transaction. Across two, a
    failure in between — a lost connection, a lock timeout on the replacement —
    leaves a pivot that does not exist at all, and `build_marts` has already
    committed to replacing it. The next build would recreate it, so the damage
    is self-repairing, but the deployment is wrong in the meantime and the
    failure that caused it is the kind that tends to repeat.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP MATERIALIZED VIEW {MARTS_SCHEMA}.{name}"))
        conn.execute(
            text(
                f"CREATE MATERIALIZED VIEW {MARTS_SCHEMA}.{name} "
                f"AS {definition.select_sql}"
            )
        )


def _record_fingerprints(
    engine: Engine, fingerprints: dict[str, str]
) -> None:
    """Write down which SQL each pivot now holds, for the next build to compare."""
    with engine.begin() as conn:
        for name, fingerprint in fingerprints.items():
            conn.execute(
                text(
                    f"INSERT INTO {MARTS_SCHEMA}.definition_fingerprints "
                    "(view_name, source_fingerprint) VALUES (:name, :fingerprint) "
                    "ON CONFLICT (view_name) DO UPDATE "
                    "SET source_fingerprint = EXCLUDED.source_fingerprint, "
                    "updated_at = CURRENT_TIMESTAMP"
                ),
                {"name": name, "fingerprint": fingerprint},
            )


def _refresh_marts(
    engine: Engine,
    definitions: dict[str, _MartDefinition],
) -> dict[str, float]:
    """Refresh every materialized view; return per-view elapsed seconds."""
    times: dict[str, float] = {}
    with engine.begin() as conn:
        for name in definitions:
            t = time.perf_counter()
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {MARTS_SCHEMA}.{name}"))
            times[name] = time.perf_counter() - t
    return times