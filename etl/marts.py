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


_ACTIVE = "decommissioning_date IS NULL OR decommissioning_date > CURRENT_DATE"

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
                    WHERE {_ACTIVE}
                    UNION ALL
                    SELECT state, energy_source FROM {core_schema}.storages
                    WHERE {_ACTIVE}
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
                WHERE {_ACTIVE}
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
                WHERE {_ACTIVE}
                GROUP BY COALESCE(state, '{OUTSIDE_STATE}'), storage_type
            """,
        ),
    }



def build_marts(engine: Engine | None = None) -> MartsReport:
    """Create, refresh, and verify the marts materialized views.

    Ensures the marts schema and the three stored pivots exist, refreshes all
    three from core, and verifies the stored pivots reconcile to the live
    active-unit aggregates.  A failed creation, refresh, or verification is
    captured in the report's errors; ``python -m etl marts`` exits non-zero
    when it happens (fail loudly).
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
            report.created = _create_marts(engine, definitions)
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
            "Marts built (%s new, refreshed %s, %.3fs)",
            ", ".join(report.created) or "none",
            ", ".join(report.refreshed) or "none",
            report.total_time,
        )
    return report


def verify_marts(engine: Engine) -> list[str]:
    """Reconcile the stored marts to live core; return the drift errors."""
    return _verify_marts(engine, _mart_definitions(CORE_SCHEMA))


def _create_marts(
    engine: Engine,
    definitions: dict[str, _MartDefinition],
) -> list[str]:
    """Create the marts schema and any materialized views that are missing."""
    _ensure_schema(engine, MARTS_SCHEMA)
    created: list[str] = []
    with engine.begin() as conn:
        for name, definition in definitions.items():
            exists = conn.execute(
                text(
                    "SELECT 1 FROM pg_matviews "
                    "WHERE schemaname = :schema AND matviewname = :name"
                ),
                {"schema": MARTS_SCHEMA, "name": name},
            ).first()
            if exists:
                continue
            conn.execute(
                text(
                    f"CREATE MATERIALIZED VIEW {MARTS_SCHEMA}.{name} "
                    f"AS {definition.select_sql}"
                )
            )
            created.append(name)
    for name in created:
        log.info("Created materialized view %s.%s", MARTS_SCHEMA, name)
    return created


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