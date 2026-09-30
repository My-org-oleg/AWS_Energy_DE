from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Sequence

import geopandas as gpd
import pandas
from psycopg2.extras import execute_values
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import get_engine
from etl.config import (
    BAD_QUALITY_PROPERTY,
    CLOSE_LOCATION_REASON,
    CLOSE_TO_PROPERTY,
    COLLISION_CLOSE_DISTANCE_M,
    COLLISION_PROPERTY,
    CORE_SCHEMA,
    DECOMPOSED_PROPERTIES,
    ONSHORE_IN_SEA_COLLISION_REASON,
    ONSHORE_SOURCES,
    OUTSIDE_LOCATION_COLLISION_REASON,
    SEA_REGIONS,
    SERVICE_SCHEMA,
    STAGING_GENERATOR_SOURCES,
    STAGING_SCHEMA,
    STORAGE_CAPACITY_COLLISION_REASON,
)
from etl.db_utils import (
    _create_core_generators,
    _create_core_storages,
    _ensure_schema,
    _table_exists,
)
from etl.reports import LoadReport
from etl.source_data import synthetic_identity
from etl.transform import enrich_geography
from etl.verify import (
    _verify_load_generators,
    _verify_load_storages,
    _verify_membership_matches_staging,
)

log = logging.getLogger(__name__)


# Value columns every Core kind is verified on after a snapshot load.
_SHARED_VERIFIED_COLUMNS = ("installed_capacity", "reference_date")


@dataclass(frozen=True)
class _CoreKind:
    """Load context for one core unit-kind (generators or storages).

    Everything the generic load path needs to differ between the two core
    tables lives here — the table names, the staging sources, the column
    mapping, the onshore-in-sea source set, and the extra storage-capacity
    collision check — so `_load` and its helpers stay shared (issue #6/#7).
    """

    core_table: str
    staging_sources: tuple[str, ...]
    properties_table: str
    units_properties_table: str
    column_map: dict[str, str]
    onshore_sources: tuple[str, ...]
    check_storage_capacity: bool
    verifier: Callable[[Engine, LoadReport], list[str]]
    create_table: Callable[[Engine], None]
    # Staging value columns a snapshot load must carry into Core unchanged;
    # checked row by row after an authoritative load (issue #4).
    verified_columns: tuple[str, ...] = ()


GENERATOR_KIND = _CoreKind(
    core_table="generators",
    staging_sources=STAGING_GENERATOR_SOURCES,
    properties_table="generator_properties",
    units_properties_table="generator_units_properties",
    column_map={
        "energy_source": "energy_source",
        "installed_capacity": "installed_capacity",
        "commissioning_date": "commissioning_date",
        "decommissioning_date": "decommissioning_date",
        "geometry": "geometry",
        "x_coordinates": "longitude",
        "y_coordinates": "latitude",
        "geo_accuracy": "geo_accuracy",
        "reference_id": "reference_id",
        "reference_date": "reference_date",
        "secondary_attributes": "secondary_attributes",
        "country_iso": "country_iso",
        "state": "state",
        "region": "region",
        "district": "district",
    },
    onshore_sources=ONSHORE_SOURCES,
    check_storage_capacity=False,
    verifier=_verify_load_generators,
    create_table=_create_core_generators,
    verified_columns=_SHARED_VERIFIED_COLUMNS,
)

STORAGE_KIND = _CoreKind(
    core_table="storages",
    staging_sources=("storage",),
    properties_table="storage_properties",
    units_properties_table="storage_units_properties",
    column_map={
        "energy_source": "energy_source",
        "storage_type": "storage_type",
        "storage_capacity": "storage_capacity",
        "installed_capacity": "installed_capacity",
        "commissioning_date": "commissioning_date",
        "decommissioning_date": "decommissioning_date",
        "geometry": "geometry",
        "x_coordinates": "longitude",
        "y_coordinates": "latitude",
        "geo_accuracy": "geo_accuracy",
        "reference_id": "reference_id",
        "reference_date": "reference_date",
        "secondary_attributes": "secondary_attributes",
        "country_iso": "country_iso",
        "state": "state",
        "region": "region",
        "district": "district",
    },
    onshore_sources=("storage",),
    check_storage_capacity=True,
    verifier=_verify_load_storages,
    create_table=_create_core_storages,
    verified_columns=(*_SHARED_VERIFIED_COLUMNS, "storage_type", "storage_capacity"),
)


def load_generators() -> LoadReport:
    """Load staging rows into core.generators with collision annotation."""
    return _load(kind=GENERATOR_KIND)


def load_storages() -> LoadReport:
    """Load staged storage rows into core.storages with collision annotation."""
    return _load(kind=STORAGE_KIND)


def load_source_snapshot(
    source: str,
    *,
    engine: Engine,
    ingestion_run_id,
) -> LoadReport:
    kind = STORAGE_KIND if source == "storage" else GENERATOR_KIND
    return _load(
        kind=kind,
        engine=engine,
        snapshot_source=source,
        ingestion_run_id=ingestion_run_id,
    )


def ensure_core_tables(engine: Engine) -> None:
    """Create both core unit tables if they do not exist yet.

    Marts read core.generators and core.storages together, so a Source
    snapshot that only ever carries one unit kind still needs both tables to
    exist. Schema resolution stays in this module so callers never repeat it.
    """
    _ensure_schema(engine, CORE_SCHEMA)
    for kind in (GENERATOR_KIND, STORAGE_KIND):
        if not _table_exists(engine, kind.core_table, CORE_SCHEMA):
            kind.create_table(engine)


def _load(
    kind: _CoreKind,
    *,
    engine: Engine | None = None,
    snapshot_source: str | None = None,
    ingestion_run_id=None,
) -> LoadReport:
    """Consolidate a kind's good staging rows into its core table.

    Reads the good (bad_quality=false) rows from the kind's staging tables
    and consolidates them into the core table with a serial surrogate key.
    Every load is a complete pass over the kind: a staging row whose record
    identity matches an existing core row is UPDATEd in place whatever its
    reference_date says, an unmatched row is INSERTed, and a core row that
    the staging tables no longer carry is left untouched. Core unit_ids are
    never regenerated and core rows are never deleted, so historical units
    survive snapshot replacement.

    After the upsert, collision checks run against the core geometry and are
    then refreshed (flags reset, collision links cleared, re-written from
    scratch so they never orphan):
    - close-location pairs (geo_accuracy=1, <10m apart)
    - state-null units (outside every boundary)
    - onshore sources in the sea
    - storage_capacity <= 0 or null (storages only)
    Collision rows are annotated with property links and remain in core.
    """
    report = LoadReport(target=kind.core_table)
    start = time.perf_counter()
    try:
        engine = engine or get_engine()

        present_sources = tuple(
            source
            for source in kind.staging_sources
            if _table_exists(engine, source, STAGING_SCHEMA)
        )
        # A snapshot load needs its own source plus whatever else of the kind is
        # already transformed, so Core stays complete. A batch load needs all of
        # them, because the local CLI transform stage is expected to have run.
        expected = (
            {snapshot_source} if snapshot_source is not None
            else set(kind.staging_sources)
        )
        if not present_sources or not expected <= set(present_sources):
            report.errors.append(
                f"staging tables missing for {report.target}; "
                "run 'python -m etl transform' first"
            )
            report.total_time = time.perf_counter() - start
            log.error("Load failed for %s: %s", report.target, report.errors[0])
            log.info("Total time: %.3fs", report.total_time)
            return report

        _ensure_schema(engine, CORE_SCHEMA)

        first_load = not _table_exists(engine, kind.core_table)
        if first_load:
            log.info("Creating core.%s (first load)...", kind.core_table)
            kind.create_table(engine)

        log.info("Reading staging rows for %s...", report.target)
        t = time.perf_counter()
        staging_df = _read_staging(engine, kind, present_sources)
        report.bad_rows_dropped = _bad_quality_count(engine, kind, present_sources)
        report.rows_read = len(staging_df)
        log.info(
            "%d good staging rows, %d bad dropped, time %.3fs",
            report.rows_read,
            report.bad_rows_dropped,
            time.perf_counter() - t,
        )

        log.info("Building core identity lookup...")
        t = time.perf_counter()
        core_lookup, existing_count = _build_core_lookup(engine, kind)
        known_before = frozenset(core_lookup)
        log.info(
            "%d existing core rows, time %.3fs", existing_count, time.perf_counter() - t
        )

        log.info("Partitioning staging into insert/update...")
        t = time.perf_counter()
        max_existing_id = _max_unit_id(engine, kind)
        insert_df, update_df = _partition_staging(
            engine,
            staging_df,
            core_lookup,
            kind,
        )
        staging_to_core = _apply_upsert(
            engine,
            insert_df,
            update_df,
            kind,
        )
        report.rows_inserted = len(insert_df)
        report.rows_updated = len(update_df)
        report.rows_retained = existing_count - len(update_df)
        log.info(
            "Inserted %d, updated %d, retained %d, time %.3fs",
            report.rows_inserted,
            report.rows_updated,
            report.rows_retained,
            time.perf_counter() - t,
        )

        has_changes = not insert_df.empty or not update_df.empty

        affected: list[int] | None
        if first_load:
            affected = None
        elif not has_changes:
            affected = []
        else:
            inserted = (
                _inserted_unit_ids(engine, max_existing_id, kind)
                if not insert_df.empty
                else []
            )
            updated = [
                int(row["_core_unit_id"])
                for _, row in update_df.iterrows()
            ]
            affected = inserted + updated

        if has_changes:
            log.info("Running collision checks...")
            t = time.perf_counter()
            collision_affected = (
                None
                if affected is None
                else _expand_collision_affected(engine, affected, kind)
            )
            _reset_collision_annotation(
                engine, affected_ids=collision_affected, kind=kind
            )
            collision_df, close_units = _detect_collisions(
                engine, affected_ids=collision_affected, kind=kind
            )
            report.collisions = len(collision_df)
            report.collision_links = _write_collision_links(
                engine, collision_df, close_units, kind=kind
            )
            log.info(
                "%d collision rows, %d links, time %.3fs",
                report.collisions,
                report.collision_links,
                time.perf_counter() - t,
            )
        else:
            log.info("No changes — skipping collision detection")
            collision_df = pandas.DataFrame(columns=["unit_id", "reasons"])
            report.collisions = 0
            report.collision_links = 0

        log.info("Transferring normalized properties...")
        t = time.perf_counter()
        if first_load or affected:
            report.properties_count, report.links_count = _transfer_properties(
                engine,
                staging_to_core,
                kind,
                present_sources,
                affected_ids=affected,
            )

        else:
            log.info("No changes — skipping property transfer")
        log.info(
            "%d properties, %d links, time %.3fs",
            report.properties_count,
            report.links_count,
            time.perf_counter() - t,
        )

        log.info("Verifying load...")
        t = time.perf_counter()
        if snapshot_source is None:
            report.errors = kind.verifier(engine, report)
        else:
            report.errors = _verify_authoritative_source(
                engine,
                kind,
                source=snapshot_source,
                ingestion_run_id=ingestion_run_id,
                known_before=known_before,
            )
        log.info("Verification done, time %.3fs", time.perf_counter() - t)

        # Idempotency check: run upsert again, counts should not change.
        log.info("Idempotency check...")
        t = time.perf_counter()
        core_lookup2, _ = _build_core_lookup(engine, kind)
        insert2, _update2 = _partition_staging(engine, staging_df, core_lookup2, kind)
        report.idempotent = len(insert2) == 0
        if not report.idempotent:
            report.errors.append(
                f"Idempotency violation: second pass would insert {len(insert2)}"
            )
        log.info("Idempotency check done, time %.3fs", time.perf_counter() - t)

    except Exception as e:
        report.errors.append(f"Load failed: {e}")
        log.exception("Load failed for %s", report.target)

    report.total_time = time.perf_counter() - start
    if report.errors:
        for err in report.errors:
            log.error("Load failed: %s", err)
    else:
        log.info(
            "Load passed for %s (%d rows inserted, %d updated)",
            report.target,
            report.rows_inserted,
            report.rows_updated,
        )
    log.info("Total time: %.3fs", report.total_time)
    return report


# ------------------------------------------------------------------ #
#  Geography rebuild after a Boundary release (issue #5)               #
# ------------------------------------------------------------------ #


@dataclass(frozen=True)
class GeographyRebuild:
    rows_updated: int
    collisions: int
    errors: tuple[str, ...]


def rebuild_core_geography(
    engine: Engine, kind: _CoreKind, boundaries
) -> GeographyRebuild:
    """Rederive state, region and district for every Core unit of a kind.

    Every Core row is rebuilt from its own geometry, so units retained from
    earlier snapshots are re-enriched too, not just those still in staging.
    Outside-location and onshore-in-sea collisions depend on the state, so the
    kind's collision annotation is then refreshed from scratch.
    """
    units = gpd.read_postgis(
        f"SELECT unit_id, geometry FROM {CORE_SCHEMA}.{kind.core_table}",
        engine,
        geom_col="geometry",
    )
    geography = enrich_geography(units, boundaries)
    records = [
        (int(unit_id), state, region, district)
        for unit_id, state, region, district in zip(
            units["unit_id"],
            geography["state"],
            geography["region"],
            geography["district"],
        )
    ]
    with engine.begin() as conn:
        if records:
            execute_values(
                conn.connection.cursor(),
                f"UPDATE {CORE_SCHEMA}.{kind.core_table} c "
                "SET state = v.state, region = v.region, district = v.district "
                "FROM (VALUES %s) AS v(unit_id, state, region, district) "
                "WHERE c.unit_id = v.unit_id",
                records,
                template="(%s::bigint, %s::text, %s::text, %s::text)",
                page_size=2000,
            )

    _reset_collision_annotation(engine, affected_ids=None, kind=kind)
    collision_df, close_units = _detect_collisions(engine, kind=kind, affected_ids=None)
    _write_collision_links(engine, collision_df, close_units, kind=kind)

    with engine.connect() as conn:
        stored = {
            int(unit_id): (state, region, district)
            for unit_id, state, region, district in conn.execute(
                text(
                    f"SELECT unit_id, state, region, district "
                    f"FROM {CORE_SCHEMA}.{kind.core_table}"
                )
            )
        }
    mismatched = [
        unit_id for unit_id, *geo in records if stored.get(unit_id) != tuple(geo)
    ]
    errors = (
        (f"Core geography of {kind.core_table} not rebuilt for {len(mismatched)} units",)
        if mismatched
        else ()
    )
    return GeographyRebuild(len(records), len(collision_df), errors)


# ------------------------------------------------------------------ #
#  Staging read                                                        #
# ------------------------------------------------------------------ #


def _read_staging(
    engine: Engine, kind: _CoreKind, sources: tuple[str, ...] | None = None
) -> pandas.DataFrame:
    """Read all good (bad_quality=false) rows from the kind's staging tables."""
    chosen = kind.staging_sources if sources is None else sources
    union_parts = [
        f"SELECT * FROM {STAGING_SCHEMA}.{source} WHERE NOT bad_quality"
        for source in chosen
    ]
    sql = " UNION ALL ".join(union_parts)
    return pandas.read_sql(text(sql), engine)


def _bad_quality_count(
    engine: Engine, kind: _CoreKind, sources: tuple[str, ...] | None = None
) -> int:
    """Count all bad_quality rows across the kind's staging tables."""
    chosen = kind.staging_sources if sources is None else sources
    sums = " + ".join(
        f"(SELECT COUNT(*) FROM {STAGING_SCHEMA}.{s} WHERE bad_quality)"
        for s in chosen
    )
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT {sums}")).scalar())


def _verify_authoritative_source(
    engine: Engine,
    kind: _CoreKind,
    *,
    source: str,
    ingestion_run_id,
    known_before: frozenset[str],
) -> list[str]:
    """Check that Core now reflects every good row of the snapshot.

    Membership must match staging; every good unit must be in Core with its
    source, Reference ID and the kind's ``verified_columns`` equal to the
    staging values; and no bad-quality unit may have *created* a Core row.
    """
    errors, _ = _verify_membership_matches_staging(
        engine, source=source, ingestion_run_id=ingestion_run_id
    )
    columns = kind.verified_columns
    staging_select = ", ".join(("unit_id", "bad_quality", "reference_id", *columns))
    with engine.connect() as connection:
        staging = connection.execute(
            text(f"SELECT {staging_select} FROM {STAGING_SCHEMA}.{source}")
        ).mappings().all()
    staging_by_key = {str(row["unit_id"]): row for row in staging}

    core_lookup, _ = _build_core_lookup(engine, kind)
    good_units = {
        unit_key: row
        for unit_key, row in staging_by_key.items()
        if not row["bad_quality"]
    }
    missing = sorted(set(good_units) - set(core_lookup))
    if missing:
        errors.append(f"Good snapshot units missing from Core: {len(missing)}")
    # A bad-quality member must not have *created* a Core row. Finding one that
    # was already there is correct history, not an error: a unit can turn
    # bad-quality in a later snapshot and its retained Core row survives.
    introduced_bad = sorted(
        (set(staging_by_key) - set(good_units)) & set(core_lookup) - known_before
    )
    if introduced_bad:
        errors.append(
            f"Bad-quality snapshot units created Core rows: {len(introduced_bad)}"
        )

    core_ids = [
        core_lookup[unit_key] for unit_key in good_units if unit_key in core_lookup
    ]
    if not core_ids:
        return errors
    core_select = ", ".join(
        ("unit_id", "energy_source", "reference_id")
        + tuple(kind.column_map[column] for column in columns)
    )
    with engine.connect() as connection:
        core_rows = {
            int(row["unit_id"]): row
            for row in connection.execute(
                text(
                    f"SELECT {core_select} FROM {CORE_SCHEMA}.{kind.core_table} "
                    "WHERE unit_id = ANY(:ids)"
                ),
                {"ids": core_ids},
            ).mappings()
        }
    for unit_key, staged in good_units.items():
        core_row = core_rows.get(core_lookup.get(unit_key))
        if core_row is None:
            continue
        if core_row["energy_source"] != source:
            errors.append(f"Core source mismatch for {unit_key}")
            break
        if core_row["reference_id"] != staged["reference_id"]:
            errors.append(f"Core Reference ID mismatch for {unit_key}")
            break
        mismatched = next(
            (
                column
                for column in columns
                if not _same_value(staged[column], core_row[kind.column_map[column]])
            ),
            None,
        )
        if mismatched is not None:
            errors.append(f"Core {mismatched} mismatch for {unit_key}")
            break
    return errors


def _same_value(staged, core) -> bool:
    """Compare a scalar staging value with its Core copy; nulls compare equal.

    Only for the scalar ``verified_columns`` (float, timestamp, text); JSON or
    list values would make ``pandas.isna`` return an array.
    """
    if pandas.isna(staged) or pandas.isna(core):
        return pandas.isna(staged) and pandas.isna(core)
    if isinstance(staged, (int, float)) or isinstance(core, (int, float)):
        return float(staged) == float(core)
    if hasattr(staged, "year") or hasattr(core, "year"):
        return pandas.Timestamp(staged) == pandas.Timestamp(core)
    return staged == core


# ------------------------------------------------------------------ #
#  Core identity lookup                                                #
# ------------------------------------------------------------------ #


def _build_core_lookup(
    engine: Engine, kind: _CoreKind
) -> tuple[dict[str, int], int]:
    lookup: dict[str, int] = {}
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                f"SELECT unit_id, reference_id, energy_source, longitude, "
                f"latitude, geo_accuracy "
                f"FROM {CORE_SCHEMA}.{kind.core_table}"
            )
        ).fetchall()
        synthetic_ids = [row[0] for row in rows if row[1] is None]
        locations: dict[int, object] = {}
        if synthetic_ids:
            location_rows = connection.execute(
                text(
                    f"SELECT up.unit_id, p.value "
                    f"FROM {CORE_SCHEMA}.{kind.units_properties_table} up "
                    f"JOIN {CORE_SCHEMA}.{kind.properties_table} p "
                    "ON p.prop_id = up.prop_id "
                    "WHERE p.name = 'location' AND up.unit_id = ANY(:ids)"
                ),
                {"ids": synthetic_ids},
            ).fetchall()
            locations = {int(unit_id): value for unit_id, value in location_rows}

    for unit_id, reference_id, energy_source, longitude, latitude, geo_accuracy in rows:
        if reference_id is not None:
            identity = f"{energy_source}_{reference_id}"
        else:
            if unit_id not in locations:
                raise RuntimeError(
                    f"core.{kind.core_table} unit {unit_id} has no Reference ID "
                    "and no location property, so its synthetic identity is "
                    "not reconstructable"
                )
            identity = synthetic_identity(
                source=energy_source,
                location=locations[unit_id],
                x=longitude,
                y=latitude,
                geo_accuracy=geo_accuracy,
            )
        lookup[identity] = unit_id
    return lookup, len(rows)


# ------------------------------------------------------------------ #
#  Partition staging into insert / update / skip                       #
# ------------------------------------------------------------------ #


def _staging_identities(df: pandas.DataFrame) -> pandas.Series:
    return df["unit_id"].astype("string")


def _partition_staging(
    engine: Engine,
    staging_df: pandas.DataFrame,
    core_lookup: dict[str, int],
    kind: _CoreKind,
) -> tuple[pandas.DataFrame, pandas.DataFrame]:
    """Split staging rows into insert and authoritative update buckets.

    A complete Source snapshot is authoritative for every row it contains, so
    a matched row is always updated regardless of reference_date. Only a row
    with no Core match is inserted. Rows absent from the snapshot are never
    touched, which keeps historical Core units.
    """
    if staging_df.empty:
        return staging_df.copy(), staging_df.copy()

    staging_df = staging_df.copy()
    staging_df["_identity"] = _staging_identities(staging_df)
    matched = staging_df["_identity"].map(core_lookup)
    is_insert = matched.isna()
    is_update = ~is_insert

    update_df = staging_df[is_update].copy()
    if not update_df.empty:
        update_df["_core_unit_id"] = (
            update_df["_identity"].map(core_lookup).astype("int64")
        )
    return staging_df[is_insert].copy(), update_df


# ------------------------------------------------------------------ #
#  Upsert into core table (generators / storages)                      #
# ------------------------------------------------------------------ #


def _apply_upsert(
    engine: Engine,
    insert_df: pandas.DataFrame,
    update_df: pandas.DataFrame,
    kind: _CoreKind,
) -> dict[str, int]:
    """Insert new rows and update existing rows in the core table."""
    mapping: dict[str, int] = {}
    with engine.begin() as conn:
        inserted_ids = _insert_core_rows(conn, insert_df, kind)
        for unit_id, core_unit_id in zip(insert_df["unit_id"], inserted_ids):
            mapping[str(unit_id)] = core_unit_id
        for _, row in update_df.iterrows():
            _update_core_row(conn, row, kind)
            mapping[str(row["unit_id"])] = int(row["_core_unit_id"])
    return mapping


def _max_unit_id(engine: Engine, kind: _CoreKind) -> int:
    """Return the current max unit_id in the core table (0 if empty)."""
    with engine.connect() as conn:
        return int(
            conn.execute(
                text(
                    f"SELECT COALESCE(MAX(unit_id), 0) FROM {CORE_SCHEMA}.{kind.core_table}"
                )
            ).scalar()
        )


def _inserted_unit_ids(engine: Engine, after: int, kind: _CoreKind) -> list[int]:
    """Return unit_ids inserted since *after* (exclusive)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT unit_id FROM {CORE_SCHEMA}.{kind.core_table} "
                f"WHERE unit_id > :after"
            ),
            {"after": after},
        ).fetchall()
    return [r[0] for r in rows]


def _sql_value(v):
    """Convert numpy/pandas missing values to SQL NULL."""
    if v is None:
        return None
    try:
        if pandas.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _core_row_values(row: pandas.Series, column_map: dict[str, str]) -> dict:
    """Map a staging row's values to the core table columns."""
    return {
        core_col: _sql_value(row.get(stg_col))
        for stg_col, core_col in column_map.items()
    }


def _insert_core_rows(
    conn,
    df: pandas.DataFrame,
    kind: _CoreKind,
) -> list[int]:
    """Bulk-insert staging rows into the core table via execute_values."""
    if df.empty:
        return []
    sub = df[list(kind.column_map)].rename(columns=kind.column_map)
    sub = sub.astype("object").where(pandas.notna(sub), None)
    cols = ", ".join(sub.columns)
    records = [tuple(r[k] for k in sub.columns) for r in sub.to_dict("records")]
    returned = execute_values(
        conn.connection.cursor(),
        f"INSERT INTO {CORE_SCHEMA}.{kind.core_table} ({cols}) "
        "VALUES %s RETURNING unit_id",
        records,
        page_size=1000,
        fetch=True,
    )
    return [int(row[0]) for row in returned]


def _update_core_row(conn, row: pandas.Series, kind: _CoreKind) -> None:
    """Update an existing core row by its core unit_id.

    The unit_id comes from the identity-matched core row; the matching itself
    was done in _partition_staging, so the update is precise.
    """
    vals = _core_row_values(row, kind.column_map)
    set_clause = ", ".join(f"{k} = :{k}" for k in vals)
    vals["core_unit_id"] = int(row["_core_unit_id"])
    conn.execute(
        text(
            f"UPDATE {CORE_SCHEMA}.{kind.core_table} SET {set_clause} "
            f"WHERE unit_id = :core_unit_id"
        ),
        vals,
    )


# ------------------------------------------------------------------ #
#  Collision detection                                                  #
# ------------------------------------------------------------------ #


def _detect_collisions(
    engine: Engine, kind: _CoreKind, affected_ids: list[int] | None = None
) -> tuple[pandas.DataFrame, dict[int, list[int]]]:
    """Detect collision conditions; return (flag df, close-pair map).

    The collision DataFrame has one row per colliding unit with a
    newline-joined ``reasons`` value.  The close-pair map records for each
    flagged unit the ids of its geo_accuracy=1 neighbours within
    COLLISION_CLOSE_DISTANCE_M (``{unit_id: [neighbour_ids]}``) — these live
    in the core table's secondary_attributes, not as property links.

    When *affected_ids* is ``None`` (first load) every core row is checked.
    When it is provided only rows in that list, plus any pairs they form with
    other core rows, are examined — the remaining rows are assumed to be
    unchanged from a previous run.
    """
    collision_units: dict[int, list[str]] = {}
    close_units: dict[int, list[int]] = {}

    _detect_close_location(engine, collision_units, close_units, kind, affected_ids)
    _detect_outside_location(engine, collision_units, kind, affected_ids)
    _detect_onshore_in_sea(engine, collision_units, kind, affected_ids)
    if kind.check_storage_capacity:
        _detect_storage_capacity(engine, collision_units, kind, affected_ids)

    if not collision_units:
        return (
            pandas.DataFrame(columns=["unit_id", "reasons"]),
            close_units,
        )

    rows = [
        (uid, "\n".join(dict.fromkeys(reasons)))
        for uid, reasons in collision_units.items()
    ]
    return pandas.DataFrame(rows, columns=["unit_id", "reasons"]), close_units


def _close_location_pairs(
    engine: Engine,
    kind: _CoreKind,
    affected_ids: list[int] | None = None,
) -> list[tuple[int, int]]:
    """Return geo_accuracy=1 pairs within COLLISION_CLOSE_DISTANCE_M.

    Pairs are returned as ``(a, b)`` with ``a < b``.  When *affected_ids* is
    given the join is restricted to rows where at least one side is affected —
    pairs between two unchanged rows are already known from the previous run.
    """
    affected_filter = ""
    params: dict[str, object] = {}
    if affected_ids is not None:
        affected_filter = (
            "AND (a.unit_id = ANY(:affected) OR b.unit_id = ANY(:affected))"
        )
        params["affected"] = affected_ids

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT a.unit_id, b.unit_id "
                f"FROM {CORE_SCHEMA}.{kind.core_table} a "
                f"JOIN {CORE_SCHEMA}.{kind.core_table} b "
                f"ON ST_DWithin(a.geometry::geography, b.geometry::geography, "
                f"{COLLISION_CLOSE_DISTANCE_M}) "
                f"WHERE a.unit_id < b.unit_id "
                f"AND a.geo_accuracy = 1 AND b.geo_accuracy = 1 "
                f"AND a.geometry IS NOT NULL AND b.geometry IS NOT NULL "
                f"{affected_filter}"
            ),
            params,
        ).fetchall()
    return [(int(a), int(b)) for a, b in rows]


def _detect_close_location(
    engine: Engine,
    collision_units: dict[int, list[str]],
    close_units: dict[int, list[int]],
    kind: _CoreKind,
    affected_ids: list[int] | None = None,
) -> None:
    """Find pairs of geo_accuracy=1 units within 10m of each other.

    Each side of a pair is flagged with the CLOSE_LOCATION_REASON and listed
    as the other's close neighbour in *close_units*.
    """
    for uid_a, uid_b in _close_location_pairs(engine, kind, affected_ids):
        collision_units.setdefault(uid_a, []).append(CLOSE_LOCATION_REASON)
        collision_units.setdefault(uid_b, []).append(CLOSE_LOCATION_REASON)
        close_units.setdefault(uid_a, []).append(uid_b)
        close_units.setdefault(uid_b, []).append(uid_a)


def _rows_referencing_ids_in_close_to(
    engine: Engine, ids: list[int], kind: _CoreKind
) -> list[int]:
    """Return rows whose secondary_attributes 'close_to' lists an id.

    An affected row that moved out of range of a former close partner no
    longer appears in the geometric pair join, yet the partner row still
    lists it (and may hold stale annotation) — such partners must be swept
    into the incremental reset/detect set too.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT DISTINCT g.unit_id "
                f"FROM {CORE_SCHEMA}.{kind.core_table} g "
                f"WHERE g.secondary_attributes IS NOT NULL "
                f"AND g.secondary_attributes::jsonb ? 'close_to' "
                f"AND EXISTS ("
                f"SELECT 1 FROM jsonb_array_elements("
                f"g.secondary_attributes::jsonb -> 'close_to') e "
                f"WHERE (e #>> '{{}}')::int = ANY(:ids))"
            ),
            {"ids": ids},
        ).fetchall()
    return [int(r[0]) for r in rows]


def _expand_collision_affected(
    engine: Engine, affected_ids: list[int], kind: _CoreKind
) -> list[int]:
    """Expand the incremental set to the full close-location component.

    A row's collision annotation depends on its neighbours: an in-place update
    can add/remove a close pair for a partner row that is itself unchanged,
    and the affected-only reset would leave that partner with a stale or
    duplicated link and a scrubbed ``close_to`` (issue #30).  This expands the
    reset/detect set to every row reachable through close-location pairs from
    an affected row — plus any row whose ``close_to`` still names an affected
    row, covering a partner the affected row moved away from — so each member
    of the component is reset and fully re-annotated in one pass.
    """
    expanded: set[int] = set(affected_ids)
    while True:
        before = len(expanded)
        ids = sorted(expanded)
        for uid_a, uid_b in _close_location_pairs(engine, kind, ids):
            expanded.add(uid_a)
            expanded.add(uid_b)
        expanded.update(_rows_referencing_ids_in_close_to(engine, ids, kind))
        if len(expanded) == before:
            break
    return sorted(expanded)


def _detect_outside_location(
    engine: Engine,
    collision_units: dict[int, list[str]],
    kind: _CoreKind,
    affected_ids: list[int] | None = None,
) -> None:
    """Flag units whose state is NULL (outside every boundary)."""
    if affected_ids is not None and not affected_ids:
        return

    state_filter = ""
    params: dict[str, object] = {}
    if affected_ids is not None:
        state_filter = "AND unit_id = ANY(:affected)"
        params["affected"] = affected_ids

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT unit_id FROM {CORE_SCHEMA}.{kind.core_table} "
                f"WHERE state IS NULL {state_filter}"
            ),
            params,
        ).fetchall()
    for (uid,) in rows:
        collision_units.setdefault(uid, []).append(OUTSIDE_LOCATION_COLLISION_REASON)


def _detect_onshore_in_sea(
    engine: Engine,
    collision_units: dict[int, list[str]],
    kind: _CoreKind,
    affected_ids: list[int] | None = None,
) -> None:
    """Flag onshore-only sources in sea/EEZ states."""
    if affected_ids is not None and not affected_ids:
        return

    sea_filter = ""
    params: dict[str, object] = {}
    if affected_ids is not None:
        sea_filter = "AND unit_id = ANY(:affected)"
        params["affected"] = affected_ids

    sources = ", ".join(f"'{s}'" for s in kind.onshore_sources)
    states = ", ".join(f"'{r}'" for r in SEA_REGIONS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT unit_id FROM {CORE_SCHEMA}.{kind.core_table} "
                f"WHERE energy_source IN ({sources}) "
                f"AND state IN ({states}) "
                f"{sea_filter}"
            ),
            params,
        ).fetchall()
    for (uid,) in rows:
        collision_units.setdefault(uid, []).append(ONSHORE_IN_SEA_COLLISION_REASON)


def _detect_storage_capacity(
    engine: Engine,
    collision_units: dict[int, list[str]],
    kind: _CoreKind,
    affected_ids: list[int] | None = None,
) -> None:
    """Flag storages with storage_capacity <= 0 or null (spec Load check)."""
    if affected_ids is not None and not affected_ids:
        return

    capacity_filter = ""
    params: dict[str, object] = {}
    if affected_ids is not None:
        capacity_filter = "AND unit_id = ANY(:affected)"
        params["affected"] = affected_ids

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT unit_id FROM {CORE_SCHEMA}.{kind.core_table} "
                f"WHERE (storage_capacity IS NULL OR storage_capacity <= 0) "
                f"{capacity_filter}"
            ),
            params,
        ).fetchall()
    for (uid,) in rows:
        collision_units.setdefault(uid, []).append(STORAGE_CAPACITY_COLLISION_REASON)


# ------------------------------------------------------------------ #
#  Collision property links                                             #
# ------------------------------------------------------------------ #


def _write_collision_links(
    engine: Engine,
    collision_df: pandas.DataFrame,
    close_units: dict[int, list[int]],
    kind: _CoreKind,
) -> int:
    """Write collision property links and close_to neighbour attributes.

    Called by the loader *after* the collision annotation was reset, so it
    only writes links for the units in *collision_df*: one 'collision' link
    per flagged unit carrying the joined reasons (a close-location pair
    contributes the CLOSE_LOCATION_REASON phrase, without unit ids) and the
    close-pair neighbours are stored on the core table's secondary_attributes
    as a ``close_to`` JSON list instead of property links.  A unit with no
    detection keeps collision=false and no annotation.  Returns the total
    number of property links written.
    """
    if collision_df.empty:
        return 0

    flagged = [int(u) for u in collision_df["unit_id"]]
    reasons_by_unit: dict[int, list[str]] = defaultdict(list)
    for _, row in collision_df.iterrows():
        reasons_by_unit[int(row["unit_id"])] = row["reasons"].split("\n")

    prop_keys: set[tuple[str, str]] = set()
    wanted: dict[tuple[str, str], list[int]] = defaultdict(list)
    for uid in flagged:
        collision_key = (COLLISION_PROPERTY, "\n".join(reasons_by_unit[uid]))
        prop_keys.add(collision_key)
        wanted[collision_key].append(uid)

    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE {CORE_SCHEMA}.{kind.core_table} SET collision = true "
                f"WHERE unit_id = ANY(:ids)"
            ),
            {"ids": flagged},
        )
        prop_map = _ensure_properties(conn, prop_keys, kind)
        pairs = [
            (uid, prop_map[key])
            for key, uids in wanted.items()
            for uid in uids
        ]
        execute_values(
            conn.connection.cursor(),
            f"INSERT INTO {CORE_SCHEMA}.{kind.units_properties_table} "
            f"(unit_id, prop_id) VALUES %s ON CONFLICT DO NOTHING",
            pairs,
            page_size=2000,
        )
        _write_close_locations(conn, close_units, kind)

    return len(pairs)


def _write_close_locations(
    conn, close_units: dict[int, list[int]], kind: _CoreKind
) -> None:
    """Persist close-pair neighbours into secondary_attributes.close_to.

    For every unit in *close_units* the ``close_to`` key of its core
    secondary_attributes JSON is set to the sorted list of neighbour unit ids
    (replacing any previous value for that key).
    """
    if not close_units:
        return

    records = [
        (uid, json.dumps(sorted(int(n) for n in neighbours)))
        for uid, neighbours in close_units.items()
    ]
    execute_values(
        conn.connection.cursor(),
        f"UPDATE {CORE_SCHEMA}.{kind.core_table} g "
        f"SET secondary_attributes = jsonb_set("
        f"COALESCE(g.secondary_attributes::jsonb, '{{}}'::jsonb), "
        f"'{{close_to}}'::text[], v.neighbours::jsonb, true)::text "
        f"FROM (VALUES %s) AS v(unit_id, neighbours) "
        f"WHERE g.unit_id = v.unit_id",
        records,
        page_size=2000,
    )


def _ensure_properties(
    conn, keys: set[tuple[str, str]], kind: _CoreKind
) -> dict[tuple[str, str], int]:
    """Ensure every (name, value) property key exists; return key → prop_id."""
    names = sorted({name for name, _ in keys})
    names_sql = ", ".join(f"'{n}'" for n in names)
    select_sql = (
        f"SELECT name, value, prop_id FROM {CORE_SCHEMA}.{kind.properties_table} "
        f"WHERE name IN ({names_sql})"
    )
    prop_map = {
        (name, value): prop_id
        for name, value, prop_id in conn.execute(text(select_sql)).fetchall()
    }
    missing = [key for key in keys if key not in prop_map]
    if missing:
        execute_values(
            conn.connection.cursor(),
            f"INSERT INTO {CORE_SCHEMA}.{kind.properties_table} (name, value) "
            f"VALUES %s ON CONFLICT DO NOTHING",
            missing,
            page_size=1000,
        )
        prop_map.update(
            {
                (name, value): prop_id
                for name, value, prop_id in conn.execute(
                    text(select_sql)
                ).fetchall()
            }
        )
    return prop_map


def _reset_collision_annotation(
    engine: Engine, affected_ids: list[int] | None, kind: _CoreKind
) -> None:
    """Clear collision flags, links, and secondary_attributes close_to data.

    When *affected_ids* is ``None`` (first load) every flag and annotation is
    wiped.  When it is provided only the affected rows' own flags and
    annotations are cleared plus the affected ids are scrubbed out of other
    rows' ``close_to`` lists — the rest are still valid from the previous
    run.  Either way deprecated ``close_to`` *property* rows are purged
    (neighbours moved to secondary_attributes.key and are no longer links).
    """
    with engine.begin() as conn:
        # Deprecated: close_to used to be a property link; purge everywhere.
        conn.execute(
            text(
                f"DELETE FROM {CORE_SCHEMA}.{kind.units_properties_table} up "
                f"USING {CORE_SCHEMA}.{kind.properties_table} p "
                f"WHERE up.prop_id = p.prop_id "
                f"AND p.name = :close_to"
            ),
            {"close_to": CLOSE_TO_PROPERTY},
        )
        conn.execute(
            text(
                f"DELETE FROM {CORE_SCHEMA}.{kind.properties_table} "
                f"WHERE name = :close_to"
            ),
            {"close_to": CLOSE_TO_PROPERTY},
        )

        if affected_ids is None:
            conn.execute(
                text(
                    f"UPDATE {CORE_SCHEMA}.{kind.core_table} SET collision = false"
                )
            )
            conn.execute(
                text(
                    f"UPDATE {CORE_SCHEMA}.{kind.core_table} "
                    f"SET secondary_attributes = "
                    f"(secondary_attributes::jsonb - 'close_to')::text "
                    f"WHERE secondary_attributes IS NOT NULL "
                    f"AND secondary_attributes::jsonb ? 'close_to'"
                )
            )
            conn.execute(
                text(
                    f"DELETE FROM {CORE_SCHEMA}.{kind.units_properties_table} up "
                    f"USING {CORE_SCHEMA}.{kind.properties_table} p "
                    f"WHERE up.prop_id = p.prop_id "
                    f"AND p.name = :collision"
                ),
                {"collision": COLLISION_PROPERTY},
            )
            return

        if not affected_ids:
            return

        # Clear collision flags for affected rows.
        conn.execute(
            text(
                f"UPDATE {CORE_SCHEMA}.{kind.core_table} SET collision = false "
                f"WHERE unit_id = ANY(:ids)"
            ),
            {"ids": affected_ids},
        )
        # Delete affected rows' own collision links.
        conn.execute(
            text(
                f"DELETE FROM {CORE_SCHEMA}.{kind.units_properties_table} up "
                f"USING {CORE_SCHEMA}.{kind.properties_table} p "
                f"WHERE up.prop_id = p.prop_id "
                f"AND up.unit_id = ANY(:ids) "
                f"AND p.name = :collision"
            ),
            {"ids": affected_ids, "collision": COLLISION_PROPERTY},
        )
        # Drop affected rows' own close_to secondary attributes before the
        # siblings added in _detect_close_location get recomputed.
        conn.execute(
            text(
                f"UPDATE {CORE_SCHEMA}.{kind.core_table} "
                f"SET secondary_attributes = "
                f"(secondary_attributes::jsonb - 'close_to')::text "
                f"WHERE unit_id = ANY(:ids) "
                f"AND secondary_attributes IS NOT NULL "
                f"AND secondary_attributes::jsonb ? 'close_to'"
            ),
            {"ids": affected_ids},
        )
        # Scrub affected ids out of every other row's close_to lists.
        conn.execute(
            text(
                f"UPDATE {CORE_SCHEMA}.{kind.core_table} g "
                f"SET secondary_attributes = CASE "
                f"WHEN v.new_value = '[]'::jsonb "
                f"THEN (g.secondary_attributes::jsonb - 'close_to')::text "
                f"ELSE jsonb_set(g.secondary_attributes::jsonb, "
                f"'{{close_to}}', v.new_value, true)::text END "
                f"FROM (SELECT unit_id, "
                f"COALESCE((SELECT jsonb_agg(elem) "
                f"FROM jsonb_array_elements("
                f"secondary_attributes::jsonb -> 'close_to') AS elem "
                f"WHERE NOT (elem #>> '{{}}')::int = ANY(:ids)), "
                f"'[]'::jsonb) AS new_value "
                f"FROM {CORE_SCHEMA}.{kind.core_table} "
                f"WHERE secondary_attributes IS NOT NULL "
                f"AND secondary_attributes::jsonb ? 'close_to') v "
                f"WHERE g.unit_id = v.unit_id"
            ),
            {"ids": affected_ids},
        )


# ------------------------------------------------------------------ #
#  Normalized property transfer (issue #8)                            #
# ------------------------------------------------------------------ #


def _transfer_properties(
    engine: Engine,
    staging_to_core: dict[str, int],
    kind: _CoreKind,
    sources: Sequence[str],
    affected_ids: list[int] | None = None,
) -> tuple[int, int]:
    """Move the staging whitelist decomposition into core for affected units.

    Collects every (name, value) pair a staging row links (bad_quality pairs
    skipped — they stay staging-only, ADR 0005), maps the staging unit_id onto
    its core serial unit_id, and reconciles the kind's core properties /
    units_properties tables for the affected units:

    * *affected_ids* ``None`` (first load) rewrites every mapped unit.
    * a list rewrites just those units — deleting their old whitelist links
      first so an in-place update reflects the new record (ADR 0001), while
      collision/close_to links and skipped units are left untouched.

    *sources* is the whole set of the kind's staging sources this load read,
    because a snapshot load upserts the complete kind: the delete below spans
    every affected unit of the kind, so the read that refills them has to span
    the kind too, or the other sources would lose their decomposed properties
    (``location`` among them, which synthetic identity needs to be rebuilt).

    Returns (properties, links) counts written in this transfer.
    """
    if affected_ids is not None and not affected_ids:
        return 0, 0

    affected = set(affected_ids) if affected_ids is not None else None

    wanted_units: dict[int, list[tuple[str, str]]] = defaultdict(list)
    with engine.connect() as conn:
        for staging_source in sources:
            rows = conn.execute(
                text(
                    f"SELECT up.unit_id, p.name, p.value "
                    f"FROM {STAGING_SCHEMA}.{staging_source}_units_properties up "
                    f"JOIN {STAGING_SCHEMA}.{staging_source}_properties p "
                    f"ON p.param_id = up.param_id "
                    f"WHERE p.name <> '{BAD_QUALITY_PROPERTY}'"
                )
            ).fetchall()
            for staging_unit_id, name, value in rows:
                core_uid = staging_to_core.get(staging_unit_id)
                if core_uid is None:
                    continue
                if affected is not None and core_uid not in affected:
                    continue
                wanted_units[core_uid].append((name, value))

    if not wanted_units:
        return 0, 0

    prop_keys = {key for keys in wanted_units.values() for key in keys}

    with engine.begin() as conn:
        if affected_ids is not None:
            whitelist = ", ".join(f"'{n}'" for n in DECOMPOSED_PROPERTIES)
            conn.execute(
                text(
                    f"DELETE FROM {CORE_SCHEMA}.{kind.units_properties_table} up "
                    f"USING {CORE_SCHEMA}.{kind.properties_table} p "
                    f"WHERE up.prop_id = p.prop_id "
                    f"AND up.unit_id = ANY(:ids) "
                    f"AND p.name IN ({whitelist})"
                ),
                {"ids": affected_ids},
            )

        prop_map = _ensure_properties(conn, prop_keys, kind)
        pairs = [
            (core_uid, prop_map[key])
            for core_uid, keys in wanted_units.items()
            for key in keys
        ]
        execute_values(
            conn.connection.cursor(),
            f"INSERT INTO {CORE_SCHEMA}.{kind.units_properties_table} "
            f"(unit_id, prop_id) VALUES %s ON CONFLICT DO NOTHING",
            pairs,
            page_size=2000,
        )

    return len(prop_keys), len(pairs)
