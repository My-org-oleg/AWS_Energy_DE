from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from etl.marts import _MartDefinition

from etl.config import (
    BAD_QUALITY_PROPERTY,
    CLOSE_LOCATION_REASON,
    COLLISION_PROPERTY,
    CORE_SCHEMA,
    DECOMPOSED_PROPERTIES,
    MARTS_SCHEMA,
    ONSHORE_IN_SEA_COLLISION_REASON,
    ONSHORE_SOURCES,
    OUTSIDE_LOCATION_COLLISION_REASON,
    RAW_SCHEMA,
    SEA_REGIONS,
    SERVICE_SCHEMA,
    STAGING_GENERATOR_SOURCES,
    STAGING_SCHEMA,
    STORAGE_CAPACITY_COLLISION_REASON,
    STORAGE_COLUMNS,
)
from etl.reports import ExtractionReport, LoadReport, TransformReport

log = logging.getLogger(__name__)


def _verify_membership_matches_staging(
    engine: Engine, *, source: str, ingestion_run_id
) -> tuple[list[str], dict[str, bool]]:
    """Reconcile a snapshot's Source membership against its staging rows.

    Membership and staging are written by the same transform, so they must agree
    on which units the snapshot carried and on each unit's bad-quality flag. The
    one home for that comparison: transform checks it as it writes staging, and
    load checks it again before trusting either side.

    Returns the errors plus the membership's per-unit bad-quality flags, which
    load needs to reason about Core.
    """
    with engine.connect() as connection:
        staging_count = int(
            connection.execute(
                text(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source}")
            ).scalar()
        )
        membership = connection.execute(
            text(
                f"SELECT unit_key, bad_quality, energy_source "
                f"FROM {SERVICE_SCHEMA}.source_memberships "
                "WHERE run_id = :run_id"
            ),
            {"run_id": str(ingestion_run_id)},
        ).fetchall()
        staging_quality = connection.execute(
            text(f"SELECT unit_id, bad_quality FROM {STAGING_SCHEMA}.{source}")
        ).fetchall()

    errors: list[str] = []
    if len(membership) != staging_count:
        errors.append(
            f"Source membership count {len(membership)} != staging {staging_count}"
        )
    wrong_source = [
        str(unit_key)
        for unit_key, _, energy_source in membership
        if energy_source != source
    ]
    if wrong_source:
        errors.append(f"Source membership has {len(wrong_source)} wrong-source rows")
    membership_by_key = {str(unit_key): bool(bad) for unit_key, bad, _ in membership}
    staging_by_key = {str(unit_id): bool(bad) for unit_id, bad in staging_quality}
    if set(membership_by_key) != set(staging_by_key):
        errors.append("Source membership keys differ from staging identities")
    for unit_key, bad in staging_by_key.items():
        if membership_by_key.get(unit_key) != bad:
            errors.append(f"Source membership quality mismatch for {unit_key}")
            break
    return errors, membership_by_key


def _verify_boundaries(engine: Engine) -> list[str]:
    """Verify the boundaries table carries levels 0-3, one level-0 row, and areas."""
    with engine.connect() as conn:
        return _verify_boundaries_on(conn)


def _verify_boundaries_on(conn) -> list[str]:
    """`_verify_boundaries` on an open connection, so a Boundary release can
    verify its replacement inside the transaction that made it (issue #5)."""
    errors: list[str] = []
    counts = dict(
        conn.execute(
            text(
                f"SELECT level, COUNT(*) FROM {SERVICE_SCHEMA}.boundaries "
                f"GROUP BY level ORDER BY level"
            )
        ).fetchall()
    )
    for level in range(4):
        if level not in counts:
            errors.append(f"Boundaries missing level {level}")

    if counts.get(0, 0) != 1:
        errors.append(f"Expected exactly 1 country-outline row at level 0, got {counts.get(0)}")

    bad_area = conn.execute(
        text(
            f"SELECT COUNT(*) FROM {SERVICE_SCHEMA}.boundaries "
            f"WHERE area IS NULL OR area <= 0"
        )
    ).scalar()
    if bad_area:
        errors.append(f"{bad_area} boundaries have null or non-positive area")

    invalid = conn.execute(
        text(
            f"SELECT COUNT(*) FROM {SERVICE_SCHEMA}.boundaries "
            f"WHERE geometry IS NULL OR NOT ST_IsValid(geometry)"
        )
    ).scalar()
    if invalid:
        errors.append(f"{invalid} boundaries have null or invalid geometry")

    if not errors:
        levels = ", ".join(f"{k}:{counts[k]}" for k in sorted(counts))
        log.info("Boundaries verified (%s rows)", levels)
    return errors


def _verify_extraction(engine: Engine, table_name: str, report: ExtractionReport) -> list[str]:
    """Verify the loaded versioned table against the extraction report.

    Every source row must reach the raw table, because a snapshot with
    duplicate identities is rejected before it is written rather than
    deduplicated. Also checks the stored count and that no non-null
    reference_id appears more than once. Returns a list of error strings,
    empty if verification passes.
    """
    errors: list[str] = []
    expected = report.source_row_count
    if report.rows_loaded != expected:
        errors.append(
            f"Row count mismatch: loaded {report.rows_loaded}, expected {expected}"
        )

    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table_name}")
        ).scalar()
        if row != report.rows_loaded:
            errors.append(
                f"Row count mismatch in {table_name}: expected {report.rows_loaded}, got {row}"
            )

        dups = conn.execute(
            text(
                f"SELECT COUNT(*) FROM ("
                f"SELECT reference_id FROM {RAW_SCHEMA}.{table_name} "
                f"WHERE reference_id IS NOT NULL "
                f"GROUP BY reference_id HAVING COUNT(*) > 1) d"
            )
        ).scalar()
        if dups > 0:
            errors.append(f"Duplicate reference_ids found in {table_name}: {dups}")

    return errors


def _verify_transform(engine: Engine, source: str, report: TransformReport) -> list[str]:
    """Verify the stored staging tables against the transform report.

    Checks row counts, natural-key uniqueness, canonical labels, join
    coverage, the bad-quality distribution and its property links, and the
    decomposition counts. Any drift between what the transform computed and
    what the database holds is reported as an error.
    """
    errors: list[str] = []

    def scalar(sql: str) -> int:
        with engine.connect() as conn:
            return int(conn.execute(text(sql)).scalar())

    stored_rows = scalar(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source}")
    if stored_rows != report.rows_written:
        errors.append(
            f"Staging row count mismatch: wrote {report.rows_written}, stored {stored_rows}"
        )

    dup_natural = scalar(
        f"SELECT COUNT(*) FROM (SELECT energy_source, reference_id "
        f"FROM {STAGING_SCHEMA}.{source} WHERE reference_id IS NOT NULL "
        f"GROUP BY energy_source, reference_id HAVING COUNT(*) > 1) d"
    )
    if dup_natural:
        errors.append(f"Duplicate natural keys (energy_source, reference_id): {dup_natural}")

    with engine.connect() as conn:
        sources = [
            r[0]
            for r in conn.execute(
                text(
                    f"SELECT DISTINCT energy_source FROM {STAGING_SCHEMA}.{source}"
                    f" ORDER BY 1"
                )
            ).fetchall()
        ]
        iso = [
            r[0]
            for r in conn.execute(
                text(
                    f"SELECT DISTINCT country_iso FROM {STAGING_SCHEMA}.{source}"
                    f" ORDER BY 1"
                )
            ).fetchall()
        ]
    if sources != [report.source]:
        errors.append(f"Canonical energy_source is {sources}, expected {[report.source]}")
    if iso != ["DEU"]:
        errors.append(f"country_iso is {iso}, expected ['DEU']")

    if source == "storage":
        missing_shape = [
            col
            for col in STORAGE_COLUMNS
            if not scalar(
                f"SELECT COUNT(*) FROM information_schema.columns "
                f"WHERE table_schema = '{STAGING_SCHEMA}' AND table_name = '{source}' "
                f"AND column_name = '{col}'"
            )
        ]
        if missing_shape:
            errors.append(
                f"Staging table {source} missing storage shape columns: {missing_shape}"
            )
        untyped = scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source} WHERE storage_type IS NULL"
        )
        if untyped:
            errors.append(
                f"{untyped} storage rows missing storage_type"
            )
        with_capacity = scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source} "
            f"WHERE storage_capacity IS NOT NULL"
        )
        log.info(
            "Storage shape: %s, storage_type non-null on all %d rows, "
            "storage_capacity present on %d rows",
            missing_shape or "ok", stored_rows, with_capacity,
        )

    for col, unmapped in report.join_unmapped.items():
        stored = scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source} WHERE {col} IS NULL"
        )
        if stored != unmapped:
            errors.append(
                f"Join coverage mismatch for {col}: computed {unmapped}, stored {stored}"
            )
        log.info(
            "Coverage %s: %d/%d mapped",
            col, report.rows_written - stored, report.rows_written,
        )

    bad_rows = scalar(
        f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source} WHERE bad_quality"
    )
    if bad_rows != report.bad_quality:
        errors.append(
            f"Bad-quality count mismatch: computed {report.bad_quality}, stored {bad_rows}"
        )

    reason_rows = scalar(
        f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source}_units_properties up "
        f"JOIN {STAGING_SCHEMA}.{source}_properties p ON p.param_id = up.param_id "
        f"WHERE p.name = '{BAD_QUALITY_PROPERTY}'"
    )
    if reason_rows != report.bad_quality:
        errors.append(
            f"Bad-quality property links mismatch: expected {report.bad_quality}, "
            f"got {reason_rows}"
        )

    stored_reasons: dict[str, int] = {}
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT p.value, COUNT(DISTINCT up.unit_id) "
                f"FROM {STAGING_SCHEMA}.{source}_units_properties up "
                f"JOIN {STAGING_SCHEMA}.{source}_properties p ON p.param_id = up.param_id "
                f"WHERE p.name = '{BAD_QUALITY_PROPERTY}' GROUP BY p.value"
            )
        ).fetchall()
    for value, count in rows:
        stored_reasons[value] = int(count)
    if stored_reasons != report.quality_reasons:
        errors.append(
            f"Bad-quality distribution mismatch: computed {report.quality_reasons}, "
            f"stored {stored_reasons}"
        )

    props = scalar(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source}_properties")
    if props != report.properties_count:
        errors.append(
            f"Properties count mismatch: computed {report.properties_count}, stored {props}"
        )

    links = scalar(
        f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source}_units_properties"
    )
    if links != report.links_count:
        errors.append(
            f"Links count mismatch: computed {report.links_count}, stored {links}"
        )

    quoted_names = ", ".join(f"'{k}'" for k in DECOMPOSED_PROPERTIES)
    leaked = scalar(
        f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source} "
        f"WHERE secondary_attributes IS NOT NULL "
        f"AND secondary_attributes::jsonb ?| ARRAY[{quoted_names}]"
    )
    if leaked:
        errors.append(
            f"{leaked} staging rows still carry whitelisted keys in secondary_attributes"
        )

    nonwhitelist = scalar(
        f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{source}_properties "
        f"WHERE name <> '{BAD_QUALITY_PROPERTY}' "
        f"AND name NOT IN ({quoted_names})"
    )
    if nonwhitelist:
        errors.append(
            f"{nonwhitelist} non-whitelisted names found in "
            f"{STAGING_SCHEMA}.{source}_properties"
        )

    if not errors:
        log.info(
            "Staging verified for %s (%d rows, %d properties, %d links, %d bad)",
            source, stored_rows, props, links, bad_rows,
        )
    return errors


def _verify_load_generators(engine: Engine, report: LoadReport) -> list[str]:
    """Verify core.generators against the load report and staging.

    Checks row and capacity reconciliation against good staging rows, no
    duplicate (energy_source, reference_id) pairs where reference_id is
    present, per-row invariants (geometry, longitude/latitude, secondary
    attributes), and that every staging bad-quality row stayed out of core.
    """
    errors: list[str] = []

    def scalar(sql: str):
        with engine.connect() as conn:
            return conn.execute(text(sql)).scalar()

    stored = int(scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generators"))

    good_staging_count = int(
        scalar(
            "SELECT " + " + ".join(
                f"(SELECT COUNT(*) FROM {STAGING_SCHEMA}.{s} WHERE NOT bad_quality)"
                for s in STAGING_GENERATOR_SOURCES
            )
        )
    )
    if stored != good_staging_count:
        errors.append(f"Core rows {stored} != good staging rows {good_staging_count}")

    dups = int(
        scalar(
            f"SELECT COUNT(*) FROM ("
            f"SELECT energy_source, reference_id FROM {CORE_SCHEMA}.generators "
            f"WHERE reference_id IS NOT NULL "
            f"GROUP BY energy_source, reference_id HAVING COUNT(*) > 1) d"
        )
    )
    if dups:
        errors.append(f"Duplicate (energy_source, reference_id) pairs: {dups}")

    staging_cap = float(
        scalar(
            "SELECT " + " + ".join(
                f"COALESCE((SELECT SUM(installed_capacity) "
                f"FROM {STAGING_SCHEMA}.{s} WHERE NOT bad_quality), 0)"
                for s in STAGING_GENERATOR_SOURCES
            )
        )
    )
    core_cap = scalar(f"SELECT SUM(installed_capacity) FROM {CORE_SCHEMA}.generators")
    if core_cap is not None and abs(float(core_cap) - staging_cap) > 0.01:
        errors.append(
            f"Capacity drift: core {core_cap} vs good staging {staging_cap}"
        )

    without_geom = int(
        scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generators WHERE geometry IS NULL")
    )
    if without_geom:
        errors.append(f"{without_geom} core rows missing geometry")

    without_coords = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generators "
            f"WHERE longitude IS NULL OR latitude IS NULL"
        )
    )
    if without_coords:
        errors.append(f"{without_coords} core rows missing longitude/latitude")

    leaked_bad = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generators g "
            f"JOIN {CORE_SCHEMA}.generator_units_properties gp ON gp.unit_id = g.unit_id "
            f"JOIN {CORE_SCHEMA}.generator_properties p ON p.prop_id = gp.prop_id "
            f"WHERE p.name = '{BAD_QUALITY_PROPERTY}' "
            f"LIMIT 1"
        )
    )
    if leaked_bad:
        errors.append("bad_quality property links leaked into core")

    # Collision annotation integrity, per unit (ADR 0005): every collision=true
    # row must carry a collision property link, and no collision=false row may.
    # A directional per-unit check (not an aggregate count) catches compensated
    # drift — a link moved off one flagged row and onto a clean row keeps the
    # aggregate totals equal.
    missing_link = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generators g "
            f"WHERE g.collision AND NOT EXISTS ("
            f"SELECT 1 FROM {CORE_SCHEMA}.generator_units_properties up "
            f"JOIN {CORE_SCHEMA}.generator_properties p ON p.prop_id = up.prop_id "
            f"WHERE up.unit_id = g.unit_id AND p.name = '{COLLISION_PROPERTY}')"
        )
    )
    if missing_link:
        errors.append(f"{missing_link} collision=true rows lack a collision link")

    stale_link = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generators g "
            f"WHERE NOT g.collision AND EXISTS ("
            f"SELECT 1 FROM {CORE_SCHEMA}.generator_units_properties up "
            f"JOIN {CORE_SCHEMA}.generator_properties p ON p.prop_id = up.prop_id "
            f"WHERE up.unit_id = g.unit_id AND p.name = '{COLLISION_PROPERTY}')"
        )
    )
    if stale_link:
        errors.append(f"{stale_link} collision=false rows carry a collision link")

    # "Present and correct" (ADR 0005): the collision link value must name the
    # reasons the row merits.  A stripped or re-phrased reason is drift even
    # though a 'collision' link still exists.
    onshore_sql = ", ".join(f"'{s}'" for s in ONSHORE_SOURCES)
    sea_sql = ", ".join(f"'{r}'" for r in SEA_REGIONS)
    for reason, condition in (
        (OUTSIDE_LOCATION_COLLISION_REASON, "g.state IS NULL"),
        (
            ONSHORE_IN_SEA_COLLISION_REASON,
            f"g.energy_source IN ({onshore_sql}) AND g.state IN ({sea_sql})",
        ),
        (
            CLOSE_LOCATION_REASON,
            "g.secondary_attributes IS NOT NULL "
            "AND g.secondary_attributes::jsonb ? 'close_to'",
        ),
    ):
        stripped = int(
            scalar(
                f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generators g "
                f"JOIN {CORE_SCHEMA}.generator_units_properties up ON up.unit_id = g.unit_id "
                f"JOIN {CORE_SCHEMA}.generator_properties p ON p.prop_id = up.prop_id "
                f"WHERE {condition} AND p.name = '{COLLISION_PROPERTY}' "
                f"AND p.value NOT LIKE '%{reason}%'"
            )
        )
        if stripped:
            errors.append(
                f"{stripped} collision=true rows carry a collision link missing "
                f"the '{reason}' reason"
            )

    whitelist_names = ", ".join(f"'{n}'" for n in DECOMPOSED_PROPERTIES)
    annotation_names = f"'{COLLISION_PROPERTY}'"

    # Every whitelist (name, value) used by a good staging row must exist in
    # core.generator_properties (deduplicated across sources; bad_quality pairs
    # are staging-only and bad rows are not loaded at all).
    staging_prop_unions = " UNION ".join(
        f"SELECT p.name, p.value FROM {STAGING_SCHEMA}.{s}_properties p "
        f"JOIN {STAGING_SCHEMA}.{s}_units_properties up ON up.param_id = p.param_id "
        f"JOIN {STAGING_SCHEMA}.{s} u ON u.unit_id = up.unit_id "
        f"WHERE p.name <> '{BAD_QUALITY_PROPERTY}' AND NOT u.bad_quality"
        for s in STAGING_GENERATOR_SOURCES
    )
    expected_props = int(scalar(f"SELECT COUNT(*) FROM ({staging_prop_unions}) d"))
    core_props = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generator_properties "
            f"WHERE name IN ({whitelist_names})"
        )
    )
    if core_props != expected_props:
        errors.append(
            f"Core whitelist properties {core_props} != staging {expected_props}"
        )

    # No property outside the whitelist and the collision annotations.
    stray = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generator_properties "
            f"WHERE name NOT IN ({whitelist_names}, {annotation_names})"
        )
    )
    if stray:
        errors.append(f"{stray} core properties outside whitelist/annotations")

    # Whitelist links in core must equal the staging whitelist links carried by
    # good rows (bad rows are never loaded; bad_quality pairs are staging-only).
    staging_link_sums = " + ".join(
        f"(SELECT COUNT(*) FROM {STAGING_SCHEMA}.{s}_units_properties up "
        f"JOIN {STAGING_SCHEMA}.{s}_properties p ON p.param_id = up.param_id "
        f"JOIN {STAGING_SCHEMA}.{s} u ON u.unit_id = up.unit_id "
        f"WHERE p.name <> '{BAD_QUALITY_PROPERTY}' AND NOT u.bad_quality)"
        for s in STAGING_GENERATOR_SOURCES
    )
    expected_links = int(scalar(f"SELECT {staging_link_sums}"))
    core_links = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generator_units_properties up "
            f"JOIN {CORE_SCHEMA}.generator_properties p ON p.prop_id = up.prop_id "
            f"WHERE p.name IN ({whitelist_names})"
        )
    )
    if core_links != expected_links:
        errors.append(
            f"Core whitelist links {core_links} != good staging {expected_links}"
        )

    # No link may reference a missing core unit_id.
    orphans = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.generator_units_properties up "
            f"LEFT JOIN {CORE_SCHEMA}.generators g ON g.unit_id = up.unit_id "
            f"WHERE g.unit_id IS NULL"
        )
    )
    if orphans:
        errors.append(f"{orphans} orphaned property links (missing unit_id)")

    if not errors:
        log.info(
            "Core generators verified (%d rows, %d whitelist properties, "
            "%d whitelist links, capacity %.1f kW)",
            stored, core_props, core_links, float(core_cap or 0),
        )
    return errors


def _verify_load_storages(engine: Engine, report: LoadReport) -> list[str]:
    """Verify core.storages against the load report and staging.

    Mirrors the generator load verification for the storage kind: row and
    capacity reconciliation against good storage staging rows, no duplicate
    (energy_source, reference_id) pairs where reference_id is present,
    per-row invariants (geometry, longitude/latitude, secondary attributes),
    collision flag/link agreement, and that every staging bad-quality row
    stayed out of core.
    """
    errors: list[str] = []

    def scalar(sql: str):
        with engine.connect() as conn:
            return conn.execute(text(sql)).scalar()

    stored = int(scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages"))

    good_staging_count = int(
        scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.storage WHERE NOT bad_quality"
        )
    )
    if stored != good_staging_count:
        errors.append(f"Core rows {stored} != good staging rows {good_staging_count}")

    dups = int(
        scalar(
            f"SELECT COUNT(*) FROM ("
            f"SELECT energy_source, reference_id FROM {CORE_SCHEMA}.storages "
            f"WHERE reference_id IS NOT NULL "
            f"GROUP BY energy_source, reference_id HAVING COUNT(*) > 1) d"
        )
    )
    if dups:
        errors.append(f"Duplicate (energy_source, reference_id) pairs: {dups}")

    # Storage capacity reconciles against good staging rows (the primary
    # capacity of the storage kind).
    staging_cap = float(
        scalar(
            f"SELECT COALESCE((SELECT SUM(storage_capacity) "
            f"FROM {STAGING_SCHEMA}.storage WHERE NOT bad_quality), 0)"
        )
    )
    core_cap = scalar(f"SELECT SUM(storage_capacity) FROM {CORE_SCHEMA}.storages")
    if core_cap is not None and abs(float(core_cap) - staging_cap) > 0.01:
        errors.append(
            f"Storage capacity drift: core {core_cap} vs good staging {staging_cap}"
        )

    staging_inst = float(
        scalar(
            f"SELECT COALESCE((SELECT SUM(installed_capacity) "
            f"FROM {STAGING_SCHEMA}.storage WHERE NOT bad_quality), 0)"
        )
    )
    core_inst = scalar(f"SELECT SUM(installed_capacity) FROM {CORE_SCHEMA}.storages")
    if core_inst is not None and abs(float(core_inst) - staging_inst) > 0.01:
        errors.append(
            f"Installed capacity drift: core {core_inst} vs good staging {staging_inst}"
        )

    without_geom = int(
        scalar(f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages WHERE geometry IS NULL")
    )
    if without_geom:
        errors.append(f"{without_geom} core rows missing geometry")

    without_coords = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages "
            f"WHERE longitude IS NULL OR latitude IS NULL"
        )
    )
    if without_coords:
        errors.append(f"{without_coords} core rows missing longitude/latitude")

    leaked_bad = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages g "
            f"JOIN {CORE_SCHEMA}.storage_units_properties gp ON gp.unit_id = g.unit_id "
            f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = gp.prop_id "
            f"WHERE p.name = '{BAD_QUALITY_PROPERTY}' "
            f"LIMIT 1"
        )
    )
    if leaked_bad:
        errors.append("bad_quality property links leaked into core")

    # Collision annotation integrity, per unit (ADR 0005): every collision=true
    # row must carry a collision property link, and no collision=false row may.
    # A directional per-unit check (not an aggregate count) catches compensated
    # drift — a link moved off one flagged row and onto a clean row keeps the
    # aggregate totals equal.
    missing_link = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages g "
            f"WHERE g.collision AND NOT EXISTS ("
            f"SELECT 1 FROM {CORE_SCHEMA}.storage_units_properties up "
            f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = up.prop_id "
            f"WHERE up.unit_id = g.unit_id AND p.name = '{COLLISION_PROPERTY}')"
        )
    )
    if missing_link:
        errors.append(f"{missing_link} collision=true rows lack a collision link")

    stale_link = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages g "
            f"WHERE NOT g.collision AND EXISTS ("
            f"SELECT 1 FROM {CORE_SCHEMA}.storage_units_properties up "
            f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = up.prop_id "
            f"WHERE up.unit_id = g.unit_id AND p.name = '{COLLISION_PROPERTY}')"
        )
    )
    if stale_link:
        errors.append(f"{stale_link} collision=false rows carry a collision link")

    # "Present and correct" (ADR 0005): the collision link value must name the
    # reasons the row merits.  A stripped or re-phrased reason is drift even
    # though a 'collision' link still exists.
    sea_sql = ", ".join(f"'{r}'" for r in SEA_REGIONS)
    for reason, condition in (
        (OUTSIDE_LOCATION_COLLISION_REASON, "g.state IS NULL"),
        (
            ONSHORE_IN_SEA_COLLISION_REASON,
            f"g.energy_source = 'storage' AND g.state IN ({sea_sql})",
        ),
        (
            STORAGE_CAPACITY_COLLISION_REASON,
            "(g.storage_capacity IS NULL OR g.storage_capacity <= 0)",
        ),
        (
            CLOSE_LOCATION_REASON,
            "g.secondary_attributes IS NOT NULL "
            "AND g.secondary_attributes::jsonb ? 'close_to'",
        ),
    ):
        stripped = int(
            scalar(
                f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storages g "
                f"JOIN {CORE_SCHEMA}.storage_units_properties up ON up.unit_id = g.unit_id "
                f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = up.prop_id "
                f"WHERE {condition} AND p.name = '{COLLISION_PROPERTY}' "
                f"AND p.value NOT LIKE '%{reason}%'"
            )
        )
        if stripped:
            errors.append(
                f"{stripped} collision=true rows carry a collision link missing "
                f"the '{reason}' reason"
            )

    whitelist_names = ", ".join(f"'{n}'" for n in DECOMPOSED_PROPERTIES)
    annotation_names = f"'{COLLISION_PROPERTY}'"

    # Every whitelist (name, value) used by a good storage staging row must
    # exist in core.storage_properties.
    staging_prop_union = (
        f"SELECT DISTINCT p.name, p.value FROM {STAGING_SCHEMA}.storage_properties p "
        f"JOIN {STAGING_SCHEMA}.storage_units_properties up ON up.param_id = p.param_id "
        f"JOIN {STAGING_SCHEMA}.storage u ON u.unit_id = up.unit_id "
        f"WHERE p.name <> '{BAD_QUALITY_PROPERTY}' AND NOT u.bad_quality"
    )
    expected_props = int(scalar(f"SELECT COUNT(*) FROM ({staging_prop_union}) d"))
    core_props = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_properties "
            f"WHERE name IN ({whitelist_names})"
        )
    )
    if core_props != expected_props:
        errors.append(
            f"Core whitelist properties {core_props} != staging {expected_props}"
        )

    # No property outside the whitelist and the collision annotations.
    stray = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_properties "
            f"WHERE name NOT IN ({whitelist_names}, {annotation_names})"
        )
    )
    if stray:
        errors.append(f"{stray} core properties outside whitelist/annotations")

    # Whitelist links in core must equal the whitelist links carried by good
    # storage staging rows (bad rows are never loaded).
    expected_links = int(
        scalar(
            f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.storage_units_properties up "
            f"JOIN {STAGING_SCHEMA}.storage_properties p ON p.param_id = up.param_id "
            f"JOIN {STAGING_SCHEMA}.storage u ON u.unit_id = up.unit_id "
            f"WHERE p.name <> '{BAD_QUALITY_PROPERTY}' AND NOT u.bad_quality"
        )
    )
    core_links = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_units_properties up "
            f"JOIN {CORE_SCHEMA}.storage_properties p ON p.prop_id = up.prop_id "
            f"WHERE p.name IN ({whitelist_names})"
        )
    )
    if core_links != expected_links:
        errors.append(
            f"Core whitelist links {core_links} != good staging {expected_links}"
        )

    # No link may reference a missing core unit_id.
    orphans = int(
        scalar(
            f"SELECT COUNT(*) FROM {CORE_SCHEMA}.storage_units_properties up "
            f"LEFT JOIN {CORE_SCHEMA}.storages g ON g.unit_id = up.unit_id "
            f"WHERE g.unit_id IS NULL"
        )
    )
    if orphans:
        errors.append(f"{orphans} orphaned property links (missing unit_id)")

    if not errors:
        log.info(
            "Core storages verified (%d rows, %d whitelist properties, "
            "%d whitelist links, storage capacity %.1f kWh)",
            stored, core_props, core_links, float(core_cap or 0),
        )
    return errors


def _verify_marts(
    engine: Engine, definitions: Mapping[str, _MartDefinition]
) -> list[str]:
    """Reconcile each stored pivot to a fresh aggregation of core.

    For every mart the stored ``(state, pivot, value)`` cells are compared
    against the live expected aggregation as a set difference (``EXCEPT``).
    A row is drifted when a cell appears on one side but not the other —
    either the mart carries a cell the expected aggregation does not, the
    mart is missing a cell core warrants, or a stored ``value`` disagrees
    with the expected one.  ``EXCEPT`` compares NULLs as equal, which is
    exactly the outside-bucket / state-NULL semantics the pivots rely on.
    The error list is non-empty on drift (fail loudly).
    """
    errors: list[str] = []
    for name, defn in definitions.items():
        p, v = defn.pivot, defn.value
        mart_fq = f"{MARTS_SCHEMA}.{name}"
        columns = f"state, {p}, {v}"

        stale = (
            f"SELECT COUNT(*) FROM ("
            f"SELECT {columns} FROM {mart_fq} "
            f"EXCEPT "
            f"{defn.select_sql}) _d"
        )
        missing = (
            f"SELECT COUNT(*) FROM ("
            f"{defn.select_sql} "
            f"EXCEPT "
            f"SELECT {columns} FROM {mart_fq}) _d"
        )

        with engine.connect() as conn:
            extra = int(conn.execute(text(stale)).scalar())
            absent = int(conn.execute(text(missing)).scalar())

        if extra or absent:
            details = []
            if extra:
                details.append(f"{extra} stale/incorrect cell(s)")
            if absent:
                details.append(f"{absent} missing cell(s)")
            errors.append(f"marts.{name} drifted from core: {', '.join(details)}")

    if not errors:
        log.info("Marts reconciled to core active-unit aggregates")
    return errors
