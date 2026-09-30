"""Boundary releases: validate, replace a level atomically, rebuild geography.

A Boundary release is one GPKG per administrative level published at a fixed
key (`boundaries/level-<n>.gpkg`). The service keeps a single non-versioned
reference layer, `service.boundaries`; a release replaces only the rows of the
levels it publishes, in one transaction, and every unit's state, region and
district is then rederived from the new layer (issue #5).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import pandas
import pyogrio
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import (
    BOUNDARY_COLUMN_MAPPING,
    BOUNDARY_SIMPLIFY_TOLERANCE,
    CORE_SCHEMA,
    SERVICE_SCHEMA,
    SOURCE_NAMES,
    STAGING_SCHEMA,
)
from etl.db_utils import _create_log_table, _ensure_schema, _table_exists
from etl.load import GENERATOR_KIND, STORAGE_KIND, rebuild_core_geography
from etl.transform import reenrich_staging
from etl.verify import _verify_boundaries_on

BOUNDARY_LEVELS = (0, 1, 2, 3)

_REQUIRED_COLUMNS = ("name", "iso", "level")
_POLYGON_TYPES = frozenset({"Polygon", "MultiPolygon"})


class BoundaryValidationError(ValueError):
    pass


@dataclass(frozen=True)
class BoundaryRelease:
    """A validated Boundary level, shaped like `service.boundaries` rows."""

    level: int
    data: gpd.GeoDataFrame


def inspect_boundary_gpkg(path: Path, *, expected_level: int) -> BoundaryRelease:
    """Validate one Boundary GPKG against the level its fixed key promises.

    Checks happen before any database write: exactly one layer, the required
    columns, rows present, every row at *expected_level*, non-null names,
    valid non-null (Multi)Polygon geometry in EPSG:4326, and a single country
    outline at level 0.
    """
    layers = pyogrio.list_layers(path)
    if len(layers) != 1:
        raise BoundaryValidationError(
            f"Boundary GPKG must contain exactly one layer, found {len(layers)}"
        )
    data = gpd.read_file(path, layer=str(layers[0][0]))

    missing = [column for column in _REQUIRED_COLUMNS if column not in data.columns]
    if missing:
        raise BoundaryValidationError(
            f"Boundary GPKG missing required columns: {', '.join(missing)}"
        )
    if data.empty:
        raise BoundaryValidationError("Boundary GPKG has no rows")

    levels = sorted({int(level) for level in data["level"].dropna()})
    if data["level"].isna().any() or levels != [expected_level]:
        raise BoundaryValidationError(
            f"Boundary GPKG expected level {expected_level}, found {levels}"
        )
    if data["name"].isna().any() or data["name"].astype(str).str.strip().eq("").any():
        raise BoundaryValidationError("Boundary rows require a non-null name")

    geometry = data.geometry
    if geometry.isna().any() or not geometry.geom_type.isin(_POLYGON_TYPES).all():
        raise BoundaryValidationError(
            "Boundary rows must use non-null Polygon or MultiPolygon geometry"
        )
    if data.crs is None or data.crs.to_epsg() != 4326:
        raise BoundaryValidationError("Boundary geometry must use EPSG:4326")
    invalid = data.loc[~geometry.is_valid, "name"].astype(str).tolist()
    if invalid:
        raise BoundaryValidationError(f"Boundary rows have invalid geometry: {invalid}")

    if expected_level == 0 and len(data) != 1:
        raise BoundaryValidationError(
            f"Level 0 must hold exactly one country outline, found {len(data)} rows"
        )

    shaped = data.rename(columns=BOUNDARY_COLUMN_MAPPING)[
        ["country_iso", "name", "level", "geometry"]
    ].copy()
    shaped["level"] = expected_level
    return BoundaryRelease(level=expected_level, data=shaped)


class BoundaryReplacementError(RuntimeError):
    pass


@dataclass(frozen=True)
class BoundaryObject:
    """A validated Boundary level together with the S3 object version it came from."""

    release: BoundaryRelease
    bucket: str
    object_key: str
    object_version_id: str
    ingestion_run_id: str


@dataclass(frozen=True)
class ReplacementResult:
    levels: tuple[int, ...]
    rows: dict[int, int]
    reused: bool


def replace_boundary_levels(
    engine: Engine, objects: Sequence[BoundaryObject]
) -> ReplacementResult:
    """Replace the published levels of `service.boundaries` in one transaction.

    Only the rows of the published levels are deleted and re-inserted; area and
    the viz GeoJSON are recomputed for them, the whole layer is verified
    (levels 0-3 complete and valid), and each object version is written to the
    load ledger — all before commit. Any failure rolls everything back, so no
    reader ever sees a partial release. A batch whose object versions are all
    in the ledger was already applied, and is not applied again.
    """
    levels = tuple(sorted(obj.release.level for obj in objects))
    if len(set(levels)) != len(levels):
        raise BoundaryReplacementError(f"Boundary batch repeats a level: {list(levels)}")
    rows = {obj.release.level: len(obj.release.data) for obj in objects}

    _ensure_schema(engine, SERVICE_SCHEMA)
    _create_log_table(engine, SERVICE_SCHEMA)
    if all(_ledgered(engine, obj) for obj in objects):
        return ReplacementResult(levels, rows, reused=True)

    frame = gpd.GeoDataFrame(
        pandas.concat([obj.release.data for obj in objects], ignore_index=True),
        geometry="geometry",
        crs="EPSG:4326",
    )
    frame["area"] = 0.0
    table_existed = _table_exists(engine, "boundaries", SERVICE_SCHEMA)
    with engine.begin() as conn:
        if table_existed:
            conn.execute(
                text(f"DELETE FROM {SERVICE_SCHEMA}.boundaries WHERE level = ANY(:levels)"),
                {"levels": list(levels)},
            )
        frame.to_postgis(
            "boundaries", conn, schema=SERVICE_SCHEMA, if_exists="append", index=False
        )
        conn.execute(
            text(
                f"ALTER TABLE {SERVICE_SCHEMA}.boundaries "
                "ADD COLUMN IF NOT EXISTS geojson TEXT"
            )
        )
        conn.execute(
            text(
                f"UPDATE {SERVICE_SCHEMA}.boundaries "
                "SET area = ST_Area(geometry::geography) / 1e6, "
                "geojson = ST_AsGeoJSON(ST_SimplifyPreserveTopology(geometry, "
                f"{BOUNDARY_SIMPLIFY_TOLERANCE})) "
                "WHERE level = ANY(:levels)"
            ),
            {"levels": list(levels)},
        )
        errors = _verify_boundaries_on(conn)
        if errors:
            raise BoundaryReplacementError("; ".join(errors))
        for obj in objects:
            conn.execute(
                text(
                    f"INSERT INTO {SERVICE_SCHEMA}.loaded_files "
                    "(ingestion_run_id, bucket, object_key, object_version_id, "
                    "loaded_at, loaded_to, verified_at) "
                    "VALUES (:run_id, :bucket, :object_key, :object_version_id, "
                    "CURRENT_TIMESTAMP, :loaded_to, CURRENT_TIMESTAMP)"
                ),
                {
                    "run_id": obj.ingestion_run_id,
                    "bucket": obj.bucket,
                    "object_key": obj.object_key,
                    "object_version_id": obj.object_version_id,
                    "loaded_to": f"{SERVICE_SCHEMA}.boundaries",
                },
            )
    return ReplacementResult(levels, rows, reused=False)


def _ledgered(engine: Engine, obj: BoundaryObject) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(
                text(
                    f"SELECT 1 FROM {SERVICE_SCHEMA}.loaded_files "
                    "WHERE bucket = :bucket AND object_key = :object_key "
                    "AND object_version_id = :object_version_id"
                ),
                {
                    "bucket": obj.bucket,
                    "object_key": obj.object_key,
                    "object_version_id": obj.object_version_id,
                },
            ).first()
            is not None
        )


@dataclass
class GeographyReport:
    """What a geography rebuild touched: staging rows per Source, Core per kind."""

    staging: dict[str, int] = field(default_factory=dict)
    core: dict[str, int] = field(default_factory=dict)
    collisions: dict[str, int] = field(default_factory=dict)
    errors: dict[str, list[str]] = field(default_factory=dict)


def rebuild_geography(engine: Engine) -> GeographyReport:
    """Rederive every unit's state, region and district from `service.boundaries`.

    Runs after any Boundary release: every Source's staging table is
    re-enriched, then every Core unit of both kinds — including units no
    longer in any staging snapshot — and the state-dependent collisions. The
    marts are refreshed afterwards by the caller, once per message.

    A step that fails is reported against its target with a row count of 0, so
    the caller sees the failure rather than a silently skipped rebuild.
    """
    report = GeographyReport()
    for source in SOURCE_NAMES:
        if _table_exists(engine, source, STAGING_SCHEMA):
            report.staging[source] = 0
    for kind in (GENERATOR_KIND, STORAGE_KIND):
        if _table_exists(engine, kind.core_table, CORE_SCHEMA):
            report.core[kind.core_table] = 0
    try:
        boundaries = gpd.read_postgis(
            f"SELECT level, name, geometry FROM {SERVICE_SCHEMA}.boundaries",
            engine,
            geom_col="geometry",
        )
    except Exception as error:
        for target in list(report.staging) + list(report.core):
            report.errors[target] = [f"Boundaries unreadable: {error}"]
        return report

    for source in list(report.staging):
        try:
            report.staging[source], report.errors[source] = reenrich_staging(
                engine, source, boundaries
            )
        except Exception as error:
            report.errors[source] = [f"Staging geography rebuild failed: {error}"]
    for kind in (GENERATOR_KIND, STORAGE_KIND):
        target = kind.core_table
        if target not in report.core:
            continue
        try:
            rebuilt = rebuild_core_geography(engine, kind, boundaries)
            report.core[target] = rebuilt.rows_updated
            report.collisions[target] = rebuilt.collisions
            report.errors[target] = list(rebuilt.errors)
        except Exception as error:
            report.errors[target] = [f"Core geography rebuild failed: {error}"]
    return report
