from __future__ import annotations

import json
import logging
import time
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import get_engine
from etl.config import (
    BOUNDARY_COLUMNS,
    BOUNDARY_COLUMN_MAPPING,
    BOUNDARY_SIMPLIFY_TOLERANCE,
    RAW_COLUMNS,
    RAW_COLUMN_MAPPING,
    RAW_SCHEMA,
    SERVICE_SCHEMA,
)
from etl.db_utils import _create_log_table, _ensure_schema
from etl.reports import BoundariesReport, ExtractionReport
from etl.source_data import SourceDataset, inspect_source_gpkg
from etl.utils import (
    _compute_boundary_areas,
    _loaded_table_for_signature,
    _log_load,
    _next_table_version,
    _read_manifest,
    _refresh_boundary_geojson,
)
from etl.verify import _verify_boundaries, _verify_extraction

log = logging.getLogger(__name__)


def extract_source(
    file_path: Path,
    force: bool = False,
    dataset: SourceDataset | None = None,
) -> ExtractionReport:
    """Extract one validated Source snapshot from a local GPKG."""
    report = ExtractionReport()
    start = time.perf_counter()
    try:
        if dataset is None:
            dataset = inspect_source_gpkg(file_path)
        stat = file_path.stat()
        report = _extract_validated_source(
            dataset,
            engine=get_engine(),
            filename=file_path.name,
            filesize=stat.st_size,
            modified_at=stat.st_mtime,
            force=force,
        )
    except Exception as error:
        report.origin = file_path.name
        report.errors.append(f"Extraction failed: {error}")
        log.exception("Extraction failed for %s", file_path.name)
    report.total_time = time.perf_counter() - start
    return report


def extract_source_snapshot(
    dataset: SourceDataset,
    *,
    engine: Engine,
    filename: str,
    bucket: str,
    object_key: str,
    object_version_id: str,
    ingestion_run_id,
) -> ExtractionReport:
    report = ExtractionReport()
    start = time.perf_counter()
    try:
        report = _extract_validated_source(
            dataset,
            engine=engine,
            filename=filename,
            bucket=bucket,
            object_key=object_key,
            object_version_id=object_version_id,
            ingestion_run_id=ingestion_run_id,
        )
    except Exception as error:
        report.origin = object_key
        report.errors.append(f"Extraction failed: {error}")
        log.exception("Extraction failed for %s", object_key)
    report.total_time = time.perf_counter() - start
    return report


def _extract_validated_source(
    dataset: SourceDataset,
    *,
    engine: Engine,
    filename: str,
    filesize: int | None = None,
    modified_at: float | None = None,
    bucket: str | None = None,
    object_key: str | None = None,
    object_version_id: str | None = None,
    ingestion_run_id=None,
    force: bool = False,
) -> ExtractionReport:
    source = dataset.source
    report = ExtractionReport(source=source, origin=filename)
    start = time.perf_counter()
    try:
        s3_identity = all(
            value is not None for value in (bucket, object_key, object_version_id)
        )
        if s3_identity:
            existing = _s3_loaded_table(
                engine,
                bucket=str(bucket),
                object_key=str(object_key),
                object_version_id=str(object_version_id),
            )
        else:
            existing = None
            if not force:
                existing = _loaded_table_for_signature(
                    engine, (filename, int(filesize), float(modified_at))
                )

        if existing:
            report.loaded_to = existing
            report.source_row_count = _raw_row_count(engine, existing)
            report.rows_loaded = report.source_row_count
            report.skipped = True
            # The reuse decision is a claim that this raw version is intact, so
            # it is verified like a freshly written one rather than trusted: a
            # run must not be completed on an unverified table.
            report.errors = _verify_extraction(engine, existing, report)
            return report

        _ensure_schema(engine, RAW_SCHEMA)
        _ensure_schema(engine, SERVICE_SCHEMA)
        _create_log_table(engine, SERVICE_SCHEMA)

        df = dataset.data.copy()
        report.source_row_count = len(df)
        for old, new in RAW_COLUMN_MAPPING.get(source, {}).items():
            df = df.rename(columns={old: new})
        df["energy_source"] = source
        _cast_types(df)
        report.attributes_empty = _build_secondary_attributes(df)
        df = df[[column for column in RAW_COLUMNS if column in df.columns]]

        table_name = _next_table_version(engine, source, date.today())
        df.to_postgis(
            table_name,
            engine,
            schema=RAW_SCHEMA,
            if_exists="fail",
            index=False,
        )
        report.loaded_to = table_name
        report.rows_loaded = len(df)
        report.errors = _verify_extraction(engine, table_name, report)
        if report.errors:
            return report

        if s3_identity:
            _log_s3_load(
                engine,
                bucket=str(bucket),
                object_key=str(object_key),
                object_version_id=str(object_version_id),
                ingestion_run_id=ingestion_run_id,
                loaded_to=table_name,
            )
        else:
            _log_load(
                engine,
                filename,
                int(filesize),
                float(modified_at),
                table_name,
            )
    except Exception as error:
        report.errors.append(f"Extraction failed: {error}")
        log.exception("Extraction failed for %s", filename)

    report.total_time = time.perf_counter() - start
    return report


def _s3_loaded_table(
    engine: Engine,
    *,
    bucket: str,
    object_key: str,
    object_version_id: str,
) -> str | None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                f"SELECT loaded_to FROM {SERVICE_SCHEMA}.loaded_files "
                "WHERE bucket = :bucket AND object_key = :object_key "
                "AND object_version_id = :object_version_id"
            ),
            {
                "bucket": bucket,
                "object_key": object_key,
                "object_version_id": object_version_id,
            },
        ).first()
    return str(row[0]) if row else None


def _raw_row_count(engine: Engine, table_name: str) -> int:
    with engine.connect() as connection:
        return int(
            connection.execute(
                text(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.\"{table_name}\"")
            ).scalar()
        )


def _log_s3_load(
    engine: Engine,
    *,
    bucket: str,
    object_key: str,
    object_version_id: str,
    ingestion_run_id,
    loaded_to: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"INSERT INTO {SERVICE_SCHEMA}.loaded_files "
                "(ingestion_run_id, bucket, object_key, object_version_id, "
                "loaded_at, loaded_to, verified_at) "
                "VALUES (:ingestion_run_id, :bucket, :object_key, "
                ":object_version_id, CURRENT_TIMESTAMP, :loaded_to, CURRENT_TIMESTAMP)"
            ),
            {
                "ingestion_run_id": str(ingestion_run_id),
                "bucket": bucket,
                "object_key": object_key,
                "object_version_id": object_version_id,
                "loaded_to": loaded_to,
            },
        )


def _cast_types(df: pandas.DataFrame) -> None:
    """Cast columns to the raw shape in place.

    Commissioning and decommissioning dates keep the day resolution of the
    source data; reference_date keeps its full timestamp including the time
    of day, because it is descriptive Source provenance rather than an
    ordering rule.
    """
    for col in ("commissioning_date", "decommissioning_date"):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x[:10] if isinstance(x, str) and len(x) >= 10 else x
            )
            df[col] = pandas.to_datetime(df[col], errors="coerce").dt.date

    if "reference_date" in df.columns:
        df["reference_date"] = pandas.to_datetime(df["reference_date"], errors="coerce")

    df["installed_capacity"] = df["installed_capacity"].astype("Float64")
    df["x_coordinates"] = df["x_coordinates"].astype("Float64")
    df["y_coordinates"] = df["y_coordinates"].astype("Float64")
    df["geo_accuracy"] = df["geo_accuracy"].astype("Int64")
    if "storage_capacity" in df.columns:
        df["storage_capacity"] = df["storage_capacity"].astype("Float64")


def _build_secondary_attributes(df: pandas.DataFrame) -> int:
    """Fold secondary attributes into a serialized JSON column stored as text.

    All columns outside RAW_COLUMNS (and the geometry) are collapsed into a
    per-row JSON document, dropping null/empty values; numpy scalars and date
    values are stringified via the JSON default hook. Returns the count of
    rows whose document is empty.
    """
    attr_cols = [c for c in df.columns if c not in RAW_COLUMNS]
    df["secondary_attributes"] = df[attr_cols].apply(
        lambda row: {k: v for k, v in row.items() if not pandas.isna(v)},
        axis=1,
    )
    empty = int((df["secondary_attributes"].apply(len) == 0).sum())
    df["secondary_attributes"] = df["secondary_attributes"].apply(
        lambda d: json.dumps(d, default=str)
    )
    return empty


def extract_boundaries(manifest: Path, force: bool = False) -> BoundariesReport:
    """Load the boundary reference files listed in a manifest into service.boundaries.

    MANIFEST lists one germany_*.gpkg file per line, resolved against the
    manifest's directory; each file's level (0-3) is read from its `level`
    column, and a file without one fails loudly rather than mislabelling.
    Every file is read into a GeoDataFrame, stripped to the raw column set
    (iso is mapped to country_iso), stamped with its level and written via
    to_postgis: the first file replaces the table, the rest append. Area is
    computed in km² via PostGIS.

    The boundaries table is rebuilt atomically from the whole manifest, so a
    load only runs when at least one file's signature (filename, filesize,
    modified_at) is not yet logged in service.loaded_files or force is set;
    every file written is then logged.  If all files are already logged the
    entire operation is skipped.

    On every run the simplified GeoJSON the viz app reads is materialized
    idempotently (`_refresh_boundary_geojson`, issue #31): the `geojson`
    column is added if missing and recomputed for every row with
    `BOUNDARY_SIMPLIFY_TOLERANCE`, even when the load itself is skipped, so
    existing databases self-upgrade on any run.
    """
    report = BoundariesReport()
    start = time.perf_counter()
    try:
        engine = get_engine()
        _ensure_schema(engine, SERVICE_SCHEMA)
        _create_log_table(engine)

        filenames = _read_manifest(manifest)

        all_logged = True
        for filename in filenames:
            f = manifest.parent / filename
            stat = f.stat()
            sig = (f.name, stat.st_size, stat.st_mtime)
            if _loaded_table_for_signature(engine, sig) is None or force:
                all_logged = False
                break

        if all_logged:
            report.skipped = True
            log.info("Skipping boundaries: all files already loaded")
        else:
            first = True
            for filename in filenames:
                f = manifest.parent / filename
                stat = f.stat()
                log.info("Loading %s", filename)

                gdf = gpd.read_file(f)
                if "name" not in gdf.columns:
                    raise ValueError(f"{filename} has no 'name' column")
                if "level" not in gdf.columns:
                    raise ValueError(f"{filename} has no 'level' column")
                levels = gdf["level"].unique()
                if len(levels) != 1:
                    raise ValueError(
                        f"{filename} must carry a single 'level' value, "
                        f"found {sorted(levels.tolist())}"
                    )
                level = int(levels[0])

                out = gdf.rename(columns=BOUNDARY_COLUMN_MAPPING)
                drop = [c for c in out.columns if c not in BOUNDARY_COLUMNS]
                out = out.drop(columns=drop)
                out["level"] = level
                out["area"] = 0.0

                out.to_postgis(
                    "boundaries", engine, schema=SERVICE_SCHEMA,
                    if_exists="replace" if first else "append",
                    index=False,
                )
                first = False
                report.rows_by_level[level] = len(out)

                _log_load(engine, f.name, stat.st_size, stat.st_mtime, "service.boundaries")

            if report.rows_by_level:
                _compute_boundary_areas(engine)
                report.loaded = True
                report.errors = _verify_boundaries(engine)

        # Materialize the simplified GeoJSON the viz app reads, idempotently
        # (issue #31): ALTER + UPDATE run on every boundaries run — even the
        # skipped one — so a database seeded before the column existed
        # self-upgrades without a force reload.
        _refresh_boundary_geojson(engine)
    except Exception as e:
        report.errors.append(f"Boundary load failed: {e}")
        log.exception("Boundary load failed")

    report.total_time = time.perf_counter() - start
    return report