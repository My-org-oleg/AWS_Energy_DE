from __future__ import annotations

import json
import logging
import time

import geopandas as gpd
import numpy
import pandas
from psycopg2.extras import execute_values
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import SOURCE_NAMES, get_engine
from etl.config import (
    BAD_QUALITY_PROPERTY,
    BOUNDARY_LEVEL_COLUMNS,
    DECOMPOSED_PROPERTIES,
    RAW_SCHEMA,
    SERVICE_SCHEMA,
    STAGING_COLUMNS,
    STAGING_SCHEMA,
    SYNTHETIC_ID_PREFIX,
)
from etl.db_utils import (
    _create_staging_tables,
    _drop_staging_tables,
    _ensure_schema,
    _table_exists,
)
from etl.reports import TransformReport
from etl.source_data import SourceValidationError, synthetic_unit_ids
from etl.utils import _latest_table_version
from etl.verify import _verify_membership_matches_staging, _verify_transform

log = logging.getLogger(__name__)

COORD_TOLERANCE_DEG = 1e-9

QUALITY_CAPACITY_NULL = "installed_capacity is null"
QUALITY_CAPACITY_NONPOSITIVE = "installed_capacity <= 0"
QUALITY_DATES = "bad pair commissioning_date/decommissioning_date"
QUALITY_COORDS = "x/y coordinates disagree with geometry"


def transform_sources(*sources: str) -> TransformReport:
    """Transform one or more sources into their staging tables.

    SOURCES is any number of SOURCE_NAMES values; the special value "all"
    (or calling with no arguments) transforms every source.  Each source
    reads raw.<source>_<YYYYMMDD>_<n> (the latest version), assigns units
    their staging identity (natural key from reference_id, synthetic hash
    where absent), enriches with state/region/district via a spatial
    join against the boundary reference layers, applies the quality gate,
    and decomposes the whitelisted secondary attributes into normalized
    property tables (the rest staying in the reduced `secondary_attributes`
    json).  A bad quality row stays in staging but is never a candidate for
    core; a state-null row is not bad quality (spec v2.2 leaves that to the
    load stage's collision checks).  The per-source reports are merged into
    one aggregate report.
    """
    if not sources or "all" in sources:
        sources = SOURCE_NAMES
    reports = [_reported(s) for s in sources]
    return _merge_transform_reports(sources, reports)


def _reported(source: str) -> TransformReport:
    """Transform one source for the report API, which reports rather than rejects.

    The event path treats a file that fails its own validation as a rejected
    object version; this path is a hand-run over the latest raw version, so the
    verdict belongs in the report the operator is reading.
    """
    try:
        return _transform_source(source)
    except SourceValidationError as error:
        return TransformReport(source=source, errors=[f"Transform failed: {error}"])


def transform_source_snapshot(
    source: str,
    raw_table: str,
    *,
    engine: Engine,
    ingestion_run_id,
) -> TransformReport:
    return _transform_source(
        source,
        engine=engine,
        raw_table=raw_table,
        ingestion_run_id=ingestion_run_id,
    )


def _transform_source(
    source: str,
    *,
    engine: Engine | None = None,
    raw_table: str | None = None,
    ingestion_run_id=None,
) -> TransformReport:
    report = TransformReport(source=source)
    start = time.perf_counter()
    try:
        engine = engine or get_engine()
        _ensure_schema(engine, STAGING_SCHEMA)

        raw_table = raw_table or _latest_table_version(engine, source)
        if raw_table is None:
            report.errors.append(
                f"raw tables missing for source {source!r}; "
                "run 'python -m etl extract' first"
            )
            report.total_time = time.perf_counter() - start
            log.error("Transform failed for %s: %s", source, report.errors[0])
            log.info("Total time: %.3fs", report.total_time)
            return report
        report.raw_table = raw_table

        if not _table_exists(engine, "boundaries", SERVICE_SCHEMA):
            report.errors.append(
                f"boundaries table missing for source {source!r}; "
                "run 'python -m etl boundaries' first"
            )
            report.total_time = time.perf_counter() - start
            log.error("Transform failed for %s: %s", source, report.errors[0])
            log.info("Total time: %.3fs", report.total_time)
            return report

        log.info("Transforming %s from %s.%s", source, RAW_SCHEMA, raw_table)
        t = time.perf_counter()
        df = gpd.read_postgis(
            f'SELECT * FROM {RAW_SCHEMA}."{raw_table}"', engine, geom_col="geometry"
        )
        report.rows_read = len(df)
        log.info(
            "%d rows read, time %.3fs", report.rows_read, time.perf_counter() - t
        )

        boundaries = gpd.read_postgis(
            f"SELECT level, name, geometry FROM {SERVICE_SCHEMA}.boundaries",
            engine,
            geom_col="geometry",
        )

        df["unit_id"] = _unit_ids(df, source)
        report.synthetic_ids = int(df["unit_id"].str.startswith(SYNTHETIC_ID_PREFIX).sum())
        df["energy_source"] = source
        df["country_iso"] = "DEU"
        df["geo_accuracy"] = df["geo_accuracy"].astype("Int64")

        log.info("Spatial join against boundary levels 1/2/3...")
        t = time.perf_counter()
        geography = enrich_geography(df, boundaries)
        for col in BOUNDARY_LEVEL_COLUMNS.values():
            df[col] = geography[col]
            report.join_unmapped[col] = int(df[col].isna().sum())
        log.info(
            "Join coverage %s, time %.3fs",
            report.join_unmapped, time.perf_counter() - t,
        )

        log.info("Running quality gate...")
        t = time.perf_counter()
        reasons = _quality_reasons(df)
        df["bad_quality"] = reasons.apply(bool)
        report.bad_quality = int(df["bad_quality"].sum())
        report.quality_reasons = _reason_histogram(reasons)
        log.info(
            "Quality gate: %d bad rows (%s), time %.3fs",
            report.bad_quality, report.quality_reasons, time.perf_counter() - t,
        )

        log.info("Decomposing attributes...")
        t = time.perf_counter()
        df["secondary_json"] = df["secondary_attributes"].apply(json.loads)
        props, links = _decompose_attributes(df, reasons)
        df["secondary_attributes"] = df["secondary_json"].map( # dropping decomposed attributes out of secondary_attributes
            lambda d: json.dumps(
                {k: v for k, v in d.items() if k not in DECOMPOSED_PROPERTIES}
            )
        )
        report.properties_count = len(props)
        report.links_count = len(links)
        log.info(
            "Decomposed into %d properties / %d links, time %.3fs",
            report.properties_count, report.links_count, time.perf_counter() - t,
        )

        log.info("Writing staging tables...")
        t = time.perf_counter()
        _create_staging_tables(engine, source)
        out = df[[c for c in STAGING_COLUMNS if c in df.columns]]
        out.to_postgis(
            source, engine, schema=STAGING_SCHEMA, if_exists="append", index=False
        )
        props.to_sql(
            f"{source}_properties", engine, schema=STAGING_SCHEMA,
            if_exists="append", index=False,
        )
        links.to_sql(
            f"{source}_units_properties", engine, schema=STAGING_SCHEMA,
            if_exists="append", index=False,
        )
        report.rows_written = len(df)
        log.info("Staging written, time %.3fs", time.perf_counter() - t)

        if ingestion_run_id is not None:
            _record_source_memberships(
                engine,
                source=source,
                ingestion_run_id=ingestion_run_id,
            )

        log.info("Verifying transform...")
        t = time.perf_counter()
        report.errors = _verify_transform(engine, source, report)
        if ingestion_run_id is not None:
            membership_errors, _ = _verify_membership_matches_staging(
                engine,
                source=source,
                ingestion_run_id=ingestion_run_id,
            )
            report.errors.extend(membership_errors)
        log.info("Verification done, time %.3fs", time.perf_counter() - t)

    except SourceValidationError:
        # The version is rejected rather than retried (issue #7), so the caller
        # has to be able to see the verdict; what was staged for it is still not
        # verified and is thrown away.
        if ingestion_run_id is not None:
            _discard_unverified_staging(engine, source)
        raise
    except Exception as e:
        report.errors.append(f"Transform failed: {e}")
        log.exception("Transform failed for %s", source)

    if report.errors and ingestion_run_id is not None:
        _discard_unverified_staging(engine, source)

    report.total_time = time.perf_counter() - start
    if report.errors:
        for err in report.errors:
            log.error("Transform failed for %s: %s", report.source, err)
    else:
        log.info("Transform passed for %s (%d rows)", report.source, report.rows_read)
    log.info("Total time: %.3fs", report.total_time)

    return report


def enrich_geography(
    points: gpd.GeoDataFrame, boundaries: gpd.GeoDataFrame
) -> pandas.DataFrame:
    """Derive state, region and district for every point from the boundary layer.

    The single definition of administrative geography, shared by the transform
    and by the rebuild after a Boundary release so the two cannot drift. A
    point takes the name of the level-1/2/3 polygon it intersects; one on a
    shared border takes the alphabetically first name, so the result is
    deterministic; one outside every polygon of a level gets null.
    """
    frame = gpd.GeoDataFrame(
        {"_row": range(len(points))}, geometry=points.geometry.values, crs=points.crs
    )
    result = pandas.DataFrame(index=points.index)
    for level, column in BOUNDARY_LEVEL_COLUMNS.items():
        layer = boundaries.loc[boundaries["level"] == level, ["name", "geometry"]]
        joined = frame.sjoin(layer, how="left", predicate="intersects")
        names = joined.groupby("_row")["name"].min()
        result[column] = pandas.Series(
            names.reindex(range(len(points))).to_numpy(), index=points.index, dtype=object
        ).where(lambda values: values.notna(), None)
    return result


def reenrich_staging(
    engine: Engine, source: str, boundaries: gpd.GeoDataFrame
) -> tuple[int, list[str]]:
    """Rederive a Source's staging geography in place after a Boundary release.

    Only state, region and district change: identity, quality and properties
    do not depend on boundaries. Returns (rows updated, verification errors).
    """
    staged = gpd.read_postgis(
        f"SELECT unit_id, geometry FROM {STAGING_SCHEMA}.{source}",
        engine,
        geom_col="geometry",
    )
    geography = enrich_geography(staged, boundaries)
    records = list(
        zip(
            staged["unit_id"],
            geography["state"],
            geography["region"],
            geography["district"],
        )
    )
    with engine.begin() as connection:
        if records:
            execute_values(
                connection.connection.cursor(),
                f"UPDATE {STAGING_SCHEMA}.{source} s "
                "SET state = v.state, region = v.region, district = v.district "
                "FROM (VALUES %s) AS v(unit_id, state, region, district) "
                "WHERE s.unit_id = v.unit_id",
                records,
                template="(%s, %s::text, %s::text, %s::text)",
                page_size=2000,
            )
    stored = pandas.read_sql(
        text(f"SELECT unit_id, state, region, district FROM {STAGING_SCHEMA}.{source}"),
        engine,
    ).set_index("unit_id")
    expected = geography.set_index(staged["unit_id"])
    errors = []
    mismatched = [
        unit_id
        for unit_id, row in expected.iterrows()
        if tuple(_none(v) for v in stored.loc[unit_id]) != tuple(_none(v) for v in row)
    ]
    if mismatched:
        errors.append(
            f"Staging geography of {source} not rebuilt for {len(mismatched)} units"
        )
    return len(records), errors


def _none(value):
    return None if value is None or pandas.isna(value) else value


def _discard_unverified_staging(engine: Engine | None, source: str) -> None:
    """Drop a Source's staging after a failed snapshot transform.

    A snapshot load upserts every staging table of its Core kind, so a staging
    table has to mean "the last transform of this Source was verified".
    Otherwise a snapshot that was written but failed verification would reach
    Core through the next successful Source of the same kind. Dropping it
    leaves that Source's Core units retained until a good snapshot arrives.
    """
    if engine is None:
        return
    try:
        _drop_staging_tables(engine, source)
    except Exception:
        log.exception("Could not discard unverified staging for %s", source)


def _record_source_memberships(
    engine: Engine,
    *,
    source: str,
    ingestion_run_id,
) -> None:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                f"SELECT unit_id, reference_id, bad_quality "
                f"FROM {STAGING_SCHEMA}.{source}"
            )
        ).mappings().all()
    statement = text(
        f"DELETE FROM {SERVICE_SCHEMA}.source_memberships WHERE run_id = :run_id"
    )
    with engine.begin() as connection:
        connection.execute(statement, {"run_id": str(ingestion_run_id)})
        if rows:
            connection.execute(
                text(
                    f"INSERT INTO {SERVICE_SCHEMA}.source_memberships "
                    "(run_id, energy_source, unit_key, reference_id, bad_quality) "
                    "VALUES (:run_id, :energy_source, :unit_key, :reference_id, "
                    ":bad_quality)"
                ),
                [
                    {
                        "run_id": str(ingestion_run_id),
                        "energy_source": source,
                        "unit_key": row["unit_id"],
                        "reference_id": row["reference_id"],
                        "bad_quality": row["bad_quality"],
                    }
                    for row in rows
                ],
            )


def _merge_transform_reports(
    sources: tuple[str, ...], reports: list[TransformReport]
) -> TransformReport:
    """Fold per-source transform reports into one aggregate report.

    ``transform_sources(*sources)`` runs each source through the single-source
    path and merges the reports: scalar counters and per-level join_unmapped
    sums across sources, quality_reasons keyed by their exact reason strings,
    raw_table set to a comma-joined list (or None when nothing transformed),
    and errors concatenated per source.  A source whose raw tables or
    boundaries are missing contributes its own clean error, so the merged
    report fails without raising.
    """
    merged = TransformReport(source="all")
    merged.rows_read = sum(r.rows_read for r in reports)
    merged.rows_written = sum(r.rows_written for r in reports)
    merged.synthetic_ids = sum(r.synthetic_ids for r in reports)
    merged.bad_quality = sum(r.bad_quality for r in reports)
    merged.properties_count = sum(r.properties_count for r in reports)
    merged.links_count = sum(r.links_count for r in reports)
    for level in ("state", "region", "district"):
        merged.join_unmapped[level] = sum(r.join_unmapped.get(level, 0) for r in reports)
    for r in reports:
        for reason, count in r.quality_reasons.items():
            merged.quality_reasons[reason] = merged.quality_reasons.get(reason, 0) + count
    for r in reports:
        merged.errors.extend(r.errors)
    raw_tables = [r.raw_table for r in reports if r.raw_table]
    merged.raw_table = ", ".join(raw_tables) if raw_tables else None
    merged.total_time = sum(r.total_time for r in reports)
    if merged.errors:
        for err in merged.errors:
            log.error("Transform failed: %s", err)
    else:
        log.info(
            "Transform passed for %s (%d rows)",
            "all" if sources == SOURCE_NAMES else ", ".join(sources),
            merged.rows_read,
        )
    log.info("Total time: %.3fs", merged.total_time)
    return merged


def _unit_ids(df: pandas.DataFrame, source: str) -> pandas.Series:
    missing = df["reference_id"].isna()
    locations = df["secondary_attributes"].map(json.loads).map(
        lambda value: value.get("location")
    )
    if missing.any() and locations[missing].isna().any():
        raise SourceValidationError(
            "Rows without reference_id require a location"
        )
    return synthetic_unit_ids(
        source=source,
        reference_ids=df["reference_id"],
        locations=locations,
        x=df["x_coordinates"],
        y=df["y_coordinates"],
        geo_accuracy=df["geo_accuracy"],
    )


def _quality_reasons(df: pandas.DataFrame) -> pandas.Series:
    """Return one list of failed-check descriptions per row.

    Checks follow the spec order: capacity, dates, coordinates. A state-null
    row is deliberately not flagged here — "outside location" is a load-stage
    collision (spec v2.2), not a staging bad-quality reason. Capacity is its
    own gate with a null and a non-positive check. A row nested in several
    polygons, nulls, and geometry/coordinate disagreements all resolve to
    boolean masks here; the order of the joined list is stable.
    """
    cap_null = df["installed_capacity"].isna().to_numpy(dtype=bool)
    cap_nonpositive = (df["installed_capacity"] <= 0).to_numpy(dtype=bool)
    dates_bad = (
        df["decommissioning_date"].notna()
        & df["commissioning_date"].notna()
        & (df["decommissioning_date"] <= df["commissioning_date"])
    ).to_numpy(dtype=bool)
    coords_bad = _coordinates_mismatch(df)

    rows: list[list[str]] = []
    for c_null, c_nonpos, dates, coords in zip(
        cap_null, cap_nonpositive, dates_bad, coords_bad
    ):
        failed = []
        if c_null:
            failed.append(QUALITY_CAPACITY_NULL)
        if c_nonpos:
            failed.append(QUALITY_CAPACITY_NONPOSITIVE)
        if dates:
            failed.append(QUALITY_DATES)
        if coords:
            failed.append(QUALITY_COORDS)
        rows.append(failed)
    return pandas.Series(rows, index=df.index, dtype=object)


def _coordinates_mismatch(df: pandas.DataFrame) -> numpy.ndarray:
    """True where a row's x/y coordinates disagree with its geometry.

    Flags rows whose (x_coordinates, y_coordinates) point differs from the
    geometry beyond the coordinate tolerance. Null coordinates or geometry
    produce NaN differences, which compare as False, so this check only fires
    on a provable disagreement. Bio is fully aligned in the source data.
    """
    x = pandas.to_numeric(df["x_coordinates"], errors="coerce").to_numpy(dtype=float)
    y = pandas.to_numeric(df["y_coordinates"], errors="coerce").to_numpy(dtype=float)
    gx = df.geometry.x.to_numpy(dtype=float)
    gy = df.geometry.y.to_numpy(dtype=float)
    return (numpy.abs(gx - x) > COORD_TOLERANCE_DEG) | (
        numpy.abs(gy - y) > COORD_TOLERANCE_DEG
    )


def _reason_histogram(reasons: pandas.Series) -> dict[str, int]:
    """Count units per newline-joined description list (the property value)."""
    hist: dict[str, int] = {}
    for failed in reasons:
        if not failed:
            continue
        value = "\n".join(failed)
        hist[value] = hist.get(value, 0) + 1
    return hist


def _decompose_attributes(
    df: pandas.DataFrame, reasons: pandas.Series
) -> tuple[pandas.DataFrame, pandas.DataFrame]:
    """Split the whitelisted secondary attributes into normalized properties.

    Distinct (name, value) pairs whose name is in DECOMPOSED_PROPERTIES become
    properties rows with a deterministic param_id in (name, value) order; each
    unit links to its pairs through units_properties. Keys outside the
    whitelist are left for the staging `secondary_attributes` json. A bad
    quality row additionally links a 'bad_quality' property holding the
    newline-joined failed-check descriptions.
    """
    unique: set[tuple[str, str]] = set()  # distinct (name, value) pairs across all units
    rows: list[tuple[object, str, str]] = []  # flat (unit_id, name, value) triples
    for unit_id, attributes, failed in zip(
        df["unit_id"], df["secondary_json"], reasons
    ):
        pairs = [
            (str(name), str(value))
            for name, value in attributes.items()
            if name in DECOMPOSED_PROPERTIES
        ]
        if failed:
            pairs.append((BAD_QUALITY_PROPERTY, "\n".join(failed)))
        unique.update(pairs)
        rows.extend((unit_id, name, value) for name, value in pairs)

    ordered = sorted(unique)
    param_id = {pair: i + 1 for i, pair in enumerate(ordered)} # sorted pairs -> deterministic param_id assignment
    props = pandas.DataFrame(
        [(i, name, value) for (name, value), i in param_id.items()],
        columns=["param_id", "name", "value"],
    )
    links = pandas.DataFrame(
        [(unit_id, param_id[(name, value)]) for unit_id, name, value in rows],
        columns=["unit_id", "param_id"],
    )
    return props, links
