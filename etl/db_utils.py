from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import (
    CORE_SCHEMA,
    RAW_SCHEMA,
    SERVICE_SCHEMA,
    STAGING_SCHEMA,
    STORAGE_COLUMNS,
)


def _table_exists(engine: Engine, table: str, schema: str | None = None) -> bool:
    """True if the named table exists in the given schema.

    The default is resolved at call time rather than in the signature, so a
    test that repoints CORE_SCHEMA is honoured by callers that omit `schema`.
    """
    schema = schema or CORE_SCHEMA
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = :name"
            ),
            {"schema": schema, "name": table},
        ).first()
    return row is not None


def _ensure_schema(engine: Engine, schema: str | None = None) -> None:
    """Create the given schema in the database if it does not exist.

    The default is resolved at call time, as in `_table_exists`.
    """
    schema = schema or RAW_SCHEMA
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()


def _create_log_table(engine: Engine, schema: str = SERVICE_SCHEMA) -> None:
    """Create the loaded_files log and keep its current schema."""
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.loaded_files (
                    filename           TEXT,
                    filesize           BIGINT,
                    modified_at        TIMESTAMPTZ,
                    loaded_at          TIMESTAMPTZ,
                    loaded_to          TEXT,
                    ingestion_run_id   UUID,
                    bucket             TEXT,
                    object_key         TEXT,
                    object_version_id  TEXT,
                    object_etag        TEXT,
                    verified_at        TIMESTAMPTZ
                )
                """
            )
        )
        for column, definition in (
            ("ingestion_run_id", "UUID"),
            ("bucket", "TEXT"),
            ("object_key", "TEXT"),
            ("object_version_id", "TEXT"),
            ("object_etag", "TEXT"),
            ("verified_at", "TIMESTAMPTZ"),
        ):
            conn.execute(
                text(
                    f"ALTER TABLE {schema}.loaded_files "
                    f"ADD COLUMN IF NOT EXISTS {column} {definition}"
                )
            )
        conn.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uq_loaded_files_s3_identity "
                f"ON {schema}.loaded_files "
                "(bucket, object_key, object_version_id) "
                f"WHERE bucket IS NOT NULL AND object_key IS NOT NULL "
                "AND object_version_id IS NOT NULL"
            )
        )


def _drop_staging_tables_in(conn, source: str) -> None:
    for table in (f"{source}_units_properties", f"{source}_properties", source):
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_SCHEMA}.{table} CASCADE"))


def _drop_staging_tables(engine: Engine, source: str) -> None:
    """Drop the source's three staging tables, if they exist."""
    with engine.begin() as conn:
        _drop_staging_tables_in(conn, source)


def _create_staging_tables(engine: Engine, source: str) -> None:
    """Drop and recreate the source's three staging tables with constraints."""
    storage_shape = ""
    if source == "storage":
        storage_shape = (
            f"{STORAGE_COLUMNS[0]}         TEXT,\n"
            f"{STORAGE_COLUMNS[1]}  DOUBLE PRECISION,\n"
        )
    with engine.begin() as conn:
        _drop_staging_tables_in(conn, source)
        conn.execute(
            text(
                f"""
                CREATE TABLE {STAGING_SCHEMA}.{source} (
                    unit_id              TEXT PRIMARY KEY,
                    energy_source        TEXT NOT NULL,
                    {storage_shape}
                    installed_capacity   DOUBLE PRECISION,
                    commissioning_date   DATE,
                    decommissioning_date DATE,
                    geometry             geometry(Point, 4326),
                    geo_accuracy         BIGINT,
                    x_coordinates        DOUBLE PRECISION,
                    y_coordinates        DOUBLE PRECISION,
                    reference_id         TEXT,
                    reference_date       TIMESTAMP,
                    secondary_attributes TEXT,
                    country_iso          TEXT,
                    state                TEXT,
                    region               TEXT,
                    district             TEXT,
                    bad_quality          BOOLEAN NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE {STAGING_SCHEMA}.{source}_properties (
                    param_id BIGINT PRIMARY KEY,
                    name     TEXT NOT NULL,
                    value    TEXT NOT NULL,
                    UNIQUE (name, value)
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE {STAGING_SCHEMA}.{source}_units_properties (
                    unit_id  TEXT NOT NULL REFERENCES {STAGING_SCHEMA}.{source}(unit_id),
                    param_id BIGINT NOT NULL REFERENCES {STAGING_SCHEMA}.{source}_properties(param_id),
                    PRIMARY KEY (unit_id, param_id)
                )
                """
            )
        )


def _create_core_storages(engine: Engine) -> None:
    """Drop and recreate core.storages with the v2.3 storage shape.

    Mirrors `_create_core_generators` for the storage kind: serial `unit_id`
    PK, the storage shape (`storage_type`, `storage_capacity`), a data-
    integrity unique index on `(energy_source, reference_id)` where present,
    a GIST index on the geometry for the close-location collision self-join,
    and the per-kind dimension tables `storage_properties` /
    `storage_units_properties` FK'd to core.storages (ADR 0006).
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.storage_units_properties CASCADE"))
        conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.storage_properties CASCADE"))
        conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.storages CASCADE"))
        conn.execute(
            text(
                f"""
                CREATE TABLE {CORE_SCHEMA}.storages (
                    unit_id              SERIAL PRIMARY KEY,
                    energy_source        TEXT NOT NULL,
                    storage_type         TEXT,
                    storage_capacity     DOUBLE PRECISION,
                    installed_capacity   DOUBLE PRECISION,
                    commissioning_date   DATE,
                    decommissioning_date DATE,
                    geometry             geometry(Point, 4326),
                    longitude            DOUBLE PRECISION,
                    latitude             DOUBLE PRECISION,
                    geo_accuracy         BIGINT,
                    reference_id         TEXT,
                    reference_date       TIMESTAMP,
                    secondary_attributes TEXT,
                    country_iso          TEXT,
                    state                TEXT,
                    region               TEXT,
                    district             TEXT,
                    collision            BOOLEAN NOT NULL DEFAULT false
                )
                """
            )
        )
        conn.execute(
            text(
                f"CREATE UNIQUE INDEX idx_storages_energy_ref "
                f"ON {CORE_SCHEMA}.storages (energy_source, reference_id) "
                f"WHERE reference_id IS NOT NULL"
            )
        )
        conn.execute(
            text(
                f"CREATE INDEX idx_storages_geog "
                f"ON {CORE_SCHEMA}.storages USING GIST ((geometry::geography))"
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE {CORE_SCHEMA}.storage_properties (
                    prop_id BIGSERIAL PRIMARY KEY,
                    name    TEXT NOT NULL,
                    value   TEXT NOT NULL,
                    UNIQUE (name, value)
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE {CORE_SCHEMA}.storage_units_properties (
                    unit_id  INT NOT NULL REFERENCES {CORE_SCHEMA}.storages(unit_id),
                    prop_id BIGINT NOT NULL REFERENCES {CORE_SCHEMA}.storage_properties(prop_id),
                    PRIMARY KEY (unit_id, prop_id)
                )
                """
            )
        )


def _create_core_generators(engine: Engine) -> None:
    """Drop and recreate core.generators with the v2.3 shape.

    Also (re)creates the per-kind dimension tables: `generator_properties`
    holds the normalized (name, value) pairs and `generator_units_properties`
    links them to core.generator serial unit_ids.  Both tables are per
    unit-kind — storages have their own `storage_properties` /
    `storage_units_properties` FK'd to core.storages (ADR 0006) — so
    `generator_units_properties.unit_id` carries a real FK instead
    of the FK-less shared `units_properties` that could not tell a generator
    id from a storage id.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.generator_units_properties CASCADE"))
        conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.generator_properties CASCADE"))
        conn.execute(text(f"DROP TABLE IF EXISTS {CORE_SCHEMA}.generators CASCADE"))
        conn.execute(
            text(
                f"""
                CREATE TABLE {CORE_SCHEMA}.generators (
                    unit_id              SERIAL PRIMARY KEY,
                    energy_source        TEXT NOT NULL,
                    installed_capacity   DOUBLE PRECISION,
                    commissioning_date   DATE,
                    decommissioning_date DATE,
                    geometry             geometry(Point, 4326),
                    longitude            DOUBLE PRECISION,
                    latitude             DOUBLE PRECISION,
                    geo_accuracy         BIGINT,
                    reference_id         TEXT,
                    reference_date       TIMESTAMP,
                    secondary_attributes TEXT,
                    country_iso          TEXT,
                    state                TEXT,
                    region               TEXT,
                    district             TEXT,
                    collision            BOOLEAN NOT NULL DEFAULT false
                )
                """
            )
        )
        # Data-integrity unique index on (energy_source, reference_id) where
        # reference_id is present — prevents silent duplicates while allowing
        # synthetic rows (reference_id null) through.
        conn.execute(
            text(
                f"CREATE UNIQUE INDEX idx_generators_energy_ref "
                f"ON {CORE_SCHEMA}.generators (energy_source, reference_id) "
                f"WHERE reference_id IS NOT NULL"
            )
        )
        # Geography expression index so the collision self-join
        # (ST_DWithin on ::geography) can use an index.
        conn.execute(
            text(
                f"CREATE INDEX idx_generators_geog "
                f"ON {CORE_SCHEMA}.generators USING GIST ((geometry::geography))"
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE {CORE_SCHEMA}.generator_properties (
                    prop_id BIGSERIAL PRIMARY KEY,
                    name    TEXT NOT NULL,
                    value   TEXT NOT NULL,
                    UNIQUE (name, value)
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE {CORE_SCHEMA}.generator_units_properties (
                    unit_id  INT NOT NULL REFERENCES {CORE_SCHEMA}.generators(unit_id),
                    prop_id BIGINT NOT NULL REFERENCES {CORE_SCHEMA}.generator_properties(prop_id),
                    PRIMARY KEY (unit_id, prop_id)
                )
                """
            )
        )
