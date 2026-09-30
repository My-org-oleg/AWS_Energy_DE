from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import BOUNDARY_SIMPLIFY_TOLERANCE, RAW_SCHEMA, SERVICE_SCHEMA


def _raw_table_versions(engine: Engine, source: str) -> list[tuple[str, int, str]]:
    """List raw.<source>_<YYYYMMDD>_<n> tables as (day, counter, name), sorted."""
    pattern = re.compile(rf"^{re.escape(source)}_(\d{{8}})_(\d+)$")
    tables: list[tuple[str, int, str]] = []
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema"
            ),
            {"schema": RAW_SCHEMA},
        ).fetchall()
    for name in (row[0] for row in rows):
        match = pattern.match(name)
        if match:
            tables.append((match.group(1), int(match.group(2)), name))
    tables.sort()
    return tables


def _next_table_version(engine: Engine, source: str, day: date) -> str:
    """Compute the next versioned table name for a source on a given day.

    Versions follow raw.<source>_<YYYYMMDD>_<n> with a per-source counter that
    resets daily: the largest existing counter for the day is incremented,
    starting at 1 when none exists.
    """
    counters = [
        n
        for (d, n, _name) in _raw_table_versions(engine, source)
        if d == f"{day:%Y%m%d}"
    ]
    return f"{source}_{day:%Y%m%d}_{max(counters, default=0) + 1}"


def _latest_table_version(engine: Engine, source: str) -> str | None:
    """Return the most recent raw.<source>_<YYYYMMDD>_<n> table name, or None."""
    versions = _raw_table_versions(engine, source)
    return versions[-1][2] if versions else None


def _loaded_table_for_signature(
    engine: Engine, signature: tuple[str, int, float]
) -> str | None:
    """Return the raw table logged for a load signature, or None if unseen."""
    filename, filesize, modified_at = signature
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"SELECT loaded_to FROM {SERVICE_SCHEMA}.loaded_files "
                "WHERE filename = :filename AND filesize = :filesize "
                "AND modified_at = to_timestamp(:modified_at) LIMIT 1"
            ),
            {"filename": filename, "filesize": filesize, "modified_at": modified_at},
        ).first()
    return str(row[0]) if row else None


def _log_load(
    engine: Engine, filename: str, filesize: int, modified_at: float, loaded_to: str
) -> None:
    """Append one row to loaded_files recording a completed load action."""
    with engine.connect() as conn:
        conn.execute(
            text(
                f"INSERT INTO {SERVICE_SCHEMA}.loaded_files "
                "(filename, filesize, modified_at, loaded_at, loaded_to) "
                "VALUES (:filename, :filesize, to_timestamp(:modified_at), now(), :loaded_to)"
            ),
            {
                "filename": filename,
                "filesize": filesize,
                "modified_at": modified_at,
                "loaded_to": loaded_to,
            },
        )
        conn.commit()


def _read_manifest(manifest: Path) -> list[str]:
    """Read a manifest file into a list of file names, one per line.

    Blank lines and '#' comments are dropped; each line is stripped exactly
    once.
    """
    names = [line.strip() for line in manifest.read_text().splitlines()]
    return [name for name in names if name and not name.startswith("#")]


def _compute_boundary_areas(engine: Engine) -> None:
    """Fill area (km²) for every boundary row via PostGIS geodesic area."""
    with engine.connect() as conn:
        conn.execute(
            text(
                f"UPDATE {SERVICE_SCHEMA}.boundaries "
                f"SET area = ST_Area(geometry::geography) / 1e6"
            )
        )
        conn.commit()


def _refresh_boundary_geojson(engine: Engine) -> None:
    """Materialize service.boundaries.geojson idempotently (issue #31).

    Backfills each boundary row's pre-simplified ``ST_AsGeoJSON`` text — the
    viz app then reads stored geometry instead of re-simplifying per rerun.
    Runs on every `extract_boundaries` call (even when the load is skipped),
    so a database seeded before the column existed self-upgrades on any
    `python -m etl boundaries` run without a force reload.  Both statements
    are cheap and idempotent on an already-populated table.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                f"ALTER TABLE {SERVICE_SCHEMA}.boundaries "
                "ADD COLUMN IF NOT EXISTS geojson TEXT"
            )
        )
        conn.execute(
            text(
                f"UPDATE {SERVICE_SCHEMA}.boundaries "
                "SET geojson = ST_AsGeoJSON("
                "ST_SimplifyPreserveTopology(geometry, "
                f"{BOUNDARY_SIMPLIFY_TOLERANCE}))"
            )
        )
