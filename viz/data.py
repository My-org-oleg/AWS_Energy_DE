"""Read-only data access for the Streamlit viz app (issues #23, render-opt).

The app queries PostGIS directly.  Under the seeded stack it connects through
the read-only `viz_reader` role via `VIZ_DATABASE_URL`, falling back to the
pipeline `DATABASE_URL` on the dev host.  `missing_core_tables` is the
standby-detection seam: with an unreachable database an engine connect raises
and the caller treats it like missing tables.

The render-opt redesign (docs/Viz_optimazation.md) moves the unit fetches to
pandas and caps the query count:

- `units_query` / `fetch_units` fetch every checked active unit in **at most
  two queries** — one against `core.generators` with
  `energy_source = ANY(:sources)` for all generator sources, one against
  `core.storages` (whose `energy_source` is the single ``"storage"`` value)
  for the storage category.  The projection broadcasts the active-level area
  attribute as ``name`` (`area_column AS name`) so the same frame feeds the
  unit layers, the per-area choropleth fill and the header totals.
- `boundaries_query` / `fetch_boundaries` return **every** area at the level
  (name, km² `area`, pre-simplified GeoJSON geometry stored by the pipeline)
  in one query — the choropleth outlines all areas and fills only the
  displayed selection, and the header's scope figures (count, km² sum, single
  name) derive from the same rows.
- The former spatial-join per-area fill (`ST_Intersects`), the header-metrics
  union and the separate areas/count queries are gone: the fill is the name
  groupby in `viz.choropleth.area_fills`, the header totals the frame's
  own sum/count, and the scope figures `viz.header.areas_summary`.

`AREA_NAME_ALIAS` (`name`) is the join key between units and boundaries at
every level.  A unit whose area attribute doesn't match a boundary name (or
is NULL — offshore units) renders and counts in the header but fills no
polygon, which reconciles the choropleth with the header by construction;
the old spatial join silently folded such units into the polygon their
(possibly imprecise) geometry fell in.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from viz.config import CORE_SCHEMA, SERVICE_SCHEMA, database_url

# The core tables the map renders; absent tables mean the app shows the
# "No core tables" standby map instead of a failed ``FROM core.<table>``
# (a dev/test run drops core while the app may stay open).
CORE_VIS_TABLES = ("generators", "storages")

# The single value `core.storages.energy_source` carries; generators carry
# the five loaded source labels instead.
STORAGE_SOURCE = "storage"

# The single active-units predicate (issue #9): commissioned inside the
# selected interval — ``BETWEEN :from AND :to``, inclusive on both ends — that
# is still running at ``:to`` (not decommissioned before ``:to``).  Used
# verbatim by the unit fetch, so map and header always resolve the same set.
ACTIVE_UNIT_PREDICATE = (
    "commissioning_date BETWEEN :from AND :to "
    "AND (decommissioning_date IS NULL OR decommissioning_date >= :to)"
)

# Columns every fetched unit row needs for the unit layer and its hover
# card.  The location address reads state · district (region left the
# card in #24) and storages add storage_capacity (kWh).  ``unit_id`` is
# dropped: it only keyed the retired spatial fill's COUNT.
UNIT_COLUMNS = (
    "energy_source",
    "installed_capacity",
    "commissioning_date",
    "decommissioning_date",
    "longitude",
    "latitude",
    "state",
    "district",
)
UNIT_COLUMNS_SQL = ", ".join(UNIT_COLUMNS)
STORAGE_COLUMNS = UNIT_COLUMNS + ("storage_capacity",)
STORAGE_COLUMNS_SQL = ", ".join(STORAGE_COLUMNS)

# The column every unit row carries for the choropleth fill and header
# drill-down: the unit attribute naming its area at the active level, aliased
# ``name`` (e.g. ``state`` at the States level).  ``name`` joins units to the
# ``service.boundaries.name`` rows; dropped when the level (Germany) has none.
AREA_NAME_ALIAS = "name"


def get_viz_engine() -> Engine:
    """Engine for the viz read path — the connection URL resolved by
    `viz.config.database_url` (VIZ_DATABASE_URL with a DATABASE_URL fallback)."""
    return create_engine(database_url())


def missing_core_tables(
    engine: Engine | None = None, tables: tuple[str, ...] = CORE_VIS_TABLES
) -> list[str]:
    """Core tables in ``tables`` that are absent from the ``core`` schema."""
    engine = engine or get_viz_engine()
    with engine.connect() as conn:
        present = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :schema"
                ),
                {"schema": CORE_SCHEMA},
            )
        }
    return [table for table in tables if table not in present]


def _unit_table(source: str) -> str:
    """Core table carrying ``source``: ``storages`` holds the single storage
    category, ``generators`` every generator source."""
    return "storages" if source == STORAGE_SOURCE else "generators"


def _needed_tables(sources: tuple[str, ...]) -> tuple[str, ...]:
    """Distinct core tables a source set touches, in render order.

    ``energy_source`` filters collapse per table — all generator sources read
    ``core.generators`` once (``ANY(:sources)``), the storage category reads
    ``core.storages`` once — so any checked set resolves in at most two
    queries (render-opt).
    """
    tables: list[str] = []
    if any(source != STORAGE_SOURCE for source in sources):
        tables.append("generators")
    if STORAGE_SOURCE in sources:
        tables.append("storages")
    return tuple(tables)


def _projection(columns: str, area_column: str | None) -> str:
    """``columns`` plus ``{area_column} AS name`` when the level has an area
    attribute; generators/storages projection constants otherwise."""
    if area_column is None:
        return columns
    return f"{columns}, {area_column} AS {AREA_NAME_ALIAS}"


def units_query(
    table: str,
    columns: str,
    *,
    sources: tuple[str, ...],
    active_from: date,
    active_to: date,
    area_column: str | None = None,
    area_names: tuple[str, ...] = (),
) -> tuple[str, dict]:
    """SQL + bound params for the active-unit fetch on ``core.<table>``.

    ``table`` is ``"generators"`` or ``"storages"`` and ``columns`` its
    projection; both are module constants, never user input.  One query covers
    every checked ``sources`` entry on the table via
    ``energy_source = ANY(:sources)`` (at most two tables total, render-opt),
    under the issue #9 timescope predicate.

    ``area_column`` (a `LEVEL_UNIT_AREA_COLUMN` constant) broadcasts the
    unit's area attribute as ``name`` — the join key to `service.boundaries`
    — and, with ``area_names`` given, narrows the rows to the selected areas
    (``ANY(:area_names)``).  Without ``area_column`` (country level) every
    active unit qualifies and no ``name`` column is projected.
    """
    sql = (
        f"SELECT {_projection(columns, area_column)} FROM {CORE_SCHEMA}.{table} "
        "WHERE energy_source = ANY(:sources) "
        f"AND {ACTIVE_UNIT_PREDICATE}"
    )
    params: dict = {"sources": list(sources), "from": active_from, "to": active_to}
    if area_column and area_names:
        sql += f" AND {area_column} = ANY(:area_names)"
        params["area_names"] = list(area_names)
    return sql, params


def run_query(engine: Engine, sql: str, params: dict) -> list[dict[str, Any]]:
    """Execute ``sql`` with bound ``params``; return rows as dicts."""
    with engine.connect() as conn:
        # ``Row`` iterates as a value sequence in SQLAlchemy 2.x, so map via
        # the mapping view rather than ``dict(row)``.
        return [dict(row._mapping) for row in conn.execute(text(sql), params)]


def run_frame(engine: Engine, sql: str, params: dict) -> pd.DataFrame:
    """Execute ``sql`` with bound ``params``; return rows as a DataFrame."""
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def _empty_units(area_column: str | None) -> pd.DataFrame:
    """Empty unit frame with the projected columns, for the no-sources case."""
    columns = list(UNIT_COLUMNS)
    if area_column is not None:
        columns.append(AREA_NAME_ALIAS)
    return pd.DataFrame(columns=columns)


def fetch_units(
    engine: Engine,
    *,
    active_from: date,
    active_to: date,
    sources: tuple[str, ...],
    area_column: str | None = None,
    area_names: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Active unit rows for the checked ``sources`` as one DataFrame.

    One query per distinct core table (render-opt): `_needed_tables` collapses
    the checked sources into generators and/or storages, so any source set
    costs at most two queries instead of one per source.  Concatenated with
    ``ignore_index``; generators rows lack ``storage_capacity`` (storages
    always have it), and ``area_column AS name`` appears when the level has
    one.  ``area_column`` / ``area_names`` narrow every fetch to the selected
    areas (issue #26); None/empty leaves the points unfiltered.

    With nothing checked an empty frame returns and no query runs — the same
    contract the old per-source fetch satisfied for an empty checkbox set.
    """
    if not sources:
        return _empty_units(area_column)
    frames = [
        run_frame(
            engine,
            *units_query(
                table,
                STORAGE_COLUMNS_SQL if table == "storages" else UNIT_COLUMNS_SQL,
                sources=sources,
                active_from=active_from,
                active_to=active_to,
                area_column=area_column,
                area_names=area_names,
            ),
        )
        for table in _needed_tables(sources)
    ]
    return pd.concat(frames, ignore_index=True)


def _scalar_or_none(value: Any) -> Any:
    """``None`` for missing (NaN/NaT/None) scalars, the value otherwise."""
    if value is None or pd.isna(value):
        return None
    return value


def _serializable_units(units: pd.DataFrame) -> pd.DataFrame:
    """JSON-ready copy of ``units``: dates as ISO strings, missing → ``None``.

    PyDeck's JSON serialization passes raw values to ``json.dumps``, which
    rejects ``datetime64``/``NaT`` and emits bare ``NaN`` tokens for floats — so
    every projected value is normalized before the frames reach the unit
    layers.
    """
    out = units.copy()
    for column in out.columns:
        series = out[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            out[column] = series.map(
                lambda v: None if _scalar_or_none(v) is None else v.strftime("%Y-%m-%d")
            )
        else:
            out[column] = series.map(_scalar_or_none)
    return out


def unit_records(units: pd.DataFrame) -> list[dict[str, Any]]:
    """JSON-ready unit dicts for one unit layer (dates ISO, NaN → null)."""
    return _serializable_units(units).to_dict("records")


def boundaries_query(level: int) -> tuple[str, dict]:
    """SQL + bound params for the choropleth boundary geometry.

    One row per area at ``level`` — every area, since the layer outlines the
    whole level and fills only the displayed selection (render-opt/drop family)
    — with its name, stored ``area`` (km², for the header scope figures) and
    its pre-simplified WGS-84 GeoJSON text.  The geometry is simplified and
    geo-encoded **once** by the pipeline when `service.boundaries` is loaded
    (`etl.extract.extract_boundaries`, issue #31), so a rerun just reads the
    stored ``geojson`` text — no per-rerun `ST_SimplifyPreserveTopology` +
    `ST_AsGeoJSON`.  ``name`` is the join key the choropleth and header seams
    match fill rows against.
    """
    sql = (
        "SELECT name, area, geojson "
        f"FROM {SERVICE_SCHEMA}.boundaries WHERE level = :level ORDER BY name"
    )
    return sql, {"level": level}


def fetch_boundaries(engine: Engine, *, level: int) -> list[dict[str, Any]]:
    """Boundary rows (name, km² area, pre-simplified GeoJSON text) for a level."""
    sql, params = boundaries_query(level)
    return run_query(engine, sql, params)


def boundary_names_query(level: int) -> tuple[str, dict]:
    """SQL + bound params for the area-multiselect options at ``level``.

    Returns the area names of ``service.boundaries`` ordered alphabetically,
    so the multiselect options (and the fit-view scope signature) stay stable
    across reruns regardless of storage order.
    """
    sql = f"SELECT name FROM {SERVICE_SCHEMA}.boundaries WHERE level = :level ORDER BY name"
    return sql, {"level": level}


def fetch_area_names(engine: Engine, level: int) -> list[str]:
    """Area names at ``level`` for the multiselect, in stable order."""
    sql, params = boundary_names_query(level)
    return [row["name"] for row in run_query(engine, sql, params)]