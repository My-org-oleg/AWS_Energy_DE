"""Configuration defaults for the Streamlit + PyDeck visualization app (issue #23).

Single home for the T1 tracer's user-facing defaults: the active-units
timescope window, the checked source set, the initial drill level, the CARTO
Light basemap style, and the Germany-overview view state.  Widgets in
`viz.app` seed from these, so a default exists exactly once.
"""

from __future__ import annotations

import base64
import json
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

# The viz package is deliberately standalone — no `etl` import — so this module
# is its single home for `.env`-driven settings (db schema names + connection
# URLs), mirroring `etl.config`'s role for the pipeline.

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Database-schema names the app reads from.  Overridable via env (defaults
# shown); `tests/test_viz_config.py` pins them to the etl originals so the two
# sides can't drift apart.
SERVICE_SCHEMA = os.environ.get("SERVICE_SCHEMA", "service")
CORE_SCHEMA = os.environ.get("CORE_SCHEMA", "core")
MARTS_SCHEMA = os.environ.get("MARTS_SCHEMA", "marts")


def database_url() -> str:
    """The viz read-path connection: ``VIZ_DATABASE_URL`` when set, else
    ``DATABASE_URL`` (one of the two must be set — mirrors viz.data's old
    fallback)."""
    url = os.environ.get("VIZ_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if url is None:
        raise KeyError("DATABASE_URL")
    return url


# Sidebar chooser order, topmost level first.  The drill levels mirror the
# spatial progression of the etl boundary levels (state, region, district)
# under the country overview.
MAP_LEVELS = ("Germany", "States (Bundesländer + EEZ)", "Regions (Regierungsbezirke)", "Districts (Landkreise)")

# The drill level selected on first run.
INITIAL_LEVEL = "Germany"

# Chooser label → boundary level number, the ``level`` column value in
# ``service.boundaries`` (0 = country, 1 = states, 2 = regions,
# 3 = districts).  Every ``MAP_LEVELS`` label has an entry, and the
# header aggregates (issue #25) filter ``service.boundaries`` through it.
LEVEL_INDEX = {
    "Germany": 0,
    "States": 1,
    "Regions": 2,
    "Districts": 3,
}

# Boundary level → the unit-row attribute naming that level's area (issue #26):
# a unit's ``state`` / ``region`` / ``district`` value names its area at
# level 1 / 2 / 3 and matches ``service.boundaries.name`` at that level, so the
# unit fetches can restrict their points to the selected areas without a
# spatial join.  Level 0 (Germany) has no attribute: the whole country is shown.
LEVEL_UNIT_AREA_COLUMN = {
    "Germany": None,
    "States": "state",
    "Regions": "region",
    "Districts": "district",
}

# Per-source sidebar checkboxes in canonical display order — the six loaded
# sources, generators first (bio → wind) then storage.  All checked by default.
DEFAULT_SOURCES = ("bio", "gas", "hydro", "solar", "wind", "storage")

# Whether the choropleth fill is shown by default (the boundary outline is
# always present).
DEFAULT_CHOROPLETH = True

# Active-units timescope default: every unit commissioned up to today counts,
# and nothing is excluded for decommissioning before the epoch default.  The
# upper bound is today, so a unit commissioned after today is not Active yet.
TIMESCOPE_START = date(1900, 1, 1)


def default_timescope() -> tuple[date, date]:
    """The default active-units window: ``1900-01-01`` to today."""
    return (TIMESCOPE_START, date.today())


# CARTO Light (Positron GL) basemap style, reachable without a style token.
LIGHT_MAP_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"


def _raster_style_data_uri(tiles: list[str], attribution: str) -> str:
    style = json.dumps(
        {
            "version": 8,
            "sources": {
                "basemap": {
                    "type": "raster",
                    "tiles": tiles,
                    "tileSize": 256,
                    "attribution": attribution,
                }
            },
            "layers": [{"id": "basemap", "type": "raster", "source": "basemap"}],
        }
    ).encode()
    return f"data:application/json;base64,{base64.b64encode(style).decode()}"


SATELLITE_MAP_STYLE = _raster_style_data_uri(
    ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
    "Tiles © Esri — Source: Esri, Maxar, Earthstar Geographics",
)

TOPOGRAPHIC_MAP_STYLE = _raster_style_data_uri(
    ["https://a.tile.opentopomap.org/{z}/{x}/{y}.png"],
    "© OpenStreetMap contributors, style CC-BY-SA",
)

MAP_STYLES = {
    "Light": LIGHT_MAP_STYLE,
    "Satellite": SATELLITE_MAP_STYLE,
    "Topographic": TOPOGRAPHIC_MAP_STYLE,
}

# Default viewport for the country overview.
GERMANY_CENTER = {"lon": 10.4, "lat": 51.1}
INITIAL_ZOOM = 5.2

# Pixel height of the map window inside the app (streamlit's pydeck_chart
# height); larger than the 500px default so the map dominates the page.
MAP_HEIGHT = 700

# Pixel height of the token basemap shown in standby ("No core tables").  The
# standby ignores MAP_HEIGHT: it is just a notice above a map hint, not the
# working map, so a modest fixed height keeps the warning visible without the
# big window covering the message.
STANDBY_MAP_HEIGHT = 500
