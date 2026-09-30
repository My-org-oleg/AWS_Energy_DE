"""Generate the small, hermetic test fixtures the suite loads instead of data/.

The integration suite used to depend on the dev database already containing
`raw.*` populated from the real 81,655-row GPKGs, plus 15.8 MB of private
boundary files. That made a fresh checkout fail in ways that looked like
product bugs, and made it possible for a test run to clobber real state.

This script writes the stand-ins the suite actually needs:

* `tests/fixtures/boundaries/` — four GPKGs (one per boundary level) plus the
  manifest `extract_boundaries` consumes. Levels are carried in the data, and
  the layer names are deliberately offset from the filenames exactly as the
  real files are (`germany_states.gpkg` holds a layer named
  `germany_regions`), so extraction has to keep reading the level from the
  data rather than trusting a name. Level 1 carries a `North Sea` polygon
  because the load stage's onshore-in-sea collision rule keys on the state
  resolving to one of `SEA_REGIONS`.
* `tests/fixtures/sources/` — one GPKG per canonical Energy source, carrying
  the real column shapes *and the real column order*: gas is keyed on
  `gas_production_capacity` and has no `installed_capacity` at all, storage
  carries both `storage_type` and `storage_capacity`, solar keeps a couple of
  null `reference_id`/`reference_date` rows so the synthetic-identity path
  stays exercised, solar and wind carry `location`, and solar carries both the
  `Solar Energy` and `Solar energy` label variants found in the real file.

Property values are drawn from the real datasets' vocabularies (e.g. hydro's
`Run-of-river hydropower`/`Storage hydropower`, solar's `Photovoltaic` and
`Ground-mounted`, wind's `Onshore`/`Offshore`, storage's `Battery`/
`Hydrogen storage`/`Pumped storage`) so the property tables are built from the
same value space the product will meet. Where the real data is null the
fixture is null too — notably `decommissioning_date`, which is null on
~93% of real rows and never an empty string.

Geometry is deliberately synthetic: the transform's spatial join only needs
containment, so a handful of nested rectangles over two small areas plus one
sea area is enough and keeps the fixtures tiny and regenerable without the
private data. Points sit well inside their polygons, so `state`/`region`/
`district` resolve deterministically. Names match the real German
administrative units the rest of the suite already asserts on (e.g. "Bayern").

Deterministic: no randomness, no clock, no dependency on data/. Re-run after
editing anything here and commit the result — the files are byte-stable, so
regenerating without editing shows no diff.

Usage:  python scripts/make_test_fixtures.py
"""

import sqlite3
import sys
from pathlib import Path

import geopandas as gpd
from shapely.geometry import MultiPolygon, Point, box

REPO_ROOT = Path(__file__).resolve().parent.parent
BOUNDARY_DIR = REPO_ROOT / "tests" / "fixtures" / "boundaries"
SOURCE_DIR = REPO_ROOT / "tests" / "fixtures" / "sources"

# GeoPandas stamps wall-clock time into gpkg_contents.last_change, which would
# make every regeneration a spurious diff on a committed fixture. Pinning it
# keeps the files byte-reproducible, so "regenerate and commit" shows no change
# unless the data itself changed.
FIXED_LAST_CHANGE = "2026-01-01T00:00:00.000Z"

# Nested boxes (minx, miny, maxx, maxy) in EPSG:4326. The two inland areas are
# far enough apart that a fixture point can only ever resolve to one
# state/region/district chain; the sea area is what the onshore-in-sea
# collision rule needs.
AREAS = {
    "Baden-Württemberg": (8.0, 47.5, 10.0, 49.0),
    "Bayern": (10.0, 47.0, 13.0, 49.5),
    "North Sea": (6.5, 53.5, 8.0, 54.5),
}
REGIONS = {
    "Stuttgart": (9.0, 48.6, 9.4, 48.9),
    "München": (11.4, 48.0, 11.8, 48.3),
}
DISTRICTS = {
    "Stuttgart (Stadt)": (9.1, 48.7, 9.3, 48.85),
    "München (Stadt)": (11.5, 48.1, 11.7, 48.25),
}

# The level-0 outline has to enclose the sea area, since a point in the sea
# still resolves to a country_iso.
COUNTRY = (5.0, 47.0, 15.0, 55.0)

# Two well-separated fixture points per area, so a spatial-join regression
# cannot pass by accident and every source spans both states.
STUTTGART_POINTS = ((9.18, 48.78), (9.22, 48.80))
MUENCHEN_POINTS = ((11.58, 48.14), (11.62, 48.16))

# Inside the level-0 country outline but outside every level-1 polygon, so the
# join yields country_iso with a null state/region/district. The real data has
# such rows, and they are what puts a unit in the marts' OUTSIDE_STATE bucket
# and flags it as an `outside location` collision — while deliberately *not*
# making it bad quality.
OUTSIDE = (11.5, 49.6)

# Inside the `North Sea` level-1 polygon: the state resolves to a SEA_REGIONS
# name, which is what flags an `onshore unit in the sea` collision for the
# onshore-only sources (bio/gas/hydro/solar). Distinct from OUTSIDE, so the two
# collision rules stay independently covered.
SEA = (7.2, 54.0)

# A pair of units ~4 m apart, both flagged geo_accuracy=1, which is what the
# `close location` collision rule keys on (both sides geo_accuracy=1 and within
# COLLISION_CLOSE_DISTANCE_M, i.e. 10 m). Both rows are good quality.
CLOSE_PAIR = ((9.20, 48.78), (9.20004, 48.78003))

# The exact labels the real files carry, including solar's case variant.
ENERGY_SOURCE_LABELS = {
    "bio": "Bioenergy",
    "gas": "Gas",
    "hydro": "Hydropower",
    "solar": "Solar Energy",
    "wind": "Wind Energy",
    "storage": "Energy Storage",
}

# The real reference_source string, so the property tables see the same shape.
REFERENCE_SOURCE = (
    "BNetzA, https://www.marktstammdatenregister.de, "
    "dl-de/by-2-0, https://www.govdata.de/dl-de/by-2-0"
)
# Every real reference_date is a full timestamp, never a bare date, and
# `extract` preserves the time — so a date-only value would exercise a shape the
# data never has. This is a real observed value.
REFERENCE_DATE = "2025-10-01 06:53:27"

# The real data's only geo_accuracy values. The close-location rule requires
# geo_accuracy=1 on *both* sides, so only the close pair uses 1.
ACCURATE = 1
COARSE = 2

# Which real Source files carry a `note` column at all. gas, hydro and storage
# have none. bio has one that is null on every row, solar and wind one that is
# null on ~98% of rows. The column is kept wherever the real file has it, since
# column shape is part of what these fixtures stand in for; the values stay
# null, because a real note is free text and an invented one would put a
# property the data never produces into the fixtures. For bio the column is
# therefore inert: the transform drops all-null values, so `note` never reaches
# the bio property dimension — which is exactly what happens with the real
# file, and what `EXPECTED_PROPERTIES["bio"]` in the transform tests records.
SOURCES_WITH_NOTE = ("bio", "solar", "wind")


def _write(frame: gpd.GeoDataFrame, path: Path, layer: str) -> None:
    """Write a fixture GPKG, reproducibly.

    Two things make re-running this script produce identical bytes:

    * `gpkg_contents.last_change` is wall-clock by default, so it is pinned.
    * SQLite bumps a file change-counter on every write, so writing over an
      existing GPKG yields different bytes than writing a fresh one. Every
      generation therefore goes to a clean temporary file and is moved into
      place, so the result never depends on what was already on disk.

    The temporary name keeps the `.gpkg` extension GDAL needs to pick the
    driver.
    """
    scratch = path.with_name(f"{path.stem}.scratch.gpkg")
    scratch.unlink(missing_ok=True)
    try:
        frame.to_file(scratch, driver="GPKG", layer=layer)
        connection = sqlite3.connect(scratch)
        try:
            connection.execute(
                "UPDATE gpkg_contents SET last_change = ?", (FIXED_LAST_CHANGE,)
            )
            connection.commit()
        finally:
            connection.close()
        scratch.replace(path)
    finally:
        # Never leave a stray `*.scratch.gpkg` in a committed fixture directory:
        # it would be picked up as a fixture by the next run or by a test.
        scratch.unlink(missing_ok=True)


def _label(path: Path) -> str:
    """Repo-relative when the fixture is inside the repo, absolute otherwise.

    Output directories are redirectable (the test suite regenerates into a temp
    dir to check the committed bytes are up to date), so the human-facing
    progress lines must not assume they are writing under the repo.
    """
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _polygons(bounds: dict) -> gpd.GeoDataFrame:
    """One polygon per named area, in the order given."""
    return gpd.GeoDataFrame(
        {"name": list(bounds), "geometry": [box(*b) for b in bounds.values()]},
        crs="EPSG:4326",
    )


def _with_level(
    frame: gpd.GeoDataFrame, level: int, **extra: tuple
) -> gpd.GeoDataFrame:
    """Add the iso/level (and any extra constant columns) a boundary level needs."""
    out = frame.copy()
    out["iso"] = "DEU"
    out["level"] = level
    for column, values in extra.items():
        out[column] = list(values)
    return out[["name", "iso", *extra, "level", "geometry"]]


def _boundary_specs() -> list:
    """(filename, layer, frame) per boundary level.

    Each frame carries its own `level` column, so the level is stated exactly
    once. The layer names are intentionally not the filename stems, matching how
    the real boundary files are organised.
    """
    germany = box(*COUNTRY)
    return [
        (
            "germany_boundary.gpkg",
            "germany_boundary",
            gpd.GeoDataFrame(
                {
                    "state": ["Federal Republic of Germany"],
                    "iso": ["DEU"],
                    "name": ["Germany"],
                    "level": [0],
                    "geometry": [MultiPolygon([germany])],
                },
                crs="EPSG:4326",
            ),
        ),
        ("germany_states.gpkg", "germany_regions", _with_level(_polygons(AREAS), 1)),
        ("germany_regions.gpkg", "germany_districts", _with_level(_polygons(REGIONS), 2)),
        (
            "germany_districts.gpkg",
            "germany_districts",
            _with_level(
                _polygons(DISTRICTS), 3, ags=("08111", "09162")
            ),
        ),
    ]


def write_boundaries() -> None:
    BOUNDARY_DIR.mkdir(parents=True, exist_ok=True)

    specs = _boundary_specs()
    for filename, layer, frame in specs:
        path = BOUNDARY_DIR / filename
        _write(frame, path, layer)
        print(f"wrote {_label(path)} layer={layer} n={len(frame)}")

    manifest = BOUNDARY_DIR / "manifest.txt"
    manifest.write_text(
        "".join(f"{filename}\n" for filename, _, _ in specs), encoding="utf-8"
    )
    print(f"wrote {_label(manifest)} ({len(specs)} files)")


def _unit(
    ref: str,
    capacity: float | None,
    x: float,
    y: float,
    properties: dict,
    note: str | None = None,
    reference_date: str | None = REFERENCE_DATE,
    geo_accuracy: int = COARSE,
    energy_source: str | None = None,
    geometry: Point | None = None,
    commissioning_date: str = "2015-06-01",
    capacity_column: str = "installed_capacity",
) -> dict:
    """One generator-shaped row, with the columns in the real files' order.

    `properties` holds the source-specific columns, which sit between
    `decommissioning_date` and `x_coordinates` in every real file. `note` is
    inserted after `geo_accuracy` when given, because that is where the real
    files carry it — and omitted entirely for the sources that have no `note`
    column at all. `capacity_column` exists because gas is the one source that
    has no `installed_capacity` column at all.
    """
    row = {
        "energy_source": energy_source,
        capacity_column: capacity,
        "commissioning_date": commissioning_date,
        # Null on ~93% of real rows, so null here rather than an empty string:
        # the real data never exercises an empty-string→NULL date coercion.
        "decommissioning_date": None,
        **properties,
        "x_coordinates": x,
        "y_coordinates": y,
        "geo_accuracy": geo_accuracy,
    }
    if note is not None:
        row["note"] = note
    row["reference_source"] = REFERENCE_SOURCE
    row["reference_id"] = ref
    row["reference_date"] = reference_date
    row["_geometry"] = Point(x, y) if geometry is None else geometry
    return row


def _gas_unit(ref: str, capacity: float, x: float, y: float, technology: str) -> dict:
    """A gas row: `gas_production_capacity` and no `installed_capacity`."""
    return _unit(
        ref,
        capacity,
        x,
        y,
        {"technology": technology},
        commissioning_date="2018-01-15",
        capacity_column="gas_production_capacity",
    )


def _insert_note(row: dict) -> dict:
    """Return *row* with a `note` key present just before `reference_source`.

    Real solar/wind rows carry `note` in that slot whether or not the value is
    null, so every row of those sources needs the key; without this, only the
    rows that happen to have a note would gain the column and the frame would
    come out ragged.
    """
    out = {}
    for key, value in row.items():
        if key == "note":
            continue
        if key == "reference_source":
            out["note"] = row.get("note")
        out[key] = value
    return out


def _frame(rows: list) -> gpd.GeoDataFrame:
    """Build a Source fixture frame from rows that each carry their geometry."""
    columns: dict[str, list] = {}
    for row in rows:
        for key, value in row.items():
            if key.startswith("_"):
                continue
            columns.setdefault(key, []).append(value)
    ragged = [name for name, values in columns.items() if len(values) != len(rows)]
    if ragged:
        raise ValueError(f"rows disagree on columns: {sorted(ragged)}")
    return gpd.GeoDataFrame(
        columns, geometry=[row["_geometry"] for row in rows], crs="EPSG:4326"
    )


def source_frames() -> dict:
    """The six Source fixtures, keyed by canonical source name.

    Each source carries the coverage the real dataset gives the suite, at
    fixture scale: a null-reference-id row for the synthetic identity path, a
    state-null row for the OUTSIDE_STATE bucket, bad-quality rows for the
    quality gate, a sea row for the onshore-in-sea collision, and a genuinely
    sub-10 m pair for the close-location collision.
    """
    st, st2 = STUTTGART_POINTS
    mu, mu2 = MUENCHEN_POINTS
    pair_a, pair_b = CLOSE_PAIR

    # The real bio file carries a `note` column that is null on every row, so
    # `note` never reaches bio's property table; the fixture omits it rather
    # than writing an empty string (which would survive the null filter and
    # invent a property the real data does not produce). `biogas_unit` is
    # numeric and `chp_unit` is a KWK plant id in the real file, both partly
    # null; neither is promoted to the property table (issue #12, out of scope),
    # so both land in secondary_attributes.
    bio_rows = [
        _unit(
            "bio-1", 750.0, *st,
            {
                "biomass_type": "Gaseous biofuel",
                "fuel_type": "Biomethane (bio natural gas)",
                "technology": "Fuel cell",
                "biogas_unit": 1.0,
                "chp_unit": "KWK900009222840",
            },
        ),
        _unit(
            "bio-2", 1200.0, *st2,
            {
                "biomass_type": "Solid biofuel",
                "fuel_type": "Bark and landscape wood",
                "technology": "Condensation machine with extraction",
                "biogas_unit": None,
                "chp_unit": None,
            },
        ),
        _unit(
            "bio-3", 500.0, *mu,
            {
                "biomass_type": "Liquid biofuel",
                "fuel_type": "Biodiesel",
                "technology": "Counterpressure machine with removal",
                "biogas_unit": 10.0,
                "chp_unit": "KWK900014055815",
            },
        ),
    ]

    # gas is the one source with no installed_capacity column at all: the raw
    # layer renames gas_production_capacity onto installed_capacity. Its
    # `technology` values are the real producer technologies, not the
    # gas-turbine/CHP names a generator file would use.
    gas_rows = [
        _gas_unit("gas-1", 4500.0, *st, "Extraction of fossil natural gas"),
        _gas_unit("gas-2", 1200.0, *mu, "Power-to-gas (methane)"),
    ]

    hydro_rows = [
        _unit(
            "hydro-1", 15000.0, *st,
            {"hydro_type": "Run-of-river hydropower", "inflow_type": "Diversion"},
        ),
        _unit(
            "hydro-2", 2200.0, *st2,
            {"hydro_type": "Storage hydropower", "inflow_type": "Run-of-river"},
        ),
        # The real file has nulls in both columns (2 and 1,134 respectively), so
        # one row leaves each null to keep the property null-filter exercised.
        _unit(
            "hydro-3", 8000.0, *mu,
            {"hydro_type": "Storage hydropower", "inflow_type": None},
        ),
        # state resolves to a SEA_REGIONS name -> onshore-in-sea collision.
        _unit(
            "hydro-4", 950.0, *SEA,
            {"hydro_type": "Wastewater hydropower", "inflow_type": "Residual water"},
        ),
        # state-null: inside the country outline, outside every state.
        _unit(
            "hydro-5", 700.0, *OUTSIDE,
            {"hydro_type": "Wastewater hydropower", "inflow_type": "Residual water"},
        ),
    ]

    # Two of the solar rows carry no reference_id/reference_date, which is what
    # routes them through the synthetic-identity path. Two more are bad quality
    # on purpose — a null installed_capacity (the only bad-capacity reason the
    # real solar file ever produces: 12 nulls, 0 non-positive) and coordinates
    # that disagree with the geometry — so the quality gate is exercised, and
    # one more is state-null. Only the state-null row sits outside the states,
    # so solar's bad-quality and state-null counts stay independent.
    solar_rows = [
        _unit(
            "solar-1", 4500.0, *st,
            {
                "solar_type": "Photovoltaic",
                "area_id": "01",
                "alignment": "South",
                "inclination": "21 - 40 degrees",
                "location": "Ground-mounted",
            },
        ),
        _unit(
            None, 2200.0, *st2,
            {
                "solar_type": "Photovoltaic",
                "area_id": "01",
                "alignment": "East",
                "inclination": "5 - 20 degrees",
                "location": "Ground-mounted",
            },
            reference_date=None,
        ),
        # The real solar file carries both casings of the label; the router must
        # accept either, so the fixture reproduces that.
        _unit(
            "solar-3", 6100.0, *mu,
            {
                "solar_type": "Photovoltaic",
                "area_id": "02",
                "alignment": "South",
                "inclination": "21 - 40 degrees",
                "location": "Agrivoltaics",
            },
            note=(
                "System information provided by Fraunhofer Institute for Solar "
                "Energy Systems ISE. https://agrivoltaicsmap.ise.fraunhofer.de/"
            ),
            energy_source="Solar energy",
        ),
        _unit(
            None, 900.0, *mu2,
            {
                "solar_type": "Photovoltaic",
                "area_id": "02",
                "alignment": "West",
                "inclination": "5 - 20 degrees",
                "location": "Big parking lot",
            },
            reference_date=None,
        ),
        # bad quality: null capacity, which is the reason the real file trips.
        _unit(
            "solar-bad-1", None, *st2,
            {
                "solar_type": "Photovoltaic",
                "area_id": "01",
                "alignment": "South",
                "inclination": "21 - 40 degrees",
                "location": "Ground-mounted",
            },
        ),
        # bad quality: x/y disagree with the geometry.
        _unit(
            "solar-bad-2", 1200.0, 2.35, 48.85,
            {
                "solar_type": "Photovoltaic",
                "area_id": "01",
                "alignment": "East",
                "inclination": "5 - 20 degrees",
                "location": "Floating-mounted",
            },
            geometry=Point(9.21, 48.79),
        ),
        # state-null, good quality.
        _unit(
            "solar-4", 700.0, *OUTSIDE,
            {
                "solar_type": "Photovoltaic",
                "area_id": "02",
                "alignment": "South",
                "inclination": "21 - 40 degrees",
                "location": "Ground-mounted",
            },
        ),
    ]

    wind_rows = [
        _unit(
            "wind-1", 3000.0, *st,
            {
                "manufacturer": "Enercon GmbH",
                "turbine_type": "E-82 E2",
                "hub_height": 78.0,
                "rotor_diameter": 82.0,
                "location": "Onshore",
            },
        ),
        _unit(
            "wind-2", 4500.0, *st2,
            {
                "manufacturer": "Vestas",
                "turbine_type": "V90-3.0 MW",
                "hub_height": 90.0,
                "rotor_diameter": 90.0,
                "location": "Onshore",
            },
            note="hub_height estimated",
        ),
        _unit(
            "wind-3", 2000.0, *mu,
            {
                "manufacturer": "Siemens Energy",
                "turbine_type": "SWT-107",
                "hub_height": 90.0,
                "rotor_diameter": 107.0,
                "location": "Onshore",
            },
        ),
        # A ~4 m pair, both geo_accuracy=1: the `close location` collision rule.
        _unit(
            "wind-pair-a", 1000.0, *pair_a,
            {
                "manufacturer": "Vestas",
                "turbine_type": "V90-3.0 MW",
                "hub_height": 90.0,
                "rotor_diameter": 90.0,
                "location": "Onshore",
            },
            geo_accuracy=ACCURATE,
        ),
        _unit(
            "wind-pair-b", 1000.0, *pair_b,
            {
                "manufacturer": "Vestas",
                "turbine_type": "V90-3.0 MW",
                "hub_height": 90.0,
                "rotor_diameter": 90.0,
                "location": "Onshore",
            },
            geo_accuracy=ACCURATE,
        ),
        # state-null, good quality.
        _unit(
            "wind-4", 2400.0, *OUTSIDE,
            {
                "manufacturer": "Siemens Energy",
                "turbine_type": "SWT-107",
                "hub_height": 90.0,
                "rotor_diameter": 107.0,
                "location": "Offshore",
            },
        ),
    ]

    # `storage_type` labels match the real file's casing exactly. storage-5 is
    # capacity-invalid (storage_capacity 0 with a positive installed capacity,
    # so it is good quality but trips the storage capacity collision rule) and
    # is the single state-null storage row, covering both in one unit.
    storage_rows = [
        _unit(
            "storage-1", 1000.0, *st,
            {
                "storage_type": "Battery",
                "storage_capacity": 4.0,
                "technology": "Lithium battery",
            },
        ),
        _unit(
            "storage-2", 800.0, *st2,
            {
                "storage_type": "Pumped storage",
                "storage_capacity": 6.0,
                "technology": "Pumped hydro",
            },
        ),
        _unit(
            "storage-3", 1500.0, *mu,
            {
                "storage_type": "Battery",
                "storage_capacity": 8.0,
                "technology": "Lead battery",
            },
        ),
        _unit(
            "storage-4", 400.0, *mu2,
            {
                "storage_type": "Hydrogen storage",
                "storage_capacity": 3.0,
                "technology": "Alkaline electrolyser",
            },
        ),
        _unit(
            "storage-5", 600.0, *OUTSIDE,
            {
                "storage_type": "Battery",
                "storage_capacity": 0.0,
                "technology": "Lithium battery",
            },
        ),
    ]

    return {
        "bio": bio_rows,
        "gas": gas_rows,
        "hydro": hydro_rows,
        "solar": solar_rows,
        "wind": wind_rows,
        "storage": storage_rows,
    }


def write_sources() -> None:
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    for source, rows in source_frames().items():
        label = ENERGY_SOURCE_LABELS[source]
        # A row may override the label to cover a known variant.
        for row in rows:
            row["energy_source"] = row["energy_source"] or label
        if source in SOURCES_WITH_NOTE:
            rows = [_insert_note(row) for row in rows]
        # `energy_source` is already the first key of every row, and `geometry`
        # comes last, so the frame keeps the real files' column order for free.
        frame = _frame(rows)

        path = SOURCE_DIR / f"{source}.gpkg"
        _write(frame, path, source)
        print(f"wrote {_label(path)} n={len(frame)} cols={len(frame.columns)}")


def main() -> int:
    write_boundaries()
    write_sources()
    return 0


if __name__ == "__main__":
    sys.exit(main())
