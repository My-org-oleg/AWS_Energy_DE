"""Extract-stage seams: Source snapshot content routing and boundary levels.

The extract CLI routes each candidate GPKG by its validated Energy source
content, and the boundary load reads its level from the gpkg data. The heavy
per-file extraction runs against the database (integration suite).
"""

import geopandas as gpd
import pandas
import pytest
from shapely.geometry import Point, Polygon

from etl.extract import extract_boundaries
from etl.source_data import SourceValidationError, inspect_source_gpkg


class TestBoundariesFailLoudlyWithoutLevel:
    def test_gpkg_lacking_level_column_is_an_error(self, tmp_path, _test_database):
        # `_test_database` is a real dependency, not a formality: `extract_boundaries`
        # opens a connection, and without a working database this asserts on a
        # connection error instead and would pass for the wrong reason.
        gdf = gpd.GeoDataFrame(
            {
                "name": ["Test"],
                "iso": ["DEU"],
                "geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            },
            crs="EPSG:4326",
        )
        gpkg = tmp_path / "test_boundary.gpkg"
        gdf.to_file(gpkg, driver="GPKG")
        manifest = tmp_path / "boundaries.txt"
        manifest.write_text("test_boundary.gpkg\n")

        report = extract_boundaries(manifest)

        assert not report.passed
        assert any("no 'level' column" in e for e in report.errors)


def _solar_frame(
    *,
    energy_sources=("Solar Energy",),
    reference_ids=("solar-1",),
    location="Agrivoltaics",
    coordinates=((10.0, 50.0),),
    geo_accuracy=1,
    capacity=100.0,
    commissioning_date="2020-01-01",
):
    count = len(reference_ids)
    energy_source_values = list(energy_sources)
    if len(energy_source_values) == 1 and count > 1:
        energy_source_values *= count
    return gpd.GeoDataFrame(
        {
            "energy_source": energy_source_values,
            "installed_capacity": [capacity] * count,
            "commissioning_date": [commissioning_date] * count,
            "decommissioning_date": [None] * count,
            "solar_type": ["Utility"] * count,
            "area_id": [None] * count,
            "alignment": [None] * count,
            "inclination": [None] * count,
            "location": [location] * count,
            "x_coordinates": [point[0] for point in coordinates],
            "y_coordinates": [point[1] for point in coordinates],
            "geo_accuracy": [geo_accuracy] * count,
            "note": [None] * count,
            "reference_source": ["test"] * count,
            "reference_id": list(reference_ids),
            "reference_date": [pandas.Timestamp("2020-01-01")] * count,
            "geometry": [Point(*point) for point in coordinates],
        },
        crs="EPSG:4326",
    )


def _write_gpkg(frame, path, *, layer="arbitrary_layer_name"):
    frame.to_file(path, layer=layer, driver="GPKG")
    return path


def test_source_content_routes_known_solar_label_variants(tmp_path):
    path = _write_gpkg(
        _solar_frame(
            energy_sources=("Solar Energy", "Solar energy"),
            reference_ids=("solar-1", "solar-2"),
            coordinates=((10.0, 50.0), (11.0, 51.0)),
        ),
        tmp_path / "filename-does-not-route.gpkg",
    )

    dataset = inspect_source_gpkg(path)

    assert dataset.source == "solar"
    assert dataset.data["energy_source"].tolist() == ["solar", "solar"]
    assert dataset.unit_ids.tolist() == ["solar_solar-1", "solar_solar-2"]


def test_source_content_requires_exactly_one_layer(tmp_path):
    path = tmp_path / "multiple-layers.gpkg"
    frame = _solar_frame()
    frame.to_file(path, layer="first", driver="GPKG")
    frame.to_file(path, layer="second", driver="GPKG")

    with pytest.raises(SourceValidationError, match="exactly one layer"):
        inspect_source_gpkg(path)


@pytest.mark.parametrize(
    ("energy_sources", "message"),
    [
        (("Coal",), "unknown Energy source"),
        ((None,), "non-null homogeneous Energy source"),
        (("Solar Energy", "Wind Energy"), "homogeneous Energy source"),
    ],
)
def test_source_content_rejects_unknown_null_and_mixed_labels(
    tmp_path, energy_sources, message
):
    path = _write_gpkg(
        _solar_frame(
            energy_sources=energy_sources,
            reference_ids=("solar-1", "solar-2")[: len(energy_sources)],
            coordinates=((10.0, 50.0), (11.0, 51.0))[: len(energy_sources)],
        ),
        tmp_path / "invalid-label.gpkg",
    )

    with pytest.raises(SourceValidationError, match=message):
        inspect_source_gpkg(path)


def test_source_content_rejects_missing_required_column(tmp_path):
    frame = _solar_frame().drop(columns="reference_id")
    path = _write_gpkg(frame, tmp_path / "missing-column.gpkg")

    with pytest.raises(SourceValidationError, match="missing required columns: reference_id"):
        inspect_source_gpkg(path)


def test_source_content_rejects_non_point_geometry(tmp_path):
    frame = _solar_frame()
    frame.loc[0, "geometry"] = Polygon([(0, 0), (1, 0), (1, 1)])
    path = _write_gpkg(frame, tmp_path / "polygon.gpkg")

    with pytest.raises(SourceValidationError, match="Point geometry"):
        inspect_source_gpkg(path)


def test_source_content_rejects_duplicate_non_null_reference_ids(tmp_path):
    path = _write_gpkg(
        _solar_frame(
            reference_ids=("solar-1", "solar-1"),
            coordinates=((10.0, 50.0), (11.0, 51.0)),
        ),
        tmp_path / "duplicate-reference.gpkg",
    )

    with pytest.raises(SourceValidationError, match="Duplicate non-null Reference IDs"):
        inspect_source_gpkg(path)


def test_source_content_rejects_duplicate_reference_ids_alongside_null_ones(tmp_path):
    """A snapshot mixing null and duplicated Reference IDs still fails clearly.

    Solar is the dataset that carries null Reference IDs, so this is the shape
    a duplicated Solar publication takes. It must surface as a validation error,
    which the worker records as a failed extract stage result with a clear
    message, rather than an unexplained pandas indexing crash.
    """
    path = _write_gpkg(
        _solar_frame(
            reference_ids=(None, "solar-1", "solar-1"),
            coordinates=((9.0, 49.0), (10.0, 50.0), (11.0, 51.0)),
        ),
        tmp_path / "duplicate-with-null-reference.gpkg",
    )

    with pytest.raises(
        SourceValidationError, match=r"Duplicate non-null Reference IDs.*solar-1"
    ):
        inspect_source_gpkg(path)


def test_source_synthetic_identity_uses_stable_attributes_across_snapshots(tmp_path):
    first = _write_gpkg(
        _solar_frame(reference_ids=(None,), capacity=100.0),
        tmp_path / "first.gpkg",
    )
    second = _write_gpkg(
        _solar_frame(
            reference_ids=(None,),
            coordinates=((10.0000004, 50.0000004),),
            capacity=250.0,
            commissioning_date="2024-09-01",
        ),
        tmp_path / "second.gpkg",
    )

    first_ids = inspect_source_gpkg(first).unit_ids
    second_ids = inspect_source_gpkg(second).unit_ids

    assert first_ids.tolist() == second_ids.tolist()
    assert first_ids.iloc[0].startswith("syn_")


def test_source_synthetic_identity_collision_fails_clearly(tmp_path):
    path = _write_gpkg(
        _solar_frame(
            reference_ids=(None, None),
            coordinates=((10.0, 50.0), (10.0, 50.0)),
            capacity=100.0,
        ),
        tmp_path / "synthetic-collision.gpkg",
    )

    with pytest.raises(SourceValidationError, match="Synthetic identity collision"):
        inspect_source_gpkg(path)
