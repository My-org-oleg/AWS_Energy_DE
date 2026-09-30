"""Boundary release content validation (issue #5).

A Boundary object published at a fixed key must prove, from its own content,
that it is the level the key promises and that its schema and geometry are
usable, before anything is written. These tests need no database.
"""

import geopandas as gpd
import pytest
from shapely.geometry import MultiPolygon, Point, Polygon, box

from etl.boundaries import BoundaryValidationError, inspect_boundary_gpkg


def _frame(level, names=("Area",), geometries=None, crs="EPSG:4326", **columns):
    geometries = geometries or [box(9.0 + i, 48.0, 9.5 + i, 48.5) for i in range(len(names))]
    data = {
        "name": list(names),
        "iso": ["DEU"] * len(names),
        "level": [level] * len(names),
        "geometry": geometries,
    }
    data.update(columns)
    return gpd.GeoDataFrame(data, crs=crs)


def _write(frame, path, layer="whatever"):
    frame.to_file(path, layer=layer, driver="GPKG")
    return path


def test_boundary_content_is_accepted_for_its_level(tmp_path):
    path = _write(_frame(2, names=("Stuttgart", "München")), tmp_path / "level-2.gpkg")

    release = inspect_boundary_gpkg(path, expected_level=2)

    assert release.level == 2
    assert list(release.data["name"]) == ["Stuttgart", "München"]
    assert list(release.data["country_iso"]) == ["DEU", "DEU"]
    assert set(release.data.columns) == {"country_iso", "name", "level", "geometry"}


def test_boundary_content_accepts_multipolygons(tmp_path):
    shape = MultiPolygon([box(9, 48, 9.5, 48.5), box(10, 48, 10.5, 48.5)])
    path = _write(_frame(0, names=("Germany",), geometries=[shape]), tmp_path / "l0.gpkg")

    assert inspect_boundary_gpkg(path, expected_level=0).level == 0


def test_boundary_content_must_match_the_level_of_its_key(tmp_path):
    path = _write(_frame(1), tmp_path / "level-3.gpkg")

    with pytest.raises(BoundaryValidationError, match="expected level 3.*found \\[1\\]"):
        inspect_boundary_gpkg(path, expected_level=3)


def test_boundary_content_rejects_mixed_levels(tmp_path):
    frame = _frame(1, names=("A", "B"))
    frame.loc[1, "level"] = 2
    path = _write(frame, tmp_path / "mixed.gpkg")

    with pytest.raises(BoundaryValidationError, match="expected level 1"):
        inspect_boundary_gpkg(path, expected_level=1)


@pytest.mark.parametrize("column", ["name", "iso", "level"])
def test_boundary_content_requires_its_schema(tmp_path, column):
    path = _write(_frame(1).drop(columns=column), tmp_path / "missing.gpkg")

    with pytest.raises(BoundaryValidationError, match=f"missing required columns: {column}"):
        inspect_boundary_gpkg(path, expected_level=1)


def test_boundary_content_requires_exactly_one_layer(tmp_path):
    path = tmp_path / "layers.gpkg"
    _frame(1).to_file(path, layer="a", driver="GPKG")
    _frame(1).to_file(path, layer="b", driver="GPKG")

    with pytest.raises(BoundaryValidationError, match="exactly one layer"):
        inspect_boundary_gpkg(path, expected_level=1)


def test_boundary_content_rejects_empty_release(tmp_path):
    path = _write(_frame(1).iloc[0:0], tmp_path / "empty.gpkg")

    with pytest.raises(BoundaryValidationError, match="no rows"):
        inspect_boundary_gpkg(path, expected_level=1)


def test_boundary_content_rejects_invalid_geometry(tmp_path):
    bowtie = Polygon([(9, 48), (10, 49), (10, 48), (9, 49)])
    path = _write(_frame(1, geometries=[bowtie]), tmp_path / "bowtie.gpkg")

    with pytest.raises(BoundaryValidationError, match="invalid geometry"):
        inspect_boundary_gpkg(path, expected_level=1)


def test_boundary_content_rejects_non_polygon_geometry(tmp_path):
    path = _write(_frame(1, geometries=[Point(9, 48)]), tmp_path / "point.gpkg")

    with pytest.raises(BoundaryValidationError, match="Polygon"):
        inspect_boundary_gpkg(path, expected_level=1)


def test_boundary_content_requires_wgs84(tmp_path):
    frame = _frame(1).to_crs("EPSG:3857")
    path = _write(frame, tmp_path / "projected.gpkg")

    with pytest.raises(BoundaryValidationError, match="EPSG:4326"):
        inspect_boundary_gpkg(path, expected_level=1)


def test_boundary_content_rejects_null_names(tmp_path):
    frame = _frame(1, names=("A", "B"))
    frame.loc[1, "name"] = None
    path = _write(frame, tmp_path / "null-name.gpkg")

    with pytest.raises(BoundaryValidationError, match="non-null name"):
        inspect_boundary_gpkg(path, expected_level=1)


def test_country_outline_level_has_exactly_one_row(tmp_path):
    path = _write(_frame(0, names=("Germany", "Elsewhere")), tmp_path / "l0.gpkg")

    with pytest.raises(BoundaryValidationError, match="exactly one country outline"):
        inspect_boundary_gpkg(path, expected_level=0)
