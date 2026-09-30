"""The committed fixtures must match what `scripts/make_test_fixtures.py` makes.

These tests need no database, so they run in any environment.

They exist because the fixtures are a silent dependency of the whole suite: if
the generator drifts from the committed files, every integration test is
quietly running against stale data and still passes. They also pin the two
source-shape quirks the pipeline actually depends on, and the byte-for-byte
reproducibility the generator's docstring promises — reproducibility is not
cosmetic here, since a fixture that churns on every regeneration turns
"regenerate and commit" into an unreviewable diff.
"""

import hashlib
import importlib.util
import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from etl.config import SOURCE_NAMES

REPO_ROOT = Path(__file__).resolve().parent.parent
GENERATOR = REPO_ROOT / "scripts" / "make_test_fixtures.py"
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="module")
def generator():
    """`scripts/make_test_fixtures.py` as a module (scripts/ is not a package)."""
    spec = importlib.util.spec_from_file_location("make_test_fixtures", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _committed_fixtures() -> list[Path]:
    return sorted(FIXTURE_DIR.rglob("*.gpkg"))


def test_all_six_sources_and_four_boundary_levels_are_committed():
    sources = sorted(p.stem for p in (FIXTURE_DIR / "sources").glob("*.gpkg"))
    boundaries = sorted(p.name for p in (FIXTURE_DIR / "boundaries").glob("*.gpkg"))
    assert sources == sorted(SOURCE_NAMES)
    assert len(boundaries) == 4


def test_manifest_lists_exactly_the_committed_boundary_files():
    listed = (FIXTURE_DIR / "boundaries" / "manifest.txt").read_text().split()
    on_disk = sorted(p.name for p in (FIXTURE_DIR / "boundaries").glob("*.gpkg"))
    assert sorted(listed) == on_disk


def _regenerate(generator, monkeypatch, root: Path) -> Path:
    monkeypatch.setattr(generator, "BOUNDARY_DIR", root / "boundaries")
    monkeypatch.setattr(generator, "SOURCE_DIR", root / "sources")
    generator.main()
    return root


def _frames(root: Path) -> dict[str, gpd.GeoDataFrame]:
    return {str(p.relative_to(root)): gpd.read_file(p) for p in sorted(root.rglob("*.gpkg"))}


def test_generator_is_byte_deterministic(generator, tmp_path, monkeypatch):
    """Two generations of the same fixtures must produce identical bytes.

    The generator promises this, and it is load-bearing: SQLite stamps a change
    counter and GeoPandas stamps `gpkg_contents.last_change`, so a regression
    here turns "regenerate and commit" into an unreviewable diff. Comparing
    bytes within one run isolates nondeterminism from the installed GDAL
    version, which is not what this test is about.
    """
    first = _regenerate(generator, monkeypatch, tmp_path / "first")
    second = _regenerate(generator, monkeypatch, tmp_path / "second")
    assert sorted(p.name for p in first.rglob("*.gpkg")) == sorted(
        p.name for p in second.rglob("*.gpkg")
    )
    unstable = [
        p.name
        for p in sorted(first.rglob("*.gpkg"))
        if _digest(p) != _digest(second / p.relative_to(first))
    ]
    assert not unstable, f"nondeterministic fixtures: {unstable}"


def test_committed_fixtures_match_the_generator(generator, tmp_path, monkeypatch):
    """The committed fixtures must be what the generator produces right now.

    Without this, editing the generator and forgetting to commit the
    regenerated files leaves every integration test running against stale data
    and still green.

    The comparison is on frame content, not bytes, so that a dependency bump
    which changes the GPKG encoding is not reported as fixture drift. Byte
    stability across runs is `test_generator_is_byte_deterministic`'s job.
    """
    fresh = _regenerate(generator, monkeypatch, tmp_path)
    committed_frames = _frames(FIXTURE_DIR)
    fresh_frames = _frames(fresh)

    assert set(fresh_frames) == set(committed_frames), (
        "fixture set drifted from the generator — regenerate and commit"
    )
    stale = [
        name
        for name in sorted(committed_frames)
        if not _same(committed_frames[name], fresh_frames[name])
    ]
    assert not stale, (
        f"stale fixtures — re-run `python scripts/make_test_fixtures.py` and "
        f"commit: {stale}"
    )


def _same(left: gpd.GeoDataFrame, right: gpd.GeoDataFrame) -> bool:
    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    try:
        pd.testing.assert_frame_equal(
            left.reset_index(drop=True), right.reset_index(drop=True), check_dtype=False
        )
    except AssertionError:
        return False
    return True


def test_gas_has_no_installed_capacity_column():
    """Gas is keyed on gas_production_capacity; the raw layer renames it.

    If this column ever appears in the gas fixture, the rename path stops being
    exercised and the suite would pass without covering it.
    """
    columns = set(gpd.read_file(FIXTURE_DIR / "sources" / "gas.gpkg", rows=1).columns)
    assert "gas_production_capacity" in columns
    assert "installed_capacity" not in columns


def test_storage_carries_both_type_and_capacity():
    columns = set(
        gpd.read_file(FIXTURE_DIR / "sources" / "storage.gpkg", rows=1).columns
    )
    assert {"storage_type", "storage_capacity"} <= columns


def test_decommissioning_dates_are_null_not_empty_strings():
    """Real rows are null; an empty string would exercise a coercion the data never needs."""
    for source in SOURCE_NAMES:
        path = FIXTURE_DIR / "sources" / f"{source}.gpkg"
        connection = sqlite3.connect(path)
        try:
            blanks, total = connection.execute(
                "SELECT COUNT(*) FILTER (WHERE decommissioning_date = ''), COUNT(*) "
                f'FROM "{source}"'
            ).fetchone()
        finally:
            connection.close()
        assert blanks == 0, f"{source}: decommissioning_date has empty strings"
        assert total > 0


def test_solar_carries_both_energy_source_label_casings():
    labels = set(
        gpd.read_file(FIXTURE_DIR / "sources" / "solar.gpkg")["energy_source"]
    )
    assert labels == {"Solar Energy", "Solar energy"}


def test_boundary_layers_are_named_differently_from_their_files():
    """The real files are organised this way, and extraction must not trust names.

    `germany_states.gpkg` holds a layer called `germany_regions`, so the level
    has to come from the data. If a fixture were renamed to match its layer, the
    filename-vs-level independence would stop being covered.
    """
    expected = {
        "germany_boundary.gpkg": "germany_boundary",
        "germany_states.gpkg": "germany_regions",
        "germany_regions.gpkg": "germany_districts",
        "germany_districts.gpkg": "germany_districts",
    }
    for filename, layer in expected.items():
        path = FIXTURE_DIR / "boundaries" / filename
        assert layer in set(gpd.list_layers(path)["name"]), filename
        # Every layer in the file, so a stray duplicate would be noticed too.
        assert len(gpd.list_layers(path)) == 1, filename
