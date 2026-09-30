"""Integration tests for the spec v2.2 transform contract (issue #11).

These run against the hermetic test database that `conftest.py` points the
session at (`TEST_DATABASE_URL`), not against dev: the staging stage reads from
raw versioned tables and writes via PostGIS. All sources are transformed once
per session by a shared fixture (the transform is idempotent — it drops and
re-creates the staging tables) and the tests assert against that shared staging
state.
"""

import os

import pytest
from sqlalchemy import create_engine, text

from etl.config import DECOMPOSED_PROPERTIES

ENGINE = create_engine(os.environ["DATABASE_URL"])

SOURCE_NAMES = ("bio", "gas", "hydro", "solar", "wind", "storage")

WHITELIST = set(DECOMPOSED_PROPERTIES)

# Note the bio property set has no `note` even though the bio fixture carries
# the column, exactly as the real file does: it is null on every row, and the
# transform drops all-null values before the property dimension is built. The
# column is in the fixture for shape fidelity, not to produce a property.
EXPECTED_PROPERTIES = {
    "bio": {"biomass_type", "fuel_type", "reference_source", "technology"},
    "gas": {"reference_source", "technology"},
    "hydro": {"hydro_type", "inflow_type", "reference_source"},
    "solar": {
        "alignment",
        "inclination",
        "location",
        "note",
        "reference_source",
        "solar_type",
    },
    "wind": {
        "hub_height",
        "location",
        "manufacturer",
        "note",
        "reference_source",
        "rotor_diameter",
    },
    "storage": {"reference_source", "technology"},
}

EXPECTED_JSON_KEYS = {
    "bio": {"biogas_unit", "chp_unit"},
    "gas": set(),
    "hydro": set(),
    "solar": {"area_id"},
    "wind": {"turbine_type"},
    "storage": set(),
}

# Counts for the committed fixtures in tests/fixtures/sources (regenerate with
# `python scripts/make_test_fixtures.py`). Each source with a state-null row
# has exactly one — the point outside every state polygon — and solar carries
# the two bad-quality rows (a null capacity, which is the only bad-capacity
# reason the real solar file produces, and coordinates that disagree with the
# geometry).
EXPECTED_STATE_NULLS = {"hydro": 1, "solar": 1, "wind": 1, "storage": 1}
EXPECTED_BAD_QUALITY = {"solar": 2}


def scalar(sql: str) -> int:
    with ENGINE.connect() as conn:
        return int(conn.execute(text(sql)).scalar())


@pytest.fixture(scope="module", autouse=True)
def _transformed_all(_staged_sources):
    """Staging is transformed once per session; nothing to do here."""


def test_staging_tables_have_secondary_attributes_column():
    for source in SOURCE_NAMES:
        with ENGINE.connect() as conn:
            cols = {
                row[0]
                for row in conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'stage' AND table_name = :name"
                    ),
                    {"name": source},
                )
            }
        assert "secondary_attributes" in cols, f"stage.{source} missing secondary_attributes"


@pytest.mark.parametrize("source", SOURCE_NAMES)
def test_staging_secondary_attributes_keep_only_nonwhitelist_keys(source):
    with ENGINE.connect() as conn:
        actual = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT DISTINCT jsonb_object_keys(secondary_attributes::jsonb) "
                    f"FROM stage.{source}"
                )
            )
        }
    assert actual == EXPECTED_JSON_KEYS[source]
    assert not (actual & WHITELIST), f"whitelisted keys leaked into stage.{source} json"


@pytest.mark.parametrize("source", SOURCE_NAMES)
def test_properties_tables_contain_only_whitelisted_keys(source):
    with ENGINE.connect() as conn:
        names = {
            row[0]
            for row in conn.execute(
                text(f"SELECT DISTINCT name FROM stage.{source}_properties")
            )
        }
    names.discard("bad_quality")
    assert names == EXPECTED_PROPERTIES[source], f"stage.{source}_properties: {names}"
    assert not (names - WHITELIST)


def test_state_null_units_no_longer_bad_quality():
    for source in SOURCE_NAMES:
        state_nulls = scalar(
            f"SELECT COUNT(*) FROM stage.{source} WHERE state IS NULL"
        )
        bad = scalar(f"SELECT COUNT(*) FROM stage.{source} WHERE bad_quality")
        bad_state = scalar(
            f"SELECT COUNT(*) FROM stage.{source} WHERE state IS NULL AND bad_quality"
        )
        assert state_nulls == EXPECTED_STATE_NULLS.get(source, 0)
        assert bad == EXPECTED_BAD_QUALITY.get(source, 0)
        assert bad_state == 0
        assert (
            scalar(
                f"SELECT COUNT(*) FROM stage.{source}_units_properties up "
                f"JOIN stage.{source}_properties p ON p.param_id = up.param_id "
                f"WHERE p.name = 'bad_quality' AND p.value LIKE '%outside location%'"
            )
            == 0
        )