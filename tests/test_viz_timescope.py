"""Integration tests for the visualization timescope against real Core (issue #9).

The visualization reads the retained Core history directly, because the marts
are current-date pivots that cannot answer an arbitrary interval (ADR 0003).
`viz.data.ACTIVE_UNIT_PREDICATE` is the single timescope rule both Core kinds
are fetched under, so this module drives the real `viz.data.fetch_units`
against real `core.generators` / `core.storages` rows and asserts *which* rows
come back — the SQL-string assertions in `tests/test_viz_data.py` cannot tell
whether an interval endpoint is inclusive.

The units under test are probe rows sharing one `state`, fetched through the
area-filter seam (`area_column="state"`), so whether a probe appears in the
frame is decided by the timescope predicate alone.  The interval
2020-01-01 .. 2020-12-31 puts a probe on each boundary exactly, so a unit
commissioned on the first or last day settles the endpoints, and a unit
commissioned after the last day settles the upper bound.
"""

import os
import uuid
from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from conftest import drop_core_tables
from etl.config import CORE_SCHEMA, SERVICE_SCHEMA
from etl.ingestion import _ensure_service_metadata
from etl.load import load_generators, load_storages
from viz.data import fetch_units

ENGINE = create_engine(os.environ["DATABASE_URL"])

# The timescope every test selects.
INTERVAL_FROM = date(2020, 1, 1)
INTERVAL_TO = date(2020, 12, 31)

# A `state` no fixture unit carries, so the area filter isolates the probes.
PROBE_STATE = "Timescope Probe"

# probe label -> (commissioning_date, decommissioning_date).  The same set is
# inserted into both Core kinds, whose only difference is the projection, so
# one expectation covers "generators and storages share the timescope rule".
# Three probes are commissioned outside the interval — one before it and two
# after it — so the commissioning bounds are decided by the commissioning rule
# alone and the decommissioning cases by the decommissioning rule alone.
PROBE_DATES = {
    "at_from": (date(2020, 1, 1), None),
    "at_to": (date(2020, 12, 31), None),
    "inside": (date(2020, 6, 15), None),
    "before_from": (date(2019, 12, 31), None),
    "after_to": (date(2021, 1, 1), None),
    "decommissioned_on_to": (date(2020, 6, 15), date(2020, 12, 31)),
    "decommissioned_after_to": (date(2020, 6, 15), date(2021, 1, 1)),
    "decommissioned_before_to": (date(2020, 6, 15), date(2020, 12, 30)),
    "after_interval_entirely": (date(2021, 6, 1), date(2022, 1, 1)),
}

# The probes an Active unit keeps for the selected interval.
ACTIVE_PROBES = frozenset(
    {
        "at_from",
        "at_to",
        "inside",
        "decommissioned_on_to",
        "decommissioned_after_to",
    }
)

# Core table -> the one `energy_source` value that table carries.
PROBE_SOURCE = {"generators": "wind", "storages": "storage"}

INSERT_GENERATOR = (
    f"INSERT INTO {CORE_SCHEMA}.generators (energy_source, installed_capacity, "
    f"commissioning_date, decommissioning_date, longitude, latitude, "
    f"geo_accuracy, reference_id, reference_date, secondary_attributes, "
    f"country_iso, state, region, district, collision) "
    f"VALUES ('{PROBE_SOURCE['generators']}', 123.5, :commissioning, "
    f":decommissioning, 10.0, 50.0, 1, :reference_id, "
    f"'2020-01-01 00:00:00', NULL, 'DEU', '{PROBE_STATE}', NULL, NULL, false)"
)

INSERT_STORAGE = (
    f"INSERT INTO {CORE_SCHEMA}.storages (energy_source, storage_type, "
    f"storage_capacity, installed_capacity, commissioning_date, "
    f"decommissioning_date, longitude, latitude, geo_accuracy, reference_id, "
    f"reference_date, secondary_attributes, country_iso, state, region, "
    f"district, collision) "
    f"VALUES ('{PROBE_SOURCE['storages']}', 'Battery', 800.0, 123.5, "
    f":commissioning, :decommissioning, 10.0, 50.0, 1, :reference_id, "
    f"'2020-01-01 00:00:00', NULL, 'DEU', '{PROBE_STATE}', NULL, NULL, false)"
)

PROBE_INSERT = {"generators": INSERT_GENERATOR, "storages": INSERT_STORAGE}


def _insert_probes(table: str) -> None:
    """Insert one probe row per `PROBE_DATES` case into ``core.<table>``."""
    with ENGINE.begin() as conn:
        for label, (commissioning, decommissioning) in PROBE_DATES.items():
            conn.execute(
                text(PROBE_INSERT[table]),
                {
                    "commissioning": commissioning,
                    "decommissioning": decommissioning,
                    "reference_id": f"viz_timescope_{label}_{uuid.uuid4().hex[:8]}",
                },
            )


def _probe_frame(table: str) -> pd.DataFrame:
    """The probe rows `fetch_units` returns for the selected interval."""
    return fetch_units(
        ENGINE,
        active_from=INTERVAL_FROM,
        active_to=INTERVAL_TO,
        sources=(PROBE_SOURCE[table],),
        area_column="state",
        area_names=(PROBE_STATE,),
    )


def _as_date(value) -> date | None:
    """``None`` for a missing date, a `datetime.date` otherwise.

    The frame's date columns come back as `datetime64` cells or as `NaT`,
    depending on how pandas dtypes the read, so both spellings are normalized
    before the lookup.
    """
    if pd.isna(value):
        return None
    return value.date() if hasattr(value, "date") else value


def _probe_labels_returned(table: str) -> set[str]:
    """Which probes the timescope kept, keyed back by their date pair.

    The unit projection does not carry `reference_id`, so the frame is mapped
    back through the probed date pairs, which are unique per label.
    """
    labels_by_dates = {dates: label for label, dates in PROBE_DATES.items()}
    frame = _probe_frame(table)
    returned = set()
    for commissioning, decommissioning in zip(
        frame["commissioning_date"], frame["decommissioning_date"]
    ):
        returned.add(
            labels_by_dates[(_as_date(commissioning), _as_date(decommissioning))]
        )
    return returned


@pytest.fixture(scope="module")
def _core_with_probes(_staged_sources):
    """Both Core kinds loaded from the fixtures, plus the probe rows."""
    drop_core_tables(ENGINE)
    for load in (load_generators, load_storages):
        report = load()
        assert report.passed, report.errors
    for table in PROBE_SOURCE:
        _insert_probes(table)
    yield
    drop_core_tables(ENGINE)


@pytest.fixture(scope="module")
def _source_memberships(_core_with_probes):
    """One recorded Source membership, for a loaded (non-probe) Core unit.

    Membership is what the timescope query must *not* depend on, so the table
    is populated here to keep the retained-unit tests from passing vacuously
    against an empty one.  Both rows are removed again in teardown.
    """
    _ensure_service_metadata(ENGINE)
    run_id = uuid.uuid4()
    with ENGINE.begin() as conn:
        member = conn.execute(
            text(
                f"SELECT reference_id FROM {CORE_SCHEMA}.generators "
                f"WHERE reference_id IS NOT NULL "
                f"AND reference_id NOT LIKE 'viz_timescope_%' LIMIT 1"
            )
        ).scalar()
        assert member, "no loaded core generator carries a reference_id"
        conn.execute(
            text(
                f"INSERT INTO {SERVICE_SCHEMA}.ingestion_runs "
                f"(run_id, bucket, object_key, object_version_id, input_kind, "
                f"state) VALUES (:run_id, 'timescope-test', 'fixture/wind.gpkg', "
                f":version, 'source', 'completed')"
            ),
            {"run_id": str(run_id), "version": uuid.uuid4().hex},
        )
        conn.execute(
            text(
                f"INSERT INTO {SERVICE_SCHEMA}.source_memberships "
                f"(run_id, energy_source, unit_key, reference_id, bad_quality) "
                f"VALUES (:run_id, '{PROBE_SOURCE['generators']}', :unit_key, "
                f":reference_id, false)"
            ),
            {"run_id": str(run_id), "unit_key": member, "reference_id": member},
        )
    yield
    with ENGINE.begin() as conn:
        conn.execute(
            text(
                f"DELETE FROM {SERVICE_SCHEMA}.source_memberships "
                f"WHERE run_id = :run_id"
            ),
            {"run_id": str(run_id)},
        )
        conn.execute(
            text(f"DELETE FROM {SERVICE_SCHEMA}.ingestion_runs WHERE run_id = :run_id"),
            {"run_id": str(run_id)},
        )


class TestIntervalEndpoints:
    @pytest.mark.parametrize("table", sorted(PROBE_SOURCE))
    def test_returns_exactly_the_units_active_in_the_interval(self, table, _core_with_probes):
        assert _probe_labels_returned(table) == ACTIVE_PROBES

    @pytest.mark.parametrize(
        ("table", "label"),
        [
            (table, label)
            for table in sorted(PROBE_SOURCE)
            for label in (
                "at_from",
                "at_to",
                "inside",
                "decommissioned_on_to",
                "decommissioned_after_to",
            )
        ],
    )
    def test_a_unit_active_in_the_interval_is_returned(self, table, label, _core_with_probes):
        assert label in _probe_labels_returned(table)

    @pytest.mark.parametrize(
        ("table", "label"),
        [
            (table, label)
            for table in sorted(PROBE_SOURCE)
            for label in (
                "before_from",
                "after_to",
                "decommissioned_before_to",
                "after_interval_entirely",
            )
        ],
    )
    def test_a_unit_outside_the_interval_is_excluded(self, table, label, _core_with_probes):
        assert label not in _probe_labels_returned(table)


class TestRetainedUnits:
    def test_a_retained_unit_without_source_membership_is_returned(
        self, _core_with_probes, _source_memberships
    ):
        # A Historical unit: Core keeps it, its dates keep it Active, and no
        # Source snapshot claims it.  Membership absence must not hide it.
        assert "inside" in _probe_labels_returned("generators")

    def test_a_loaded_core_unit_absent_from_source_membership_is_returned(
        self, _core_with_probes, _source_memberships
    ):
        # The same rule against a unit the fixtures really loaded, not a
        # probe: it is a member of no Source snapshot here, so the only thing
        # that can return it is the Core timescope.
        with ENGINE.connect() as conn:
            member = conn.execute(
                text(
                    f"SELECT commissioning_date, decommissioning_date "
                    f"FROM {CORE_SCHEMA}.generators "
                    f"WHERE reference_id IS NOT NULL "
                    f"AND reference_id NOT LIKE 'viz_timescope_%' LIMIT 1"
                )
            ).one()
        units = fetch_units(
            ENGINE,
            active_from=date(1900, 1, 1),
            active_to=date.today(),
            sources=(PROBE_SOURCE["generators"],),
        )
        returned = {
            (_as_date(commissioning), _as_date(decommissioning))
            for commissioning, decommissioning in zip(
                units["commissioning_date"], units["decommissioning_date"]
            )
        }
        assert (member[0], member[1]) in returned

    def test_the_membership_table_holds_no_probe(self, _core_with_probes, _source_memberships):
        with ENGINE.connect() as conn:
            recorded = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {SERVICE_SCHEMA}.source_memberships")
                ).scalar()
            )
            probes = int(
                conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM {SERVICE_SCHEMA}.source_memberships "
                        f"WHERE reference_id LIKE 'viz_timescope_%'"
                    )
                ).scalar()
            )
        assert recorded == 1
        assert probes == 0


class TestProjectedFields:
    def test_generator_fields_survive_the_timescope(self, _core_with_probes):
        units = _probe_frame("generators")
        assert set(units["energy_source"]) == {PROBE_SOURCE["generators"]}
        assert set(units["installed_capacity"]) == {123.5}
        assert set(units["commissioning_date"].map(_as_date)) == {
            INTERVAL_FROM,
            INTERVAL_TO,
            date(2020, 6, 15),
        }
        assert set(units["longitude"]) == {10.0}
        assert set(units["latitude"]) == {50.0}
        assert set(units["name"]) == {PROBE_STATE}

    def test_storage_fields_survive_the_timescope(self, _core_with_probes):
        units = _probe_frame("storages")
        assert set(units["energy_source"]) == {PROBE_SOURCE["storages"]}
        assert set(units["storage_capacity"]) == {800.0}
        assert set(units["installed_capacity"]) == {123.5}
        assert set(units["longitude"]) == {10.0}
        assert set(units["latitude"]) == {50.0}
        assert set(units["name"]) == {PROBE_STATE}
