"""Unit tests for the viz data-access seams (issues #23, #25, render-opt, #9).

Seams live in `viz.data`: the engine selection (`VIZ_DATABASE_URL` with a
`DATABASE_URL` fallback), the standby detection (`missing_core_tables`), the
render-optimized unit fetch (`units_query`/`fetch_units` — one query per
distinct core table via `energy_source = ANY(:sources)` into a pandas frame),
the boundary fetch (`boundaries_query`/`fetch_boundaries` — all areas at a
level, simplified geometry + km² area), and the JSON-readiness seam
(`unit_records`).  None touch a real database: the engine tests only build
engines, the standby tests drive a stub engine whose rows mimic
`information_schema`, and the fetch tests drive recording or row-returning
stub engines.  `fetch_units`'s pandas orchestration stubs the `run_frame`
seam, so no rows ever hit a real reader.

Issue #9 covers the timescope rule itself: the unit query's closed interval
(commissioned on/after `:from` and on/before `:to`, still running at `:to`),
that both Core kinds share it, that the marts are never read as a substitute,
and that Source membership is never joined.  `tests/test_viz_timescope.py`
drives the same rule against real Core rows to prove the endpoints.
"""

import os
from contextlib import nullcontext
from datetime import date

import pandas as pd
import pytest
from sqlalchemy import create_engine

from viz import data as data_module
from viz.data import (
    ACTIVE_UNIT_PREDICATE,
    CORE_VIS_TABLES,
    STORAGE_COLUMNS_SQL,
    UNIT_COLUMNS,
    UNIT_COLUMNS_SQL,
    boundaries_query,
    boundary_names_query,
    fetch_area_names,
    fetch_boundaries,
    fetch_units,
    get_viz_engine,
    missing_core_tables,
    run_query,
    unit_records,
    units_query,
)


class _StubRows:
    def __init__(self, names):
        self._names = list(names)

    def __iter__(self):
        return iter((name,) for name in self._names)


class _StubConnection:
    def __init__(self, names):
        self._names = names

    def execute(self, query, params=None):
        return _StubRows(self._names)


class _StubEngine:
    """Fake SQLAlchemy engine: connect() yields rows for the named tables."""

    def __init__(self, names):
        self._names = names

    def connect(self):
        return nullcontext(_StubConnection(self._names))


class TestEngineSelection:
    def test_uses_database_url_when_viz_url_absent(self, monkeypatch):
        monkeypatch.delenv("VIZ_DATABASE_URL", raising=False)
        engine = get_viz_engine()
        expected = create_engine(os.environ["DATABASE_URL"])
        assert str(engine.url) == str(expected.url)

    def test_prefers_viz_database_url(self, monkeypatch):
        # The driver is explicit because requirements-viz.txt installs psycopg2
        # while SQLAlchemy 2.1 resolves a bare postgresql:// to psycopg3.
        monkeypatch.setenv(
            "VIZ_DATABASE_URL",
            "postgresql+psycopg2://viz_reader@localhost:5432/energy_de",
        )
        engine = get_viz_engine()
        assert engine.url.username == "viz_reader"
        assert engine.url.database == "energy_de"


class TestStandbyDetection:
    def test_no_missing_tables_when_all_present(self):
        assert missing_core_tables(_StubEngine(["generators", "storages"])) == []

    def test_returns_absent_tables_in_order(self):
        assert missing_core_tables(_StubEngine(["storages"])) == ["generators"]

    def test_all_missing_when_core_is_empty(self):
        assert missing_core_tables(_StubEngine([])) == ["generators", "storages"]

    def test_known_core_vis_tables(self):
        assert CORE_VIS_TABLES == ("generators", "storages")


class _RecordingConnection:
    """Connection that records (sql, params) pairs and returns no rows."""

    def __init__(self, engine):
        self.engine = engine

    def execute(self, query, params=None):
        self.engine.calls.append((str(query), params))
        return []


class _RecordingEngine:
    """Fake SQLAlchemy engine: records queries, executes nothing."""

    def __init__(self):
        self.calls = []

    def connect(self):
        return nullcontext(_RecordingConnection(self))


class _MappingRow:
    def __init__(self, **values):
        self._mapping = values


class _RowsConnection:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, query, params=None):
        return self.rows


class _RowsEngine:
    def __init__(self, rows):
        self.rows = rows

    def connect(self):
        return nullcontext(_RowsConnection(self.rows))


class TestUnitsQuery:
    def test_generator_query_matches_all_sources_in_one_pass(self):
        sql, params = units_query(
            "generators",
            UNIT_COLUMNS_SQL,
            sources=("solar", "wind"),
            active_from=date(1990, 1, 1),
            active_to=date(2010, 1, 1),
            area_column=None,
        )
        assert sql == (
            "SELECT energy_source, installed_capacity, commissioning_date, "
            "decommissioning_date, longitude, latitude, state, district "
            "FROM core.generators "
            "WHERE energy_source = ANY(:sources) "
            "AND commissioning_date BETWEEN :from AND :to "
            "AND (decommissioning_date IS NULL OR decommissioning_date >= :to)"
        )
        assert params == {
            "sources": ["solar", "wind"],
            "from": date(1990, 1, 1),
            "to": date(2010, 1, 1),
        }

    def test_storage_query_adds_storage_capacity(self):
        sql, _ = units_query(
            "storages",
            STORAGE_COLUMNS_SQL,
            sources=("storage",),
            active_from=date(1990, 1, 1),
            active_to=date(2010, 1, 1),
        )
        assert sql.startswith(
            "SELECT energy_source, installed_capacity, commissioning_date, "
            "decommissioning_date, longitude, latitude, state, district, "
            "storage_capacity FROM core.storages "
        )

    def test_timescope_is_the_single_active_predicate_with_bound_params(self):
        sql, params = units_query(
            "generators",
            UNIT_COLUMNS_SQL,
            sources=("solar",),
            active_from=date(1990, 1, 1),
            active_to=date(2010, 1, 1),
        )
        assert "commissioning_date BETWEEN :from AND :to" in sql
        assert "decommissioning_date IS NULL OR decommissioning_date >= :to" in sql
        assert params == {
            "sources": ["solar"],
            "from": date(1990, 1, 1),
            "to": date(2010, 1, 1),
        }

    def test_both_core_kinds_are_fetched_under_the_one_timescope_predicate(self):
        # One rule, both kinds: the same predicate constant is interpolated
        # into the generator and the storage query (issue #9).
        for table, columns, sources in (
            ("generators", UNIT_COLUMNS_SQL, ("solar",)),
            ("storages", STORAGE_COLUMNS_SQL, ("storage",)),
        ):
            sql, _ = units_query(
                table,
                columns,
                sources=sources,
                active_from=date(1990, 1, 1),
                active_to=date(2010, 1, 1),
            )
            assert ACTIVE_UNIT_PREDICATE in sql

    def test_the_unit_query_reads_core_and_never_the_marts(self):
        # The marts are current-date pivots and cannot answer an interval
        # (ADR 0003), so the timescope fetch reads Core only.
        sql, params = units_query(
            "generators",
            UNIT_COLUMNS_SQL,
            sources=("solar",),
            active_from=date(1990, 1, 1),
            active_to=date(2010, 1, 1),
        )
        assert sql.startswith("SELECT ") and "FROM core.generators" in sql
        assert "marts" not in sql
        assert set(params) == {"sources", "from", "to"}

    def test_the_unit_query_never_joins_source_memberships(self):
        # A unit a newer Source snapshot dropped stays in Core and stays
        # Active by its dates; membership absence must not filter it out.
        sql, _ = units_query(
            "generators",
            UNIT_COLUMNS_SQL,
            sources=("solar",),
            active_from=date(1990, 1, 1),
            active_to=date(2010, 1, 1),
        )
        assert "source_memberships" not in sql
        assert " JOIN " not in sql.upper()

    def test_area_column_broadcasts_the_area_attribute_as_name(self):
        sql, params = units_query(
            "generators",
            UNIT_COLUMNS_SQL,
            sources=("solar",),
            active_from=date(1990, 1, 1),
            active_to=date(2010, 1, 1),
            area_column="state",
            area_names=("Berlin", "Hamburg"),
        )
        assert "state AS name" in sql
        assert "AND state = ANY(:area_names)" in sql
        assert params["area_names"] == ["Berlin", "Hamburg"]

    def test_country_level_projects_no_area_alias_or_filter(self):
        sql, params = units_query(
            "generators",
            UNIT_COLUMNS_SQL,
            sources=("solar",),
            active_from=date(1990, 1, 1),
            active_to=date(2010, 1, 1),
            area_column=None,
            area_names=("Berlin",),
        )
        assert " AS name" not in sql
        assert "ANY(:area_names)" not in sql
        assert "area_names" not in params


class _RecordingFrameEngine:
    """Stub that hands units_query/run_frame calls back as recorded calls."""

    def __init__(self):
        self.calls = []

    @staticmethod
    def stub_run_frame(calls):
        def run_frame(engine, sql, params):
            calls.append((sql, params))
            return pd.DataFrame()
        return run_frame


class TestFetchUnits:
    def test_generators_once_and_storages_once(self, monkeypatch):
        engine = object()
        calls = []
        monkeypatch.setattr(
            data_module, "run_frame", _RecordingFrameEngine.stub_run_frame(calls)
        )
        fetch_units(
            engine,
            active_from=date(2020, 1, 1),
            active_to=date(2021, 1, 1),
            sources=("solar", "wind", "storage"),
            area_column="state",
        )
        assert [sql for sql, _ in calls] == [
            "SELECT energy_source, installed_capacity, commissioning_date, "
            "decommissioning_date, longitude, latitude, state, district, "
            "state AS name FROM core.generators "
            "WHERE energy_source = ANY(:sources) "
            "AND commissioning_date BETWEEN :from AND :to "
            "AND (decommissioning_date IS NULL OR decommissioning_date >= :to)",
            "SELECT energy_source, installed_capacity, commissioning_date, "
            "decommissioning_date, longitude, latitude, state, district, "
            "storage_capacity, state AS name FROM core.storages "
            "WHERE energy_source = ANY(:sources) "
            "AND commissioning_date BETWEEN :from AND :to "
            "AND (decommissioning_date IS NULL OR decommissioning_date >= :to)",
        ]
        assert [params for _, params in calls] == [
            {
                "sources": ["solar", "wind", "storage"],
                "from": date(2020, 1, 1),
                "to": date(2021, 1, 1),
            },
            {
                "sources": ["solar", "wind", "storage"],
                "from": date(2020, 1, 1),
                "to": date(2021, 1, 1),
            },
        ]

    def test_generator_only_sources_skip_storages(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            data_module, "run_frame", _RecordingFrameEngine.stub_run_frame(calls)
        )
        fetch_units(
            object(),
            active_from=date(2020, 1, 1),
            active_to=date(2021, 1, 1),
            sources=("bio",),
            area_column=None,
        )
        assert [sql for sql, _ in calls] == [
            "SELECT energy_source, installed_capacity, commissioning_date, "
            "decommissioning_date, longitude, latitude, state, district "
            "FROM core.generators "
            "WHERE energy_source = ANY(:sources) "
            "AND commissioning_date BETWEEN :from AND :to "
            "AND (decommissioning_date IS NULL OR decommissioning_date >= :to)"
        ]

    def test_area_filter_flows_into_every_table_query(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            data_module, "run_frame", _RecordingFrameEngine.stub_run_frame(calls)
        )
        fetch_units(
            object(),
            active_from=date(2020, 1, 1),
            active_to=date(2021, 1, 1),
            sources=("solar", "storage"),
            area_column="state",
            area_names=("Berlin",),
        )
        assert all(
            "AND state = ANY(:area_names)" in sql for sql, _ in calls
        )
        assert [params for _, params in calls] == [
            {
                "sources": ["solar", "storage"],
                "from": date(2020, 1, 1),
                "to": date(2021, 1, 1),
                "area_names": ["Berlin"],
            },
            {
                "sources": ["solar", "storage"],
                "from": date(2020, 1, 1),
                "to": date(2021, 1, 1),
                "area_names": ["Berlin"],
            },
        ]

    def test_empty_sources_run_no_queries(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            data_module, "run_frame", _RecordingFrameEngine.stub_run_frame(calls)
        )
        units = fetch_units(
            object(),
            active_from=date(2020, 1, 1),
            active_to=date(2021, 1, 1),
            sources=(),
            area_column="state",
        )
        assert calls == []
        assert units.empty
        assert set(units.columns) == set(UNIT_COLUMNS) | {"name"}

    def test_concatenates_both_table_frames(self, monkeypatch):
        frames = [
            pd.DataFrame(
                [{
                    "energy_source": "solar",
                    "installed_capacity": 120.0,
                    "commissioning_date": pd.Timestamp("2015-03-01"),
                    "decommissioning_date": None,
                    "longitude": 10.5,
                    "latitude": 50.5,
                    "state": "Bavaria",
                    "district": None,
                    "name": "Bavaria",
                }]
            ),
            pd.DataFrame(
                [{
                    "energy_source": "storage",
                    "installed_capacity": 300.0,
                    "commissioning_date": pd.Timestamp("2010-01-01"),
                    "decommissioning_date": None,
                    "longitude": 11.5,
                    "latitude": 51.5,
                    "state": "Berlin",
                    "district": "Berlin",
                    "name": "Berlin",
                    "storage_capacity": 800.0,
                }]
            ),
        ]
        monkeypatch.setattr(
            data_module,
            "run_frame",
            lambda engine, sql, params: frames.pop(0),
        )
        units = fetch_units(
            object(),
            active_from=date(2020, 1, 1),
            active_to=date(2021, 1, 1),
            sources=("solar", "storage"),
            area_column="state",
        )
        assert list(units["energy_source"]) == ["solar", "storage"]
        assert list(units["name"]) == ["Bavaria", "Berlin"]
        assert "storage_capacity" in units.columns
        assert pd.isna(units.loc[0, "storage_capacity"])


class TestRunQuery:
    def test_rows_come_back_as_dicts(self):
        rows = [_MappingRow(unit_id=1, energy_source="solar", installed_capacity=120.0)]
        result = run_query(
            _RowsEngine(rows),
            "SELECT ... FROM core.generators",
            {"source": "solar"},
        )
        assert result == [
            {"unit_id": 1, "energy_source": "solar", "installed_capacity": 120.0}
        ]


class TestUnitRecords:
    def test_dates_become_iso_strings_and_missing_becomes_none(self):
        frame = pd.DataFrame(
            [{
                "energy_source": "solar",
                "installed_capacity": 120.0,
                "commissioning_date": pd.Timestamp("2015-03-01"),
                "decommissioning_date": None,
                "longitude": 10.5,
                "latitude": 50.5,
                "state": None,
                "district": None,
                "name": None,
                "storage_capacity": float("nan"),
            }]
        )
        record = unit_records(frame)[0]
        assert record["commissioning_date"] == "2015-03-01"
        assert record["decommissioning_date"] is None
        assert record["name"] is None
        assert record["storage_capacity"] is None
        assert record["state"] is None

    def test_missing_dates_normalize_to_none(self):
        frame = pd.DataFrame(
            [{"commissioning_date": pd.NaT, "installed_capacity": 1.0}]
        )
        assert unit_records(frame)[0]["commissioning_date"] is None


class TestBoundariesQuery:
    def test_selects_name_area_and_stored_geojson_at_the_level(self):
        sql, params = boundaries_query(1)
        assert sql == (
            "SELECT name, area, geojson "
            "FROM service.boundaries WHERE level = :level ORDER BY name"
        )
        assert params == {"level": 1}

    def test_no_simplification_or_geoencoding_in_the_query(self):
        sql, _ = boundaries_query(3)
        assert "ST_SimplifyPreserveTopology" not in sql
        assert "ST_AsGeoJSON" not in sql
        assert "tolerance" not in sql


class TestFetchBoundaries:
    def test_returns_the_boundary_rows_untouched(self):
        rows = [
            _MappingRow(
                name="Berlin",
                area=891.0,
                geojson='{"type": "Polygon"}',
            )
        ]
        result = fetch_boundaries(_RowsEngine(rows), level=1)
        assert result == [{"name": "Berlin", "area": 891.0, "geojson": '{"type": "Polygon"}'}]


class TestBoundaryNamesQuery:
    def test_selects_the_levels_area_names_in_order(self):
        sql, params = boundary_names_query(1)
        assert sql == (
            "SELECT name FROM service.boundaries "
            "WHERE level = :level ORDER BY name"
        )
        assert params == {"level": 1}


class TestFetchAreaNames:
    def test_returns_the_levels_names_in_order(self):
        rows = [_MappingRow(name="Berlin"), _MappingRow(name="Hamburg")]
        assert fetch_area_names(_RowsEngine(rows), 1) == ["Berlin", "Hamburg"]