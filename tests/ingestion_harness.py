"""The ingestion seam the test suite shares: fake AWS over real PostGIS.

Three stand-ins carry the ingestion tests: a bucket that keeps immutable
object versions, a queue, and a notifier. The object bodies are the committed
GPKG fixtures. They live here rather than inside one test module, so the
acceptance walkthrough (`test_acceptance_workflow.py`) can drive the deployed
entry points -- `run_startup`, `run_worker`, `redrive` -- over the same
adapters the rest of the suite uses instead of a second copy that could drift.

Everything on the database side is real: the helpers below read the throwaway
schemas of `isolated_pipeline`, which stand in for the deployment's own
schemas, and they assert through the production checks
(`marts.verify_marts`, `verify._verify_boundaries`) rather than re-deriving
their rules.
"""

import contextlib
import itertools
import json
import os
import uuid
from pathlib import Path

import geopandas as gpd
import pandas
import pytest
from shapely.geometry import Point, box
from sqlalchemy import create_engine, text

from etl import (
    boundaries,
    db_utils,
    extract,
    ingestion,
    load,
    marts,
    transform,
    utils,
    verify,
)
from etl.config import SOURCE_NAMES


ENGINE = create_engine(os.environ["DATABASE_URL"])


# ------------------------------------------------------------------ #
#  AWS stand-ins                                                      #
# ------------------------------------------------------------------ #


class VersionedS3:
    """A versioned bucket: what was put under a key, and every version of it."""

    def __init__(self):
        self.current = {}
        self.objects = {}
        self.last_modified = {}
        self.reads = []
        # Per key, how many more reads raise instead of returning the body, so
        # a test can make one object version unreadable and then redeliver it.
        self.read_failures = {}

    def put(self, key: str, version_id: str, body: bytes) -> None:
        self.current[key] = version_id
        self.objects[version_id] = body

    def head_current(self, bucket: str, key: str):
        version_id = self.current.get(key)
        if version_id is None:
            return None
        return ingestion.S3ObjectId(
            bucket, key, version_id, self.last_modified.get(version_id)
        )

    def read_version(self, object_id: ingestion.S3ObjectId) -> bytes:
        self.reads.append(object_id)
        if self.read_failures.get(object_id.key, 0) > 0:
            self.read_failures[object_id.key] -= 1
            raise OSError(f"transient S3 read failure for {object_id.key}")
        return self.objects[object_id.version_id]


class FakeSQS:
    """The half of the queue the worker uses: acknowledge and visibility."""

    def __init__(self):
        self.deleted = []
        self.visibility = []

    def delete_message(self, receipt_handle: str) -> None:
        self.deleted.append(receipt_handle)

    def change_message_visibility(self, receipt_handle: str, timeout_seconds: int):
        self.visibility.append((receipt_handle, timeout_seconds))


class _Queued:
    """One message the queue holds, with the deliveries it has been given."""

    def __init__(self, body: str):
        self.body = body
        self.deliveries = 0


class QueuedSQS(FakeSQS):
    """The queue the deployed pipeline has: what startup sends, the worker takes.

    `RecordingSQS` only records what the startup path sent and `ScriptedSQS`
    replays a fixed script, so neither can carry a message across the two
    halves of the real loop. This one holds what it is sent, hands the oldest
    message to the worker, and only takes it off the queue when the worker
    deletes it.

    A message the worker did not acknowledge is released with a zero visibility
    timeout, which is how SQS makes it visible again, so the next delivery is
    the redelivery; after `MAX_DELIVERY_ATTEMPTS` of them the redrive policy
    moves the message to `dlq` instead, exactly as the real queue would.
    """

    def __init__(self):
        super().__init__()
        self.waiting = []
        self.in_flight = {}
        self.dlq = []
        self.receives = 0
        self._handles = itertools.count(1)

    @property
    def queued(self) -> int:
        """Messages waiting plus deliveries the worker has not settled yet."""
        return len(self.waiting) + len(self.in_flight)

    @property
    def settled(self) -> int:
        """Messages the worker deleted, i.e. the ones SQS no longer holds."""
        return len(self.deleted)

    def send_message(self, body: str) -> None:
        self.waiting.append(_Queued(body))

    def receive_message(self):
        self.receives += 1
        if not self.waiting:
            return None
        message = self.waiting.pop(0)
        message.deliveries += 1
        handle = f"receipt-{next(self._handles)}"
        self.in_flight[handle] = message
        return ingestion.SqsMessage(
            receipt_handle=handle,
            body=message.body,
            attempt=message.deliveries,
        )

    def delete_message(self, receipt_handle: str) -> None:
        super().delete_message(receipt_handle)
        self.in_flight.pop(receipt_handle, None)

    def change_message_visibility(self, receipt_handle: str, timeout_seconds: int):
        super().change_message_visibility(receipt_handle, timeout_seconds)
        released = self.in_flight.get(receipt_handle)
        if released is None or timeout_seconds:
            return
        del self.in_flight[receipt_handle]
        if released.deliveries >= ingestion.MAX_DELIVERY_ATTEMPTS:
            self.dlq.append(released)
        else:
            self.waiting.insert(0, released)


class FakeSNS:
    """The notifier: every message the pipeline would have published."""

    def __init__(self):
        self.messages = []

    def publish(self, subject: str, message: str) -> None:
        self.messages.append((subject, message))

    @property
    def bodies(self) -> str:
        return "\n".join(message for _, message in self.messages)


# ------------------------------------------------------------------ #
#  S3 notifications as the queue carries them                         #
# ------------------------------------------------------------------ #


def _s3_record(key: str, version_id: str):
    return {
        "eventSource": "aws:s3",
        "s3": {
            "bucket": {"name": "energy-data"},
            "object": {"key": key, "versionId": version_id},
        },
    }


def _message(handle: str, records) -> ingestion.SqsMessage:
    """One SQS message carrying one S3 record per (key, version) pair."""
    return ingestion.SqsMessage(
        receipt_handle=handle,
        body=json.dumps(
            {"Records": [_s3_record(key, version_id) for key, version_id in records]}
        ),
    )


def _records_message(handle, records):
    """One SQS message carrying one S3 record per (source, version) pair."""
    return _message(
        handle, [(f"sources/{source}.gpkg", version_id) for source, version_id in records]
    )


def _enqueue(queue, handle: str, records) -> None:
    """Put one S3 event on the queue, as S3's notification would."""
    queue.send_message(_message(handle, records).body)


# ------------------------------------------------------------------ #
#  The committed fixture data                                         #
# ------------------------------------------------------------------ #


FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
SOURCE_FIXTURE_DIR = os.path.join(FIXTURE_DIR, "sources")
BOUNDARY_FIXTURE_MANIFEST = os.path.join(FIXTURE_DIR, "boundaries", "manifest.txt")

_BOUNDARY_FIXTURES = {
    0: "germany_boundary",
    1: "germany_states",
    2: "germany_regions",
    3: "germany_districts",
}

# The Source column that carries a unit's capacity in the published GPKG.
_CAPACITY_COLUMN = {
    source: "gas_production_capacity" if source == "gas" else "installed_capacity"
    for source in SOURCE_NAMES
}

# One decomposed property every fixture of the source carries, so the test
# can prove the per-kind property tables are populated for every source.
_SIGNATURE_PROPERTY = {
    "bio": "biomass_type",
    "gas": "technology",
    "hydro": "hydro_type",
    "solar": "solar_type",
    "wind": "manufacturer",
    "storage": "technology",
}


def _kind(source):
    return load.STORAGE_KIND if source == "storage" else load.GENERATOR_KIND


def _core_kind(source):
    return _kind(source).core_table


def _properties_tables(source):
    kind = _kind(source)
    return kind.units_properties_table, kind.properties_table


def _fixture_frame(source):
    return gpd.read_file(os.path.join(SOURCE_FIXTURE_DIR, f"{source}.gpkg"))


def _boundary_frame(level):
    return gpd.read_file(
        os.path.join(FIXTURE_DIR, "boundaries", f"{_BOUNDARY_FIXTURES[level]}.gpkg")
    )


def _renamed_states():
    """Level 1 with Baden-Württemberg shrunk west of x=9.2, Bayern grown north and renamed."""
    states = _boundary_frame(1)
    bw = states["name"] == "Baden-Württemberg"
    bayern = states["name"] == "Bayern"
    states.loc[bw, "geometry"] = box(8.0, 47.5, 9.2, 49.0)
    states.loc[bayern, "geometry"] = box(10.0, 47.0, 13.0, 49.7)
    states.loc[bayern, "name"] = "Freistaat Bayern"
    return states


def _frame_bytes(tmp_path, name, frame):
    """Publish a frame as a single-layer GPKG whose layer name says nothing."""
    path = tmp_path / name
    frame.to_file(path, layer="published", driver="GPKG")
    return path.read_bytes()


def _write_mixed_source_snapshot(tmp_path, name):
    """A GPKG whose single layer claims two different Energy sources."""
    mixed = gpd.GeoDataFrame(
        [
            {
                "energy_source": label,
                "installed_capacity": 100.0,
                "commissioning_date": "2020-01-01",
                "decommissioning_date": None,
                "solar_type": "Utility",
                "area_id": None,
                "alignment": None,
                "inclination": None,
                "location": "Roof",
                "x_coordinates": 10.0,
                "y_coordinates": 50.0,
                "geo_accuracy": 1,
                "note": None,
                "reference_source": "test",
                "reference_id": "mixed",
                "reference_date": pandas.Timestamp("2024-01-01"),
                "geometry": Point(10.0, 50.0),
            }
            for label in ("Solar Energy", "Wind Energy")
        ],
        crs="EPSG:4326",
    )
    path = tmp_path / name
    mixed.to_file(path, layer="mixed_layer", driver="GPKG")
    return path.read_bytes()


# ------------------------------------------------------------------ #
#  What the database holds                                            #
# ------------------------------------------------------------------ #


def _query(sql, parameters=None):
    with ENGINE.connect() as connection:
        return connection.execute(text(sql), parameters or {}).mappings().all()


def _unit_key(reference_id, x, y):
    """A stable key for a unit: its Reference ID, or where it is when null."""
    if reference_id is None or pandas.isna(reference_id):
        return f"@{round(float(x), 6)},{round(float(y), 6)}"
    return str(reference_id)


def _frame_keys(frame):
    return [
        _unit_key(r, x, y)
        for r, x, y in zip(
            frame["reference_id"], frame["x_coordinates"], frame["y_coordinates"]
        )
    ]


def _raw_versions(schemas, source):
    """Raw snapshot tables extracted for one Source."""
    return len(
        _query(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name LIKE :prefix",
            {"schema": schemas["raw"], "prefix": f"{source}\\_%"},
        )
    )


def _run_ids(schemas, version_id):
    return {
        row["object_key"]: str(row["run_id"])
        for row in _query(
            f"SELECT run_id, object_key FROM {schemas['service']}.ingestion_runs "
            "WHERE object_version_id = :version_id",
            {"version_id": version_id},
        )
    }


def _runs_for_key(schemas, key):
    return [
        dict(row)
        for row in _query(
            f"SELECT state, attempt_count, terminal_error "
            f"FROM {schemas['service']}.ingestion_runs "
            f"WHERE object_key = :key",
            {"key": key},
        )
    ]


def _good_members(schemas, run_id):
    """Unit keys of the good-quality members of one Source snapshot."""
    source = _query(
        f"SELECT energy_source FROM {schemas['service']}.source_memberships "
        "WHERE run_id = :run_id LIMIT 1",
        {"run_id": run_id},
    )[0]["energy_source"]
    return {
        _unit_key(row["reference_id"], row["x_coordinates"], row["y_coordinates"])
        for row in _query(
            f"SELECT m.reference_id, s.x_coordinates, s.y_coordinates "
            f"FROM {schemas['service']}.source_memberships m "
            f"JOIN {schemas['stage']}.{source} s ON s.unit_id = m.unit_key "
            "WHERE m.run_id = :run_id AND NOT m.bad_quality",
            {"run_id": run_id},
        )
    }


def _core_units(schemas, source):
    """unit key -> (core unit_id, capacity) for one source's Core rows."""
    return {
        _unit_key(row["reference_id"], row["longitude"], row["latitude"]): (
            row["unit_id"],
            row["installed_capacity"],
        )
        for row in _query(
            f"SELECT unit_id, reference_id, longitude, latitude, installed_capacity "
            f"FROM {schemas['core']}.{_core_kind(source)} "
            "WHERE energy_source = :source",
            {"source": source},
        )
    }


def _geography(schemas, table_schema, table, source=None):
    """(energy_source, reference_id or coordinates) -> (state, region, district)."""
    x, y = ("x_coordinates", "y_coordinates") if table_schema == "stage" else (
        "longitude",
        "latitude",
    )
    where = "WHERE energy_source = :source" if source else ""
    return {
        (row["energy_source"], _unit_key(row["reference_id"], row["x"], row["y"])): (
            row["state"],
            row["region"],
            row["district"],
        )
        for row in _query(
            f"SELECT energy_source, reference_id, {x} AS x, {y} AS y, state, region, "
            f"district FROM {schemas[table_schema]}.{table} {where}",
            {"source": source},
        )
    }


def _boundary_snapshot(schemas):
    """Every boundary row as (level, name) -> (geometry hash, area, has geojson)."""
    return {
        (row["level"], row["name"]): (row["shape"], row["area"], row["geojson"])
        for row in _query(
            f"SELECT level, name, md5(ST_AsEWKB(geometry)) AS shape, area, "
            f"geojson IS NOT NULL AS geojson FROM {schemas['service']}.boundaries"
        )
    }


def _matviews(schemas):
    return _query(
        "SELECT count(*) AS n FROM pg_matviews WHERE schemaname = :schema "
        "AND matviewname IN ('installation_counts','generation_capacity',"
        "'storage_capacity')",
        {"schema": schemas["marts"]},
    )[0]["n"]


# ------------------------------------------------------------------ #
#  Publishing object versions and processing their events             #
# ------------------------------------------------------------------ #


def _publish_boundary_levels(s3, tmp_path, handle="startup"):
    """Put a valid GPKG at every fixed Boundary key as one object version."""
    for level in range(4):
        key = f"boundaries/level-{level}.gpkg"
        s3.put(
            key,
            f"{handle}-level-{level}",
            _frame_bytes(tmp_path, f"{handle}-l{level}.gpkg", _boundary_frame(level)),
        )


def _publish_source_snapshots(s3, tmp_path, handle="startup"):
    """Put a valid Source GPKG at every fixed Source key as one object version."""
    for source in SOURCE_NAMES:
        s3.put(
            f"sources/{source}.gpkg",
            f"{handle}-{source}",
            _frame_bytes(tmp_path, f"{handle}-{source}.gpkg", _fixture_frame(source)),
        )


def _publish_source_version(s3, tmp_path, source, version_id, frame):
    """Put one Source snapshot in the bucket; return the S3 event for it."""
    record = (f"sources/{source}.gpkg", version_id)
    s3.put(*record, _frame_bytes(tmp_path, f"{version_id}.gpkg", frame))
    return record


def _publish_boundary_releases(s3, tmp_path, releases, handle):
    """Put {level: frame} in the bucket as new object versions; return the events."""
    records = []
    for level, frame in releases.items():
        key = f"boundaries/level-{level}.gpkg"
        version_id = f"{handle}-level-{level}"
        s3.put(key, version_id, _frame_bytes(tmp_path, f"{version_id}.gpkg", frame))
        records.append((key, version_id))
    return records


def _deliver(pipeline, tmp_path, source, version_id, frame):
    """Publish a frame as a new object version and process its S3 event."""
    s3 = pipeline["s3"]
    s3.put(
        f"sources/{source}.gpkg",
        version_id,
        _frame_bytes(tmp_path, f"{version_id}.gpkg", frame),
    )
    return ingestion.process_one_message(
        _records_message(f"receipt-{version_id}", [(source, version_id)]),
        engine=ENGINE,
        s3=s3,
        sqs=pipeline["sqs"],
        sns=FakeSNS(),
        processor=pipeline["processor"],
    )


def _deliver_attempt(pipeline, message, *, sns, attempt=1):
    """Process one delivery of a message, as SQS would hand it to the worker."""
    return ingestion.process_one_message(
        ingestion.SqsMessage(
            receipt_handle=message.receipt_handle,
            body=message.body,
            attempt=attempt,
        ),
        engine=ENGINE,
        s3=pipeline["s3"],
        sqs=pipeline["sqs"],
        sns=sns,
        processor=pipeline["processor"],
    )


def _publish_all_sources(pipeline, tmp_path):
    s3 = pipeline["s3"]
    for source in SOURCE_NAMES:
        s3.put(
            f"sources/{source}.gpkg",
            f"{source}-v1",
            _frame_bytes(tmp_path, f"{source}-v1.gpkg", _fixture_frame(source)),
        )
    result = ingestion.process_one_message(
        _records_message("all-sources", [(s, f"{s}-v1") for s in SOURCE_NAMES]),
        engine=ENGINE,
        s3=s3,
        sqs=pipeline["sqs"],
        sns=FakeSNS(),
        processor=pipeline["processor"],
    )
    assert result.acknowledged


def _publish_boundaries(pipeline, tmp_path, releases, handle, processor=None):
    """Publish {level: frame} as new object versions in one message."""
    records = _publish_boundary_releases(
        pipeline["s3"], tmp_path, releases, handle
    )
    return ingestion.process_one_message(
        _message(handle, records),
        engine=ENGINE,
        s3=pipeline["s3"],
        sqs=pipeline["sqs"],
        sns=FakeSNS(),
        processor=processor or pipeline["processor"],
    )


# ------------------------------------------------------------------ #
#  The deployment the tests drive                                     #
# ------------------------------------------------------------------ #


class CountingProcessor(ingestion.PipelineProcessor):
    """The real processor, counting how often the shared marts refresh runs."""

    def __init__(self, engine, *, s3):
        super().__init__(engine, s3=s3)
        self.finalized = 0

    def finalize(self):
        self.finalized += 1
        return super().finalize()


@contextlib.contextmanager
def isolated_pipeline(tmp_path, monkeypatch):
    """The whole pipeline in throwaway schemas, with a fixture bucket.

    Every schema constant in the pipeline modules is pointed at a fresh set of
    schemas for the duration, so a test can load and query the real tables
    without touching another test's data. The bucket is a `VersionedS3` seeded
    at every fixed key with a bootstrap version, which is the state `bootstrap`
    expects a deployment to start from.
    """
    suffix = uuid.uuid4().hex
    schemas = {
        "raw": f"raw_test_{suffix}",
        "stage": f"stage_test_{suffix}",
        "core": f"core_test_{suffix}",
        "service": f"service_test_{suffix}",
        "marts": f"marts_test_{suffix}",
    }
    module_schemas = {
        ingestion: ("SERVICE_SCHEMA",),
        boundaries: ("SERVICE_SCHEMA", "STAGING_SCHEMA", "CORE_SCHEMA"),
        extract: ("RAW_SCHEMA", "SERVICE_SCHEMA"),
        transform: ("RAW_SCHEMA", "STAGING_SCHEMA", "SERVICE_SCHEMA"),
        load: ("STAGING_SCHEMA", "CORE_SCHEMA", "SERVICE_SCHEMA"),
        marts: ("CORE_SCHEMA", "MARTS_SCHEMA"),
        verify: (
            "RAW_SCHEMA",
            "STAGING_SCHEMA",
            "CORE_SCHEMA",
            "SERVICE_SCHEMA",
            "MARTS_SCHEMA",
        ),
        db_utils: ("RAW_SCHEMA", "STAGING_SCHEMA", "CORE_SCHEMA", "SERVICE_SCHEMA"),
        utils: ("RAW_SCHEMA", "SERVICE_SCHEMA"),
    }
    for module, names in module_schemas.items():
        for name in names:
            key = name.removesuffix("_SCHEMA").lower().replace("staging", "stage")
            monkeypatch.setattr(module, name, schemas[key])

    s3 = VersionedS3()
    for key in ingestion.BOUNDARY_KEYS + ingestion.SOURCE_KEYS:
        s3.current[key] = "bootstrap"
        s3.objects["bootstrap"] = b""
    ingestion.bootstrap(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
    )

    test_layer = gpd.GeoDataFrame(
        {
            "country_iso": ["DEU", "DEU", "DEU"],
            "name": ["Test State", "Test Region", "Test District"],
            "level": [1, 2, 3],
            "area": [1.0, 1.0, 1.0],
            "geometry": [box(9.0, 49.0, 11.0, 51.0)] * 3,
        },
        crs="EPSG:4326",
    )
    test_layer.to_postgis(
        "boundaries",
        ENGINE,
        schema=schemas["service"],
        if_exists="replace",
        index=False,
    )
    try:
        yield {
            "engine": ENGINE,
            "schemas": schemas,
            "s3": s3,
            "sqs": FakeSQS(),
            "processor": ingestion.PipelineProcessor(ENGINE, s3=s3),
        }
    finally:
        with ENGINE.begin() as connection:
            for schema in reversed(tuple(schemas.values())):
                connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def _load_boundary_fixtures():
    """Extract the committed Boundary fixtures into the current staging schema."""
    report = extract.extract_boundaries(Path(BOUNDARY_FIXTURE_MANIFEST), force=True)
    assert report.passed, report.errors


@pytest.fixture
def source_pipeline(tmp_path, monkeypatch):
    with isolated_pipeline(tmp_path, monkeypatch) as pipeline:
        yield pipeline


@pytest.fixture
def fixture_boundaries(source_pipeline):
    """Replace the single test box with the committed boundary fixtures.

    The Source fixtures sit inside these polygons, so every unit gets a real
    state/region/district and the marts pivot on real state names.
    """
    _load_boundary_fixtures()
    return source_pipeline
