import dataclasses
import json
import threading
import time
import uuid
from datetime import datetime, timezone

import geopandas as gpd
import pandas
from click.testing import CliRunner
import pytest
from shapely.geometry import Point
from sqlalchemy import text

from etl import boundaries, ingestion, load, marts, transform, verify
from etl.config import SOURCE_NAMES, STAGING_GENERATOR_SOURCES
import etl.__main__ as cli_module
from etl.__main__ import cli

# The fake AWS adapters, the fixture GPKG bodies and the throwaway-schema
# deployment every ingestion test runs against live in one module, so this file
# holds the behaviour it verifies and the acceptance walkthrough uses the same
# seam. An underscore marks a name internal to the suite, not to one test.
from ingestion_harness import (  # noqa: F401  (fixtures, resolved by pytest)
    _CAPACITY_COLUMN,
    _SIGNATURE_PROPERTY,
    CountingProcessor,
    ENGINE,
    FakeSQS,
    FakeSNS,
    VersionedS3,
    _boundary_frame,
    _boundary_snapshot,
    _core_kind,
    _core_units,
    _deliver,
    _deliver_attempt,
    _fixture_frame,
    _frame_bytes,
    _frame_keys,
    _geography,
    _good_members,
    _matviews,
    _message,
    _properties_tables,
    _publish_all_sources,
    _publish_boundaries,
    _publish_boundary_levels,
    _publish_source_snapshots,
    _query,
    _raw_versions,
    _records_message,
    _renamed_states,
    _run_ids,
    _runs_for_key,
    _unit_key,
    _write_mixed_source_snapshot,
    fixture_boundaries,
    source_pipeline,
)


def test_ingestion_identity_requires_immutable_s3_version():
    with pytest.raises(ValueError, match="immutable version id"):
        ingestion.S3ObjectId("energy-data", "sources/solar.gpkg", "null")


class FakeS3:
    def __init__(self, *missing_keys: str, current_version: str = "version-1"):
        self.missing_keys = set(missing_keys)
        self.current_version = current_version
        self.reads = []
        self.bodies = {}

    def head_current(self, bucket: str, key: str):
        if key in self.missing_keys:
            return None
        if key.startswith("boundaries/") or key.startswith("sources/"):
            return ingestion.S3ObjectId(bucket, key, self.current_version)
        return None

    def read_version(self, object_id: ingestion.S3ObjectId) -> bytes:
        self.reads.append(object_id)
        return self.bodies.get(object_id.key, b"version-42-content")


class ScriptedSQS(FakeSQS):
    """A queue the worker can receive from, one scripted receive per poll.

    `None` in the script stands for an empty receive, so a test can put an idle
    poll between two messages.
    """

    def __init__(self, *polls):
        super().__init__()
        self.polls = list(polls)
        self.receives = 0

    def receive_message(self):
        self.receives += 1
        if not self.polls:
            return None
        return self.polls.pop(0)


class RecordingSQS(FakeSQS):
    """A queue that also records what was sent to it (the startup/redrive path)."""

    def __init__(self):
        super().__init__()
        self.sent = []

    def send_message(self, body: str) -> None:
        self.sent.append(body)


class FakeProcessor:
    def __init__(self):
        self.objects = []
        self.finalized = 0

    def process(self, object_id, body: bytes):
        assert body == b"version-42-content"
        self.objects.append(object_id)
        return (
            ingestion.StageResult(
                target=object_id.key,
                stage="extract",
                outcome="succeeded",
                row_count=1,
            ),
        )

    def finalize(self):
        self.finalized += 1
        return (
            ingestion.StageResult(
                target="marts",
                stage="marts",
                outcome="succeeded",
                row_count=2,
            ),
        )


class FailingProcessor:
    def __init__(self):
        self.finalized = 0

    def process(self, object_id, body: bytes):
        results = (
            ingestion.StageResult(
                target=object_id.key,
                stage="extract",
                outcome="succeeded",
                row_count=3,
            ),
            ingestion.StageResult(
                target=object_id.key,
                stage="load",
                outcome="failed",
                row_count=0,
                error="load failed: boom",
            ),
        )
        raise ingestion.SourceSnapshotError(results, "load failed: boom")

    def finalize(self):
        self.finalized += 1
        return ()


def _two_record_message(handle: str = "receipt-1") -> ingestion.SqsMessage:
    return _message(
        handle,
        [("sources/solar.gpkg", "version-1"), ("sources/wind.gpkg", "version-1")],
    )


def test_marts_run_once_per_message_not_once_per_object(monkeypatch):
    schema = f"service_test_{uuid.uuid4().hex}"
    monkeypatch.setattr(ingestion, "SERVICE_SCHEMA", schema)
    s3 = FakeS3()
    sqs = FakeSQS()
    processor = FakeProcessor()

    try:
        ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=s3,
        )
        result = ingestion.process_one_message(
            _two_record_message(),
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
        )

        assert result.acknowledged
        assert [state.value for state in result.states] == [
            "succeeded",
            "succeeded",
        ]
        assert len(processor.objects) == 2
        # One refresh for the message...
        assert processor.finalized == 1
        with ENGINE.connect() as connection:
            marts_rows = connection.execute(
                text(
                    f"SELECT run_id, stage FROM {schema}.stage_results "
                    "WHERE stage = 'marts'"
                )
            ).all()
            runs = connection.execute(
                text(f"SELECT state FROM {schema}.ingestion_runs ORDER BY created_at")
            ).scalars().all()
        # ...recorded against every run that took part in it, so no run claims
        # completion without the marts step it shared.
        assert len(marts_rows) == 2
        assert len({row[0] for row in marts_rows}) == 2
        assert runs == ["succeeded", "succeeded"]
    finally:
        with ENGINE.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def test_partial_stage_results_survive_a_failed_object(monkeypatch):
    schema = f"service_test_{uuid.uuid4().hex}"
    monkeypatch.setattr(ingestion, "SERVICE_SCHEMA", schema)
    s3 = FakeS3()
    sqs = FakeSQS()
    processor = FailingProcessor()

    try:
        ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=s3,
        )
        message = ingestion.SqsMessage(
            receipt_handle="receipt-2",
            body=json.dumps(
                {
                    "Records": [
                        {
                            "eventSource": "aws:s3",
                            "s3": {
                                "bucket": {"name": "energy-data"},
                                "object": {
                                    "key": "sources/solar.gpkg",
                                    "versionId": "version-1",
                                },
                            },
                        }
                    ]
                }
            ),
        )

        result = ingestion.process_one_message(
            message,
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
        )

        assert not result.acknowledged
        assert [state.value for state in result.states] == ["retryable"]
        assert sqs.deleted == []
        assert processor.finalized == 0
        with ENGINE.connect() as connection:
            run = connection.execute(
                text(
                    f"SELECT state, terminal_error FROM {schema}.ingestion_runs"
                )
            ).mappings().one()
            stages = connection.execute(
                text(
                    f"SELECT stage, outcome, error FROM {schema}.stage_results "
                    "ORDER BY stage"
                )
            ).mappings().all()
        assert run["state"] == "retryable"
        assert "load failed" in run["terminal_error"]
        assert [dict(stage) for stage in stages] == [
            {"stage": "extract", "outcome": "succeeded", "error": None},
            {
                "stage": "load",
                "outcome": "failed",
                "error": "load failed: boom",
            },
        ]
    finally:
        with ENGINE.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def test_bootstrap_prepares_service_metadata_for_all_boundary_levels(monkeypatch):
    schema = f"service_test_{uuid.uuid4().hex}"
    monkeypatch.setattr(ingestion, "SERVICE_SCHEMA", schema)

    try:
        result = ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=FakeS3(),
        )

        assert result.metadata_ready
        assert result.worker_start_allowed
        assert [check.key for check in result.checks if check.required] == [
            "boundaries/level-0.gpkg",
            "boundaries/level-1.gpkg",
            "boundaries/level-2.gpkg",
            "boundaries/level-3.gpkg",
        ]
        with ENGINE.connect() as connection:
            tables = {
                row.name
                for row in connection.execute(
                    text(
                        "SELECT table_name AS name FROM information_schema.tables "
                        "WHERE table_schema = :schema"
                    ),
                    {"schema": schema},
                )
            }
        assert {
            "ingestion_runs",
            "loaded_files",
            "source_memberships",
            "stage_results",
        } <= tables
    finally:
        with ENGINE.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def test_worker_processes_exact_object_version_and_acknowledges_message(monkeypatch):
    schema = f"service_test_{uuid.uuid4().hex}"
    monkeypatch.setattr(ingestion, "SERVICE_SCHEMA", schema)
    s3 = FakeS3(current_version="version-42")
    sqs = FakeSQS()
    sns = FakeSNS()
    processor = FakeProcessor()

    try:
        ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=s3,
        )
        message = ingestion.SqsMessage(
            receipt_handle="receipt-1",
            body=json.dumps(
                {
                    "Records": [
                        {
                            "eventSource": "aws:s3",
                            "s3": {
                                "bucket": {"name": "energy-data"},
                                "object": {
                                    "key": "sources/solar.gpkg",
                                    "versionId": "version-42",
                                },
                            },
                        }
                    ]
                }
            ),
        )

        result = ingestion.process_one_message(
            message,
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=sns,
            processor=processor,
        )

        assert {state.value for state in ingestion.RunState} == {
            "pending",
            "running",
            "succeeded",
            "retryable",
            "terminal",
            "stale",
        }
        assert result.acknowledged
        assert sqs.deleted == ["receipt-1"]
        expected_object = ingestion.S3ObjectId(
            "energy-data", "sources/solar.gpkg", "version-42"
        )
        assert s3.reads == [expected_object]
        assert processor.objects == [expected_object]
        assert processor.finalized == 1
        with ENGINE.connect() as connection:
            run = connection.execute(
                text(
                    f"SELECT bucket, object_key, object_version_id, input_kind, state "
                    f"FROM {schema}.ingestion_runs"
                )
            ).mappings().one()
            stages = connection.execute(
                text(
                    f"SELECT target, stage, outcome, row_count "
                    f"FROM {schema}.stage_results ORDER BY stage"
                )
            ).mappings().all()
        assert dict(run) == {
            "bucket": "energy-data",
            "object_key": "sources/solar.gpkg",
            "object_version_id": "version-42",
            "input_kind": "source",
            "state": "succeeded",
        }
        assert [dict(stage) for stage in stages] == [
            {
                "target": "sources/solar.gpkg",
                "stage": "extract",
                "outcome": "succeeded",
                "row_count": 1,
            },
            {
                "target": "marts",
                "stage": "marts",
                "outcome": "succeeded",
                "row_count": 2,
            },
        ]
    finally:
        with ENGINE.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def test_bootstrap_blocks_worker_for_missing_boundary_but_not_missing_source(monkeypatch):
    schema = f"service_test_{uuid.uuid4().hex}"
    monkeypatch.setattr(ingestion, "SERVICE_SCHEMA", schema)

    try:
        source_missing = ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=FakeS3("sources/bio.gpkg"),
        )
        boundary_missing = ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=FakeS3("boundaries/level-2.gpkg"),
        )

        assert source_missing.worker_start_allowed
        assert next(
            check
            for check in source_missing.checks
            if check.key == "sources/bio.gpkg"
        ).message == "missing"
        assert not boundary_missing.worker_start_allowed
        assert next(
            check
            for check in boundary_missing.checks
            if check.key == "boundaries/level-2.gpkg"
        ).message == "missing"
    finally:
        with ENGINE.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def test_bootstrap_command_reports_fatal_boundary_result(monkeypatch):
    result = ingestion.BootstrapResult(
        metadata_ready=True,
        worker_start_allowed=False,
        checks=(
            ingestion.BootstrapCheck(
                key="boundaries/level-0.gpkg",
                required=True,
                available=False,
                object_id=None,
                message="missing",
            ),
        ),
    )
    monkeypatch.setattr("etl.__main__.get_engine", lambda: object())
    monkeypatch.setattr("etl.__main__._s3_adapter", lambda: object(), raising=False)
    monkeypatch.setattr(
        "etl.__main__.bootstrap_ingestion",
        lambda config, *, engine, s3: result,
        raising=False,
    )

    invocation = CliRunner().invoke(cli, ["bootstrap", "--bucket", "energy-data"])

    assert invocation.exit_code == 1
    assert "Metadata ready   : yes" in invocation.output
    assert "Worker start     : blocked" in invocation.output
    assert "boundaries/level-0.gpkg (required): missing" in invocation.output


def test_cli_keeps_local_stage_commands_and_run_all():
    invocation = CliRunner().invoke(cli, ["--help"])

    assert invocation.exit_code == 0
    for command in (
        "boundaries",
        "extract",
        "transform",
        "load",
        "marts",
        "run-all",
        "bootstrap",
        "worker",
        "startup",
        "redrive",
    ):
        assert command in invocation.output


def test_run_all_keeps_existing_stage_order(monkeypatch):
    calls = []
    monkeypatch.setattr(cli_module, "boundaries", lambda force: calls.append("boundaries"))
    monkeypatch.setattr(cli_module, "extract", lambda force: calls.append("extract"))
    monkeypatch.setattr(cli_module, "transform", lambda: calls.append("transform"))
    monkeypatch.setattr(cli_module, "load", lambda: calls.append("load"))
    monkeypatch.setattr(cli_module, "marts", lambda: calls.append("marts"))

    invocation = CliRunner().invoke(cli, ["run-all"])

    assert invocation.exit_code == 0
    assert calls == ["boundaries", "extract", "transform", "load", "marts"]


def _insert_run(schemas, object_id, state, terminal_error=None):
    """Insert an Ingestion run directly, without the worker processing it."""
    with ENGINE.begin() as connection:
        connection.execute(
            text(
                f"INSERT INTO {schemas['service']}.ingestion_runs "
                "(run_id, bucket, object_key, object_version_id, input_kind, state, "
                "terminal_error) "
                "VALUES (:run_id, :bucket, :object_key, :object_version_id, 'source', "
                ":state, :error)"
            ),
            {
                "run_id": str(uuid.uuid4()),
                "bucket": object_id.bucket,
                "object_key": object_id.key,
                "object_version_id": object_id.version_id,
                "state": state.value,
                "error": terminal_error,
            },
        )


def test_startup_applies_boundaries_then_enqueues_current_source_versions(
    fixture_boundaries, tmp_path
):
    schemas = fixture_boundaries["schemas"]
    s3 = fixture_boundaries["s3"]
    sqs = RecordingSQS()
    _publish_boundary_levels(s3, tmp_path)
    _publish_source_snapshots(s3, tmp_path)

    result = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        processor=fixture_boundaries["processor"],
    )

    assert result.worker_start_allowed
    assert result.metadata_ready
    assert result.enqueued == tuple(
        ingestion.S3ObjectId("energy-data", f"sources/{source}.gpkg", f"startup-{source}")
        for source in SOURCE_NAMES
    )
    assert result.known == ()
    # The four releases were applied and ledgered, and the layer is complete.
    # (The fixture's own filename-based load logged bucket-less rows; the
    # S3 releases are the ones with a bucket.)
    assert _query(
        f"SELECT count(*) AS n FROM {schemas['service']}.loaded_files "
        "WHERE bucket IS NOT NULL"
    ) == [{"n": 4}]
    assert verify._verify_boundaries(ENGINE) == []
    # The marts were refreshed once by the bootstrap itself.
    assert _matviews(schemas) == 3
    # One S3-record message per source version, in the worker's format.
    assert len(sqs.sent) == len(SOURCE_NAMES)
    assert json.loads(sqs.sent[0])["Records"][0]["s3"]["object"] == {
        "key": "sources/bio.gpkg",
        "versionId": "startup-bio",
    }
    # The bootstrap created no Ingestion run: a run belongs to the worker's
    # processing of the enqueued message, not to the enqueue.
    assert _query(f"SELECT 1 FROM {schemas['service']}.ingestion_runs") == []


def test_startup_refuses_the_worker_for_a_missing_boundary(fixture_boundaries, tmp_path):
    s3 = fixture_boundaries["s3"]
    sqs = RecordingSQS()
    _publish_boundary_levels(s3, tmp_path)
    del s3.current["boundaries/level-2.gpkg"]

    result = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        processor=fixture_boundaries["processor"],
    )

    assert not result.worker_start_allowed
    assert sqs.sent == []
    schemas = fixture_boundaries["schemas"]
    assert _query(
        f"SELECT 1 FROM {schemas['service']}.loaded_files WHERE bucket IS NOT NULL"
    ) == []


def test_startup_refuses_the_worker_for_an_invalid_boundary(fixture_boundaries, tmp_path):
    s3 = fixture_boundaries["s3"]
    sqs = RecordingSQS()
    _publish_boundary_levels(s3, tmp_path)
    wrong_level = _boundary_frame(3)
    wrong_level["level"] = 2  # published at the level-3 key
    s3.put(
        "boundaries/level-3.gpkg",
        "startup-bad-level",
        _frame_bytes(tmp_path, "bad-level.gpkg", wrong_level),
    )

    result = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        processor=fixture_boundaries["processor"],
    )

    assert not result.worker_start_allowed
    assert sqs.sent == []
    schemas = fixture_boundaries["schemas"]
    assert _query(
        f"SELECT 1 FROM {schemas['service']}.loaded_files WHERE bucket IS NOT NULL"
    ) == []


def test_startup_treats_a_missing_source_as_non_fatal(fixture_boundaries, tmp_path):
    s3 = fixture_boundaries["s3"]
    sqs = RecordingSQS()
    _publish_boundary_levels(s3, tmp_path)
    _publish_source_snapshots(s3, tmp_path)
    del s3.current["sources/bio.gpkg"]

    result = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        processor=fixture_boundaries["processor"],
    )

    assert result.worker_start_allowed
    assert result.enqueued == tuple(
        ingestion.S3ObjectId("energy-data", f"sources/{source}.gpkg", f"startup-{source}")
        for source in SOURCE_NAMES
        if source != "bio"
    )


def test_startup_does_not_reenqueue_versions_the_worker_settled(
    fixture_boundaries, tmp_path
):
    schemas = fixture_boundaries["schemas"]
    s3 = fixture_boundaries["s3"]
    sqs = RecordingSQS()
    processor = fixture_boundaries["processor"]
    _publish_boundary_levels(s3, tmp_path)
    _publish_source_snapshots(s3, tmp_path)

    first = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        processor=processor,
    )
    assert len(first.enqueued) == len(SOURCE_NAMES)

    # The worker drains the enqueued messages: every version ends succeeded.
    for index, body in enumerate(sqs.sent):
        processed = ingestion.process_one_message(
            ingestion.SqsMessage(receipt_handle=f"receipt-{index}", body=body),
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
        )
        assert processed.acknowledged

    second = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        processor=processor,
    )

    assert second.worker_start_allowed
    assert second.enqueued == ()
    assert len(second.known) == len(SOURCE_NAMES)
    assert len(sqs.sent) == len(SOURCE_NAMES)


@pytest.mark.parametrize(
    "state,error",
    [
        (ingestion.RunState.SUCCEEDED, None),
        (ingestion.RunState.STALE, "Superseded object version"),
        (ingestion.RunState.TERMINAL, "extract failed: rejected"),
        (
            ingestion.RunState.RETRYABLE,
            "delivery attempts exhausted after 5 deliveries, so the message "
            "goes to the DLQ",
        ),
    ],
)
def test_startup_does_not_reenqueue_a_known_version_in_any_settled_state(
    fixture_boundaries, tmp_path, state, error
):
    schemas = fixture_boundaries["schemas"]
    s3 = fixture_boundaries["s3"]
    sqs = RecordingSQS()
    _publish_boundary_levels(s3, tmp_path)
    _publish_source_snapshots(s3, tmp_path)
    object_id = ingestion.S3ObjectId("energy-data", "sources/wind.gpkg", "startup-wind")
    _insert_run(schemas, object_id, state, terminal_error=error)

    result = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket="energy-data"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        processor=fixture_boundaries["processor"],
    )

    assert result.worker_start_allowed
    assert object_id in result.known
    assert object_id not in result.enqueued
    # The other five sources are still unknown, so only wind's message is absent.
    assert len(sqs.sent) == len(SOURCE_NAMES) - 1
    assert all(
        json.loads(body)["Records"][0]["s3"]["object"]["key"] != object_id.key
        for body in sqs.sent
    )


def test_redrive_reenqueues_failed_and_dlq_versions_only(fixture_boundaries):
    schemas = fixture_boundaries["schemas"]
    sqs = RecordingSQS()
    succeeded = ingestion.S3ObjectId("energy-data", "sources/bio.gpkg", "v-succeeded")
    stale = ingestion.S3ObjectId("energy-data", "sources/gas.gpkg", "v-stale")
    terminal = ingestion.S3ObjectId("energy-data", "sources/hydro.gpkg", "v-terminal")
    failed = ingestion.S3ObjectId("energy-data", "sources/solar.gpkg", "v-failed")
    dlq = ingestion.S3ObjectId("energy-data", "sources/wind.gpkg", "v-dlq")
    _insert_run(schemas, succeeded, ingestion.RunState.SUCCEEDED)
    _insert_run(schemas, stale, ingestion.RunState.STALE, terminal_error="superseded")
    _insert_run(schemas, terminal, ingestion.RunState.TERMINAL, terminal_error="rejected")
    _insert_run(schemas, failed, ingestion.RunState.RETRYABLE, terminal_error="marts failed")
    _insert_run(
        schemas,
        dlq,
        ingestion.RunState.RETRYABLE,
        terminal_error="delivery attempts exhausted after 5 deliveries",
    )

    redriven = ingestion.redrive(engine=ENGINE, sqs=sqs)

    assert redriven == (failed, dlq)
    assert len(sqs.sent) == 2
    assert json.loads(sqs.sent[0])["Records"][0]["s3"]["object"] == {
        "key": "sources/solar.gpkg",
        "versionId": "v-failed",
    }


def test_redrive_with_a_key_narrows_to_that_key(fixture_boundaries):
    schemas = fixture_boundaries["schemas"]
    sqs = RecordingSQS()
    other = ingestion.S3ObjectId("energy-data", "sources/bio.gpkg", "v-failed")
    mine = ingestion.S3ObjectId("energy-data", "sources/solar.gpkg", "v-failed")
    _insert_run(schemas, other, ingestion.RunState.RETRYABLE)
    _insert_run(schemas, mine, ingestion.RunState.RETRYABLE)

    redriven = ingestion.redrive(engine=ENGINE, sqs=sqs, key="sources/solar.gpkg")

    assert redriven == (mine,)
    assert len(sqs.sent) == 1


def _startup_cli(monkeypatch, result):
    """The startup command with its AWS seam replaced by recorders."""
    calls = []
    monkeypatch.setattr("etl.__main__.get_engine", lambda: object())
    monkeypatch.setattr("etl.__main__._s3_adapter", lambda: object(), raising=False)
    monkeypatch.setattr("etl.__main__._sqs_adapter", lambda url: object(), raising=False)
    monkeypatch.setattr("etl.__main__._sns_adapter", lambda arn: object(), raising=False)
    monkeypatch.setattr(
        "etl.__main__.run_startup", lambda *args, **kwargs: calls.append("startup") or result
    )
    monkeypatch.setattr(
        "etl.__main__.run_worker", lambda **kwargs: calls.append("worker")
    )
    return calls


def test_startup_command_bootstraps_before_the_worker(monkeypatch):
    result = ingestion.StartupResult(
        metadata_ready=True,
        worker_start_allowed=True,
        checks=(),
        release_results=(),
        enqueued=(),
        known=(),
    )
    calls = _startup_cli(monkeypatch, result)

    invocation = CliRunner().invoke(
        cli,
        ["startup", "--bucket", "energy-data", "--queue-url", "https://queue", "--topic-arn", "arn"],
    )

    assert invocation.exit_code == 0
    assert calls == ["startup", "worker"]
    assert "Worker start     : allowed" in invocation.output


def test_startup_command_refuses_the_worker_after_a_fatal_boundary(monkeypatch):
    result = ingestion.StartupResult(
        metadata_ready=True,
        worker_start_allowed=False,
        checks=(),
        release_results=(),
        enqueued=(),
        known=(),
    )
    calls = _startup_cli(monkeypatch, result)

    invocation = CliRunner().invoke(
        cli,
        ["startup", "--bucket", "energy-data", "--queue-url", "https://queue", "--topic-arn", "arn"],
    )

    assert invocation.exit_code == 1
    assert calls == ["startup"]
    assert "Worker start     : blocked" in invocation.output


def test_redrive_command_reenqueues_failed_versions(monkeypatch):
    redriven = (ingestion.S3ObjectId("energy-data", "sources/solar.gpkg", "v-failed"),)
    monkeypatch.setattr("etl.__main__.get_engine", lambda: object())
    monkeypatch.setattr("etl.__main__._sqs_adapter", lambda url: object(), raising=False)
    monkeypatch.setattr(
        "etl.__main__.redrive_versions", lambda *args, **kwargs: redriven
    )

    invocation = CliRunner().invoke(cli, ["redrive", "--queue-url", "https://queue"])

    assert invocation.exit_code == 0
    assert "Redriven 1 object version(s)" in invocation.output
    assert "sources/solar.gpkg (v-failed)" in invocation.output


def _solar_snapshot(rows):
    records = []
    for row in rows:
        coordinates = row.get("coordinates", (10.0, 50.0))
        records.append(
            {
                "energy_source": row.get("energy_source", "Solar Energy"),
                "installed_capacity": row.get("installed_capacity", 100.0),
                "commissioning_date": row.get("commissioning_date", "2020-01-01"),
                "decommissioning_date": row.get("decommissioning_date"),
                "solar_type": row.get("solar_type", "Utility"),
                "area_id": None,
                "alignment": None,
                "inclination": None,
                "location": row.get("location", "Agrivoltaics"),
                "x_coordinates": coordinates[0],
                "y_coordinates": coordinates[1],
                "geo_accuracy": row.get("geo_accuracy", 1),
                "note": None,
                "reference_source": "test",
                "reference_id": row.get("reference_id"),
                "reference_date": pandas.Timestamp(row.get("reference_date", "2024-01-01")),
                "geometry": Point(*coordinates),
            }
        )
    return gpd.GeoDataFrame(records, crs="EPSG:4326")


def _write_source_snapshot(tmp_path, name, rows):
    path = tmp_path / name
    _solar_snapshot(rows).to_file(path, layer="content_layer", driver="GPKG")
    return path.read_bytes()


def _source_message(version_id, source="solar"):
    return _message(f"receipt-{version_id}", [(f"sources/{source}.gpkg", version_id)])


def _decomposed_properties(engine, core_schema, energy_source, reference_id):
    """The (name, value) whitelist links one core unit keeps after a load."""
    with engine.connect() as connection:
        return {
            row[0]: row[1]
            for row in connection.execute(
                text(
                    f"SELECT p.name, p.value "
                    f"FROM {core_schema}.generators g "
                    f"JOIN {core_schema}.generator_units_properties up "
                    f"ON up.unit_id = g.unit_id "
                    f"JOIN {core_schema}.generator_properties p "
                    f"ON p.prop_id = up.prop_id "
                    f"WHERE g.energy_source = :source AND g.reference_id = :ref"
                ),
                {"source": energy_source, "ref": reference_id},
            )
        }


def _write_storage_snapshot(tmp_path, name, rows):
    records = []
    for row in rows:
        coordinates = row.get("coordinates", (12.0, 52.0))
        records.append(
            {
                "energy_source": "Energy Storage",
                "storage_type": row.get("storage_type", "Battery"),
                "storage_capacity": row.get("storage_capacity", 5.0),
                "installed_capacity": row.get("storage_capacity", 5.0),
                "commissioning_date": "2021-01-01",
                "decommissioning_date": None,
                "location": row.get("location", "Grid"),
                "x_coordinates": coordinates[0],
                "y_coordinates": coordinates[1],
                "geo_accuracy": 1,
                "note": None,
                "reference_source": "test",
                "reference_id": row.get("reference_id"),
                "reference_date": pandas.Timestamp(row.get("reference_date", "2024-01-01")),
                "geometry": Point(*coordinates),
            }
        )
    path = tmp_path / name
    gpd.GeoDataFrame(records, crs="EPSG:4326").to_file(
        path, layer="storage_layer", driver="GPKG"
    )
    return path.read_bytes()


def _storage_message(version_id, key="sources/storage.gpkg"):
    return _message(f"receipt-{version_id}", [(key, version_id)])


def test_worker_loads_a_storage_snapshot_into_the_storage_kind(source_pipeline, tmp_path):
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]
    body = _write_storage_snapshot(
        tmp_path,
        "storage-1.gpkg",
        [
            {"reference_id": "bess-1", "storage_capacity": 4.0},
            {"reference_id": None, "storage_capacity": 9.0, "coordinates": (12.5, 52.5)},
        ],
    )

    s3.put("sources/storage.gpkg", "storage-1", body)
    result = ingestion.process_one_message(
        _storage_message("storage-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert result.acknowledged
    with ENGINE.connect() as connection:
        storages = connection.execute(
            text(
                f"SELECT reference_id, storage_type, storage_capacity "
                f"FROM {schemas['core']}.storages ORDER BY unit_id"
            )
        ).mappings().all()
        run = connection.execute(
            text(
                f"SELECT input_kind, state FROM {schemas['service']}.ingestion_runs"
            )
        ).mappings().one()
        stages = {
            row["stage"]
            for row in connection.execute(
                text(
                    f"SELECT stage FROM {schemas['service']}.stage_results"
                )
            ).mappings()
        }
    assert [row["storage_type"] for row in storages] == ["Battery", "Battery"]
    assert [row["storage_capacity"] for row in storages] == [4.0, 9.0]
    assert [row["reference_id"] for row in storages][0] == "bess-1"
    assert [row["reference_id"] for row in storages][1] is None
    assert dict(run) == {"input_kind": "source", "state": "succeeded"}
    assert stages == {"extract", "transform", "load", "marts"}


def test_worker_rejects_an_invalid_snapshot_without_touching_the_database(
    source_pipeline, tmp_path
):
    initial = _write_source_snapshot(
        tmp_path,
        "good.gpkg",
        [{"reference_id": "keep", "installed_capacity": 100.0}],
    )
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]

    s3.put("sources/solar.gpkg", "good-1", initial)
    first = ingestion.process_one_message(
        _source_message("good-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )
    assert first.acknowledged

    s3.put("sources/solar.gpkg", "bad-1", _write_mixed_source_snapshot(tmp_path, "invalid.gpkg"))

    result = ingestion.process_one_message(
        _source_message("bad-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert result.acknowledged
    assert result.states == (ingestion.RunState.TERMINAL,)
    assert sqs.deleted == ["receipt-good-1", "receipt-bad-1"]
    with ENGINE.connect() as connection:
        core = connection.execute(
            text(
                f"SELECT reference_id, installed_capacity "
                f"FROM {schemas['core']}.generators"
            )
        ).mappings().all()
        raw_tables = connection.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema"
            ),
            {"schema": schemas["raw"]},
        ).scalars().all()
        bad_run = connection.execute(
            text(
                f"SELECT run_id, state, current_stage "
                f"FROM {schemas['service']}.ingestion_runs "
                "WHERE object_version_id = 'bad-1'"
            )
        ).mappings().one()
        bad_stages = connection.execute(
            text(
                f"SELECT stage, outcome, error FROM {schemas['service']}.stage_results "
                "WHERE run_id = :run_id"
            ),
            {"run_id": str(bad_run["run_id"])},
        ).mappings().all()
    assert [dict(row) for row in core] == [
        {"reference_id": "keep", "installed_capacity": 100.0}
    ]
    assert len(raw_tables) == 1
    assert dict(bad_run)["state"] == "terminal"
    assert dict(bad_run)["current_stage"] == "extract"
    # The rejection is explained by a stage result, not a bare retry.
    assert [row["stage"] for row in bad_stages] == ["extract"]
    assert bad_stages[0]["outcome"] == "failed"
    assert bad_stages[0]["error"] == (
        "Source GPKG requires homogeneous Energy source"
    )


def test_worker_keeps_core_when_a_unit_turns_bad_quality(source_pipeline, tmp_path):
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]
    healthy = _write_source_snapshot(
        tmp_path,
        "healthy.gpkg",
        [
            {"reference_id": "stable", "installed_capacity": 100.0},
            {"reference_id": "degrades", "installed_capacity": 80.0},
        ],
    )
    degraded = _write_source_snapshot(
        tmp_path,
        "degraded.gpkg",
        [
            {"reference_id": "stable", "installed_capacity": 100.0},
            {"reference_id": "degrades", "installed_capacity": None},
        ],
    )

    s3.put("sources/solar.gpkg", "healthy-1", healthy)
    assert ingestion.process_one_message(
        _source_message("healthy-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    ).acknowledged

    s3.put("sources/solar.gpkg", "degraded-1", degraded)
    result = ingestion.process_one_message(
        _source_message("degraded-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert result.acknowledged
    with ENGINE.connect() as connection:
        core = {
            row["reference_id"]: row["installed_capacity"]
            for row in connection.execute(
                text(
                    f"SELECT reference_id, installed_capacity "
                    f"FROM {schemas['core']}.generators"
                )
            ).mappings()
        }
        membership = {
            row["reference_id"]: row["bad_quality"]
            for row in connection.execute(
                text(
                    f"SELECT reference_id, bad_quality "
                    f"FROM {schemas['service']}.source_memberships "
                    "WHERE run_id = (SELECT run_id FROM "
                    f"{schemas['service']}.ingestion_runs "
                    "WHERE object_version_id = 'degraded-1')"
                )
            ).mappings()
        }
    assert core == {"stable": 100.0, "degrades": 80.0}
    assert membership == {"stable": False, "degrades": True}


def test_worker_marks_every_run_retryable_when_shared_marts_fail(monkeypatch):
    """A shared marts failure leaves no run in the message complete."""
    schema = f"service_test_{uuid.uuid4().hex}"
    monkeypatch.setattr(ingestion, "SERVICE_SCHEMA", schema)
    s3 = FakeS3()
    sqs = FakeSQS()
    processor = FakeProcessor()

    class FailingMarts(FakeProcessor):
        def finalize(self):
            self.finalized += 1
            result = ingestion.StageResult(
                target="marts",
                stage="marts",
                outcome="failed",
                error="refresh failed",
            )
            raise ingestion.SourceSnapshotError(
                (result,), "marts failed: refresh failed"
            )

    processor = FailingMarts()
    try:
        ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=s3,
        )
        result = ingestion.process_one_message(
            _two_record_message(),
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
        )

        assert not result.acknowledged
        assert [state.value for state in result.states] == [
            "retryable",
            "retryable",
        ]
        assert processor.finalized == 1
        with ENGINE.connect() as connection:
            runs = connection.execute(
                text(
                    f"SELECT object_key, state, current_stage "
                    f"FROM {schema}.ingestion_runs ORDER BY object_key"
                )
            ).mappings().all()
            marts_per_run = connection.execute(
                text(
                    f"SELECT run_id, outcome FROM {schema}.stage_results "
                    "WHERE stage = 'marts' ORDER BY run_id"
                )
            ).all()
        assert [dict(row) for row in runs] == [
            {
                "object_key": "sources/solar.gpkg",
                "state": "retryable",
                "current_stage": "marts",
            },
            {
                "object_key": "sources/wind.gpkg",
                "state": "retryable",
                "current_stage": "marts",
            },
        ]
        assert [row[1] for row in marts_per_run] == ["failed", "failed"]
    finally:
        with ENGINE.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def test_worker_keeps_stage_results_when_marts_fail(source_pipeline, tmp_path, monkeypatch):
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]
    body = _write_source_snapshot(
        tmp_path, "marts-fail.gpkg", [{"reference_id": "only", "installed_capacity": 5.0}]
    )

    def failing_marts(engine=None):
        report = marts.MartsReport()
        report.errors = ["materialized view refresh failed"]
        return report

    monkeypatch.setattr(ingestion, "build_marts", failing_marts)
    s3.put("sources/solar.gpkg", "marts-fail-1", body)

    result = ingestion.process_one_message(
        _source_message("marts-fail-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert not result.acknowledged
    assert result.states == (ingestion.RunState.RETRYABLE,)
    with ENGINE.connect() as connection:
        run = connection.execute(
            text(
                f"SELECT state, current_stage FROM {schemas['service']}.ingestion_runs "
                "WHERE object_version_id = 'marts-fail-1'"
            )
        ).mappings().one()
        stages = connection.execute(
            text(
                f"SELECT stage, outcome FROM {schemas['service']}.stage_results "
                "ORDER BY stage"
            )
        ).mappings().all()
    assert dict(run) == {"state": "retryable", "current_stage": "marts"}
    assert [dict(stage) for stage in stages] == [
        {"stage": "extract", "outcome": "succeeded"},
        {"stage": "load", "outcome": "succeeded"},
        {"stage": "marts", "outcome": "failed"},
        {"stage": "transform", "outcome": "succeeded"},
    ]


def test_worker_keeps_a_message_with_an_unreadable_record(monkeypatch):
    """A record that cannot be read as an S3 identity is not dropped silently."""
    schema = f"service_test_{uuid.uuid4().hex}"
    monkeypatch.setattr(ingestion, "SERVICE_SCHEMA", schema)
    s3 = FakeS3()
    sqs = FakeSQS()
    processor = FakeProcessor()
    message = ingestion.SqsMessage(
        receipt_handle="receipt-1",
        body=json.dumps(
            {
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "s3": {
                            "bucket": {"name": "energy-data"},
                            "object": {
                                "key": "sources/solar.gpkg",
                                "versionId": "version-1",
                            },
                        },
                    },
                    {"eventSource": "aws:s3", "s3": {"bucket": {"name": "energy-data"}}},
                ]
            }
        ),
    )

    try:
        ingestion.bootstrap(
            ingestion.BootstrapConfig(bucket="energy-data"),
            engine=ENGINE,
            s3=s3,
        )
        result = ingestion.process_one_message(
            message,
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
        )

        # The readable record still loads, but the message is left for
        # redelivery rather than deleted with a record nobody claimed.
        assert [state.value for state in result.states] == ["succeeded"]
        assert not result.acknowledged
        assert sqs.deleted == []
    finally:
        with ENGINE.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))


def test_worker_ignores_events_for_unaccepted_keys(source_pipeline, tmp_path):
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    body = _write_source_snapshot(tmp_path, "stray.gpkg", [{"reference_id": "stray"}])
    s3.put("random/whatever.gpkg", "stray-1", body)
    message = ingestion.SqsMessage(
        receipt_handle="receipt-stray",
        body=json.dumps(
            {
                "Records": [
                    {
                        "eventSource": "aws:s3",
                        "s3": {
                            "bucket": {"name": "energy-data"},
                            "object": {
                                "key": "random/whatever.gpkg",
                                "versionId": "stray-1",
                            },
                        },
                    }
                ]
            }
        ),
    )

    result = ingestion.process_one_message(
        message,
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert result.acknowledged
    assert result.states == ()
    assert s3.reads == []
    with ENGINE.connect() as connection:
        runs = connection.execute(
            text(
                f"SELECT run_id FROM {source_pipeline['schemas']['service']}"
                ".ingestion_runs"
            )
        ).scalars().all()
    assert list(runs) == []
    assert sqs.deleted == ["receipt-stray"]


def test_worker_marks_a_delayed_version_stale_after_a_newer_one_loaded(
    source_pipeline, tmp_path
):
    """A queued event for an older version never overwrites a newer snapshot.

    A lifecycle rule can remove the version that was ingested, leaving the older
    one current again, so S3 starts serving it; the event for it that was still
    queued arrives afterwards and is marked stale rather than loaded on top.
    """
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]
    s3.last_modified["v-old"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    s3.last_modified["v-new"] = datetime(2026, 2, 3, tzinfo=timezone.utc)

    current = _write_source_snapshot(
        tmp_path, "current.gpkg", [{"reference_id": "row", "installed_capacity": 2.0}]
    )
    s3.put("sources/solar.gpkg", "v-new", current)
    second = ingestion.process_one_message(
        _source_message("v-new"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )
    assert second.states == (ingestion.RunState.SUCCEEDED,)

    # v-new is gone, so S3 serves the older version and its event turns up.
    older = _write_source_snapshot(
        tmp_path, "older.gpkg", [{"reference_id": "row", "installed_capacity": 1.0}]
    )
    s3.put("sources/solar.gpkg", "v-old", older)
    reads_before = len(s3.reads)
    delayed = ingestion.process_one_message(
        _source_message("v-old"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert delayed.acknowledged
    assert delayed.states == (ingestion.RunState.STALE,)
    assert len(s3.reads) == reads_before
    with ENGINE.connect() as connection:
        old_run = connection.execute(
            text(
                f"SELECT state, terminal_error FROM {schemas['service']}.ingestion_runs "
                "WHERE object_version_id = 'v-old'"
            )
        ).mappings().one()
    assert dict(old_run) == {
        "state": "stale",
        "terminal_error": (
            "Delayed object version v-old of sources/solar.gpkg "
            "is older than already-loaded v-new"
        ),
    }


def test_worker_keeps_other_sources_properties_when_loading_the_kind(
    source_pipeline, tmp_path
):
    """A whole-kind snapshot load must not strip the other sources' links.

    The property transfer deletes the whitelist links of every unit the load
    upserted, so it has to refill them from the whole kind, not just from the
    snapshot's own source.
    """
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]

    wind = _write_source_snapshot(
        tmp_path,
        "wind.gpkg",
        [
            {
                "energy_source": "Wind Energy",
                "reference_id": "wind-1",
                "location": "Offshore",
                "solar_type": None,
                "coordinates": (12.0, 52.0),
            }
        ],
    )
    s3.put("sources/wind.gpkg", "wind-1", wind)
    assert ingestion.process_one_message(
        _source_message("wind-1", source="wind"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    ).acknowledged
    before = _decomposed_properties(ENGINE, schemas["core"], "wind", "wind-1")
    assert before["location"] == "Offshore"

    solar = _write_source_snapshot(
        tmp_path, "solar.gpkg", [{"reference_id": "solar-1", "location": "Roof"}]
    )
    s3.put("sources/solar.gpkg", "solar-1", solar)
    assert ingestion.process_one_message(
        _source_message("solar-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    ).acknowledged

    after = _decomposed_properties(ENGINE, schemas["core"], "wind", "wind-1")
    assert after == before
    assert _decomposed_properties(ENGINE, schemas["core"], "solar", "solar-1")


def test_worker_keeps_a_redelivered_succeeded_version_succeeded(
    source_pipeline, tmp_path
):
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]
    s3.last_modified["v-first"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = _write_source_snapshot(
        tmp_path, "first.gpkg", [{"reference_id": "row", "installed_capacity": 1.0}]
    )
    second = _write_source_snapshot(
        tmp_path, "second.gpkg", [{"reference_id": "row", "installed_capacity": 2.0}]
    )
    s3.put("sources/solar.gpkg", "v-first", first)
    assert ingestion.process_one_message(
        _source_message("v-first"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    ).acknowledged

    s3.put("sources/solar.gpkg", "v-second", second)
    assert ingestion.process_one_message(
        _source_message("v-second"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    ).acknowledged

    redelivered = ingestion.process_one_message(
        _source_message("v-first"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert redelivered.acknowledged
    assert redelivered.states == (ingestion.RunState.SUCCEEDED,)
    with ENGINE.connect() as connection:
        first_run = connection.execute(
            text(
                f"SELECT state, terminal_error FROM {schemas['service']}.ingestion_runs "
                "WHERE object_version_id = 'v-first'"
            )
        ).mappings().one()
        capacity = connection.execute(
            text(
                f"SELECT installed_capacity FROM {schemas['core']}.generators"
            )
        ).scalar()
    assert dict(first_run) == {"state": "succeeded", "terminal_error": None}
    assert capacity == 2.0


def test_worker_processes_authoritative_solar_snapshots_with_lineage(source_pipeline, tmp_path):
    initial = _write_source_snapshot(
        tmp_path,
        "version-1.gpkg",
        [
            {
                "reference_id": "keep",
                "installed_capacity": 100.0,
                "reference_date": "2024-01-01",
                "coordinates": (10.0, 50.0),
            },
            {
                "energy_source": "Solar energy",
                "reference_id": "reappear",
                "installed_capacity": 200.0,
                "reference_date": "2024-02-01",
                "coordinates": (10.1, 50.0),
            },
            {
                "reference_id": None,
                "installed_capacity": 50.0,
                "reference_date": "2023-01-01",
                "coordinates": (10.2, 50.0),
            },
        ],
    )
    newer = _write_source_snapshot(
        tmp_path,
        "version-2.gpkg",
        [
            {
                "reference_id": "keep",
                "installed_capacity": 150.0,
                "reference_date": "2000-01-01",
                "coordinates": (10.0, 50.0),
            },
            {
                "reference_id": "new-bad",
                "installed_capacity": None,
                "reference_date": "2025-01-01",
                "coordinates": (10.3, 50.0),
            },
        ],
    )
    latest = _write_source_snapshot(
        tmp_path,
        "version-3.gpkg",
        [
            {
                "reference_id": "keep",
                "installed_capacity": 160.0,
                "reference_date": "1999-01-01",
                "coordinates": (10.0, 50.0),
            },
            {
                "reference_id": "reappear",
                "installed_capacity": 225.0,
                "reference_date": "1998-01-01",
                "coordinates": (10.1, 50.0),
            },
            {
                "reference_id": None,
                "installed_capacity": 75.0,
                "reference_date": "1997-01-01",
                "coordinates": (10.2, 50.0),
            },
        ],
    )
    delayed = _write_source_snapshot(
        tmp_path,
        "version-0.gpkg",
        [{"reference_id": "never-seen", "coordinates": (10.4, 50.0)}],
    )
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]
    for version_id, body in (
        ("version-1", initial),
        ("version-2", newer),
    ):
        s3.put("sources/solar.gpkg", version_id, body)
        result = ingestion.process_one_message(
            _source_message(version_id),
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
        )
        assert result.acknowledged
        assert result.states == (ingestion.RunState.SUCCEEDED,)

    def rows(sql, parameters=None):
        with ENGINE.connect() as connection:
            return connection.execute(text(sql), parameters or {}).mappings().all()

    run_ids = {
        row["object_version_id"]: row["run_id"]
        for row in rows(
            f"SELECT run_id, object_version_id FROM {schemas['service']}.ingestion_runs"
        )
    }
    initial_members = {
        row["unit_key"]: row["bad_quality"]
        for row in rows(
            f"SELECT unit_key, bad_quality FROM {schemas['service']}.source_memberships "
            "WHERE run_id = :run_id",
            {"run_id": run_ids["version-1"]},
        )
    }
    newer_members = {
        row["reference_id"]: row["bad_quality"]
        for row in rows(
            f"SELECT reference_id, bad_quality FROM {schemas['service']}.source_memberships "
            "WHERE run_id = :run_id",
            {"run_id": run_ids["version-2"]},
        )
    }
    assert len(initial_members) == 3
    assert newer_members == {"keep": False, "new-bad": True}

    raw_tables = rows(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = :schema ORDER BY table_name",
        {"schema": schemas["raw"]},
    )
    ledger = rows(
        f"SELECT object_version_id, loaded_to FROM {schemas['service']}.loaded_files "
        "WHERE object_key = 'sources/solar.gpkg' ORDER BY loaded_at"
    )
    assert len(raw_tables) == 2
    assert {row["object_version_id"] for row in ledger} == {
        "version-1",
        "version-2",
    }
    assert {row["loaded_to"] for row in ledger} == {
        row["table_name"] for row in raw_tables
    }

    core_after_newer = {
        row["reference_id"] or row["unit_id"]: row["installed_capacity"]
        for row in rows(
            f"SELECT unit_id, reference_id, installed_capacity "
            f"FROM {schemas['core']}.generators"
        )
    }
    assert len(core_after_newer) == 3
    assert core_after_newer["keep"] == 150.0
    assert core_after_newer["reappear"] == 200.0
    assert "new-bad" not in core_after_newer
    assert rows(
        f"SELECT state, energy_source, installation_count "
        f"FROM {schemas['marts']}.installation_counts "
        "WHERE energy_source = 'solar'"
    )[0]["installation_count"] == 3
    assert float(
        rows(
            f"SELECT generation_capacity FROM {schemas['marts']}.generation_capacity "
            "WHERE energy_source = 'solar'"
        )[0]["generation_capacity"]
    ) == 400.0

    synthetic_identity = next(
        unit_key
        for unit_key in initial_members
        if unit_key.startswith("syn_")
    )
    synthetic_core_id = rows(
        f"SELECT unit_id FROM {schemas['core']}.generators "
        "WHERE reference_id IS NULL"
    )[0]["unit_id"]

    s3.put("sources/solar.gpkg", "version-3", latest)
    latest_result = ingestion.process_one_message(
        _source_message("version-3"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )
    assert latest_result.acknowledged
    assert latest_result.states == (ingestion.RunState.SUCCEEDED,)
    assert len(
        rows(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema",
            {"schema": schemas["raw"]},
        )
    ) == 3

    s3.put("sources/solar.gpkg", "version-0", delayed)
    s3.current["sources/solar.gpkg"] = "version-3"
    stale_result = ingestion.process_one_message(
        _source_message("version-0"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )
    assert stale_result.acknowledged
    assert stale_result.states == (ingestion.RunState.STALE,)
    assert len(
        rows(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema",
            {"schema": schemas["raw"]},
        )
    ) == 3

    s3.put("sources/solar.gpkg", "version-3", latest)
    duplicate_result = ingestion.process_one_message(
        _source_message("version-3"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )
    assert duplicate_result.acknowledged
    assert duplicate_result.states == (ingestion.RunState.SUCCEEDED,)
    assert len(
        rows(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema",
            {"schema": schemas["raw"]},
        )
    ) == 3

    core_after_latest = {
        row["reference_id"] or row["unit_id"]: row["installed_capacity"]
        for row in rows(
            f"SELECT unit_id, reference_id, installed_capacity "
            f"FROM {schemas['core']}.generators"
        )
    }
    assert len(core_after_latest) == 3
    assert core_after_latest["keep"] == 160.0
    assert core_after_latest["reappear"] == 225.0
    assert synthetic_identity in initial_members
    assert {
        row["unit_id"]
        for row in rows(
            f"SELECT unit_id FROM {schemas['core']}.generators "
            "WHERE reference_id IS NULL"
        )
    } == {synthetic_core_id}
    assert {row["state"] for row in rows(
        f"SELECT state FROM {schemas['service']}.ingestion_runs"
    )} == {"succeeded", "stale"}
    assert {row["stage"] for row in rows(
        f"SELECT stage FROM {schemas['service']}.stage_results "
        f"WHERE run_id = :run_id",
        {"run_id": run_ids["version-2"]},
    )} >= {"extract", "transform", "load", "marts"}


# ------------------------------------------------------------------ #
#  Every Source dataset and both Core kinds (issue #4)                 #
# ------------------------------------------------------------------ #

def test_worker_publishes_every_source_dataset_into_its_core_kind(
    fixture_boundaries, tmp_path
):
    """One message carrying all six Source datasets loads both Core kinds.

    Each record is routed from its GPKG content, transformed from the exact raw
    table its own extraction wrote, and loaded into the Core kind it belongs to;
    the three marts are refreshed once for the message and reconcile to Core.
    """
    s3 = fixture_boundaries["s3"]
    sqs = fixture_boundaries["sqs"]
    schemas = fixture_boundaries["schemas"]
    processor = CountingProcessor(ENGINE, s3=s3)
    frames = {source: _fixture_frame(source) for source in SOURCE_NAMES}
    for source, frame in frames.items():
        s3.put(
            f"sources/{source}.gpkg",
            f"{source}-v1",
            _frame_bytes(tmp_path, f"{source}.gpkg", frame),
        )

    result = ingestion.process_one_message(
        _records_message("batch-1", [(s, f"{s}-v1") for s in SOURCE_NAMES]),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert result.acknowledged
    assert result.states == (ingestion.RunState.SUCCEEDED,) * len(SOURCE_NAMES)
    assert processor.finalized == 1
    assert sqs.deleted == ["batch-1"]

    for source, frame in frames.items():
        run_id = _run_ids(schemas, f"{source}-v1")[f"sources/{source}.gpkg"]
        stages = {
            row["stage"]: row
            for row in _query(
                f"SELECT stage, outcome, details FROM {schemas['service']}.stage_results "
                "WHERE run_id = :run_id",
                {"run_id": run_id},
            )
        }
        assert set(stages) == {"extract", "transform", "load", "marts"}, source
        assert {row["outcome"] for row in stages.values()} == {"succeeded"}, source
        # Routed by content: the record's key names the source, but the
        # extracted table is named from the Energy source value in the GPKG.
        raw_table = stages["extract"]["details"]["raw_table"]
        assert stages["extract"]["details"]["energy_source"] == source
        assert raw_table.startswith(f"{source}_")
        assert stages["transform"]["details"]["raw_table"] == raw_table
        assert stages["load"]["details"]["inserted"] >= 0

        # Core holds exactly the good members of the snapshot, in its kind.
        good = _good_members(schemas, run_id)
        core = _core_units(schemas, source)
        assert set(core) == good, source
        assert good, f"{source} fixture has no good rows"

        # Capacity reaches Core under the kind's capacity column, including
        # gas, whose Source column has a different name.
        expected = dict(zip(_frame_keys(frame), frame[_CAPACITY_COLUMN[source]]))
        for unit_key, (_, capacity) in core.items():
            assert capacity == pytest.approx(float(expected[unit_key])), (
                source,
                unit_key,
            )

        # The per-kind property tables carry the source's decomposed attributes.
        links, properties = _properties_tables(source)
        linked = _query(
            f"SELECT DISTINCT p.name FROM {schemas['core']}.{_core_kind(source)} u "
            f"JOIN {schemas['core']}.{links} up ON up.unit_id = u.unit_id "
            f"JOIN {schemas['core']}.{properties} p ON p.prop_id = up.prop_id "
            "WHERE u.energy_source = :source",
            {"source": source},
        )
        assert _SIGNATURE_PROPERTY[source] in {row["name"] for row in linked}, source

    # Generators never land in storages and vice versa.
    assert {
        row["energy_source"]
        for row in _query(f"SELECT DISTINCT energy_source FROM {schemas['core']}.generators")
    } == set(STAGING_GENERATOR_SOURCES)
    assert {
        row["energy_source"]
        for row in _query(f"SELECT DISTINCT energy_source FROM {schemas['core']}.storages")
    } == {"storage"}

    # Storage keeps its type and capacity per unit.
    storage_frame = frames["storage"].set_index("reference_id")
    for row in _query(
        f"SELECT reference_id, storage_type, storage_capacity "
        f"FROM {schemas['core']}.storages"
    ):
        assert row["storage_type"] == storage_frame.loc[row["reference_id"], "storage_type"]
        assert row["storage_capacity"] == pytest.approx(
            float(storage_frame.loc[row["reference_id"], "storage_capacity"])
        )

    # The three marts were refreshed after all the source work and reconcile.
    assert marts.verify_marts(ENGINE) == []
    counts = {
        row["energy_source"]: int(row["total"])
        for row in _query(
            f"SELECT energy_source, SUM(installation_count) AS total "
            f"FROM {schemas['marts']}.installation_counts GROUP BY energy_source"
        )
    }
    assert set(counts) == set(SOURCE_NAMES)
    storage_total = _query(
        f"SELECT COALESCE(SUM(storage_capacity), 0) AS total "
        f"FROM {schemas['marts']}.storage_capacity"
    )[0]["total"]
    core_storage_total = _query(
        f"SELECT COALESCE(SUM(storage_capacity), 0) AS total "
        f"FROM {schemas['core']}.storages "
        "WHERE decommissioning_date IS NULL OR decommissioning_date > CURRENT_DATE"
    )[0]["total"]
    assert float(storage_total) == pytest.approx(float(core_storage_total))
    # Generation capacity reconciles to the good fixture rows, computed here
    # independently of the mart SQL (all fixture units are active).
    generation = {
        row["energy_source"]: float(row["total"])
        for row in _query(
            f"SELECT energy_source, SUM(generation_capacity) AS total "
            f"FROM {schemas['marts']}.generation_capacity GROUP BY energy_source"
        )
    }
    for source in STAGING_GENERATOR_SOURCES:
        run_id = _run_ids(schemas, f"{source}-v1")[f"sources/{source}.gpkg"]
        good = _good_members(schemas, run_id)
        frame = frames[source]
        expected_total = sum(
            float(capacity)
            for key, capacity in zip(_frame_keys(frame), frame[_CAPACITY_COLUMN[source]])
            if key in good
        )
        assert generation[source] == pytest.approx(expected_total), source


@pytest.mark.parametrize("source", SOURCE_NAMES)
def test_worker_keeps_core_history_and_identity_for_every_source(
    fixture_boundaries, tmp_path, source
):
    """Every Source dataset has the same complete-snapshot Core semantics.

    A newer snapshot updates the units it carries, leaves the units it omits in
    Core with their identity, a redelivered or republished identical snapshot
    changes nothing, and an omitted unit that reappears keeps its Core unit_id.
    """
    s3 = fixture_boundaries["s3"]
    sqs = fixture_boundaries["sqs"]
    schemas = fixture_boundaries["schemas"]
    processor = fixture_boundaries["processor"]
    key = f"sources/{source}.gpkg"
    capacity_column = _CAPACITY_COLUMN[source]

    def publish(version_id, frame):
        result = _deliver(fixture_boundaries, tmp_path, source, version_id, frame)
        assert result.acknowledged, version_id
        assert result.states == (ingestion.RunState.SUCCEEDED,), version_id
        return _run_ids(schemas, version_id)[key]

    full = _fixture_frame(source)
    publish("v1", full)
    initial = _core_units(schemas, source)
    assert len(initial) >= 2, f"{source} fixture needs two good units"
    omitted, changed = sorted(initial)[:2]

    keys = pandas.Series(_frame_keys(full), index=full.index)
    newer = full[keys != omitted].copy()
    is_changed = keys[newer.index] == changed
    newer.loc[is_changed, capacity_column] = (
        float(full.loc[keys == changed, capacity_column].iloc[0]) + 1.5
    )
    newer_run = publish("v2", newer)
    after_newer = _core_units(schemas, source)

    # The omitted unit is retained with its identity; it just is not a member.
    assert after_newer[omitted] == initial[omitted]
    assert omitted not in _good_members(schemas, newer_run)
    # The carried unit is updated in place from the authoritative snapshot.
    assert after_newer[changed][0] == initial[changed][0]
    assert after_newer[changed][1] == pytest.approx(initial[changed][1] + 1.5)
    # No unit was re-created: identities are stable across snapshots.
    assert {r: ids[0] for r, ids in after_newer.items()} == {
        r: ids[0] for r, ids in initial.items()
    }

    # Republishing identical content under a new version is idempotent.
    publish("v3", newer)
    assert _core_units(schemas, source) == after_newer
    # A duplicate delivery of an already-loaded version is not re-extracted.
    duplicate = ingestion.process_one_message(
        _records_message("receipt-v3-again", [(source, "v3")]),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )
    assert duplicate.acknowledged
    assert duplicate.states == (ingestion.RunState.SUCCEEDED,)
    # One raw version per accepted object version (v1-v3), none for the redelivery.
    assert _raw_versions(schemas, source) == 3

    # The omitted unit reappears under its original Core identity.
    publish("v4", full)
    reappeared = _core_units(schemas, source)
    assert reappeared[omitted][0] == initial[omitted][0]
    assert len(reappeared) == len(initial)
    assert marts.verify_marts(ENGINE) == []


def _with_rows(frame, source, rows):
    """Append variations of the fixture's first row, keeping x/y and geometry equal."""
    base = frame.iloc[0].to_dict()
    capacity_column = _CAPACITY_COLUMN[source]
    records = []
    for row in rows:
        record = dict(base)
        x, y = row.get("coordinates", (base["x_coordinates"], base["y_coordinates"]))
        record.update(
            {
                "reference_id": row.get("reference_id"),
                "x_coordinates": x,
                "y_coordinates": y,
                "geometry": Point(x, y),
                "geo_accuracy": row.get("geo_accuracy", base["geo_accuracy"]),
                "location": row.get("location", "Fixture site"),
                capacity_column: row.get("capacity", base[capacity_column]),
            }
        )
        if source == "storage":
            record["storage_capacity"] = row.get(
                "storage_capacity", base["storage_capacity"]
            )
        records.append(record)
    widened = frame.copy()
    if "location" not in widened.columns:
        widened["location"] = "Fixture site"
    return gpd.GeoDataFrame(
        pandas.concat([widened, pandas.DataFrame(records)], ignore_index=True),
        geometry="geometry",
        crs=frame.crs,
    )


def _collision_reasons(schemas, source, reference_id):
    links, properties = _properties_tables(source)
    return {
        row["value"]
        for row in _query(
            f"SELECT p.value FROM {schemas['core']}.{_core_kind(source)} u "
            f"JOIN {schemas['core']}.{links} up ON up.unit_id = u.unit_id "
            f"JOIN {schemas['core']}.{properties} p ON p.prop_id = up.prop_id "
            "WHERE p.name = 'collision' AND u.energy_source = :source "
            "AND u.reference_id = :reference_id",
            {"source": source, "reference_id": reference_id},
        )
    }


@pytest.mark.parametrize("source", SOURCE_NAMES)
def test_worker_applies_quality_collision_and_identity_rules_for_every_source(
    fixture_boundaries, tmp_path, source
):
    """Bad quality, collisions, synthetic identity and membership agree across kinds.

    Every source — generator or storage — keeps a bad-quality row as Source
    membership only, gives a null-Reference-ID row a stable synthetic identity,
    keeps an outside-location unit in Core flagged as a collision, and rejects
    an ambiguous snapshot before changing the database.
    """
    schemas = fixture_boundaries["schemas"]
    key = f"sources/{source}.gpkg"
    frame = _fixture_frame(source)
    base_x, base_y = frame.iloc[0][["x_coordinates", "y_coordinates"]]

    def send(version_id, published):
        return _deliver(fixture_boundaries, tmp_path, source, version_id, published)

    snapshot = _with_rows(
        frame,
        source,
        [
            {"reference_id": "bad-quality", "capacity": None},
            # geo_accuracy 2 keeps it out of the close-location pair check.
            {"reference_id": None, "location": "Unnamed site", "geo_accuracy": 2},
            {"reference_id": "outside", "coordinates": (0.5, 0.5)},
            # Inside the fixture's North Sea polygon.
            {"reference_id": "at-sea", "coordinates": (7.0, 54.0)},
            # Two precisely located units ~1 m apart.
            {"reference_id": "near-a", "coordinates": (10.5, 48.5), "geo_accuracy": 1},
            {
                "reference_id": "near-b",
                "coordinates": (10.50001, 48.5),
                "geo_accuracy": 1,
            },
        ]
        + (
            [{"reference_id": "no-storage-capacity", "storage_capacity": None}]
            if source == "storage"
            else []
        ),
    )
    result = send("v1", snapshot)
    assert result.acknowledged
    assert result.states == (ingestion.RunState.SUCCEEDED,)
    run_id = _run_ids(schemas, "v1")[key]

    members = {
        row["unit_key"]: row
        for row in _query(
            f"SELECT unit_key, reference_id, bad_quality, energy_source "
            f"FROM {schemas['service']}.source_memberships WHERE run_id = :run_id",
            {"run_id": run_id},
        )
    }
    # Membership carries every row of the snapshot, bad quality included.
    assert len(members) == len(snapshot)
    assert {row["energy_source"] for row in members.values()} == {source}
    assert members[f"{source}_bad-quality"]["bad_quality"] is True
    synthetic = [k for k, row in members.items() if row["reference_id"] is None]
    assert synthetic and all(k.startswith("syn_") for k in synthetic)

    core = _core_units(schemas, source)
    # Bad quality never reaches Core; the synthetic unit and the outside unit do.
    assert "bad-quality" not in core
    assert _unit_key(None, base_x, base_y) in core
    assert "outside" in core
    outside = _query(
        f"SELECT collision, state FROM {schemas['core']}.{_core_kind(source)} "
        "WHERE energy_source = :source AND reference_id = 'outside'",
        {"source": source},
    )[0]
    assert outside["collision"] is True and outside["state"] is None
    assert any(
        "outside location" in reason
        for reason in _collision_reasons(schemas, source, "outside")
    )
    for reference_id in ("near-a", "near-b"):
        assert any(
            "close location" in reason
            for reason in _collision_reasons(schemas, source, reference_id)
        ), reference_id
    at_sea = _collision_reasons(schemas, source, "at-sea")
    # Wind may stand at sea; every other source, storage included, may not.
    assert any("onshore unit in the sea" in r for r in at_sea) == (source != "wind")
    if source == "storage":
        zero = frame.loc[frame["storage_capacity"] <= 0, "reference_id"].iloc[0]
        for reference_id in (zero, "no-storage-capacity"):
            assert any(
                "storage_capacity <= 0 or null" in reason
                for reason in _collision_reasons(schemas, source, reference_id)
            ), reference_id

    # The synthetic identity is stable: republishing keeps the same Core row.
    synthetic_core_id = core[_unit_key(None, base_x, base_y)][0]
    assert send("v2", snapshot).acknowledged
    assert _core_units(schemas, source)[_unit_key(None, base_x, base_y)][0] == (
        synthetic_core_id
    )

    # Ambiguous snapshots are rejected before any write, for every kind.
    before = _core_units(schemas, source)
    raw_before = _raw_versions(schemas, source)
    collision = _with_rows(
        frame,
        source,
        [
            {"reference_id": None, "location": "Twin", "geo_accuracy": 2},
            {"reference_id": None, "location": "Twin", "geo_accuracy": 2},
        ],
    )
    duplicate = _with_rows(frame, source, [{"reference_id": "twin"}] * 2)
    for version_id, published, message in (
        ("v-collision", collision, "Synthetic identity collision"),
        ("v-duplicate", duplicate, "Duplicate non-null Reference IDs"),
    ):
        rejected = send(version_id, published)
        assert rejected.acknowledged
        assert rejected.states == (ingestion.RunState.TERMINAL,)
        stage = _query(
            f"SELECT stage, outcome, error FROM {schemas['service']}.stage_results "
            "WHERE run_id = :run_id",
            {"run_id": _run_ids(schemas, version_id)[key]},
        )
        assert [(row["stage"], row["outcome"]) for row in stage] == [
            ("extract", "failed")
        ]
        assert message in stage[0]["error"]
    assert _core_units(schemas, source) == before
    assert _raw_versions(schemas, source) == raw_before


@pytest.mark.parametrize("dropped", ["storage_type", "storage_capacity"])
def test_worker_verifies_storage_type_and_capacity_reach_core(
    source_pipeline, tmp_path, monkeypatch, dropped
):
    """The storage load is verified on its own columns, not only the shared ones.

    If an in-place update stopped carrying storage_type or storage_capacity,
    the load must fail verification rather than mark stale storage values as a
    verified snapshot.
    """
    s3 = source_pipeline["s3"]
    sqs = source_pipeline["sqs"]
    processor = source_pipeline["processor"]
    schemas = source_pipeline["schemas"]

    s3.put(
        "sources/storage.gpkg",
        "storage-1",
        _write_storage_snapshot(
            tmp_path, "s1.gpkg", [{"reference_id": "bess-1", "storage_capacity": 4.0}]
        ),
    )
    assert ingestion.process_one_message(
        _storage_message("storage-1"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    ).acknowledged

    original_update = load._update_core_row

    def update_without_storage_columns(conn, row, kind):
        if kind.core_table == "storages":
            kind = dataclasses.replace(
                kind,
                column_map={k: v for k, v in kind.column_map.items() if k != dropped},
            )
        original_update(conn, row, kind)

    monkeypatch.setattr(load, "_update_core_row", update_without_storage_columns)
    s3.put(
        "sources/storage.gpkg",
        "storage-2",
        _write_storage_snapshot(
            tmp_path,
            "s2.gpkg",
            [
                {
                    "reference_id": "bess-1",
                    "storage_capacity": 7.0,
                    "storage_type": "Pumped hydro",
                }
            ],
        ),
    )
    result = ingestion.process_one_message(
        _storage_message("storage-2"),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
    )

    assert not result.acknowledged
    assert result.states == (ingestion.RunState.RETRYABLE,)
    load_stage = _query(
        f"SELECT outcome, error FROM {schemas['service']}.stage_results "
        "WHERE stage = 'load' AND run_id = (SELECT run_id FROM "
        f"{schemas['service']}.ingestion_runs WHERE object_version_id = 'storage-2')"
    )
    assert [row["outcome"] for row in load_stage] == ["failed"]
    assert f"{dropped} mismatch" in load_stage[0]["error"]


def test_worker_never_loads_a_failed_sources_staging_through_another_source(
    fixture_boundaries, tmp_path, monkeypatch
):
    """A Source whose transform failed must not reach Core via its kind's next load.

    The whole-kind load reads every staging table of the kind, so a snapshot
    that was written to staging but failed verification would otherwise be
    upserted by the next successful generator Source.
    """
    s3 = fixture_boundaries["s3"]
    sqs = fixture_boundaries["sqs"]
    schemas = fixture_boundaries["schemas"]
    processor = fixture_boundaries["processor"]

    def send(source, version_id, frame):
        s3.put(
            f"sources/{source}.gpkg",
            version_id,
            _frame_bytes(tmp_path, f"{version_id}.gpkg", frame),
        )
        return ingestion.process_one_message(
            _records_message(f"receipt-{version_id}", [(source, version_id)]),
            engine=ENGINE,
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
        )

    gas = _fixture_frame("gas")
    assert send("gas", "gas-v1", gas).acknowledged
    verified_gas = _core_units(schemas, "gas")

    unverified = gas.copy()
    unverified["gas_production_capacity"] = 999.0
    original_verify = transform._verify_transform

    def failing_gas_verification(engine, source, report):
        if source == "gas":
            return ["forced gas verification failure"]
        return original_verify(engine, source, report)

    monkeypatch.setattr(transform, "_verify_transform", failing_gas_verification)
    rejected = send("gas", "gas-v2", unverified)
    assert rejected.states == (ingestion.RunState.RETRYABLE,)

    # The failing gas retry and a good wind snapshot share one message: wind
    # still succeeds, the marts refresh once, and the message is kept.
    counting = CountingProcessor(ENGINE, s3=s3)
    s3.put(
        "sources/wind.gpkg",
        "wind-v1",
        _frame_bytes(tmp_path, "wind-v1.gpkg", _fixture_frame("wind")),
    )
    mixed = ingestion.process_one_message(
        _records_message("receipt-mixed", [("gas", "gas-v2"), ("wind", "wind-v1")]),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=counting,
    )
    assert mixed.states == (
        ingestion.RunState.RETRYABLE,
        ingestion.RunState.SUCCEEDED,
    )
    assert not mixed.acknowledged
    assert counting.finalized == 1

    assert _core_units(schemas, "gas") == verified_gas
    assert _core_units(schemas, "wind")
    assert marts.verify_marts(ENGINE) == []


# ------------------------------------------------------------------ #
#  Boundary releases (issue #5)                                        #
# ------------------------------------------------------------------ #

def test_boundary_release_replaces_one_level_and_rebuilds_all_geography(
    fixture_boundaries, tmp_path
):
    schemas = fixture_boundaries["schemas"]
    _publish_all_sources(fixture_boundaries, tmp_path)
    # bio-3 (in Bayern) becomes a retained historical Core unit, absent from staging.
    bio = _fixture_frame("bio")
    assert _deliver(
        fixture_boundaries, tmp_path, "bio", "bio-v2", bio[bio["reference_id"] != "bio-3"]
    ).acknowledged
    before = _boundary_snapshot(schemas)
    assert _geography(schemas, "core", "generators", "bio")[("bio", "bio-3")][0] == "Bayern"
    assert "outside location" not in " ".join(
        _collision_reasons(schemas, "bio", "bio-2")
    )
    assert any(
        "outside location" in r for r in _collision_reasons(schemas, "storage", "storage-5")
    )
    counting = CountingProcessor(ENGINE, s3=fixture_boundaries["s3"])

    result = _publish_boundaries(
        fixture_boundaries, tmp_path, {1: _renamed_states()}, "states-v2", counting
    )

    assert result.acknowledged
    assert result.states == (ingestion.RunState.SUCCEEDED,)
    assert counting.finalized == 1

    # Only level 1 was replaced; levels 0, 2 and 3 are byte-for-byte unchanged.
    after = _boundary_snapshot(schemas)
    assert {k: v for k, v in after.items() if k[0] != 1} == {
        k: v for k, v in before.items() if k[0] != 1
    }
    assert {name for level, name in after if level == 1} == {
        "Baden-Württemberg",
        "Freistaat Bayern",
        "North Sea",
    }
    assert all(area > 0 and has_geojson for _, area, has_geojson in after.values())
    assert verify._verify_boundaries(ENGINE) == []

    # Every Source's staging was re-enriched against the new release.
    for source in SOURCE_NAMES:
        staged = _geography(schemas, "stage", source)
        states = {geo[0] for geo in staged.values()}
        assert "Bayern" not in states, source
        assert states <= {"Baden-Württemberg", "Freistaat Bayern", "North Sea", None}

    # Core geography was rebuilt for every unit, historical ones included.
    core = {
        **_geography(schemas, "core", "generators"),
        **_geography(schemas, "core", "storages"),
    }
    assert "Bayern" not in {geo[0] for geo in core.values()}
    assert core[("bio", "bio-3")] == ("Freistaat Bayern", "München", "München (Stadt)")
    # Units at x=9.22 fell out of the shrunk state but keep their region/district.
    assert core[("bio", "bio-2")] == (None, "Stuttgart", "Stuttgart (Stadt)")
    assert core[("storage", "storage-5")][0] == "Freistaat Bayern"
    # Staging and Core agree for every unit both hold.
    for source in SOURCE_NAMES:
        for key, geo in _geography(schemas, "stage", source).items():
            if key in core:
                assert core[key] == geo, key

    # State-dependent collisions were recomputed in both directions.
    assert any("outside location" in r for r in _collision_reasons(schemas, "bio", "bio-2"))
    assert not any(
        "outside location" in r
        for r in _collision_reasons(schemas, "storage", "storage-5")
    )

    # Marts were refreshed on the new geography and reconcile to Core.
    assert marts.verify_marts(ENGINE) == []
    mart_states = {
        row["state"]
        for row in _query(f"SELECT state FROM {schemas['marts']}.installation_counts")
    }
    assert "Freistaat Bayern" in mart_states and "Bayern" not in mart_states

    # The release has an Ingestion run, stage results and a ledger entry.
    run = _query(
        f"SELECT run_id, input_kind, state FROM {schemas['service']}.ingestion_runs "
        "WHERE object_key = 'boundaries/level-1.gpkg'"
    )[0]
    assert (run["input_kind"], run["state"]) == ("boundary", "succeeded")
    stages = {
        (row["stage"], row["target"])
        for row in _query(
            f"SELECT stage, target FROM {schemas['service']}.stage_results "
            "WHERE run_id = :run_id",
            {"run_id": str(run["run_id"])},
        )
    }
    assert ("extract", "boundaries/level-1.gpkg") in stages
    assert {("transform", source) for source in SOURCE_NAMES} <= stages
    assert {("load", "generators"), ("load", "storages"), ("marts", "marts")} <= stages
    assert _query(
        f"SELECT loaded_to FROM {schemas['service']}.loaded_files "
        "WHERE object_key = 'boundaries/level-1.gpkg' "
        "AND object_version_id = 'states-v2-level-1'"
    ) == [{"loaded_to": f"{schemas['service']}.boundaries"}]

    # A redelivery of the applied release changes nothing.
    redelivered = ingestion.process_one_message(
        _message("again", [("boundaries/level-1.gpkg", "states-v2-level-1")]),
        engine=ENGINE,
        s3=fixture_boundaries["s3"],
        sqs=fixture_boundaries["sqs"],
        sns=FakeSNS(),
        processor=fixture_boundaries["processor"],
    )
    assert redelivered.acknowledged
    assert _boundary_snapshot(schemas) == after


def test_boundary_levels_in_one_message_apply_as_one_batch_with_one_rebuild(
    fixture_boundaries, tmp_path, monkeypatch
):
    schemas = fixture_boundaries["schemas"]
    _publish_all_sources(fixture_boundaries, tmp_path)
    before = _boundary_snapshot(schemas)
    rebuilds = []
    original_rebuild = ingestion.rebuild_geography

    def counting_rebuild(engine):
        rebuilds.append(engine)
        return original_rebuild(engine)

    monkeypatch.setattr(ingestion, "rebuild_geography", counting_rebuild)
    regions = _boundary_frame(2)
    regions.loc[regions["name"] == "Stuttgart", "name"] = "Region Stuttgart"
    districts = _boundary_frame(3)
    districts.loc[districts["name"] == "München (Stadt)", "name"] = "Landeshauptstadt München"
    counting = CountingProcessor(ENGINE, s3=fixture_boundaries["s3"])

    result = _publish_boundaries(
        fixture_boundaries, tmp_path, {2: regions, 3: districts}, "rd-v2", counting
    )

    assert result.acknowledged
    assert result.states == (ingestion.RunState.SUCCEEDED,) * 2
    assert len(rebuilds) == 1
    assert counting.finalized == 1
    after = _boundary_snapshot(schemas)
    assert {k: v for k, v in after.items() if k[0] in (0, 1)} == {
        k: v for k, v in before.items() if k[0] in (0, 1)
    }
    core = _geography(schemas, "core", "generators")
    assert core[("bio", "bio-1")] == ("Baden-Württemberg", "Region Stuttgart", "Stuttgart (Stadt)")
    assert core[("bio", "bio-3")] == ("Bayern", "München", "Landeshauptstadt München")
    assert marts.verify_marts(ENGINE) == []


def test_boundary_batch_with_an_invalid_level_changes_nothing(
    fixture_boundaries, tmp_path
):
    schemas = fixture_boundaries["schemas"]
    _publish_all_sources(fixture_boundaries, tmp_path)
    before = _boundary_snapshot(schemas)
    core_before = _geography(schemas, "core", "generators")
    regions = _boundary_frame(2)
    regions.loc[regions["name"] == "Stuttgart", "name"] = "Region Stuttgart"
    wrong_level = _boundary_frame(3)
    wrong_level["level"] = 2  # published at the level-3 key

    result = _publish_boundaries(
        fixture_boundaries, tmp_path, {2: regions, 3: wrong_level}, "bad-batch"
    )

    assert result.acknowledged
    assert result.states == (ingestion.RunState.TERMINAL,) * 2
    assert _boundary_snapshot(schemas) == before
    assert _geography(schemas, "core", "generators") == core_before
    assert _query(
        f"SELECT 1 FROM {schemas['service']}.loaded_files "
        "WHERE object_key LIKE 'boundaries/%'"
    ) == []
    failures = {
        row["target"]: row["error"]
        for row in _query(
            f"SELECT target, error FROM {schemas['service']}.stage_results "
            "WHERE outcome = 'failed'"
        )
    }
    assert "expected level 3" in failures["boundaries/level-3.gpkg"]


def test_boundary_release_that_leaves_the_layer_incomplete_is_rolled_back(
    source_pipeline, tmp_path
):
    """The replacement is verified inside its transaction, so a failure leaves no trace.

    The plain `source_pipeline` layer has no level 0; replacing level 1 would
    leave it incomplete, so the delete and insert must be rolled back.
    """
    schemas = source_pipeline["schemas"]
    before = _query(
        f"SELECT level, name FROM {schemas['service']}.boundaries ORDER BY level, name"
    )

    result = _publish_boundaries(source_pipeline, tmp_path, {1: _renamed_states()}, "partial")

    assert result.states == (ingestion.RunState.RETRYABLE,)
    assert (
        _query(
            f"SELECT level, name FROM {schemas['service']}.boundaries ORDER BY level, name"
        )
        == before
    )
    error = _query(
        f"SELECT error FROM {schemas['service']}.stage_results WHERE outcome = 'failed'"
    )[0]["error"]
    assert "missing level 0" in error


def test_a_failed_geography_rebuild_fails_the_boundary_release(
    fixture_boundaries, tmp_path, monkeypatch
):
    """A rebuild that cannot re-enrich a Source fails the release.

    The Boundary rows are already replaced when the rebuild runs, so a silently
    skipped staging source would acknowledge a release whose units still carry
    the old geography.
    """
    schemas = fixture_boundaries["schemas"]
    _publish_all_sources(fixture_boundaries, tmp_path)

    def broken(engine, source, boundaries):
        raise RuntimeError("staging table is locked")

    real_reenrich = boundaries.reenrich_staging
    monkeypatch.setattr(boundaries, "reenrich_staging", broken)

    result = _publish_boundaries(fixture_boundaries, tmp_path, {1: _renamed_states()}, "broken")

    assert result.states == (ingestion.RunState.RETRYABLE,)
    assert "staging table is locked" in (
        _query(
            f"SELECT error FROM {schemas['service']}.stage_results WHERE outcome = 'failed'"
        )[0]["error"]
    )

    # Redelivery: the release itself is already applied, so only the geography
    # is redone — the release is not applied a second time.
    monkeypatch.setattr(boundaries, "reenrich_staging", real_reenrich)
    redelivered = _publish_boundaries(
        fixture_boundaries, tmp_path, {1: _renamed_states()}, "broken"
    )

    assert redelivered.acknowledged
    assert redelivered.states == (ingestion.RunState.SUCCEEDED,)
    assert "Freistaat Bayern" in {
        state for state, _, _ in _geography(schemas, "core", "generators", "bio").values()
    }

# ------------------------------------------------------------------ #
#  One SQS message at a time (issue #6)                              #
# ------------------------------------------------------------------ #

class ThrottledSQS(ScriptedSQS):
    """A queue whose first receive fails, the way SQS throttles a request."""

    def __init__(self, error, *polls):
        super().__init__(*polls)
        self.error = error

    def receive_message(self):
        if self.error is not None:
            error, self.error = self.error, None
            raise error
        return super().receive_message()


class FakeSqsClient:
    """The boto3 sqs client surface the adapter uses, one canned reply per call."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []

    def receive_message(self, **kwargs):
        self.calls.append(kwargs)
        return self.replies.pop(0) if self.replies else {}

    def change_message_visibility(self, **kwargs):
        self.calls.append(kwargs)

    def delete_message(self, **kwargs):
        self.calls.append(kwargs)


def test_boto3_sqs_receives_exactly_one_message_per_call():
    body = _two_record_message().body
    client = FakeSqsClient(
        {"Messages": [{"MessageId": "id-1", "ReceiptHandle": "receipt-1", "Body": body}]},
        {},
    )
    sqs = ingestion.Boto3SQSAdapter(client, queue_url="https://sqs.eu/queue")

    first = sqs.receive_message()

    assert (first.receipt_handle, first.body) == ("receipt-1", body)
    # An empty receive is not an error: the worker simply has nothing to do.
    assert sqs.receive_message() is None
    # One receive, one message. SQS batches the S3 records of a single upload
    # inside the body already, and asking for more would fuse separate messages
    # into one work unit with one acknowledgement.
    assert [call["MaxNumberOfMessages"] for call in client.calls] == [1, 1]
    assert client.calls[0]["WaitTimeSeconds"] == ingestion.LONG_POLL_SECONDS
    assert {call["QueueUrl"] for call in client.calls} == {"https://sqs.eu/queue"}

    sqs.delete_message("receipt-1")

    assert client.calls[-1] == {
        "QueueUrl": "https://sqs.eu/queue",
        "ReceiptHandle": "receipt-1",
    }


def _queue_sources(s3, *sources, version="version-42"):
    """Put the given Sources in the bucket at one version, and say so."""
    for source in sources:
        s3.put(f"sources/{source}.gpkg", version, b"version-42-content")
    return [
        _message(f"receipt-{source}", [(f"sources/{source}.gpkg", version)])
        for source in sources
    ]


def test_run_worker_processes_queued_messages_one_at_a_time(source_pipeline):
    s3 = source_pipeline["s3"]
    schemas = source_pipeline["schemas"]
    sqs = ScriptedSQS(*_queue_sources(s3, "solar", "wind", "gas"))
    processor = FakeProcessor()

    processed = ingestion.run_worker(
        engine=source_pipeline["engine"],
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
        max_messages=3,
        idle_sleep=lambda seconds: pytest.fail("the queue never ran dry"),
    )

    assert processed == 3
    # Each message was received on its own and settled on its own terms.
    assert sqs.receives == 3
    assert sqs.deleted == ["receipt-solar", "receipt-wind", "receipt-gas"]
    assert [object_id.key for object_id in processor.objects] == [
        "sources/solar.gpkg",
        "sources/wind.gpkg",
        "sources/gas.gpkg",
    ]
    assert [dict(row) for row in _query(
        f"SELECT object_key, state FROM {schemas['service']}.ingestion_runs "
        "ORDER BY created_at"
    )] == [
        {"object_key": f"sources/{source}.gpkg", "state": "succeeded"}
        for source in ("solar", "wind", "gas")
    ]


def test_run_worker_keeps_polling_after_an_empty_receive(source_pipeline):
    sqs = ScriptedSQS(
        None, *_queue_sources(source_pipeline["s3"], "solar")
    )
    processor = FakeProcessor()
    slept = []

    processed = ingestion.run_worker(
        engine=source_pipeline["engine"],
        s3=source_pipeline["s3"],
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
        max_messages=1,
        idle_sleep=slept.append,
    )

    # Queued work resumes on their own after a restart, so an empty queue
    # pauses the worker instead of ending it.
    assert processed == 1
    assert slept == [ingestion.IDLE_POLL_SECONDS]
    assert sqs.receives == 2
    assert sqs.deleted == ["receipt-solar"]


def test_run_worker_survives_a_failed_receive(source_pipeline):
    """A receive that fails must not take the worker down.

    A failure outside the per-record handling — SQS throttling, a dropped
    connection — leaves the queue untouched, and the message behind it is a
    different piece of work that still gets processed.
    """
    sqs = ThrottledSQS(
        RuntimeError("SQS throttled the request"),
        *_queue_sources(source_pipeline["s3"], "wind"),
    )
    processor = FakeProcessor()
    slept = []

    processed = ingestion.run_worker(
        engine=source_pipeline["engine"],
        s3=source_pipeline["s3"],
        sqs=sqs,
        sns=FakeSNS(),
        processor=processor,
        max_messages=1,
        idle_sleep=slept.append,
    )

    assert processed == 1
    assert [object_id.key for object_id in processor.objects] == ["sources/wind.gpkg"]
    assert sqs.deleted == ["receipt-wind"]
    assert slept == [ingestion.IDLE_POLL_SECONDS]


def test_worker_command_runs_the_message_loop(source_pipeline, monkeypatch):
    sqs = ScriptedSQS(_message("receipt-solar", [("sources/solar.gpkg", "version-42")]))
    processor = FakeProcessor()
    monkeypatch.setattr(cli_module, "get_engine", lambda: source_pipeline["engine"])
    monkeypatch.setattr(cli_module, "_s3_adapter", lambda: source_pipeline["s3"])
    monkeypatch.setattr(cli_module, "_sqs_adapter", lambda queue_url: sqs)
    monkeypatch.setattr(cli_module, "_sns_adapter", lambda topic_arn: FakeSNS())
    monkeypatch.setattr(cli_module, "PipelineProcessor", lambda engine, *, s3: processor)

    invocation = CliRunner().invoke(
        cli,
        [
            "worker",
            "--queue-url",
            "https://sqs.eu/queue",
            "--topic-arn",
            "arn:aws:sns:eu:123456789012:ingestion",
            "--max-messages",
            "1",
        ],
    )

    assert invocation.exit_code == 0, invocation.output
    assert "Worker processed 1 message(s)." in invocation.output
    assert sqs.receives == 1
    assert sqs.deleted == ["receipt-solar"]


def test_boundary_and_source_records_in_one_message_apply_in_ingestion_order(
    fixture_boundaries, tmp_path, monkeypatch
):
    """One message carrying a Boundary release and a Source snapshot.

    The Source record is listed first, so the order cannot come from the body:
    the release is applied and its geography rebuilt before the Source is
    extracted, which is why the rebuild finds no staging of that Source to
    re-enrich — the Source enriched itself against the new polygons. Both
    records get their own Ingestion run and their own stage results, and one
    message is acknowledged once both are done.
    """
    s3 = fixture_boundaries["s3"]
    sqs = fixture_boundaries["sqs"]
    schemas = fixture_boundaries["schemas"]
    staging_at_rebuild = []
    original_rebuild = ingestion.rebuild_geography

    def recording_rebuild(engine):
        staging_at_rebuild.append(
            [
                row["table_name"]
                for row in _query(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :schema",
                    {"schema": schemas["stage"]},
                )
            ]
        )
        return original_rebuild(engine)

    monkeypatch.setattr(ingestion, "rebuild_geography", recording_rebuild)
    counting = CountingProcessor(ENGINE, s3=s3)
    s3.put(
        "sources/bio.gpkg",
        "bio-v1",
        _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")),
    )
    s3.put(
        "boundaries/level-1.gpkg",
        "states-v2",
        _frame_bytes(tmp_path, "states-v2.gpkg", _renamed_states()),
    )

    result = ingestion.process_one_message(
        _message(
            "mixed-1",
            [("sources/bio.gpkg", "bio-v1"), ("boundaries/level-1.gpkg", "states-v2")],
        ),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=counting,
    )

    assert result.acknowledged
    assert result.states == (ingestion.RunState.SUCCEEDED,) * 2
    assert sqs.deleted == ["mixed-1"]
    # One release, one geography rebuild, one marts refresh for the message.
    assert staging_at_rebuild == [[]]
    assert counting.finalized == 1

    runs = {
        row["object_key"]: row
        for row in _query(
            f"SELECT run_id, object_key, input_kind, state "
            f"FROM {schemas['service']}.ingestion_runs"
        )
    }
    assert set(runs) == {"sources/bio.gpkg", "boundaries/level-1.gpkg"}
    assert runs["boundaries/level-1.gpkg"]["input_kind"] == "boundary"
    assert {row["state"] for row in runs.values()} == {"succeeded"}

    def stages_of(object_key):
        return {
            (row["stage"], row["target"])
            for row in _query(
                f"SELECT stage, target FROM {schemas['service']}.stage_results "
                "WHERE run_id = :run_id",
                {"run_id": str(runs[object_key]["run_id"])},
            )
        }

    # The release owns its own extract; the rebuild had nothing to re-enrich.
    assert stages_of("boundaries/level-1.gpkg") == {
        ("extract", "boundaries/level-1.gpkg"),
        ("marts", "marts"),
    }
    # The Source record carries the full chain of its own, on its own table.
    assert stages_of("sources/bio.gpkg") == {
        ("extract", "sources/bio.gpkg"),
        ("transform", "bio"),
        ("load", "generators"),
        ("marts", "marts"),
    }

    # Each record's stages name the table that record itself produced: the
    # mixed message does not let one record work on another's snapshot.
    details = {
        row["stage"]: row["details"]
        for row in _query(
            f"SELECT stage, details FROM {schemas['service']}.stage_results "
            "WHERE run_id = :run_id",
            {"run_id": str(runs["sources/bio.gpkg"]["run_id"])},
        )
    }
    raw_table = details["extract"]["raw_table"]
    assert raw_table.startswith("bio_")
    assert details["transform"]["raw_table"] == raw_table
    assert _query(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_name = :name",
        {"schema": schemas["raw"], "name": raw_table},
    ) == [{"?column?": 1}]

    # The Source was enriched against the new release, and everything reconciles.
    assert _geography(schemas, "core", "generators", "bio")[
        ("bio", "bio-3")
    ] == ("Freistaat Bayern", "München", "München (Stadt)")
    assert _query(
        f"SELECT loaded_to FROM {schemas['service']}.loaded_files "
        "WHERE object_version_id = 'states-v2'"
    ) == [{"loaded_to": f"{schemas['service']}.boundaries"}]
    assert marts.verify_marts(ENGINE) == []


def test_a_rejected_snapshot_does_not_block_the_other_records(
    fixture_boundaries, tmp_path
):
    """A snapshot that fails validation is explained and settled, not retried.

    The valid record in the same message still runs its whole chain, so one
    malformed upload cannot hold up the files beside it, and the message is
    acknowledged once every record has a settled state.
    """
    s3 = fixture_boundaries["s3"]
    sqs = fixture_boundaries["sqs"]
    schemas = fixture_boundaries["schemas"]
    s3.put(
        "sources/solar.gpkg",
        "solar-bad",
        _write_mixed_source_snapshot(tmp_path, "mixed.gpkg"),
    )
    s3.put(
        "sources/bio.gpkg",
        "bio-v1",
        _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")),
    )

    result = ingestion.process_one_message(
        _message(
            "solar-bad",
            [("sources/solar.gpkg", "solar-bad"), ("sources/bio.gpkg", "bio-v1")],
        ),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=fixture_boundaries["processor"],
    )

    assert result.states == (ingestion.RunState.TERMINAL, ingestion.RunState.SUCCEEDED)
    assert result.acknowledged
    assert sqs.deleted == ["solar-bad"]

    # The healthy record progressed all the way, against its own run.
    bio_run = _run_ids(schemas, "bio-v1")["sources/bio.gpkg"]
    assert set(_core_units(schemas, "bio")) == _good_members(schemas, bio_run)
    assert {
        row["stage"]
        for row in _query(
            f"SELECT stage FROM {schemas['service']}.stage_results "
            "WHERE run_id = :run_id",
            {"run_id": bio_run},
        )
    } == {"extract", "transform", "load", "marts"}

    # The rejected record left nothing behind and says why it failed.
    assert _core_units(schemas, "solar") == {}
    assert _raw_versions(schemas, "solar") == 0
    solar_run = _run_ids(schemas, "solar-bad")["sources/solar.gpkg"]
    failed = _query(
        f"SELECT stage, target, outcome, error FROM {schemas['service']}.stage_results "
        "WHERE run_id = :run_id",
        {"run_id": solar_run},
    )
    assert [dict(row) for row in failed] == [
        {
            "stage": "extract",
            "target": "sources/solar.gpkg",
            "outcome": "failed",
            "error": "Source GPKG requires homogeneous Energy source",
        }
    ]
    assert marts.verify_marts(ENGINE) == []


def test_a_rejected_boundary_release_does_not_block_the_source_records(
    fixture_boundaries, tmp_path
):
    """A Boundary record rejected by validation leaves the geography as it was.

    The Source records behind it in the same message are enriched against the
    unchanged boundaries, so a bad release cannot half-apply itself, and the
    rejected level settles the message instead of holding it up.
    """
    s3 = fixture_boundaries["s3"]
    sqs = fixture_boundaries["sqs"]
    schemas = fixture_boundaries["schemas"]
    before = _boundary_snapshot(schemas)
    mislabelled = _boundary_frame(3)
    mislabelled["level"] = 2  # published at the level-3 key
    s3.put(
        "boundaries/level-3.gpkg",
        "districts-bad",
        _frame_bytes(tmp_path, "districts-bad.gpkg", mislabelled),
    )
    s3.put(
        "sources/bio.gpkg",
        "bio-v1",
        _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")),
    )

    result = ingestion.process_one_message(
        _message(
            "boundary-bad",
            [
                ("boundaries/level-3.gpkg", "districts-bad"),
                ("sources/bio.gpkg", "bio-v1"),
            ],
        ),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=fixture_boundaries["processor"],
    )

    assert result.states == (ingestion.RunState.TERMINAL, ingestion.RunState.SUCCEEDED)
    assert result.acknowledged
    assert sqs.deleted == ["boundary-bad"]

    # Nothing of the release was written, not even the load ledger.
    assert _boundary_snapshot(schemas) == before
    assert _query(
        f"SELECT 1 FROM {schemas['service']}.loaded_files "
        "WHERE object_key LIKE 'boundaries/%'"
    ) == []
    boundary_run = _run_ids(schemas, "districts-bad")["boundaries/level-3.gpkg"]
    rejected = _query(
        f"SELECT target, outcome, error FROM {schemas['service']}.stage_results "
        "WHERE run_id = :run_id",
        {"run_id": boundary_run},
    )
    assert [dict(row) for row in rejected] == [
        {
            "target": "boundaries/level-3.gpkg",
            "outcome": "failed",
            "error": "Boundary GPKG expected level 3, found [2]",
        }
    ]

    # The Source behind it was enriched against the boundaries still in place.
    assert _geography(schemas, "core", "generators", "bio")[
        ("bio", "bio-1")
    ] == ("Baden-Württemberg", "Stuttgart", "Stuttgart (Stadt)")
    assert marts.verify_marts(ENGINE) == []


def test_redelivery_of_a_message_processes_only_the_unsettled_records(
    fixture_boundaries, tmp_path
):
    """A message kept for redelivery is safe to send again.

    The settled record is not redone — one raw snapshot, one attempt, one set of
    stage results — while the record that could not be read is retried, and the
    message is acknowledged once every record has a settled state.
    """
    s3 = fixture_boundaries["s3"]
    sqs = fixture_boundaries["sqs"]
    schemas = fixture_boundaries["schemas"]
    counting = CountingProcessor(ENGINE, s3=s3)
    for source in ("solar", "bio"):
        s3.put(
            f"sources/{source}.gpkg",
            f"{source}-v1",
            _frame_bytes(tmp_path, f"{source}-v1.gpkg", _fixture_frame(source)),
        )
    s3.read_failures["sources/solar.gpkg"] = 1
    records = [("sources/solar.gpkg", "solar-v1"), ("sources/bio.gpkg", "bio-v1")]

    first = ingestion.process_one_message(
        _message("receipt-1", records),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=counting,
    )

    assert first.states == (ingestion.RunState.RETRYABLE, ingestion.RunState.SUCCEEDED)
    assert not first.acknowledged
    assert sqs.deleted == []
    assert _core_units(schemas, "solar") == {}
    assert set(_core_units(schemas, "bio")) == _good_members(
        schemas, _run_ids(schemas, "bio-v1")["sources/bio.gpkg"]
    )
    assert counting.finalized == 1

    redelivered = ingestion.process_one_message(
        _message("receipt-2", records),
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=counting,
    )

    assert redelivered.acknowledged
    assert redelivered.states == (ingestion.RunState.SUCCEEDED,) * 2
    assert sqs.deleted == ["receipt-2"]
    assert counting.finalized == 2
    assert set(_core_units(schemas, "solar")) == _good_members(
        schemas, _run_ids(schemas, "solar-v1")["sources/solar.gpkg"]
    )

    runs = {
        row["object_key"]: row
        for row in _query(
            f"SELECT object_key, state, attempt_count "
            f"FROM {schemas['service']}.ingestion_runs"
        )
    }
    assert runs["sources/solar.gpkg"]["attempt_count"] == 2
    # The record that already succeeded was skipped, not repeated.
    assert runs["sources/bio.gpkg"]["attempt_count"] == 1
    assert _raw_versions(schemas, "bio") == 1
    assert [
        row["attempt"]
        for row in _query(
            f"SELECT DISTINCT attempt FROM {schemas['service']}.stage_results "
            f"WHERE run_id = :run_id",
            {"run_id": _run_ids(schemas, "bio-v1")["sources/bio.gpkg"]},
        )
    ] == [1]
    assert marts.verify_marts(ENGINE) == []

# ------------------------------------------------------------------ #
#  Retry, DLQ, SNS, visibility, and recovery (issue #7)             #
# ------------------------------------------------------------------ #


def test_a_rejected_snapshot_is_terminal_and_alerts_once(
    fixture_boundaries, tmp_path
):
    """A file that fails its own validation is settled, not retried five times.

    The object version is immutable, so validating it again cannot produce a
    different answer: the run is terminal, the operator is told once, and the
    message is acknowledged so the files behind it are not held up by it.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    sqs = pipeline["sqs"]
    schemas = pipeline["schemas"]
    sns = FakeSNS()
    s3.put(
        "sources/solar.gpkg",
        "solar-bad",
        _write_mixed_source_snapshot(tmp_path, "mixed.gpkg"),
    )

    result = _deliver_attempt(pipeline, _message("solar-bad", [("sources/solar.gpkg", "solar-bad")]), sns=sns)

    assert result.states == (ingestion.RunState.TERMINAL,)
    assert result.acknowledged
    assert sqs.deleted == ["solar-bad"]
    assert _runs_for_key(schemas, "sources/solar.gpkg") == [
        {
            "state": "terminal",
            "attempt_count": 1,
            "terminal_error": (
                "extract failed: Source GPKG requires homogeneous Energy source"
            ),
        }
    ]
    # The alert names the file, its version, and how to get it ingested.
    assert [subject for subject, _ in sns.messages] == [
        "Rejected sources/solar.gpkg"
    ]
    alert = " ".join(sns.bodies.split())
    assert "solar-bad" in alert
    assert "energy-data" in alert
    assert "Upload a new version of the file to ingest it" in alert
    # The rejected version left nothing behind: no raw table, and no Core
    # either, because nothing in this message got far enough to build one.
    assert _raw_versions(schemas, "solar") == 0
    assert _query(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = :schema AND table_name LIKE 'solar\\_%'",
        {"schema": schemas["raw"]},
    ) == []


def test_a_failed_alert_does_not_hold_up_the_rest_of_the_message(
    fixture_boundaries, tmp_path
):
    """An alert that cannot be sent is logged; the file is still settled.

    The rejection is decided by the pipeline, not by the alerting path, so a
    broken SNS topic cannot leave a rejected version redelivered forever or
    stop the healthy record behind it from running.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    sqs = pipeline["sqs"]
    schemas = pipeline["schemas"]

    class BrokenSNS:
        def publish(self, subject: str, message: str) -> None:
            raise RuntimeError("topic unreachable")

    s3.put(
        "sources/solar.gpkg",
        "solar-bad",
        _write_mixed_source_snapshot(tmp_path, "mixed.gpkg"),
    )
    s3.put(
        "sources/bio.gpkg",
        "bio-good",
        _write_source_snapshot(
            tmp_path, "good.gpkg", [{"reference_id": "keep", "installed_capacity": 5.0}]
        ),
    )

    result = _deliver_attempt(
        pipeline,
        _message(
            "mixed",
            [
                ("sources/solar.gpkg", "solar-bad"),
                ("sources/bio.gpkg", "bio-good"),
            ],
        ),
        sns=BrokenSNS(),
    )

    assert result.states == (ingestion.RunState.TERMINAL, ingestion.RunState.SUCCEEDED)
    assert result.acknowledged
    assert sqs.deleted == ["mixed"]
    assert _runs_for_key(schemas, "sources/solar.gpkg") == [
        {
            "state": "terminal",
            "attempt_count": 1,
            "terminal_error": (
                "extract failed: Source GPKG requires homogeneous Energy source"
            ),
        }
    ]
    # The healthy record still reached Core.
    assert _query(
        f"SELECT reference_id FROM {schemas['core']}.generators"
    ) == [{"reference_id": "keep"}]


def test_a_rejection_found_later_in_the_chain_is_also_terminal(
    fixture_boundaries, tmp_path
):
    """The inspect step is not the only validator, so neither is it the only door.

    A row that has neither a Reference ID nor a location cannot be given a
    synthetic identity, and the transform says so — after the file has been
    extracted. That verdict is just as much a property of the bytes, so it
    settles the run and alerts instead of being redelivered four more times.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    sqs = pipeline["sqs"]
    schemas = pipeline["schemas"]
    sns = FakeSNS()
    s3.put(
        "sources/solar.gpkg",
        "solar-v1",
        _write_source_snapshot(
            tmp_path, "no-identity.gpkg", [{"reference_id": None, "location": None}]
        ),
    )

    result = _deliver_attempt(
        pipeline, _message("solar-1", [("sources/solar.gpkg", "solar-v1")]), sns=sns
    )

    assert result.states == (ingestion.RunState.TERMINAL,)
    assert result.acknowledged
    assert sqs.deleted == ["solar-1"]
    assert _runs_for_key(schemas, "sources/solar.gpkg") == [
        {
            "state": "terminal",
            "attempt_count": 1,
            "terminal_error": (
                "transform failed: Rows without reference_id require a location"
            ),
        }
    ]
    # The rejection names the stage that found it, and the extract it did finish.
    assert _query(
        f"SELECT stage, outcome, error FROM {schemas['service']}.stage_results "
        "WHERE run_id = :run_id ORDER BY stage",
        {"run_id": _run_ids(schemas, "solar-v1")["sources/solar.gpkg"]},
    ) == [
        {
            "stage": "extract",
            "outcome": "succeeded",
            "error": None,
        },
        {
            "stage": "transform",
            "outcome": "failed",
            "error": "Rows without reference_id require a location",
        },
    ]
    assert [subject for subject, _ in sns.messages] == ["Rejected sources/solar.gpkg"]


def test_an_unacknowledged_message_is_handed_back_to_the_queue(
    fixture_boundaries, tmp_path
):
    """A message the worker keeps is made visible again, not parked for six hours.

    SQS applies its redrive policy when a message becomes visible, so a message
    left retryable has to be returned promptly: otherwise the retry, and the move
    to the DLQ once the deliveries run out, waits out the visibility timeout the
    heartbeat had just granted.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    schemas = pipeline["schemas"]
    s3.put(
        "sources/bio.gpkg",
        "bio-v1",
        _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")),
    )
    s3.read_failures["sources/bio.gpkg"] = 1
    sqs = ScriptedSQS(
        ingestion.SqsMessage(
            receipt_handle="bio-1",
            body=_message(
                "bio-1", [("sources/bio.gpkg", "bio-v1")]
            ).body,
        )
    )

    processed = ingestion.run_worker(
        engine=ENGINE,
        s3=s3,
        sqs=sqs,
        sns=FakeSNS(),
        processor=pipeline["processor"],
        max_messages=1,
        idle_sleep=lambda seconds: None,
    )

    assert processed == 1
    assert sqs.deleted == []
    # Granted the maximum while the work ran, then handed straight back.
    assert sqs.visibility == [
        ("bio-1", ingestion.VISIBILITY_TIMEOUT_SECONDS),
        ("bio-1", 0),
    ]
    assert _runs_for_key(schemas, "sources/bio.gpkg")[0]["state"] == "retryable"


def test_a_rejected_file_alerts_once_across_redeliveries(
    fixture_boundaries, tmp_path
):
    """The alert is tied to the run, so a redelivery does not repeat it.

    The message survives the first delivery only because the second record is
    still retryable; the rejected record is settled and must stay silent.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    sqs = pipeline["sqs"]
    schemas = pipeline["schemas"]
    sns = FakeSNS()
    s3.put(
        "sources/solar.gpkg",
        "solar-bad",
        _write_mixed_source_snapshot(tmp_path, "mixed.gpkg"),
    )
    s3.put("sources/bio.gpkg", "bio-v1", _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")))
    s3.read_failures["sources/bio.gpkg"] = 1
    message = _message(
        "mixed-bad",
        [("sources/solar.gpkg", "solar-bad"), ("sources/bio.gpkg", "bio-v1")],
    )

    first = _deliver_attempt(pipeline, message, sns=sns)

    assert first.states == (ingestion.RunState.TERMINAL, ingestion.RunState.RETRYABLE)
    assert not first.acknowledged
    assert sqs.deleted == []
    assert len(sns.messages) == 1

    second = _deliver_attempt(pipeline, message, sns=sns, attempt=2)

    # The rejected record stays settled, the readable one is ingested.
    assert second.states == (ingestion.RunState.TERMINAL, ingestion.RunState.SUCCEEDED)
    assert second.acknowledged
    assert sqs.deleted == ["mixed-bad"]
    # One alert for one rejected file, not one per delivery.
    assert len(sns.messages) == 1
    assert _runs_for_key(schemas, "sources/solar.gpkg")[0]["attempt_count"] == 1
    assert _runs_for_key(schemas, "sources/bio.gpkg") == [
        {"state": "succeeded", "attempt_count": 2, "terminal_error": None}
    ]


def test_an_infrastructure_failure_stays_retryable_and_settles_on_redelivery(
    fixture_boundaries, tmp_path
):
    """A failure that is not the file's fault keeps the message for redelivery.

    An S3 read that fails is infrastructure, not input: the run stays retryable,
    nothing is alerted, and the next delivery of the same version is processed
    from where the pipeline is idempotent.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    sqs = pipeline["sqs"]
    schemas = pipeline["schemas"]
    sns = FakeSNS()
    s3.put("sources/bio.gpkg", "bio-v1", _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")))
    s3.read_failures["sources/bio.gpkg"] = 1
    message = _message("bio-1", [("sources/bio.gpkg", "bio-v1")])

    first = _deliver_attempt(pipeline, message, sns=sns)

    assert first.states == (ingestion.RunState.RETRYABLE,)
    assert not first.acknowledged
    assert sqs.deleted == []
    assert sns.messages == []
    assert _runs_for_key(schemas, "sources/bio.gpkg") == [
        {
            "state": "retryable",
            "attempt_count": 1,
            "terminal_error": "transient S3 read failure for sources/bio.gpkg",
        }
    ]
    assert _raw_versions(schemas, "bio") == 0

    second = _deliver_attempt(pipeline, message, sns=sns, attempt=2)

    assert second.states == (ingestion.RunState.SUCCEEDED,)
    assert second.acknowledged
    assert sqs.deleted == ["bio-1"]
    # The DLQ alarm is the only terminal infrastructure alert.
    assert sns.messages == []
    assert set(_core_units(schemas, "bio")) == _good_members(
        schemas, _run_ids(schemas, "bio-v1")["sources/bio.gpkg"]
    )


def test_delivery_attempts_are_exhausted_into_the_dlq(
    fixture_boundaries, tmp_path
):
    """The fifth delivery is the last: the run says so and the queue DLQs it.

    The worker neither deletes the message (the redrive policy needs it) nor
    alerts (the DLQ alarm is the single terminal infrastructure alert), so the
    operator finds the message in the DLQ and its state in the run.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    sqs = pipeline["sqs"]
    schemas = pipeline["schemas"]
    sns = FakeSNS()
    s3.put("sources/bio.gpkg", "bio-v1", _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")))
    message = _message("bio-1", [("sources/bio.gpkg", "bio-v1")])

    for attempt in range(1, ingestion.MAX_DELIVERY_ATTEMPTS):
        s3.read_failures["sources/bio.gpkg"] = 1
        result = _deliver_attempt(pipeline, message, sns=sns, attempt=attempt)

        # Before the last attempt the run is an ordinary retryable failure.
        assert result.states == (ingestion.RunState.RETRYABLE,), attempt
        assert _runs_for_key(schemas, "sources/bio.gpkg")[0]["attempt_count"] == attempt

    s3.read_failures["sources/bio.gpkg"] = 1
    exhausted = _deliver_attempt(
        pipeline, message, sns=sns, attempt=ingestion.MAX_DELIVERY_ATTEMPTS
    )

    # Still unsettled, so the message is not deleted and SQS moves it to the DLQ.
    assert exhausted.states == (ingestion.RunState.RETRYABLE,)
    assert not exhausted.acknowledged
    assert sqs.deleted == []
    assert sns.messages == []
    run = _runs_for_key(schemas, "sources/bio.gpkg")[0]
    assert run["state"] == "retryable"
    assert run["attempt_count"] == ingestion.MAX_DELIVERY_ATTEMPTS
    assert "DLQ" in run["terminal_error"]
    # The exhaustion is a per-table result of its own, not just a state.
    assert [
        dict(row)
        for row in _query(
            f"SELECT target, stage, outcome, error, details "
            f"FROM {schemas['service']}.stage_results "
            f"WHERE run_id = :run_id AND stage = 'processing' AND attempt = :attempt",
            {
                "run_id": _run_ids(schemas, "bio-v1")["sources/bio.gpkg"],
                "attempt": ingestion.MAX_DELIVERY_ATTEMPTS,
            },
        )
    ] == [
        {
            "target": "sources/bio.gpkg",
            "stage": "processing",
            "outcome": "failed",
            # The cause is kept, so the operator reads what actually failed
            # before the "and now it is in the DLQ" part.
            "error": (
                "transient S3 read failure for sources/bio.gpkg; delivery "
                "attempts exhausted after 5 deliveries, so the message goes to "
                "the DLQ"
            ),
            "details": {"delivery_attempt": 5, "delivery_attempts": 5, "exhausted": True},
        }
    ]


def test_a_crashed_message_resumes_on_redelivery(
    fixture_boundaries, tmp_path, monkeypatch
):
    """A worker that dies mid-record leaves the run resumable, not stuck.

    The crash is not an exception the pipeline can catch, so the run keeps its
    running state and the half-finished work; the next delivery claims the same
    run, counts the attempt, and finishes it.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    schemas = pipeline["schemas"]
    sns = FakeSNS()
    s3.put("sources/bio.gpkg", "bio-v1", _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")))
    message = _message("bio-1", [("sources/bio.gpkg", "bio-v1")])
    crashed = pipeline["processor"]

    def crash_after_extract(*_args, **_kwargs):
        raise KeyboardInterrupt("the worker was stopped mid-record")

    # The extract is already committed when the worker dies, so the crash leaves
    # a raw version behind and the run open.
    original_transform = ingestion.transform_source_snapshot
    monkeypatch.setattr(ingestion, "transform_source_snapshot", crash_after_extract)
    worker_sqs = ScriptedSQS(
        ingestion.SqsMessage(
            receipt_handle="bio-1",
            body=message.body,
            attempt=1,
        )
    )

    with pytest.raises(KeyboardInterrupt):
        ingestion.run_worker(
            engine=ENGINE,
            s3=s3,
            sqs=worker_sqs,
            sns=sns,
            processor=crashed,
            max_messages=1,
            idle_sleep=lambda seconds: None,
        )

    # The run is still open, so nothing is acknowledged and nothing alerted, and
    # the message is handed back rather than left behind the granted timeout.
    assert worker_sqs.deleted == []
    assert worker_sqs.visibility[-1] == ("bio-1", 0)
    assert sns.messages == []
    assert _runs_for_key(schemas, "sources/bio.gpkg") == [
        {"state": "running", "attempt_count": 1, "terminal_error": None}
    ]
    assert _raw_versions(schemas, "bio") == 1
    monkeypatch.setattr(ingestion, "transform_source_snapshot", original_transform)

    resumed = _deliver_attempt(pipeline, message, sns=sns, attempt=2)

    assert resumed.states == (ingestion.RunState.SUCCEEDED,)
    assert resumed.acknowledged
    assert _runs_for_key(schemas, "sources/bio.gpkg") == [
        {"state": "succeeded", "attempt_count": 2, "terminal_error": None}
    ]
    # The snapshot was ingested once, not twice: the redelivery picked up the
    # version the crashed attempt had already extracted.
    assert _raw_versions(schemas, "bio") == 1
    # Both attempts are in the history, so the crash is visible after the fact.
    assert _query(
        f"SELECT attempt, stage, outcome FROM {schemas['service']}.stage_results "
        f"WHERE run_id = :run_id ORDER BY attempt, stage",
        {"run_id": _run_ids(schemas, "bio-v1")["sources/bio.gpkg"]},
    ) == [
        {"attempt": 2, "stage": "extract", "outcome": "succeeded"},
        {"attempt": 2, "stage": "load", "outcome": "succeeded"},
        {"attempt": 2, "stage": "marts", "outcome": "succeeded"},
        {"attempt": 2, "stage": "transform", "outcome": "succeeded"},
    ]


def test_runs_and_stage_results_distinguish_the_four_outcomes(
    fixture_boundaries, tmp_path
):
    """One schema, four outcomes: an operator can read what happened per attempt.

    A rejected file is terminal, a failing read is retryable, a superseded
    version is stale, and a good file succeeds — each with the attempt history
    that produced it.
    """
    pipeline = fixture_boundaries
    s3 = pipeline["s3"]
    schemas = pipeline["schemas"]
    sns = FakeSNS()
    s3.put("sources/solar.gpkg", "solar-bad", _write_mixed_source_snapshot(tmp_path, "mixed.gpkg"))
    s3.put("sources/gas.gpkg", "gas-v1", _frame_bytes(tmp_path, "gas-v1.gpkg", _fixture_frame("gas")))
    s3.read_failures["sources/gas.gpkg"] = 1
    s3.put("sources/bio.gpkg", "bio-v1", _frame_bytes(tmp_path, "bio-v1.gpkg", _fixture_frame("bio")))
    s3.put("sources/hydro.gpkg", "hydro-v1", _frame_bytes(tmp_path, "hydro-v1.gpkg", _fixture_frame("hydro")))

    # The first delivery ingests the bio snapshot and fails the gas read; the
    # rejected solar file settles in the first pass already.
    _deliver_attempt(
        pipeline,
        _message(
            "batch-1",
            [
                ("sources/solar.gpkg", "solar-bad"),
                ("sources/gas.gpkg", "gas-v1"),
                ("sources/bio.gpkg", "bio-v1"),
            ],
        ),
        sns=sns,
    )
    # S3 now serves a newer hydro version, so the queued one is stale when it
    # finally arrives, and the gas read works this time.
    s3.put("sources/hydro.gpkg", "hydro-v2", _frame_bytes(tmp_path, "hydro-v2.gpkg", _fixture_frame("hydro")))
    _deliver_attempt(
        pipeline,
        _message(
            "batch-2",
            [("sources/gas.gpkg", "gas-v1"), ("sources/hydro.gpkg", "hydro-v1")],
        ),
        sns=sns,
        attempt=2,
    )

    runs = {
        row["object_key"]: row
        for row in _query(
            f"SELECT object_key, state, attempt_count, terminal_error "
            f"FROM {schemas['service']}.ingestion_runs"
        )
    }

    assert {key: row["state"] for key, row in runs.items()} == {
        "sources/solar.gpkg": "terminal",
        "sources/gas.gpkg": "succeeded",
        "sources/bio.gpkg": "succeeded",
        "sources/hydro.gpkg": "stale",
    }
    assert runs["sources/solar.gpkg"]["attempt_count"] == 1
    assert runs["sources/gas.gpkg"]["attempt_count"] == 2
    assert runs["sources/hydro.gpkg"]["terminal_error"].startswith(
        "Superseded object version"
    )
    # The failed first attempt of the retryable run is kept as its own row.
    assert [
        (row["attempt"], row["stage"], row["outcome"])
        for row in _query(
            f"SELECT attempt, stage, outcome FROM {schemas['service']}.stage_results "
            f"WHERE run_id = :run_id ORDER BY attempt, stage",
            {"run_id": _run_ids(schemas, "gas-v1")["sources/gas.gpkg"]},
        )
    ] == [
        (1, "processing", "failed"),
        (2, "extract", "succeeded"),
        (2, "load", "succeeded"),
        (2, "marts", "succeeded"),
        (2, "transform", "succeeded"),
    ]
    # The rejected file's failure is a per-table result of the extract stage.
    assert [
        (row["stage"], row["outcome"])
        for row in _query(
            f"SELECT stage, outcome FROM {schemas['service']}.stage_results "
            f"WHERE run_id = :run_id",
            {"run_id": _run_ids(schemas, "solar-bad")["sources/solar.gpkg"]},
        )
    ] == [("extract", "failed")]


def test_boto3_sqs_reports_the_delivery_attempt_and_extends_visibility():
    client = FakeSqsClient(
        {
            "Messages": [
                {
                    "MessageId": "id-1",
                    "ReceiptHandle": "receipt-1",
                    "Body": '{"Records": []}',
                    "Attributes": {"ApproximateReceiveCount": "4"},
                }
            ]
        }
    )
    sqs = ingestion.Boto3SQSAdapter(client, queue_url="https://sqs.eu/queue")

    message = sqs.receive_message()

    # The attempt count is what the worker counts retries with.
    assert message.attempt == 4
    assert client.calls[0]["AttributeNames"] == ["ApproximateReceiveCount"]
    # A message without the attribute is the first delivery.
    assert ingestion.Boto3SQSAdapter(
        FakeSqsClient({"Messages": [{"ReceiptHandle": "r", "Body": "{}"}]}),
        queue_url="https://sqs.eu/queue",
    ).receive_message().attempt == 1

    sqs.change_message_visibility("receipt-1", ingestion.VISIBILITY_TIMEOUT_SECONDS)

    assert client.calls[-1] == {
        "QueueUrl": "https://sqs.eu/queue",
        "ReceiptHandle": "receipt-1",
        "VisibilityTimeout": ingestion.VISIBILITY_TIMEOUT_SECONDS,
    }
    # SQS will not grant more than six hours.
    assert ingestion.VISIBILITY_TIMEOUT_SECONDS == 6 * 60 * 60


def test_a_slow_message_keeps_its_visibility_extended(source_pipeline, monkeypatch):
    """Long work would outlive the queue's visibility timeout without a heartbeat.

    A Boundary release re-enriches the whole country and can take minutes; the
    worker refreshes the six-hour visibility while it holds the message, so the
    queue does not hand the same message to a second worker.
    """
    holding = threading.Event()
    released = threading.Event()

    class SlowSQS(ScriptedSQS):
        def change_message_visibility(self, receipt_handle, timeout_seconds):
            super().change_message_visibility(receipt_handle, timeout_seconds)
            released.set()

    s3 = source_pipeline["s3"]
    s3.put("sources/solar.gpkg", "version-42", b"version-42-content")
    sqs = SlowSQS(_message("slow-1", [("sources/solar.gpkg", "version-42")]))
    processor = FakeProcessor()

    def slow_finalize():
        holding.wait(5)
        return FakeProcessor.finalize(processor)

    processor.finalize = slow_finalize
    monkeypatch.setattr(ingestion, "VISIBILITY_HEARTBEAT_SECONDS", 0.01)

    def run():
        ingestion.run_worker(
            engine=source_pipeline["engine"],
            s3=s3,
            sqs=sqs,
            sns=FakeSNS(),
            processor=processor,
            max_messages=1,
            idle_sleep=lambda seconds: None,
        )

    worker = threading.Thread(target=run)
    worker.start()
    assert released.wait(5), "the worker never extended the message's visibility"
    time.sleep(0.05)  # long enough for several beats at the patched interval
    holding.set()
    worker.join(10)

    assert not worker.is_alive()
    assert processor.objects, "the worker never processed the message"
    # The beat repeats while the work runs, and every beat asks for the maximum.
    assert len(sqs.visibility) >= 2, "the heartbeat did not repeat"
    assert {timeout for _, timeout in sqs.visibility} == {
        ingestion.VISIBILITY_TIMEOUT_SECONDS
    }
    assert sqs.deleted == ["slow-1"]
