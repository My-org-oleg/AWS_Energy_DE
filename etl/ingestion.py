from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from typing import NamedTuple, Protocol
from urllib.parse import unquote_plus

from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.boundaries import (
    BoundaryObject,
    BoundaryReplacementError,
    BoundaryValidationError,
    GeographyReport,
    inspect_boundary_gpkg,
    rebuild_geography,
    replace_boundary_levels,
)
from etl.config import SERVICE_SCHEMA, SOURCE_NAMES
from etl.db_utils import _create_log_table, _ensure_schema
from etl.extract import extract_source_snapshot
from etl.load import ensure_core_tables, load_source_snapshot
from etl.marts import build_marts
from etl.source_data import (
    SourceDataset,
    SourceValidationError,
    inspect_source_gpkg,
)
from etl.transform import transform_source_snapshot


BOUNDARY_KEYS = tuple(f"boundaries/level-{level}.gpkg" for level in range(4))
SOURCE_KEYS = tuple(f"sources/{source}.gpkg" for source in SOURCE_NAMES)
ACCEPTED_KEYS = frozenset(BOUNDARY_KEYS + SOURCE_KEYS)

# How long one receive waits for a message before returning empty, and how long
# the worker pauses after an empty receive before asking again. The wait is
# SQS long polling, so an idle worker costs no requests; the pause only paces
# the loop once a receive has come back empty.
LONG_POLL_SECONDS = 20
IDLE_POLL_SECONDS = 5.0

# How many deliveries one message gets before SQS moves it to the DLQ, which is
# the queue's redrive `maxReceiveCount`: the worker has to count the same way or
# it would keep working a message the queue has already given up on.
MAX_DELIVERY_ATTEMPTS = 5

# The visibility timeout is how long the queue keeps a message invisible to
# other workers while this one holds it. Six hours is the SQS maximum, and the
# worker refreshes it on a heartbeat for as long as it works: one Boundary
# release re-enriches the whole country and outlives a shorter timeout.
VISIBILITY_TIMEOUT_SECONDS = 6 * 60 * 60
VISIBILITY_HEARTBEAT_SECONDS = 60.0

# The three conditions that refuse a startup, and the alert subject each one
# publishes. Both halves are keyed on these strings: the reason goes into the
# alert claim's fingerprint, so renaming one retires the old claim and lets the
# condition alert once more — which is the wanted behaviour for a genuinely
# renamed condition, and never a good reason to rename one.
REFUSAL_PRECONDITION = "precondition"
REFUSAL_MISSING_BOUNDARY = "missing-boundary"
REFUSAL_BOUNDARY_RELEASE = "boundary-release"
REFUSAL_ENQUEUE = "enqueue"

_REFUSAL_SUBJECTS = {
    REFUSAL_PRECONDITION: "Startup precondition failed",
    REFUSAL_MISSING_BOUNDARY: "Boundary release missing",
    REFUSAL_BOUNDARY_RELEASE: "Boundary release rejected",
    REFUSAL_ENQUEUE: "Startup could not enqueue work",
}

# What to do about each, written per reason rather than once for all of them.
# The Boundary reference layer is only ever the subject of two of these: a
# release that is missing and a release that was rejected. A database that will
# not answer, or a queue that will not accept, is a different fault with a
# different fix, and saying "upload the release" for those would be a confident
# pointer at the wrong thing.
_REFUSAL_REMEDIES = {
    REFUSAL_MISSING_BOUNDARY: (
        "A missing Source dataset is non-fatal, so this is about the Boundary "
        "reference layer. Publish the missing release under its fixed S3 key."
    ),
    REFUSAL_PRECONDITION: (
        "This is a failure to reach or prepare the deployment's own "
        "infrastructure (the database or the bucket), not a missing file. Check "
        "that the database is reachable and the bucket readable."
    ),
    REFUSAL_BOUNDARY_RELEASE: (
        "The Boundary release was found but could not be applied. Check the "
        "pipeline logs for the underlying error; a re-upload under the same "
        "fixed key is not the remedy."
    ),
    REFUSAL_ENQUEUE: (
        "The Boundary layer applied, but the work could not be handed to the "
        "worker. This is the queue or the SQS side of the deployment, not the "
        "Boundary data."
    ),
}

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BootstrapConfig:
    bucket: str


@dataclass(frozen=True)
class S3ObjectId:
    bucket: str
    key: str
    version_id: str
    last_modified: datetime | None = None

    def __post_init__(self) -> None:
        if not self.bucket or not self.key:
            raise ValueError("S3 object identity requires bucket and key")
        if not self.version_id.strip() or self.version_id == "null":
            raise ValueError("S3 object identity requires an immutable version id")


@dataclass(frozen=True)
class BootstrapCheck:
    key: str
    required: bool
    available: bool
    object_id: S3ObjectId | None
    message: str


@dataclass(frozen=True)
class BootstrapResult:
    metadata_ready: bool
    worker_start_allowed: bool
    checks: tuple[BootstrapCheck, ...]


class RunState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRYABLE = "retryable"
    TERMINAL = "terminal"
    STALE = "stale"


class StaleObjectError(RuntimeError):
    pass


class SourceSnapshotError(RuntimeError):
    """A pipeline stage refused the input.

    Retryable by default, because most stage failures are about the state
    around the file — the database, the tables a previous run left, the
    boundaries — and not about the file itself. `RejectedObjectError` is the
    exception: that one is about the object version and can never succeed.
    """

    def __init__(self, results: Sequence[StageResult], message: str):
        super().__init__(message)
        self.results = tuple(results)


class RejectedObjectError(SourceSnapshotError):
    """The object version failed its own content validation.

    An S3 object version is immutable, so validating it again returns the same
    rejection: the run is settled as terminal and the operator is alerted
    instead of the queue spending five deliveries on it.
    """


@dataclass(frozen=True)
class StageResult:
    target: str
    stage: str
    outcome: str
    row_count: int | None = None
    error: str | None = None
    details: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class SqsMessage:
    receipt_handle: str
    body: str
    attempt: int = 1
    """How many times the queue has delivered this message, first delivery 1."""


@dataclass(frozen=True)
class MessageResult:
    acknowledged: bool
    states: tuple[RunState, ...]


class S3Adapter(Protocol):
    def head_current(self, bucket: str, key: str) -> S3ObjectId | None: ...

    def read_version(self, object_id: S3ObjectId) -> bytes: ...


class SQSAdapter(Protocol):
    def receive_message(self) -> SqsMessage | None: ...

    def send_message(self, body: str) -> None: ...

    def change_message_visibility(
        self, receipt_handle: str, timeout_seconds: int
    ) -> None: ...

    def delete_message(self, receipt_handle: str) -> None: ...


class SNSAdapter(Protocol):
    def publish(self, subject: str, message: str) -> None: ...


class MessageProcessor(Protocol):
    def process(
        self, object_id: S3ObjectId, body: bytes
    ) -> Sequence[StageResult]: ...

    def process_boundaries(
        self, objects: Sequence[tuple[S3ObjectId, bytes]]
    ) -> Sequence[StageResult]: ...

    def finalize(self) -> Sequence[StageResult]: ...


class Boto3S3Adapter:
    def __init__(self, client):
        self.client = client

    def head_current(self, bucket: str, key: str) -> S3ObjectId | None:
        from botocore.exceptions import ClientError

        try:
            response = self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return S3ObjectId(
            bucket, key, response.get("VersionId") or "", response.get("LastModified")
        )

    def read_version(self, object_id: S3ObjectId) -> bytes:
        response = self.client.get_object(
            Bucket=object_id.bucket,
            Key=object_id.key,
            VersionId=object_id.version_id,
        )
        return response["Body"].read()


class Boto3SQSAdapter:
    """The one ingestion queue, received one message at a time.

    SQS batches the S3 records of a single upload into the body of one message
    already, and this worker keeps that a unit of work: asking for more than
    one message would combine independent messages whose records then share a
    single acknowledgement.
    """

    def __init__(self, client, queue_url: str):
        self.client = client
        self.queue_url = queue_url

    def receive_message(self) -> SqsMessage | None:
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=LONG_POLL_SECONDS,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = response.get("Messages") or []
        if not messages:
            return None
        message = messages[0]
        attributes = message.get("Attributes") or {}
        return SqsMessage(
            message["ReceiptHandle"],
            message["Body"],
            attempt=int(attributes.get("ApproximateReceiveCount", 1)),
        )

    def send_message(self, body: str) -> None:
        self.client.send_message(QueueUrl=self.queue_url, MessageBody=body)

    def change_message_visibility(
        self, receipt_handle: str, timeout_seconds: int
    ) -> None:
        self.client.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=timeout_seconds,
        )

    def delete_message(self, receipt_handle: str) -> None:
        self.client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
        )


class Boto3SNSAdapter:
    """The one topic carrying all three alert paths (issues #7 and #8).

    A rejected object version from the worker, a blocked startup from the
    bootstrap, and — from CloudWatch rather than from here — the DLQ alarm.
    """

    def __init__(self, client, topic_arn: str):
        self.client = client
        self.topic_arn = topic_arn

    def publish(self, subject: str, message: str) -> None:
        self.client.publish(
            TopicArn=self.topic_arn, Subject=subject, Message=message
        )


def _rejection(
    object_id: S3ObjectId,
    stage: str,
    error: SourceValidationError,
    results: Sequence[StageResult] = (),
) -> RejectedObjectError:
    """Report a content-validation error as a failed stage of a rejected version.

    A file that fails its own validation is a deterministic input error, so it
    gets a stage result like any other stage outcome rather than surfacing as an
    unexplained exception — and, since the version is immutable, it settles the
    run as terminal rather than asking the queue to deliver it again. The stage
    that caught it is named, because a rejection found by the transform is a
    different kind of bad file than one found by the inspect, and the stages that
    did pass are kept, so the attempt history says how far the file got.
    """
    return RejectedObjectError(
        [
            *results,
            StageResult(
                target=object_id.key,
                stage=stage,
                outcome="failed",
                error=str(error),
            ),
        ],
        f"{stage} failed: {error}",
    )


class PipelineProcessor:
    """The production processor: Source snapshots, Boundary releases, marts."""

    def __init__(self, engine: Engine, *, s3: S3Adapter):
        self.engine = engine
        self.s3 = s3

    def process(
        self, object_id: S3ObjectId, body: bytes
    ) -> Sequence[StageResult]:
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as handle:
            path = Path(handle.name)
            handle.write(body)
        try:
            dataset = self._inspect(object_id, path)
        finally:
            path.unlink(missing_ok=True)

        run_id = self._run_id(object_id)
        try:
            extraction = extract_source_snapshot(
                dataset,
                engine=self.engine,
                filename=object_id.key,
                bucket=object_id.bucket,
                object_key=object_id.key,
                object_version_id=object_id.version_id,
                ingestion_run_id=run_id,
            )
        except SourceValidationError as error:
            raise _rejection(object_id, "extract", error) from error
        result = StageResult(
            target=object_id.key,
            stage="extract",
            outcome="failed" if extraction.errors else "succeeded",
            row_count=extraction.rows_loaded,
            error="; ".join(extraction.errors) or None,
            details={
                "energy_source": dataset.source,
                "raw_table": extraction.loaded_to,
                "object_version_id": object_id.version_id,
                "reused": extraction.skipped,
            },
        )
        results = [result]
        if extraction.errors:
            raise SourceSnapshotError(
                results, f"extract failed: {'; '.join(extraction.errors)}"
            )
        raw_table = extraction.loaded_to
        if raw_table is None:
            raise SourceSnapshotError(results, "extract produced no raw version")

        try:
            transform = transform_source_snapshot(
                dataset.source,
                raw_table,
                engine=self.engine,
                ingestion_run_id=run_id,
            )
        except SourceValidationError as error:
            raise _rejection(object_id, "transform", error, results) from error
        results.append(
            StageResult(
                target=dataset.source,
                stage="transform",
                outcome="failed" if transform.errors else "succeeded",
                row_count=transform.rows_written,
                error="; ".join(transform.errors) or None,
                details={
                    "raw_table": raw_table,
                    "bad_quality": transform.bad_quality,
                    "synthetic_ids": transform.synthetic_ids,
                },
            )
        )
        if transform.errors:
            raise SourceSnapshotError(
                results, f"transform failed: {'; '.join(transform.errors)}"
            )

        try:
            load = load_source_snapshot(
                dataset.source,
                engine=self.engine,
                ingestion_run_id=run_id,
            )
        except SourceValidationError as error:
            raise _rejection(object_id, "load", error, results) from error
        results.append(
            StageResult(
                target=load.target,
                stage="load",
                outcome="failed" if load.errors else "succeeded",
                row_count=load.rows_inserted + load.rows_updated,
                error="; ".join(load.errors) or None,
                details={
                    "inserted": load.rows_inserted,
                    "updated": load.rows_updated,
                    "retained": load.rows_retained,
                    "collisions": load.collisions,
                },
            )
        )
        if load.errors:
            raise SourceSnapshotError(
                results, f"load failed: {'; '.join(load.errors)}"
            )

        return tuple(results)

    def process_boundaries(
        self,
        objects: Sequence[tuple[S3ObjectId, bytes]],
        *,
        run_id_factory: Callable[[S3ObjectId], str] | None = None,
    ) -> Sequence[StageResult]:
        """Apply the Boundary objects of one message as one release (issue #5).

        Every level is validated before anything is written; one invalid level
        rejects the whole batch. The levels are then replaced in a single
        transaction and the geography of every staging Source and Core unit is
        rebuilt once. Each object gets its own `extract` result; the rebuild
        results are shared by the batch.

        The rebuild runs even when the release was already applied, which is
        what makes a redelivery of a message whose rebuild failed safe: the
        replacement is skipped, the geography is redone.

        `run_id_factory` overrides where the load ledger's `ingestion_run_id`
        comes from. The worker passes nothing and uses the run it started for
        the object version; the startup bootstrap (issue #8) passes a factory,
        because it applies releases without creating Ingestion runs.
        """
        run_id_of = run_id_factory or self._run_id
        validated: list[BoundaryObject] = []
        failures: dict[str, str] = {}
        for object_id, body in objects:
            try:
                release = _inspect_boundary(object_id, body)
            except BoundaryValidationError as error:
                failures[object_id.key] = str(error)
                continue
            validated.append(
                BoundaryObject(
                    release=release,
                    bucket=object_id.bucket,
                    object_key=object_id.key,
                    object_version_id=object_id.version_id,
                    ingestion_run_id=run_id_of(object_id),
                )
            )
        if failures:
            rejected = ", ".join(sorted(failures))
            raise RejectedObjectError(
                [
                    StageResult(
                        target=object_id.key,
                        stage="extract",
                        outcome="failed",
                        error=failures.get(
                            object_id.key,
                            f"not applied: Boundary batch rejected by {rejected}",
                        ),
                    )
                    for object_id, _ in objects
                ],
                f"extract failed: Boundary batch rejected by {rejected}",
            )

        try:
            replaced = replace_boundary_levels(self.engine, validated)
        except BoundaryReplacementError as error:
            raise SourceSnapshotError(
                [
                    StageResult(
                        target=obj.object_key,
                        stage="extract",
                        outcome="failed",
                        error=str(error),
                    )
                    for obj in validated
                ],
                f"extract failed: {error}",
            ) from error
        results = [
            StageResult(
                target=obj.object_key,
                stage="extract",
                outcome="succeeded",
                row_count=replaced.rows[obj.release.level],
                details={
                    "level": obj.release.level,
                    "object_version_id": obj.object_version_id,
                    "batch_levels": list(replaced.levels),
                    "reused": replaced.reused,
                },
            )
            for obj in validated
        ]

        try:
            geography = rebuild_geography(self.engine)
        except Exception as error:
            raise SourceSnapshotError(
                [*results, _rebuild_result("boundaries", "transform", 0, GeographyReport(), error=f"{error}")],
                f"geography rebuild failed: {error}",
            ) from error
        for target, rows in geography.staging.items():
            results.append(_rebuild_result(target, "transform", rows, geography))
        for target, rows in geography.core.items():
            results.append(
                _rebuild_result(
                    target,
                    "load",
                    rows,
                    geography,
                    collisions=geography.collisions.get(target, 0),
                )
            )
        failed = [result for result in results if result.outcome == "failed"]
        if failed:
            raise SourceSnapshotError(
                results,
                "geography rebuild failed: "
                + "; ".join(f"{r.target}: {r.error}" for r in failed),
            )
        return tuple(results)

    def finalize(self) -> Sequence[StageResult]:
        ensure_core_tables(self.engine)
        marts = build_marts(self.engine)
        result = StageResult(
            target="marts",
            stage="marts",
            outcome="failed" if marts.errors else "succeeded",
            row_count=len(marts.refreshed),
            error="; ".join(marts.errors) or None,
            details={
                "refreshed": marts.refreshed,
                "created": marts.created,
                "verified": marts.verified,
            },
        )
        if marts.errors:
            raise SourceSnapshotError(
                (result,), f"marts failed: {'; '.join(marts.errors)}"
            )
        return (result,)

    def _inspect(self, object_id: S3ObjectId, path: Path) -> SourceDataset:
        """Validate the GPKG, reporting a rejection as a failed extract stage."""
        try:
            return inspect_source_gpkg(path)
        except SourceValidationError as error:
            raise _rejection(object_id, "extract", error) from error

    def _run_id(self, object_id: S3ObjectId) -> str:
        with self.engine.connect() as connection:
            run_id = connection.execute(
                text(
                    f"SELECT run_id FROM {SERVICE_SCHEMA}.ingestion_runs "
                    "WHERE bucket = :bucket AND object_key = :object_key "
                    "AND object_version_id = :object_version_id"
                ),
                {
                    "bucket": object_id.bucket,
                    "object_key": object_id.key,
                    "object_version_id": object_id.version_id,
                },
            ).scalar()
        if run_id is None:
            raise RuntimeError("No ingestion run for object version")
        return str(run_id)


def _inspect_boundary(object_id: S3ObjectId, body: bytes):
    """Validate a Boundary object body against the level of its fixed key."""
    level = BOUNDARY_KEYS.index(object_id.key)
    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as handle:
        path = Path(handle.name)
        handle.write(body)
    try:
        return inspect_boundary_gpkg(path, expected_level=level)
    except BoundaryValidationError:
        raise
    except Exception as error:
        raise BoundaryValidationError(f"Unreadable Boundary GPKG: {error}") from error
    finally:
        path.unlink(missing_ok=True)


def _rebuild_result(target, stage, rows, geography, **details) -> StageResult:
    errors = geography.errors.get(target, [])
    return StageResult(
        target=target,
        stage=stage,
        outcome="failed" if errors else "succeeded",
        row_count=rows,
        error="; ".join(errors) or None,
        details={"geography_rebuild": True, **details},
    )


def _ensure_service_metadata(engine: Engine) -> None:
    _ensure_schema(engine, SERVICE_SCHEMA)
    _create_log_table(engine, SERVICE_SCHEMA)
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {SERVICE_SCHEMA}.ingestion_runs (
                    run_id UUID PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    input_kind TEXT,
                    object_last_modified TIMESTAMPTZ,
                    state TEXT NOT NULL,
                    current_stage TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 1,
                    terminal_error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    UNIQUE (bucket, object_key, object_version_id)
                )
                """
            )
        )
        connection.execute(
            text(
                f"ALTER TABLE {SERVICE_SCHEMA}.ingestion_runs "
                "ADD COLUMN IF NOT EXISTS input_kind TEXT"
            )
        )
        connection.execute(
            text(
                f"ALTER TABLE {SERVICE_SCHEMA}.ingestion_runs "
                "ADD COLUMN IF NOT EXISTS object_last_modified TIMESTAMPTZ"
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {SERVICE_SCHEMA}.stage_results (
                    stage_result_id BIGSERIAL PRIMARY KEY,
                    run_id UUID NOT NULL REFERENCES {SERVICE_SCHEMA}.ingestion_runs(run_id),
                    attempt INTEGER NOT NULL,
                    target TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    row_count BIGINT,
                    error TEXT,
                    details JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMPTZ,
                    UNIQUE (run_id, attempt, target, stage)
                )
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {SERVICE_SCHEMA}.source_memberships (
                    run_id UUID NOT NULL REFERENCES {SERVICE_SCHEMA}.ingestion_runs(run_id),
                    energy_source TEXT NOT NULL,
                    unit_key TEXT NOT NULL,
                    reference_id TEXT,
                    bad_quality BOOLEAN NOT NULL,
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (run_id, unit_key)
                )
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {SERVICE_SCHEMA}.bootstrap_alerts (
                    fingerprint TEXT PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    missing_keys TEXT[] NOT NULL DEFAULT '{{}}',
                    error TEXT,
                    alerted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )


def bootstrap(
    config: BootstrapConfig,
    *,
    engine: Engine,
    s3: S3Adapter,
) -> BootstrapResult:
    _ensure_service_metadata(engine)
    checks = []
    for required, keys in ((True, BOUNDARY_KEYS), (False, SOURCE_KEYS)):
        for key in keys:
            object_id = s3.head_current(config.bucket, key)
            checks.append(
                BootstrapCheck(
                    key=key,
                    required=required,
                    available=object_id is not None,
                    object_id=object_id,
                    message="available" if object_id is not None else "missing",
                )
            )
    return BootstrapResult(
        metadata_ready=True,
        worker_start_allowed=all(
            check.available for check in checks if check.required
        ),
        checks=tuple(checks),
    )


@dataclass(frozen=True)
class StartupResult:
    """What the explicit startup bootstrap did, and whether the worker may start.

    `release_results` carries the boundary-release, geography-rebuild and
    marts stage results of the bootstrap itself (empty when every release was
    already current). `enqueued` holds the Source object versions sent to the
    queue; `known` the ones skipped because the service already holds a run
    for them.
    """

    metadata_ready: bool
    worker_start_allowed: bool
    checks: tuple[BootstrapCheck, ...]
    release_results: tuple[StageResult, ...]
    enqueued: tuple[S3ObjectId, ...]
    known: tuple[S3ObjectId, ...]
    alerted: bool = False


def run_startup(
    config: BootstrapConfig,
    *,
    engine: Engine,
    s3: S3Adapter,
    sqs: SQSAdapter,
    sns: SNSAdapter,
    processor: MessageProcessor,
) -> StartupResult:
    """Bootstrap a server deployment, then report whether the worker may start.

    The explicit startup path (issue #8), in order:

    1. database preconditions and the fixed S3 key checks (`bootstrap`);
    2. the current Boundary releases applied as one batch, with the downstream
       geography rebuilt and the marts refreshed once;
    3. the current Source object versions enqueued for the worker.

    A missing or invalid Boundary condition is fatal: the result says the
    worker must not start, so the caller refuses it, and the refusal is
    announced on the alert topic, because the deployment's answer to a fatal
    condition is to exit and let the restart policy bring it straight back. A
    missing Source object is non-fatal and is not what that alert is about. No
    Ingestion run is created here — a run belongs to the worker's processing of
    a message, not to the bootstrap that only prepares and enqueues it.
    """
    try:
        report = bootstrap(config, engine=engine, s3=s3)
    except Exception as error:
        return _refused(
            config,
            engine,
            sns,
            report=BootstrapResult(
                metadata_ready=False,
                worker_start_allowed=False,
                checks=(),
            ),
            reason=REFUSAL_PRECONDITION,
            error=error,
        )
    if not report.worker_start_allowed:
        return _refused(
            config, engine, sns, report=report, reason=REFUSAL_MISSING_BOUNDARY
        )

    # The two steps are refused separately, because they are two different
    # problems: an alert titled "Boundary release rejected" that was really a
    # queue that could not be reached sends the operator to re-upload a release
    # that is already published and fine.
    # The two steps are refused separately, because they are two different
    # problems: an alert titled "Boundary release rejected" that was really a
    # queue that could not be reached sends the operator to re-upload a release
    # that is already published and fine.
    try:
        release_results = tuple(
            _apply_boundary_releases(config, engine, s3, processor)
        )
    except Exception as error:
        return _refused(
            config,
            engine,
            sns,
            report=report,
            reason=REFUSAL_BOUNDARY_RELEASE,
            error=error,
        )

    try:
        enqueued, known = _enqueue_current_sources(config, engine, s3, sqs)
    except Exception as error:
        return _refused(
            config,
            engine,
            sns,
            report=report,
            reason=REFUSAL_ENQUEUE,
            error=error,
        )

    _clear_bootstrap_claims(engine, config.bucket)
    return StartupResult(
        metadata_ready=report.metadata_ready,
        worker_start_allowed=True,
        checks=report.checks,
        release_results=release_results,
        enqueued=enqueued,
        known=known,
    )


def _refused(
    config: BootstrapConfig,
    engine: Engine,
    sns: SNSAdapter,
    *,
    report: BootstrapResult,
    reason: str,
    error: Exception | None = None,
) -> StartupResult:
    """The startup result for a deployment the worker must not start against.

    The alert is part of the refusal, not a courtesy attached to it: the
    container is about to exit and the restart policy is about to run this
    again, and a condition nobody was told about is an outage nobody is looking
    for. `alerted` reports whether this attempt was the one that sent it.
    """
    alerted = _alert_bootstrap(
        engine,
        sns,
        config,
        reason=reason,
        checks=report.checks,
        error=error,
    )
    return StartupResult(
        metadata_ready=report.metadata_ready,
        worker_start_allowed=False,
        checks=report.checks,
        release_results=(),
        enqueued=(),
        known=(),
        alerted=alerted,
    )


def _apply_boundary_releases(
    config: BootstrapConfig,
    engine: Engine,
    s3: S3Adapter,
    processor: MessageProcessor,
) -> Sequence[StageResult]:
    """Load the new Boundary releases, then refresh the marts once.

    A release already in the load ledger was applied from that exact immutable
    version before, so only never-seen versions are processed — an S3 version
    cannot change under a ledgered id. One invalid level rejects the whole
    batch, so a bad file can never partially apply. The marts are refreshed
    once afterwards, so a boundary-only change leaves no stale pivots even when
    no Source object is available to carry a worker message.
    """
    pending: list[tuple[S3ObjectId, bytes]] = []
    for key in BOUNDARY_KEYS:
        object_id = s3.head_current(config.bucket, key)
        if object_id is None:
            continue
        body = s3.read_version(object_id)
        if _release_ledgered(engine, object_id):
            continue
        pending.append((object_id, body))
    if not pending:
        return ()
    results = list(
        processor.process_boundaries(
            pending, run_id_factory=lambda _: str(uuid.uuid4())
        )
    )
    results.extend(processor.finalize())
    return results


def _release_ledgered(engine: Engine, object_id: S3ObjectId) -> bool:
    return _has_row(engine, "loaded_files", object_id)


def _enqueue_current_sources(
    config: BootstrapConfig,
    engine: Engine,
    s3: S3Adapter,
    sqs: SQSAdapter,
) -> tuple[tuple[S3ObjectId, ...], tuple[S3ObjectId, ...]]:
    """Enqueue the current version of every available Source object.

    A version the service already holds a run for is left alone, whatever that
    run's state: succeeded, stale and terminal versions are settled, a DLQ
    version's run is still there (the message left the queue, not the run), and
    an in-flight retryable version's message is still in the queue for SQS to
    redeliver. Re-enqueuing any of them would duplicate work, so only
    never-seen versions are sent — and no Ingestion run is created for them.
    """
    enqueued: list[S3ObjectId] = []
    known: list[S3ObjectId] = []
    for key in SOURCE_KEYS:
        object_id = s3.head_current(config.bucket, key)
        if object_id is None:
            continue
        if _has_run(engine, object_id):
            known.append(object_id)
            continue
        sqs.send_message(_s3_event_body(object_id))
        enqueued.append(object_id)
    return tuple(enqueued), tuple(known)


def _has_run(engine: Engine, object_id: S3ObjectId) -> bool:
    """True if the service holds an Ingestion run for this exact object version."""
    return _has_row(engine, "ingestion_runs", object_id)


def _has_row(engine: Engine, table: str, object_id: S3ObjectId) -> bool:
    """True if `service.<table>` holds a row for this exact S3 object version."""
    with engine.connect() as connection:
        return (
            connection.execute(
                text(
                    f"SELECT 1 FROM {SERVICE_SCHEMA}.{table} "
                    "WHERE bucket = :bucket AND object_key = :object_key "
                    "AND object_version_id = :version"
                ),
                {
                    "bucket": object_id.bucket,
                    "object_key": object_id.key,
                    "version": object_id.version_id,
                },
            ).first()
            is not None
        )


def _s3_event_body(object_id: S3ObjectId) -> str:
    """The S3 ObjectCreated event body the worker's message parser accepts."""
    return json.dumps(
        {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": object_id.bucket},
                        "object": {
                            "key": object_id.key,
                            "versionId": object_id.version_id,
                        },
                    },
                }
            ]
        }
    )


def redrive(
    engine: Engine,
    *,
    sqs: SQSAdapter,
    key: str | None = None,
) -> tuple[S3ObjectId, ...]:
    """Re-enqueue failed or DLQ object versions for another attempt.

    The explicit recovery path (issue #8): every object version the worker left
    unsettled — a retryable failure, or a message the queue spent its deliveries
    on and moved to the DLQ — is sent back to the queue, whose redelivery
    resumes the version's existing run. Settled versions (succeeded, stale,
    terminal) are left alone, and `key` narrows the redrive to one object key.
    """
    query = (
        f"SELECT bucket, object_key, object_version_id "
        f"FROM {SERVICE_SCHEMA}.ingestion_runs WHERE state = :state"
    )
    parameters: dict[str, object] = {"state": RunState.RETRYABLE.value}
    if key is not None:
        query += " AND object_key = :object_key"
        parameters["object_key"] = key
    query += " ORDER BY object_key"
    with engine.connect() as connection:
        rows = connection.execute(text(query), parameters).mappings().all()
    redriven: list[S3ObjectId] = []
    for row in rows:
        object_id = S3ObjectId(
            row["bucket"], row["object_key"], row["object_version_id"]
        )
        sqs.send_message(_s3_event_body(object_id))
        redriven.append(object_id)
    return tuple(redriven)


@dataclass(frozen=True)
class MessageObjects:
    """The accepted objects of a message and whether it is safe to delete it.

    A record for an unaccepted key is settled work: it is deliberately ignored,
    so the message can be deleted. A record that cannot be read as an S3
    identity is *not* settled — nothing has claimed it, and acknowledging would
    drop it without a run, so such a message is left for redelivery until the
    deterministic-failure path (issue #36, stories 28-29) can announce it.
    """

    object_ids: tuple[S3ObjectId, ...] = ()
    acknowledgable: bool = False


def _object_ids(message: SqsMessage) -> MessageObjects:
    try:
        records = json.loads(message.body)["Records"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return MessageObjects()
    object_ids = []
    malformed = False
    for record in records:
        try:
            s3_record = record["s3"]
            object_id = S3ObjectId(
                bucket=s3_record["bucket"]["name"],
                key=unquote_plus(s3_record["object"]["key"]),
                version_id=s3_record["object"]["versionId"],
            )
        except (KeyError, TypeError, ValueError):
            log.warning("Ignoring unreadable S3 record in message: %r", record)
            malformed = True
            continue
        if object_id.key not in ACCEPTED_KEYS:
            log.info("Ignoring event for unaccepted key %s", object_id.key)
            continue
        object_ids.append(object_id)
    return MessageObjects(tuple(object_ids), not malformed)


def _input_kind(object_id: S3ObjectId) -> str:
    return "boundary" if object_id.key in BOUNDARY_KEYS else "source"


def _resolve_current(engine: Engine, s3: S3Adapter, event: S3ObjectId) -> S3ObjectId:
    """Return the announced version stamped with the timestamp S3 serves it.

    The version check rejects an event S3 has already superseded. The age check
    then rejects a delayed redelivery of a version whose key has since been
    ingested from a newer upload — reachable when a lifecycle rule has removed
    that newer version, leaving the older one current again. Both need the HEAD
    timestamp, because an SQS event carries no `LastModified`, and the run row
    has to be stamped before the run starts.
    """
    current = s3.head_current(event.bucket, event.key)
    if current is None or current.version_id != event.version_id:
        raise StaleObjectError(
            f"Superseded object version: {event.key} {event.version_id}"
        )
    resolved = S3ObjectId(
        event.bucket,
        event.key,
        event.version_id,
        current.last_modified or event.last_modified,
    )
    newer = _newer_succeeded_run(engine, resolved)
    if newer is not None:
        raise StaleObjectError(
            f"Delayed object version {event.version_id} of {event.key} "
            f"is older than already-loaded {newer}"
        )
    return resolved


def _stamp_last_modified(
    engine: Engine, run_id: uuid.UUID, last_modified: datetime | None
) -> None:
    """Record the served timestamp of the version this run is processing.

    The run row is created from the SQS event, which carries no `LastModified`,
    so the HEAD result is written here. A superseded version has no timestamp to
    record and keeps a null column rather than a guessed one.
    """
    if last_modified is None:
        return
    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {SERVICE_SCHEMA}.ingestion_runs "
                "SET object_last_modified = :last_modified "
                "WHERE run_id = :run_id"
            ),
            {"last_modified": last_modified, "run_id": str(run_id)},
        )


def _newer_succeeded_run(engine: Engine, object_id: S3ObjectId) -> str | None:
    """Return the version id of a newer already-succeeded run for this key."""
    if object_id.last_modified is None:
        return None
    with engine.connect() as connection:
        row = connection.execute(
            text(
                f"SELECT object_version_id FROM {SERVICE_SCHEMA}.ingestion_runs "
                "WHERE bucket = :bucket AND object_key = :object_key "
                "AND state = :state "
                "AND object_last_modified > :last_modified "
                "ORDER BY object_last_modified DESC LIMIT 1"
            ),
            {
                "bucket": object_id.bucket,
                "object_key": object_id.key,
                "state": RunState.SUCCEEDED.value,
                "last_modified": object_id.last_modified,
            },
        ).scalar()
    return str(row) if row is not None else None


def _start_run(engine: Engine, object_id: S3ObjectId):
    with engine.begin() as connection:
        row = connection.execute(
            text(
                f"SELECT run_id, state, attempt_count "
                f"FROM {SERVICE_SCHEMA}.ingestion_runs "
                "WHERE bucket = :bucket "
                "AND object_key = :object_key "
                "AND object_version_id = :object_version_id"
            ),
            {
                "bucket": object_id.bucket,
                "object_key": object_id.key,
                "object_version_id": object_id.version_id,
            },
        ).mappings().first()
        if row is None:
            run_id = uuid.uuid4()
            attempt = 1
            connection.execute(
                text(
                    f"INSERT INTO {SERVICE_SCHEMA}.ingestion_runs "
                    "(run_id, bucket, object_key, object_version_id, "
                    "input_kind, object_last_modified, state) "
                    "VALUES (:run_id, :bucket, :object_key, :object_version_id, "
                    ":input_kind, :object_last_modified, :state)"
                ),
                {
                    "run_id": str(run_id),
                    "bucket": object_id.bucket,
                    "object_key": object_id.key,
                    "object_version_id": object_id.version_id,
                    "input_kind": _input_kind(object_id),
                    "object_last_modified": object_id.last_modified,
                    "state": RunState.PENDING.value,
                },
            )
        else:
            run_id = row["run_id"]
            existing_state = RunState(row["state"])
            if existing_state in {
                RunState.SUCCEEDED,
                RunState.TERMINAL,
                RunState.STALE,
            }:
                return run_id, row["attempt_count"], existing_state
            attempt = row["attempt_count"] + 1
        connection.execute(
            text(
                f"UPDATE {SERVICE_SCHEMA}.ingestion_runs "
                "SET input_kind = :input_kind, "
                "object_last_modified = COALESCE(:object_last_modified, "
                "object_last_modified), "
                "state = :state, "
                "current_stage = :current_stage, "
                "attempt_count = :attempt, started_at = CURRENT_TIMESTAMP, "
                "finished_at = NULL, terminal_error = NULL "
                "WHERE run_id = :run_id"
            ),
            {
                "run_id": str(run_id),
                "input_kind": _input_kind(object_id),
                "object_last_modified": object_id.last_modified,
                "state": RunState.RUNNING.value,
                "current_stage": "processing",
                "attempt": attempt,
            },
        )
    return run_id, attempt, RunState.RUNNING


def _record_success(
    engine: Engine,
    run_id: uuid.UUID,
    attempt: int,
    results: Sequence[StageResult],
) -> None:
    _record_stage_results(engine, run_id, attempt, results)
    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {SERVICE_SCHEMA}.ingestion_runs "
                "SET state = :state, current_stage = 'complete', "
                "finished_at = CURRENT_TIMESTAMP "
                "WHERE run_id = :run_id"
            ),
            {"run_id": str(run_id), "state": RunState.SUCCEEDED.value},
        )


def _record_stage_results(
    engine: Engine,
    run_id: uuid.UUID,
    attempt: int,
    results: Sequence[StageResult],
) -> None:
    with engine.begin() as connection:
        for result in results:
            connection.execute(
                text(
                    f"INSERT INTO {SERVICE_SCHEMA}.stage_results "
                    "(run_id, attempt, target, stage, outcome, row_count, error, "
                    "details, finished_at) "
                    "VALUES (:run_id, :attempt, :target, :stage, :outcome, "
                    ":row_count, :error, CAST(:details AS jsonb), CURRENT_TIMESTAMP)"
                ),
                {
                    "run_id": str(run_id),
                    "attempt": attempt,
                    "target": result.target,
                    "stage": result.stage,
                    "outcome": result.outcome,
                    "row_count": result.row_count,
                    "error": result.error,
                    "details": json.dumps(result.details),
                },
            )


def _record_retryable(
    engine: Engine,
    run_id: uuid.UUID,
    error: Exception,
    stage: str = "processing",
) -> None:
    _record_failure(engine, run_id, RunState.RETRYABLE, error, stage)


def _record_terminal(
    engine: Engine,
    run_id: uuid.UUID,
    error: Exception,
    stage: str = "processing",
) -> None:
    _record_failure(engine, run_id, RunState.TERMINAL, error, stage)


def _alert_rejected(
    sns: SNSAdapter,
    object_id: S3ObjectId,
    run_id: uuid.UUID,
    error: Exception,
) -> None:
    """Tell the operator that a file will never be ingested as it stands.

    This is the one direct alert the worker sends. A message that runs out of
    deliveries is not announced here: it lands in the DLQ, and the DLQ alarm
    publishes that, so an infrastructure problem is reported once. The run is
    already recorded terminal when this is called, so a worker that dies here
    loses the alert but never sends it twice; a failed send is logged and does
    not hold up the other records in the message.
    """
    log.error("Rejected %s: %s", object_id.key, error)
    try:
        sns.publish(
            f"Rejected {object_id.key}",
            f"Ingestion run {run_id} rejected {object_id.key} "
            f"version {object_id.version_id} from {object_id.bucket}: {error}\n"
            "The object version is immutable, so it is not retried. Upload a new "
            "version of the file to ingest it; the run and its per-table results "
            "are in service.ingestion_runs and service.stage_results.",
        )
    except Exception:
        log.exception("Could not alert about the rejected %s", object_id.key)


def _refusal_fingerprint(
    bucket: str,
    reason: str,
    missing: tuple[str, ...],
    error: Exception | None,
) -> str:
    """The identity of one refused startup, for deciding whether to alert.

    A fatal condition refuses the worker and the container exits, so the restart
    policy brings the bootstrap straight back — the same refusal, as many times
    as the operator takes to look. A message per restart is how a topic teaches
    its subscribers to ignore it, so the alert is keyed on what is wrong rather
    than on how many times it has been seen.

    The error contributes its type and not its message. A message carrying a
    row number, a timestamp or an object version would mint a fresh fingerprint
    on every restart, which is the storm this fingerprint exists to prevent.
    """
    return "|".join(
        (
            bucket,
            reason,
            ",".join(missing) if missing else "-",
            type(error).__name__ if error is not None else "-",
        )
    )


def _missing_required_keys(checks: Sequence[BootstrapCheck]) -> tuple[str, ...]:
    """The required fixed keys the bootstrap did not find, in key order."""
    return tuple(
        check.key for check in checks if check.required and not check.available
    )


def _alert_bootstrap(
    engine: Engine,
    sns: SNSAdapter,
    config: BootstrapConfig,
    *,
    reason: str,
    checks: Sequence[BootstrapCheck],
    error: Exception | None,
) -> bool:
    """Tell the operator the deployment must not start; True if it was sent.

    The second of the three alert paths on the one topic, and the only one the
    worker does not own: this fires before there is a worker. It is what turns a
    container that restart-policies itself into a loop into an outage somebody
    is told about.

    The claim row is what keeps that from becoming a message per restart. The
    fingerprint goes in first and the publish only happens if the insert was
    new, so one distinct refusal alerts once however many times the restart
    policy re-runs it, while a refusal that *changes* — a different level
    missing, a different error — alerts again because it is a different fact.

    Unlike `_alert_rejected`, a failed publish gives the claim back. There the
    bounded retries of one message are spent, so a lost alert is the price of
    never sending it twice; here every restart is another chance, and swallowing
    the claim on a transient SNS error would silence the alert about an outage
    that is still happening. It cannot change the refusal either way: the
    deployment is already blocked and stays blocked.
    """
    missing = _missing_required_keys(checks)
    fingerprint = _refusal_fingerprint(config.bucket, reason, missing, error)
    log.error(
        "Startup refused (%s) for %s: %s",
        reason,
        config.bucket,
        error if error is not None else f"missing {', '.join(missing)}",
    )
    try:
        with engine.begin() as connection:
            claimed = connection.execute(
                text(
                    f"INSERT INTO {SERVICE_SCHEMA}.bootstrap_alerts "
                    "(fingerprint, bucket, reason, missing_keys, error) "
                    "VALUES (:fingerprint, :bucket, :reason, :missing_keys, :error) "
                    "ON CONFLICT (fingerprint) DO NOTHING RETURNING fingerprint"
                ),
                {
                    "fingerprint": fingerprint,
                    "bucket": config.bucket,
                    "reason": reason,
                    "missing_keys": list(missing),
                    "error": str(error) if error is not None else None,
                },
            ).first()
    except Exception:
        log.exception("Could not record the blocked startup")
        return False
    if not claimed:
        log.info(
            "Already alerted for this blocked startup (%s); not alerting again", reason
        )
        return False
    try:
        subject, message = _bootstrap_alert(config, reason, checks, missing, error)
        sns.publish(subject, message)
    except Exception:
        log.exception("Could not alert about the blocked startup")
        _release_bootstrap_claim(engine, fingerprint)
        return False
    return True


def _release_bootstrap_claim(engine: Engine, fingerprint: str) -> None:
    """Hand the claim back so a later restart can alert after all."""
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {SERVICE_SCHEMA}.bootstrap_alerts "
                    "WHERE fingerprint = :fingerprint"
                ),
                {"fingerprint": fingerprint},
            )
    except Exception:
        log.exception("Could not release the blocked-startup alert claim")


def _clear_bootstrap_claims(engine: Engine, bucket: str) -> None:
    """Forget every alert already sent for this bucket, now that it can start.

    A claim that is never forgotten silences a *recurrence* as well as a repeat.
    The missing release gets published, the deployment starts, months later the
    release is deleted by something careless — and the deployment crashes
    restarting in exactly the way it did before, to nobody, because the claim for
    that condition is still sitting in the table from the first incident. The
    dedup this table exists for is over a run of restarts, not over the lifetime
    of the bucket, and a successful startup is where the boundary between the two
    falls: the condition was fixed, so if it comes back it is a new incident and
    the operator has never been told about this one.

    Best effort, and deliberately not fatal: failing to tidy an alert record
    must not stop a deployment that has just successfully prepared itself. The
    cost of getting this wrong is one missed alert on a later recurrence; the
    cost of refusing to start would be the outage this whole path exists to
    report.
    """
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {SERVICE_SCHEMA}.bootstrap_alerts "
                    "WHERE bucket = :bucket"
                ),
                {"bucket": bucket},
            )
    except Exception:
        log.exception("Could not clear the sent blocked-startup alerts")


def _bootstrap_alert(
    config: BootstrapConfig,
    reason: str,
    checks: Sequence[BootstrapCheck],
    missing: tuple[str, ...],
    error: Exception | None,
) -> tuple[str, str]:
    """The subject and body of the blocked-startup alert.

    What an operator needs from this is the key to publish and the fact that
    nothing is running, so the key is named and the consequence is stated. The
    keys that *are* there are listed too: "a release is missing" is a much
    shorter search when the answer is which of four.

    The remedy is the part that has to be right. A body that tells an operator
    to re-upload a Boundary release when the queue was what failed sends them
    away from the actual fault, so each reason states its own next step and
    nothing claims a cause it cannot see.
    """
    available = ", ".join(
        check.key for check in checks if check.required and check.available
    )
    if error is not None:
        headline = f"Startup could not prepare {config.bucket}: {error}"
    else:
        headline = (
            f"Startup refused to start the ingestion worker: "
            f"{len(missing)} required Boundary release(s) are missing from "
            f"{config.bucket}."
        )
    lines = [headline, f"Bucket: {config.bucket}"]
    if missing:
        lines.append(f"Missing: {', '.join(missing)}")
    if available:
        lines.append(f"Published: {available}")
    lines.append(f"{_REFUSAL_REMEDIES[reason]}")
    lines.append(
        "The worker has not started, and the container exits so the restart "
        "policy retries the bootstrap; this alert is sent once per distinct "
        "condition, so a repeat of the same one is silent by design."
    )
    # A KeyError here would be an internal mistake — a refusal reason with no
    # subject — and it is raised rather than defaulted because a wrong subject
    # still reaches an operator and quietly mislabels the outage.
    return _REFUSAL_SUBJECTS[reason], "\n".join(lines)


def _settle_failure(
    engine: Engine,
    run_id: uuid.UUID,
    attempt: int,
    object_id: S3ObjectId,
    results: Sequence[StageResult],
    error: Exception,
    *,
    delivery_attempt: int,
    sns: SNSAdapter,
) -> RunState:
    """Settle one record that failed, and return the state the run is left in.

    A rejection of the object version itself is terminal: it is alerted once and
    never retried, because the version is immutable. Anything else is about the
    state around the file and stays retryable, and once the queue has spent its
    deliveries the run records that it was given up on.
    """
    if isinstance(error, RejectedObjectError):
        _record_stage_results(engine, run_id, attempt, results)
        _record_terminal(engine, run_id, error, _failing_stage_of(results))
        _alert_rejected(sns, object_id, run_id, error)
        return RunState.TERMINAL
    exhausted = delivery_attempt >= MAX_DELIVERY_ATTEMPTS
    if exhausted:
        # The run stays unsettled on purpose: an unsettled record is what keeps
        # the message from being acknowledged, and the redrive policy needs the
        # message to move it to the DLQ, where the DLQ alarm alerts.
        error = RuntimeError(
            f"{error}; delivery attempts exhausted after {delivery_attempt} "
            "deliveries, so the message goes to the DLQ"
        )
        log.error(
            "Giving up on %s after %s deliveries; the message goes to the DLQ",
            object_id.key,
            delivery_attempt,
        )
    if results:
        _record_stage_results(engine, run_id, attempt, results)
    else:
        # A failure before any stage ran still leaves a row of its own, so the
        # attempt history of a run is complete in stage_results and not only on
        # the run row.
        _record_stage_results(
            engine,
            run_id,
            attempt,
            [
                StageResult(
                    target=object_id.key,
                    stage="processing",
                    outcome="failed",
                    error=str(error),
                    details=(
                        {
                            "delivery_attempt": delivery_attempt,
                            "delivery_attempts": MAX_DELIVERY_ATTEMPTS,
                            "exhausted": True,
                        }
                        if exhausted
                        else {}
                    ),
                )
            ],
        )
    _record_retryable(engine, run_id, error, _failing_stage_of(results))
    return RunState.RETRYABLE


def _record_stale(
    engine: Engine,
    run_id: uuid.UUID,
    error: Exception,
) -> None:
    _record_failure(engine, run_id, RunState.STALE, error, "version-check")


def _record_failure(
    engine: Engine,
    run_id: uuid.UUID,
    state: RunState,
    error: Exception,
    stage: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {SERVICE_SCHEMA}.ingestion_runs "
                "SET state = :state, current_stage = :stage, "
                "terminal_error = :error, finished_at = CURRENT_TIMESTAMP "
                "WHERE run_id = :run_id"
            ),
            {
                "run_id": str(run_id),
                "state": state.value,
                "stage": stage,
                "error": str(error),
            },
        )


def _claim(
    engine: Engine, s3: S3Adapter, event: S3ObjectId, states: list[RunState]
) -> tuple[uuid.UUID, int, S3ObjectId] | None:
    """Start or resume the run for an event, or settle it without work.

    Returns None — having appended the settled state — for a version whose run
    is already settled or that S3 has superseded; otherwise the run and the
    version stamped with its served timestamp.
    """
    run_id, attempt, state = _start_run(engine, event)
    if state in {RunState.SUCCEEDED, RunState.TERMINAL, RunState.STALE}:
        states.append(state)
        return None
    try:
        object_id = _resolve_current(engine, s3, event)
    except StaleObjectError as error:
        _record_stale(engine, run_id, error)
        states.append(RunState.STALE)
        return None
    _stamp_last_modified(engine, run_id, object_id.last_modified)
    return run_id, attempt, object_id


def _results_for(
    object_id: S3ObjectId, results: Sequence[StageResult]
) -> list[StageResult]:
    """A Boundary run's share of batch results: its own extract, plus the rest."""
    return [
        result
        for result in results
        if result.stage != "extract" or result.target == object_id.key
    ]


def _failing_stage_of(results: Sequence[StageResult]) -> str:
    for result in reversed(results):
        if result.outcome == "failed":
            return result.stage
    return "processing"


class _PendingBoundary(NamedTuple):
    """A Boundary record claimed for the batch, with where its state goes."""

    run_id: uuid.UUID
    attempt: int
    object_id: S3ObjectId
    body: bytes
    index: int


class _SourceSucceeded(NamedTuple):
    """A Source record whose own stages passed, pending the shared marts."""

    run_id: uuid.UUID
    attempt: int
    object_id: S3ObjectId
    results: list[StageResult]
    index: int


def process_one_message(
    message: SqsMessage,
    *,
    engine: Engine,
    s3: S3Adapter,
    sqs: SQSAdapter,
    sns: SNSAdapter,
    processor: MessageProcessor,
) -> MessageResult:
    states: list[RunState] = []
    successes: list[_SourceSucceeded] = []

    def fail(
        run_id: uuid.UUID,
        attempt: int,
        object_id: S3ObjectId,
        results: Sequence[StageResult],
        error: Exception,
    ) -> RunState:
        return _settle_failure(
            engine,
            run_id,
            attempt,
            object_id,
            results,
            error,
            delivery_attempt=message.attempt,
            sns=sns,
        )

    parsed = _object_ids(message)
    boundary_events = [e for e in parsed.object_ids if _input_kind(e) == "boundary"]
    source_events = [e for e in parsed.object_ids if _input_kind(e) == "source"]

    # Boundary records first, as one release, so Source records in the same
    # message are enriched against the newest geography.
    boundary_batch: list[_PendingBoundary] = []
    for event in boundary_events:
        claimed = _claim(engine, s3, event, states)
        if claimed is None:
            continue
        run_id, attempt, object_id = claimed
        try:
            body = s3.read_version(object_id)
        except Exception as error:
            states.append(fail(run_id, attempt, object_id, (), error))
            continue
        boundary_batch.append(
            _PendingBoundary(run_id, attempt, object_id, body, len(states))
        )
        states.append(RunState.RUNNING)
    if boundary_batch:
        try:
            shared = list(
                processor.process_boundaries(
                    [(pending.object_id, pending.body) for pending in boundary_batch]
                )
            )
        except SourceSnapshotError as error:
            for pending in boundary_batch:
                states[pending.index] = fail(
                    pending.run_id,
                    pending.attempt,
                    pending.object_id,
                    _results_for(pending.object_id, error.results),
                    error,
                )
        except Exception as error:
            for pending in boundary_batch:
                states[pending.index] = fail(
                    pending.run_id, pending.attempt, pending.object_id, (), error
                )
        else:
            for pending in boundary_batch:
                successes.append(
                    _SourceSucceeded(
                        pending.run_id,
                        pending.attempt,
                        pending.object_id,
                        _results_for(pending.object_id, shared),
                        pending.index,
                    )
                )
                states[pending.index] = RunState.SUCCEEDED

    for event in source_events:
        claimed = _claim(engine, s3, event, states)
        if claimed is None:
            continue
        run_id, attempt, object_id = claimed
        try:
            body = s3.read_version(object_id)
            results = list(processor.process(object_id, body))
            successes.append(
                _SourceSucceeded(run_id, attempt, object_id, results, len(states))
            )
            states.append(RunState.SUCCEEDED)
        except SourceSnapshotError as error:
            states.append(
                fail(run_id, attempt, object_id, error.results, error)
            )
        except Exception as error:
            states.append(fail(run_id, attempt, object_id, (), error))

    if successes:
        try:
            marts_results = list(processor.finalize())
        except SourceSnapshotError as error:
            # Marts are shared by the whole message, so a refresh failure leaves
            # no record in it complete: every successful source run in the
            # message is downgraded to retryable and carries the marts failure,
            # rather than only the last one being marked incomplete.
            for success in successes:
                states[success.index] = fail(
                    success.run_id,
                    success.attempt,
                    success.object_id,
                    (*success.results, *error.results),
                    error,
                )
        else:
            for success in successes:
                _record_success(
                    engine,
                    success.run_id,
                    success.attempt,
                    (*success.results, *marts_results),
                )

    acknowledged = parsed.acknowledgable and all(
        state in {RunState.SUCCEEDED, RunState.TERMINAL, RunState.STALE}
        for state in states
    )
    if acknowledged:
        sqs.delete_message(message.receipt_handle)
    return MessageResult(acknowledged=acknowledged, states=tuple(states))


class _VisibilityHeartbeat:
    """Keep a received message invisible to other workers while it is worked on.

    The queue hands the message to one worker for the length of its visibility
    timeout. A message can take much longer than that — a Boundary release
    re-enriches the whole country — so the worker refreshes the timeout to the
    SQS maximum on a heartbeat for as long as it holds the message. A crashed
    worker stops heartbeating, and the message becomes visible again by itself.
    """

    def __init__(self, sqs: SQSAdapter, receipt_handle: str):
        self.sqs = sqs
        self.receipt_handle = receipt_handle
        self.interval = VISIBILITY_HEARTBEAT_SECONDS
        self._stop = threading.Event()

    def __enter__(self) -> _VisibilityHeartbeat:
        self._beat()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exception) -> None:
        self._stop.set()
        self._thread.join(timeout=self.interval)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self._beat()

    def _beat(self) -> None:
        try:
            self.sqs.change_message_visibility(
                self.receipt_handle, VISIBILITY_TIMEOUT_SECONDS
            )
        except Exception:
            # Losing one heartbeat is not fatal: the message is still invisible
            # for the timeout that was last set, and the work is not lost — a
            # redelivery resumes the runs that were left open.
            log.exception("Could not extend the visibility of the message")


def _release_visibility(sqs: SQSAdapter, receipt_handle: str) -> None:
    """Make the message visible again now, instead of after the granted timeout.

    The heartbeat hands the queue six hours, which is what protects work in
    progress. Once the work is over that has to be undone: SQS applies its
    redrive policy when a message becomes visible, so a message that is left
    unacknowledged has to be returned promptly for the retry — or the move to the
    DLQ — to happen at all.
    """
    try:
        sqs.change_message_visibility(receipt_handle, 0)
    except Exception:
        # The message is still safe: it becomes visible when the timeout the
        # heartbeat last set lapses, just later than it should.
        log.exception("Could not return the message to the queue for redelivery")


def run_worker(
    *,
    engine: Engine,
    s3: S3Adapter,
    sqs: SQSAdapter,
    sns: SNSAdapter,
    processor: MessageProcessor,
    max_messages: int | None = None,
    idle_sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Process queued messages one at a time until the worker is stopped.

    `max_messages` stops the loop after that many received messages (a one-shot
    drain); the production setting of None runs until the process is stopped.
    An empty receive only pauses the loop, because queued work resumes on its
    own after a restart and an empty queue is a pause, not an exit. Returns the
    number of messages processed.
    """
    processed = 0
    while max_messages is None or processed < max_messages:
        try:
            message = sqs.receive_message()
            if message is None:
                idle_sleep(IDLE_POLL_SECONDS)
                continue
            acknowledged = False
            with _VisibilityHeartbeat(sqs, message.receipt_handle):
                try:
                    result = process_one_message(
                        message,
                        engine=engine,
                        s3=s3,
                        sqs=sqs,
                        sns=sns,
                        processor=processor,
                    )
                    acknowledged = result.acknowledged
                finally:
                    if not acknowledged:
                        # A crash or a retryable failure must not park the
                        # message behind the six hours the heartbeat granted.
                        _release_visibility(sqs, message.receipt_handle)
        except Exception:
            # The message was not acknowledged, so SQS redelivers it. One
            # message that cannot be processed must not take the worker down:
            # the next message is a different piece of work.
            log.exception("Message processing failed, continuing with the queue")
            idle_sleep(IDLE_POLL_SECONDS)
            continue
        log.info(
            "Message %s: %s [%s]",
            message.receipt_handle,
            "acknowledged" if result.acknowledged else "kept for redelivery",
            ", ".join(state.value for state in result.states) or "no records",
        )
        processed += 1
    return processed
