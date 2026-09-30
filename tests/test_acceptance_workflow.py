"""The event-driven workflow, end to end (issue #36).

One deployment, driven through its own entry points — `run_startup`, then
`run_worker` for every message, then `redrive` — over the fake AWS adapters and
the real PostGIS pipeline, and finished by reading the result back the way the
viz app does. The other ingestion tests each pin one behaviour in isolation;
this one shows that the behaviours hold *in sequence*, which is the only way to
know the deployment works: what startup enqueues, what the worker does with
those messages, how a later snapshot and a later boundary release replace
Core without losing history, what a superseded or rejected file does, how a
transient failure is recovered, and what a past date still shows of all that.

The flow therefore runs once, in order, in a module fixture, and each test below
asserts one of its guarantees against what the flow left behind. The assertions
are about the resulting service state — the runs, the ledger, Core, the marts,
and what the read path returns — not about the return values of the internal
helpers, so they are the questions an operator actually asks after a run.
"""

import itertools
import json
from datetime import date, timedelta
from typing import NamedTuple

import pandas
import pytest

from etl import ingestion, marts, verify
from etl.config import SOURCE_NAMES
import viz.data
from ingestion_harness import (
    ENGINE,
    CountingProcessor,
    FakeSNS,
    QueuedSQS,
    VersionedS3,
    _boundary_snapshot,
    _core_kind,
    _core_units,
    _enqueue,
    _fixture_frame,
    _frame_keys,
    _geography,
    _good_members,
    _load_boundary_fixtures,
    _matviews,
    _publish_boundary_levels,
    _publish_boundary_releases,
    _publish_source_snapshots,
    _publish_source_version,
    _query,
    _raw_versions,
    _renamed_states,
    _run_ids,
    _unit_key,
    _write_mixed_source_snapshot,
    isolated_pipeline,
)

BUCKET = "energy-data"

# The three units the later Solar snapshots act on: one the newest snapshot
# leaves out, one it restates, and one it decommissions.
DROPPED = "solar-3"
CHANGED = "solar-1"
RETIRED = "solar-4"
RETIRED_ON = date(2020, 1, 1)

# Every fixture unit was commissioned on this date, so a window that starts
# before it and ends on RETIRED_ON shows what was running on the day the unit
# was decommissioned, and a day later shows what is running now.
FIRST_DAY = date(2015, 1, 1)


def _stop_when_empty():
    """An idle poll that fails the drain instead of spinning on an empty queue.

    `run_worker` keeps polling an empty queue by design, so a budget larger than
    the messages the worker will ever receive would hang the suite rather than
    fail it. The first empty poll is a pause; a second one means the drain
    outlived its budget, or a message keeps failing, and both are bugs worth
    stopping on.
    """
    polls = itertools.count()

    def idle_sleep(seconds):
        if next(polls):
            raise AssertionError("the queue went empty before the worker was done")

    return idle_sleep


def _runs(schemas):
    """Every Ingestion run as (object key, version) -> state."""
    return {
        (row["object_key"], row["object_version_id"]): row["state"]
        for row in _query(
            f"SELECT object_key, object_version_id, state "
            f"FROM {schemas['service']}.ingestion_runs"
        )
    }


def _ledger(schemas):
    """The load ledger's S3-identified rows: (object key, version) applied.

    The rows `extract_boundaries` writes when it loads boundary files straight
    from a manifest name no bucket and no version, so they carry no object
    identity and say nothing about which object versions were applied.
    """
    return {
        (row["object_key"], row["object_version_id"])
        for row in _query(
            f"SELECT object_key, object_version_id "
            f"FROM {schemas['service']}.loaded_files WHERE object_key IS NOT NULL"
        )
    }


def _run(schemas, key, version_id):
    """The Ingestion run of one object version, with its delivery count."""
    return dict(
        _query(
            f"SELECT state, attempt_count, terminal_error "
            f"FROM {schemas['service']}.ingestion_runs "
            f"WHERE object_key = :key AND object_version_id = :version",
            {"key": key, "version": version_id},
        )[0]
    )


def _core_rows(schemas, source):
    """Every Core row of one source, as unit key -> the row itself."""
    return {
        _unit_key(row["reference_id"], row["longitude"], row["latitude"]): dict(row)
        for row in _query(
            f"SELECT unit_id, reference_id, longitude, latitude, installed_capacity, "
            f"commissioning_date, decommissioning_date, state "
            f"FROM {schemas['core']}.{_core_kind(source)} "
            f"WHERE energy_source = :source",
            {"source": source},
        )
    }


def _solar_names(frame):
    """The unit keys of a Solar snapshot, aligned with its rows."""
    return pandas.Series(_frame_keys(frame), index=frame.index)


def _newer_snapshot(frame, keys, *, dropped=None, edits=()):
    """The snapshot with one unit left out and the given edits applied.

    Every later Solar snapshot in the walkthrough is derived from the one before
    it, the way a real republication is: the newest file is the whole picture,
    not a diff, so what a snapshot no longer carries is stated by its absence.
    """
    newer = frame[keys != dropped].copy() if dropped else frame.copy()
    for reference_id, column, value in edits:
        newer.loc[keys[newer.index] == reference_id, column] = value
    return newer


def _viewed(frame, coordinates):
    """The unit keys the read path returned.

    The read path projects no Reference ID (`viz.data.UNIT_COLUMNS`), so a unit
    is identified in its frame by the coordinates it was loaded at, which is
    what `_unit_key` falls back to.
    """
    by_position = {
        (round(x, 6), round(y, 6)): key for key, (x, y) in coordinates.items()
    }
    return {
        by_position[(round(row.longitude, 6), round(row.latitude, 6))]
        for row in frame.itertuples()
    }



class _Deployment(NamedTuple):
    """The collaborators every step of the walkthrough shares."""

    s3: VersionedS3
    queue: QueuedSQS
    sns: FakeSNS
    processor: CountingProcessor
    schemas: dict

    def drain(self, *, receives: int) -> dict:
        """Run the worker's own loop for exactly `receives` receives."""
        settled = self.queue.settled
        processed = ingestion.run_worker(
            engine=ENGINE,
            s3=self.s3,
            sqs=self.queue,
            sns=self.sns,
            processor=self.processor,
            max_messages=receives,
            idle_sleep=_stop_when_empty(),
        )
        return {
            "processed": processed,
            "acknowledged": self.queue.settled - settled,
            "queued": self.queue.queued,
        }

    def upload(self, tmp_path, source, version_id, frame):
        """Put a Source snapshot in the bucket, without notifying the queue."""
        return _publish_source_version(self.s3, tmp_path, source, version_id, frame)

    def notify(self, handle, records, *, receives: int = 1) -> dict:
        """Announce the object versions the bucket already holds, and drain."""
        _enqueue(self.queue, handle, records)
        return self.drain(receives=receives)


def _walkthrough(pipeline, tmp_path, monkeypatch):
    """The deployment's whole life, in the order it happens, recorded.

    Each phase leaves the service in the state the next one starts from, so the
    flow is what makes the guarantees below provable together rather than one at
    a time.
    """
    schemas = pipeline["schemas"]
    deployment = _Deployment(
        s3=pipeline["s3"],
        queue=QueuedSQS(),
        sns=FakeSNS(),
        processor=CountingProcessor(ENGINE, s3=pipeline["s3"]),
        schemas=schemas,
    )
    s3 = deployment.s3
    seen = {"schemas": schemas}

    # 1. The bucket holds a Boundary release and a Source snapshot per fixed
    #    key, and the server starts: bootstrap, apply the release, enqueue the
    #    Source versions. No run belongs to the bootstrap.
    _publish_boundary_levels(s3, tmp_path, handle="release")
    _publish_source_snapshots(s3, tmp_path, handle="snapshot")
    seen["startup"] = startup = ingestion.run_startup(
        ingestion.BootstrapConfig(bucket=BUCKET),
        engine=ENGINE,
        s3=s3,
        sqs=deployment.queue,
        processor=deployment.processor,
    )
    seen["boundaries_at_startup"] = _boundary_snapshot(schemas)
    seen["runs_at_startup"] = _runs(schemas)
    seen["matviews_at_startup"] = _matviews(schemas)
    seen["queued_at_startup"] = deployment.queue.queued

    # 2. The worker takes the six messages startup left on the queue.
    seen["first_drain"] = deployment.drain(receives=len(startup.enqueued))
    seen["core_after_first_drain"] = {
        source: _core_units(schemas, source) for source in SOURCE_NAMES
    }
    seen["runs_after_first_drain"] = _runs(schemas)
    seen["ledger_after_first_drain"] = _ledger(schemas)
    seen["marts_after_first_drain"] = marts.verify_marts(ENGINE)
    seen["mart_refreshes"] = deployment.processor.finalized

    # 3. A new Boundary release arrives on the queue: level 1 is replaced, every
    #    unit's geography is rebuilt against it, the marts refresh once.
    records = _publish_boundary_releases(s3, tmp_path, {1: _renamed_states()}, "states-v2")
    _enqueue(deployment.queue, "states-v2", records)
    seen["boundary_release"] = deployment.drain(receives=1)
    seen["boundaries_after_release"] = _boundary_snapshot(schemas)
    seen["geography_after_release"] = {
        **_geography(schemas, "core", "generators"),
        **_geography(schemas, "core", "storages"),
    }
    seen["staged_states"] = {
        source: {geo[0] for geo in _geography(schemas, "stage", source).values()}
        for source in SOURCE_NAMES
    }
    seen["mart_states"] = {
        row["state"]
        for row in _query(
            f"SELECT state FROM {schemas['marts']}.installation_counts"
        )
    }
    seen["marts_after_release"] = marts.verify_marts(ENGINE)

    # 4. A later Solar snapshot: one unit restated, one left out, one
    #    decommissioned.
    solar = _fixture_frame("solar")
    keys = _solar_names(solar)
    seen["solar_positions"] = dict(
        zip(_frame_keys(solar), zip(solar.x_coordinates, solar.y_coordinates))
    )
    newer = _newer_snapshot(
        solar,
        keys,
        dropped=DROPPED,
        edits=[
            (CHANGED, "installed_capacity", 5000.0),
            (RETIRED, "decommissioning_date", RETIRED_ON),
        ],
    )
    seen["solar_v2"] = deployment.notify(
        "solar-v2", [deployment.upload(tmp_path, "solar", "solar-v2", newer)]
    )
    seen["core_after_v2"] = _core_rows(schemas, "solar")
    seen["members_of_v2"] = _good_members(
        schemas, _run_ids(schemas, "solar-v2")["sources/solar.gpkg"]
    )
    seen["staged_after_v2"] = set(_geography(schemas, "stage", "solar"))
    seen["marts_after_v2"] = marts.verify_marts(ENGINE)

    # 5. S3 notifies the same object version twice: the settled run is returned
    #    without work, and nothing moves.
    seen["core_before_duplicate"] = _core_rows(schemas, "solar")
    seen["raw_versions_before_duplicate"] = _raw_versions(schemas, "solar")
    seen["duplicate"] = deployment.notify(
        "solar-v2-again", [("sources/solar.gpkg", "solar-v2")]
    )
    seen["core_after_duplicate"] = _core_rows(schemas, "solar")
    seen["raw_versions_after_duplicate"] = _raw_versions(schemas, "solar")

    # 6. Two snapshots are uploaded back to back, so the notification for the
    #    older one arrives after the newer one is already the bucket's current
    #    version: the delayed version is settled as stale and the newer one
    #    stands.
    latest = _newer_snapshot(
        newer, _solar_names(newer), edits=[(CHANGED, "installed_capacity", 6000.0)]
    )
    superseded = deployment.upload(tmp_path, "solar", "solar-v3", newer)
    current = deployment.upload(tmp_path, "solar", "solar-v4", latest)
    seen["out_of_order_v3"] = deployment.notify("solar-v3-late", [superseded])
    seen["current_v4"] = deployment.notify("solar-v4", [current])
    seen["core_after_v4"] = _core_rows(schemas, "solar")
    seen["marts_after_v4"] = marts.verify_marts(ENGINE)

    # 7. A file the pipeline rejects is settled, alerted once and left alone;
    #    the corrected file the operator uploads for the same key recovers.
    s3.put(
        "sources/solar.gpkg",
        "solar-bad",
        _write_mixed_source_snapshot(tmp_path, "solar-bad.gpkg"),
    )
    seen["rejected"] = deployment.notify(
        "solar-bad", [("sources/solar.gpkg", "solar-bad")]
    )
    seen["alerts"] = list(deployment.sns.messages)
    seen["core_after_rejected"] = _core_rows(schemas, "solar")
    recovered = _newer_snapshot(
        solar,
        keys,
        edits=[
            (CHANGED, "installed_capacity", 5500.0),
            (RETIRED, "decommissioning_date", RETIRED_ON),
        ],
    )
    seen["recovered"] = deployment.notify(
        "solar-v5", [deployment.upload(tmp_path, "solar", "solar-v5", recovered)]
    )
    seen["core_after_recovery"] = _core_rows(schemas, "solar")
    seen["members_of_v5"] = _good_members(
        schemas, _run_ids(schemas, "solar-v5")["sources/solar.gpkg"]
    )
    seen["marts_after_recovery"] = marts.verify_marts(ENGINE)

    # 8. A Source object the bucket cannot be read from: the worker keeps the
    #    message, the queue spends its deliveries and moves it to the DLQ, and
    #    the operator's redrive puts it back once the bucket answers.
    s3.read_failures["sources/hydro.gpkg"] = ingestion.MAX_DELIVERY_ATTEMPTS
    hydro = deployment.upload(tmp_path, "hydro", "hydro-v2", _fixture_frame("hydro"))
    seen["exhausted"] = deployment.notify(
        "hydro-v2", [hydro], receives=ingestion.MAX_DELIVERY_ATTEMPTS
    )
    seen["hydro_run_exhausted"] = _run(schemas, "sources/hydro.gpkg", "hydro-v2")
    seen["dlq"] = [json.loads(message.body) for message in deployment.queue.dlq]
    seen["alerts_after_exhaustion"] = list(deployment.sns.messages)
    del s3.read_failures["sources/hydro.gpkg"]
    seen["redriven"] = ingestion.redrive(ENGINE, sqs=deployment.queue)
    seen["after_redrive"] = deployment.drain(receives=1)
    seen["hydro_run_recovered"] = _run(schemas, "sources/hydro.gpkg", "hydro-v2")
    seen["hydro_runs"] = {
        version: _run(schemas, "sources/hydro.gpkg", version)["state"]
        for version in ("snapshot-hydro", "hydro-v2")
    }

    # 9. The read path the viz app uses, against the Core this flow produced.
    monkeypatch.setattr(viz.data, "CORE_SCHEMA", schemas["core"])
    monkeypatch.setattr(viz.data, "SERVICE_SCHEMA", schemas["service"])
    seen["units_on_the_day"] = viz.data.fetch_units(
        ENGINE,
        active_from=FIRST_DAY,
        active_to=RETIRED_ON,
        sources=("solar",),
        area_column="state",
    )
    seen["units_today"] = viz.data.fetch_units(
        ENGINE,
        active_from=FIRST_DAY,
        active_to=RETIRED_ON + timedelta(days=1),
        sources=("solar",),
        area_column="state",
    )
    seen["level_one"] = viz.data.fetch_boundaries(ENGINE, level=1)
    seen["runs"] = _runs(schemas)
    seen["ledger"] = _ledger(schemas)
    return seen


@pytest.fixture(scope="module")
def walkthrough(tmp_path_factory):
    """The flow above, run once against real PostGIS in throwaway schemas."""
    monkeypatch = pytest.MonkeyPatch()
    try:
        seeds = tmp_path_factory.mktemp("snapshots")
        with isolated_pipeline(seeds, monkeypatch) as pipeline:
            _load_boundary_fixtures()
            yield _walkthrough(
                pipeline,
                tmp_path_factory.mktemp("published"),
                monkeypatch,
            )
    finally:
        monkeypatch.undo()


def test_startup_applies_the_boundary_release_and_enqueues_every_source_version(
    walkthrough,
):
    """The deployment is ready to serve, with its work already on the queue.

    The four fixed Boundary keys are one applied release and the six fixed
    Source keys are six messages, and no Ingestion run exists yet: a run belongs
    to the worker processing a message, not to the bootstrap that queues it.
    """
    startup = walkthrough["startup"]
    assert startup.metadata_ready and startup.worker_start_allowed
    assert [object_id.key for object_id in startup.enqueued] == [
        f"sources/{source}.gpkg" for source in SOURCE_NAMES
    ]
    assert startup.known == ()
    assert walkthrough["runs_at_startup"] == {}
    assert walkthrough["queued_at_startup"] == len(SOURCE_NAMES)

    boundaries = walkthrough["boundaries_at_startup"]
    assert {level for level, _ in boundaries} == {0, 1, 2, 3}
    assert all(area > 0 and has_geojson for _, area, has_geojson in boundaries.values())
    # The boundary-only bootstrap left the marts in place with no Source loaded.
    assert walkthrough["matviews_at_startup"] == 3


def test_the_worker_ingests_exactly_the_queue_startup_left_it(walkthrough):
    """Six messages, six object versions, both Core kinds and reconciling marts.

    Only the worker's messages are runs: the release startup applied is in the
    ledger without one, which is the difference between the bootstrap that
    queues work and the worker that records it.
    """
    drain = walkthrough["first_drain"]
    assert (drain["processed"], drain["acknowledged"], drain["queued"]) == (6, 6, 0)

    core = walkthrough["core_after_first_drain"]
    assert set(core) == set(SOURCE_NAMES)
    assert all(units for units in core.values())
    assert walkthrough["marts_after_first_drain"] == []
    assert walkthrough["mart_refreshes"] == 7  # startup's release, then one per message

    source_runs = {
        (f"sources/{source}.gpkg", f"snapshot-{source}"): "succeeded"
        for source in SOURCE_NAMES
    }
    assert walkthrough["runs_after_first_drain"] == source_runs
    assert walkthrough["ledger_after_first_drain"] == set(source_runs) | {
        (f"boundaries/level-{level}.gpkg", f"release-level-{level}")
        for level in range(4)
    }


def test_a_boundary_release_replaces_one_level_and_rebuilds_geography(walkthrough):
    """The new states are the only thing that changed, everywhere they are used.

    Levels 0, 2 and 3 are byte-for-byte the same, the renamed and grown level 1
    is in force for staging and Core alike — historical rows included — and the
    marts pivot on the new names without a Source snapshot to carry them.
    """
    before, after = (
        walkthrough["boundaries_at_startup"],
        walkthrough["boundaries_after_release"],
    )
    assert walkthrough["boundary_release"]["processed"] == 1
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

    assert all(
        "Bayern" not in states for states in walkthrough["staged_states"].values()
    )
    geography = walkthrough["geography_after_release"]
    assert {geo[0] for geo in geography.values()} <= {
        "Baden-Württemberg",
        "Freistaat Bayern",
        "North Sea",
        None,
    }
    schemas = walkthrough["schemas"]
    for source in SOURCE_NAMES:
        staged = _geography(schemas, "stage", source)
        for key, geo in staged.items():
            if key in geography:
                assert geography[key] == geo, key
    assert "Freistaat Bayern" in walkthrough["mart_states"]
    assert "Bayern" not in walkthrough["mart_states"]
    assert walkthrough["marts_after_release"] == []


def test_a_later_snapshot_replaces_core_without_losing_history(walkthrough):
    """A complete snapshot is authoritative for what it carries and keeps the rest.

    The restated unit is updated in place, the unit the snapshot leaves out keeps
    its Core identity but is not a member of the new run, and the unit the
    snapshot decommissions carries the date to the view.
    """
    core = walkthrough["core_after_v2"]
    assert core[CHANGED]["installed_capacity"] == pytest.approx(5000.0)
    assert core[CHANGED]["unit_id"] == walkthrough["core_after_first_drain"]["solar"][
        CHANGED
    ][0]
    assert core[RETIRED]["decommissioning_date"] == RETIRED_ON
    assert DROPPED in core
    assert core[DROPPED]["unit_id"] == walkthrough["core_after_first_drain"]["solar"][
        DROPPED
    ][0]
    assert DROPPED not in walkthrough["members_of_v2"]
    assert ("solar", DROPPED) not in walkthrough["staged_after_v2"]
    assert walkthrough["marts_after_v2"] == []


def test_a_duplicate_notification_changes_nothing(walkthrough):
    """S3 re-announcing a loaded object version is settled without work."""
    duplicate = walkthrough["duplicate"]
    assert (duplicate["processed"], duplicate["acknowledged"]) == (1, 1)
    assert walkthrough["core_after_duplicate"] == walkthrough["core_before_duplicate"]
    assert (
        walkthrough["raw_versions_after_duplicate"]
        == walkthrough["raw_versions_before_duplicate"]
    )
    assert walkthrough["runs"][("sources/solar.gpkg", "solar-v2")] == "succeeded"


def test_a_delayed_old_version_is_settled_as_stale(walkthrough):
    """The notification that arrives after the newer upload carries no work.

    The version S3 has already superseded is settled stale and acknowledged, so
    the file it announced is not left holding the queue up, and the Core data is
    the newer snapshot's.
    """
    assert walkthrough["out_of_order_v3"]["acknowledged"] == 1
    assert walkthrough["runs"][("sources/solar.gpkg", "solar-v3")] == "stale"
    assert walkthrough["current_v4"]["acknowledged"] == 1
    assert walkthrough["runs"][("sources/solar.gpkg", "solar-v4")] == "succeeded"
    assert walkthrough["core_after_v4"][CHANGED]["installed_capacity"] == pytest.approx(
        6000.0
    )
    # A version S3 superseded was never loaded, so it is not in the ledger.
    assert ("sources/solar.gpkg", "solar-v3") not in walkthrough["ledger"]
    assert walkthrough["marts_after_v4"] == []


def test_a_rejected_file_is_terminal_alerts_once_and_recovers_on_a_new_version(
    walkthrough,
):
    """A file that fails its own validation is settled and never retried.

    The object version is immutable, so validating it again cannot give another
    answer: the run is terminal, the operator is told once, the database is left
    as it was, and the corrected file uploaded for the same key ingests — with
    the dropped unit back under the identity it always had.
    """
    rejected = walkthrough["rejected"]
    assert (rejected["processed"], rejected["acknowledged"]) == (1, 1)
    assert walkthrough["runs"][("sources/solar.gpkg", "solar-bad")] == "terminal"
    assert walkthrough["core_after_rejected"] == walkthrough["core_after_v4"]
    assert [subject for subject, _ in walkthrough["alerts"]] == [
        "Rejected sources/solar.gpkg"
    ]

    assert walkthrough["recovered"]["acknowledged"] == 1
    assert walkthrough["runs"][("sources/solar.gpkg", "solar-v5")] == "succeeded"
    assert ("sources/solar.gpkg", "solar-bad") not in walkthrough["ledger"]
    assert ("sources/solar.gpkg", "solar-v5") in walkthrough["ledger"]
    assert DROPPED in walkthrough["members_of_v5"]
    core = walkthrough["core_after_recovery"]
    assert core[DROPPED]["unit_id"] == walkthrough["core_after_v4"][DROPPED]["unit_id"]
    assert core[CHANGED]["installed_capacity"] == pytest.approx(5500.0)
    assert core[RETIRED]["decommissioning_date"] == RETIRED_ON
    assert walkthrough["marts_after_recovery"] == []


def test_a_transient_failure_reaches_the_dlq_and_redrive_recovers_it(walkthrough):
    """An unreadable object is the operator's problem, not a silent loss.

    The worker keeps the message instead of deleting it, the run stays
    retryable and says the deliveries ran out, the queue moves the message to
    the DLQ, and nothing is alerted — the DLQ alarm is the one report. Once the
    bucket answers again, `redrive` puts the version back and the same run
    finishes.
    """
    exhausted = walkthrough["exhausted"]
    assert (exhausted["processed"], exhausted["acknowledged"]) == (5, 0)
    assert exhausted["queued"] == 0
    assert walkthrough["alerts_after_exhaustion"] == walkthrough["alerts"]
    assert [
        record["s3"]["object"]
        for message in walkthrough["dlq"]
        for record in message["Records"]
    ] == [{"key": "sources/hydro.gpkg", "versionId": "hydro-v2"}]
    run = walkthrough["hydro_run_exhausted"]
    assert (run["state"], run["attempt_count"]) == (
        "retryable",
        ingestion.MAX_DELIVERY_ATTEMPTS,
    )
    assert "DLQ" in run["terminal_error"]

    assert [object_id.key for object_id in walkthrough["redriven"]] == [
        "sources/hydro.gpkg"
    ]
    assert walkthrough["after_redrive"]["acknowledged"] == 1
    assert walkthrough["hydro_runs"] == {
        "snapshot-hydro": "succeeded",
        "hydro-v2": "succeeded",
    }
    assert walkthrough["hydro_run_recovered"]["state"] == "succeeded"


def test_the_view_of_a_past_date_still_shows_what_today_decommissioned(walkthrough):
    """The read path answers a past date from the Core this flow produced.

    The Core the pipeline left is the app's only input, so the timescope is
    asked the operator's question directly: on the day the unit was
    decommissioned it is still on the map, the next day it is not, and both
    answers carry the state names the boundary release put in force.
    """
    on_the_day = _viewed(walkthrough["units_on_the_day"], walkthrough["solar_positions"])
    today = _viewed(walkthrough["units_today"], walkthrough["solar_positions"])
    assert on_the_day - today == {RETIRED}
    assert CHANGED in today and DROPPED in today
    assert (
        walkthrough["units_on_the_day"]["installed_capacity"].sum()
        > walkthrough["units_today"]["installed_capacity"].sum()
    )
    assert set(walkthrough["units_today"]["name"].dropna()) <= {
        "Baden-Württemberg",
        "Freistaat Bayern",
        "North Sea",
    }
    # The unit the shrunk state fell out of carries no area, so the choropleth
    # leaves it unfilled rather than joining it to a neighbouring state.
    assert walkthrough["units_today"]["name"].isna().any()
    assert [row["name"] for row in walkthrough["level_one"]] == [
        "Baden-Württemberg",
        "Freistaat Bayern",
        "North Sea",
    ]
