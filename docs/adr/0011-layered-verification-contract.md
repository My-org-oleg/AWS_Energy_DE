# Verification is layered: cheap contracts in CI, one acceptance walkthrough against real PostGIS

What is verified, and by which seam, is decided per layer rather than per
feature. Three seams, each answering a different question: `tests/test_compose_config.py`
renders the Compose stacks and asserts the deployment contract, CI gates the
Terraform configuration, and `tests/test_acceptance_workflow.py` runs the whole
event-driven workflow once through its own entry points against a real PostGIS
database with fake AWS adapters.

## Context

The event-driven workflow (#36) is the part of this project where a mistake is
expensive: the worker runs unattended on an EC2 host, and the failures that hurt
are the ones nothing notices — a message that is never deleted, a snapshot that
overwrites a newer one, a rejected file that silently stops a whole queue.

Two things made the existing tests a poor answer to "does the deployment
work?":

- **The unit and integration tests drove internal helpers.** They called
  `ingestion.process_one_message` directly, with a queue that could not hold a
  message. That pins each behaviour exactly, and it cannot show the two halves
  of the real loop fitting together: nothing ever put a message on a queue that a
  worker loop then drained, so "startup enqueues, the worker ingests" was two
  separate claims about two separate fakes.
- **The container contract was only checked by a script that needs the private
  data set.** `scripts/smoke_etl_container.sh` builds images and seeds a volume
  with raw GPKG files, so a drift between a compose file and the CLI command it
  runs surfaced only on a machine that could afford that.

## Decision

- **The acceptance walkthrough drives the deployment's own entry points.**
  `tests/test_acceptance_workflow.py` runs `run_startup`, then `run_worker` for
  every message, then `redrive`, over a `QueuedSQS` that holds what startup
  sent and hands it back with SQS's own delivery counting, a `VersionedS3`, a
  `FakeSNS` and the real `PipelineProcessor` against a real PostGIS database in
  throwaway schemas. It asserts the state the service is left in — runs, the
  ledger, Core, the marts, and what `viz.data.fetch_units` returns — rather than
  the return values of internal helpers, because that is the question an
  operator asks after a run.
- **The flow runs once, in order, in a module fixture; the tests assert its
  phases.** The guarantees only hold in sequence (a later snapshot's history
  depends on the first one having loaded), so splitting the flow across
  independent tests would test a different system. Each phase is recorded as it
  happens — including the queue's own state, which is mutated later — and one
  test per guarantee reads what its phase recorded. Failures name the guarantee,
  not a line in a long test.
- **The fake AWS adapters are one module, not two copies.**
  `tests/ingestion_harness.py` holds the bucket, the queue, the notifier, the
  fixture GPKG bodies and the isolated-schema deployment, and both
  `test_ingestion.py` and the walkthrough import from it. A second copy of the
  fakes could drift from the first and make the walkthrough prove something the
  rest of the suite does not.
- **A drain that outlives its budget fails instead of hanging.** `run_worker`
  polls an empty queue by design, so a `max_messages` larger than the messages
  it will ever receive would hang the suite. The walkthrough's `idle_sleep`
  raises on the second empty poll, which also stops a message that keeps
  failing from looping forever.
- **The container contract is asserted where it is cheap.** The Compose
  contract tests render all three variants with `docker compose config`
  (interpolation only — no stack, no image, no database) and assert the command,
  the mounts, the published ports, the environment, and that every command is one
  the CLI really registers. They skip when `docker compose` is unavailable, and
  they run in CI as their own job, so a renamed command or a changed port fails a
  pull request instead of a deployment.
- **The heavy flow stays off CI.** The walkthrough needs a scratch PostGIS
  database, which CI has no service for, so it runs with the rest of the local
  suite. CI keeps the gates that need nothing: byte-compile, image builds,
  Terraform `fmt`/`validate` plus the config test that pins the delivery
  contract against `etl/ingestion.py`, and the Compose contract tests.

## Consequences

- "Does the whole workflow work?" is now one command with a named failure per
  guarantee: Source ingestion, Boundary replacement, duplicate and out-of-order
  delivery, terminal rejection and recovery on a new version, exhaustion into the
  DLQ and redrive recovery, and the historical visualization reading back the
  Core that flow produced.
- The container contract is a pull-request gate instead of a manual script run,
  and the smoke script still exercises the real images and a real database.
- The suite is bigger and the harness is a module both suites depend on, so a
  change to a fake's behaviour is felt by every ingestion test — which is the
  point: the fakes model S3 and SQS, and a wrong model would have been shared
  silently.
- The walkthrough deliberately duplicates a little of what `test_ingestion.py`
  asserts. That is the cost of asserting a behaviour end to end: the unit test
  pins the rule, the walkthrough shows the rule surviving the whole loop.
