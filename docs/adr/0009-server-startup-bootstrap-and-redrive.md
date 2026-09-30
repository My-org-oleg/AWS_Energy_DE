# The server deployment bootstraps explicitly before the worker starts

The containerized server stack (`compose.yaml`, `build_compose.yaml`) runs the
pipeline container as `python -m etl startup`: an explicit bootstrap — database
preconditions, fixed S3 key checks, current Boundary releases, current Source
object versions enqueued — and only then the ingestion worker. The bootstrap
creates no Ingestion run, and the server stack mounts no private source-data
seed volume.

## Context

The local workflow (`local_compose.yaml`) is a CLI pass over a mounted data
volume: `run-all` reads `data/sources` and `data/boundaries` from disk. The
server deployment is event-driven: the worker reads S3 object versions through
the SQS queue. Giving the server stack the local volume would put private raw
data on a container that has no use for it, and starting the worker without
preparing the deployment would let it process Source events against missing or
stale Boundary levels. Issue #8 requires a safe, repeatable operator startup.

## Decision

- **Startup order is explicit and refusing.** `run_startup` runs the bootstrap
  (service metadata + fixed S3 key checks), applies the current Boundary
  releases as one batch with the downstream geography rebuilt and the marts
  refreshed once, enqueues the current Source object versions, and only then
  does the CLI start the worker. A missing or invalid Boundary condition is
  fatal: the command exits non-zero before the worker starts, so the
  container's restart policy crash-loops until the operator fixes it. A
  missing Source object is non-fatal.
- **The bootstrap creates no Ingestion run.** A run belongs to the worker's
  processing of a message, not to the enqueue. Boundary releases applied by
  the bootstrap are recorded in the `loaded_files` ledger with a synthetic run
  id (the column is a plain UUID, no FK to `ingestion_runs`); Source versions
  are enqueued only when no run exists for their exact `(bucket, key,
  version)` — succeeded, stale, terminal, DLQ and in-flight versions are never
  re-enqueued, so repeated Compose starts are idempotent.
- **The server stack mounts no data volume and publishes no database port.**
  Settings (`DATABASE_URL`, `S3_BUCKET`, `SQS_QUEUE_URL`, `SNS_TOPIC_ARN`,
  `VIZ_DATABASE_URL`) come from the environment; the compose files declare
  them required (`${VAR:?}`) so `up` fails fast. `local_compose.yaml` keeps
  the `etl_data` seed volume, the published db port, and the `run-all` CLI
  workflow.
- **Recovery is an explicit redrive operation.** `python -m etl redrive`
  re-enqueues every object version the worker left unsettled (retryable —
  failed, or exhausted into the DLQ), whose redelivery resumes the version's
  existing run. Settled versions are left alone; `--key` narrows to one key.

## Consequences

- The worker is never started into an unprepared deployment: Boundary levels
  are complete and current before the first event is processed.
- The bootstrap is idempotent across restarts: ledgered releases are skipped,
  known Source versions are not re-enqueued, and the marts refresh is
  repeatable.
- Operators get one fatal path (Boundary), one non-fatal path (Source), and
  one explicit recovery path (redrive) — each observable in the container logs
  and the `service` schema.
- The startup order is verified by `tests/test_ingestion.py` against real
  PostGIS with fake AWS adapters; the deployment contract (no seed volume, no
  published db port, startup command, environment settings) is asserted by
  `scripts/smoke_etl_container.sh` via `docker compose -f compose.yaml config`.
