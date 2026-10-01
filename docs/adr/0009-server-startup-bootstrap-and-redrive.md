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
- **A refusal is published, once per distinct condition.** The crash loop is
  the deployment saying "still broken" to itself, and it says it to nobody:
  the container's console does not survive the restart. So each refusal
  publishes on the one alert topic, naming the bucket and either the missing
  fixed keys or the error, and the worker is refused either way — the alert is
  a report, never a decision. The dedup key is a fingerprint of the refusal
  (bucket, reason, missing required keys, and the error's *type*), recorded in
  `service.bootstrap_alerts`, because the restart policy re-runs the identical
  refusal and a message per iteration is how a topic teaches its subscribers to
  ignore it. The error contributes its type rather than its message for the
  same reason: a message carrying a row number or a timestamp would mint a new
  fingerprint on every restart. A refusal that *changes* is a new fact and does
  alert. A publish that fails hands the claim back rather than swallowing it,
  because every restart is another chance and a transient SNS error must not
  silence the alert about an outage that is still happening.
- **A claim is forgotten once the bucket starts.** The dedup is over a run of
  restarts, not over the lifetime of the bucket. A claim kept forever would
  silence a *recurrence* as well as a repeat: the release is published, the
  deployment starts, the release is later deleted by something careless, and the
  deployment crash-loops in exactly the way it did before — to nobody, because
  that condition's claim is still in the table from the first incident. So a
  successful startup clears the bucket's claims, and the boundary between one
  incident and the next is the fix. Clearing is best effort and never fatal: it
  must not stop a deployment that has just prepared itself, and the cost of
  getting it wrong is one missed alert on a later recurrence.
- **Each refusal names its own remedy.** The reason is a closed set
  (`precondition`, `missing-boundary`, `boundary-release`, `enqueue`) and the
  alert body carries the fix for that one. Applying the Boundary releases and
  enqueueing the Source versions are two steps, refused separately: an alert
  titled "Boundary release rejected" that was really a queue that could not be
  reached sends the operator to re-upload a release that is published and fine.
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
  one explicit recovery path (redrive) — each observable in the container logs,
  on the alert topic, and in the `service` schema.
- The startup order is verified by `tests/test_ingestion.py` against real
  PostGIS with fake AWS adapters; the deployment contract (no seed volume, no
  published db port, startup command, environment settings) is asserted by
  `scripts/smoke_etl_container.sh` via `docker compose -f compose.yaml config`.
