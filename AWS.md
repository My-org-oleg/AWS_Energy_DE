# AWS. Event driven ETL

## 1. Data source
- Project deployed on AWS ec2 instance under Ubuntu 26  
- All .gpkg files landed in AWS s3 bucket
- Boundaries files stored in their own folder from the very beginning
- Link to boundaries folder leaves in .env
- Boundaries extraction strats with *docker compose up* 
- Source files are additionally loaded

## 2. Event driven workflow
- When source file appears in source/ folder, S3 Event Notification triggers ETL
- ETL-worker runs constantly
- Event s3:ObjectCreated goes to SQS + DLQ
- The worker reads the object *version* S3 named in the event, writes its bytes to a
  temporary `.gpkg` only long enough for GeoPandas to open the layer, and unlinks that file
  before the stages run. Nothing is left on the host to clean up afterwards
- Energy source resolves through energy_source column in dataframe (not through filename) 
- Transform stage receives table name to transform. 
- All files are treated consequently via SQS 
- All extracted files are logged in loaded_files table and not extracted twice.

### The accepted keys and the snapshot contract (issue #36)
- The bucket has **ten accepted keys and no others**: `sources/<energy source>.gpkg` for the
  six Source datasets and `boundaries/level-<0-3>.gpkg` for the four Boundary levels
  (`etl.ingestion.ACCEPTED_KEYS`, mirrored in `terraform` as `local.accepted_keys` and pinned
  against Python by `tests/test_terraform_config.py`). There is no manifest, no discovery and
  no folder convention: an upload to a key that is not one of the ten is never read, and the
  instance profile is not granted a read on it either
- S3 **versioning is what identifies an upload**. Every Ingestion run, ledger row and duplicate
  check is keyed by `(bucket, key, object version)`, so a corrected publication is a new
  version and a new run, and two uploads of the same filename can never be confused for one
  another. Turn versioning on before the first upload (`terraform/s3.tf`)
- A **Source snapshot is complete and authoritative** for the units it contains: it restates
  every one of them, and what it does not carry is stated by its absence. A newer snapshot
  therefore updates the units it carries in place, never re-creates them, and never deletes
  the units it omits — those stay in Core as history with their identity, and stop being
  members of the Source (lineage in `service.source_memberships`). Only commissioning and
  decommissioning dates decide whether a unit is Active
- A **Boundary release replaces only the level it was published under** and rederives every
  unit's state/region/district from the new polygons, historical Core rows included, so the
  marts pivot on the new names without any Source snapshot to carry them

### What the service records (ADR 0007)
Operational state lives in the `service` schema, deliberately apart from the versioned raw
datalake, so "what happened to that file" is one place to look:
- `ingestion_runs` — one row per S3 object version: `source` or `boundary` input kind, state
  (`running`, `succeeded`, `stale`, `terminal`, `retryable`), attempt count, and the terminal
  error. This is the answer to "was it loaded, skipped, retried or failed, and why"
- `loaded_files` — the success ledger: an object version is loaded once and never re-extracted
- `source_memberships` — which units a given snapshot contained, and which of them were bad
  quality
- `boundaries` — the level-coded reference layer the spatial joins read

### The message contract (issue #6)
- The worker receives **one** SQS message at a time (long polling) and decodes every S3 record it carries. Separate messages are never combined, because one message is also one acknowledgement
- Within a message the order is fixed: Boundary records first as a single release, then Source records. Source rows are enriched with Boundary geography, so the release has to be applied first
- Records are processed independently: each one is its own Ingestion run with its own result, and a failing record never blocks the others in the same message
- The geography rebuild and the three materialized views are refreshed **once** per message, after the last record
- The message is deleted only when every record is successful, terminally skipped, or stale. If one record is still retryable the message stays, and redelivery processes only the records that are not settled yet
- The topic is read from `SNS_TOPIC_ARN`, the queue from `SQS_QUEUE_URL`, the bucket from `S3_BUCKET`, and the region from `AWS_DEFAULT_REGION` — the container cannot reach the instance metadata service, so the region is handed to it rather than discovered

### Startup and recovery (issue #8)
- The server container runs `python -m etl startup`: an explicit bootstrap — database preconditions, the fixed S3 key checks, the current Boundary releases applied with their downstream rebuild, the current Source object versions enqueued — and only then the worker. A missing or invalid Boundary is fatal: the container refuses the worker start and crash-loops until it is fixed; a missing Source is non-fatal
- **A refusal is announced, not just logged.** A crash loop is the deployment's own way of saying "still broken", and it says it to nobody: the container's console is gone the moment it restarts. So the bootstrap publishes the refusal on the alert topic, naming the bucket and the fixed keys that are missing (or the error, when the release is present and unusable), and the worker start is blocked either way. The alert is sent **once per distinct condition**, not once per restart: the refusal's fingerprint — bucket, reason, missing keys, and the error's *type* — is recorded in `service.bootstrap_alerts`, and a restart that finds the same one already recorded does not publish again. A refusal that *changes* is a new fact and does alert, so an operator who has just published a level learns that a different one is still absent. Once a startup finally succeeds, the bucket's claims are cleared, so the same condition returning later alerts as the new incident it is. `python -m etl startup` prints whether this attempt was the one that alerted
- The bootstrap creates no Ingestion run and mounts no data volume — the worker reads S3 through the event queue. Repeated starts are idempotent: ledgered Boundary releases are skipped and Source versions that already have a run (succeeded, stale, terminal, DLQ, in-flight) are not re-enqueued
- Recovery: `python -m etl redrive` re-enqueues failed or DLQ object versions (`--key` narrows to one key); the queue's redelivery resumes each version's existing run

### Retries, rejection, DLQ, and alerting (issue #7)
- **Rejected object version → terminal, alerted once.** A file that fails its own content validation can never be ingested as it stands, because an S3 version is immutable and validating it again gives the same answer. The worker does not spend five deliveries finding that out: the run becomes `terminal`, one SNS alert names the key, version, and the fix (upload a new version), and the message is deleted so the files behind it are not held up. A redelivery of the same message does not alert again
- **Infrastructure failure → retried, then DLQ.** Database, S3, and marts failures are retried on the next delivery, up to the queue's `maxReceiveCount = 5`. The worker takes the attempt number from SQS's `ApproximateReceiveCount`, so a redelivery after a crashed worker also counts
- **Exhaustion is not alerted by the worker.** On the fifth delivery the run stays `retryable`, its `terminal_error` records that the message is going to the DLQ, and the message is left for the redrive policy. The DLQ CloudWatch alarm is then the single alert for infrastructure trouble — the worker only ever alerts directly on a rejected object version, so each problem is reported once by exactly one path
- **Queue settings the worker relies on:** `maxReceiveCount = 5` (matching the worker's `MAX_DELIVERY_ATTEMPTS`) and a redrive policy onto the DLQ. The IAM role needs `sqs:ChangeMessageVisibility`, which the worker uses
- **Visibility.** While a message is processed its visibility timeout is extended to the six-hour SQS maximum and re-extended every minute, so a long ingest is not picked up by a second worker. When the work is over the worker hands the message back — `ChangeMessageVisibility: 0` — unless it was acknowledged, because SQS applies the redrive policy when a message becomes visible: a message that is kept would otherwise wait out the six hours it was just granted. A worker that is killed outright (`SIGKILL`, a lost node) cannot hand anything back, so that message waits for the last timeout it was given, and the next delivery resumes its runs from their recorded stage results

## 3. Logging
- All ETL logs are sent to CloudWatch Logs
- Two log groups under `log_group_prefix`: the Compose stack's container logs (one stream per
  container — a container's log file cannot be told apart from its path, so the stack shares
  one group rather than claiming a group per service), and the CloudWatch agent's own log, so
  that "the logs stopped arriving" is answerable. 30-day retention, set by Terraform
- Worker metrics are derived from log lines the worker already prints (started,
  bootstrap blocked, boundary applied, ETL errors). Metrics only, no alarm: the two alert
  paths below already report problems
- A pattern only fires if it can match the line it was written for, and CloudWatch
  rejects a whole filter over one bad term: a term may only carry `[A-Za-z0-9_.-]`
  unquoted (the colon in the two report lines does not), and `?term` is CloudWatch's
  JSON-field selector, not an anchor — on plain text it parses and never fires. Both
  rules are checked against the output of `python -m etl startup` in
  `tests/test_terraform_config.py`, so a pattern that would fail the apply fails the
  build instead

## 4. Notifications
- One topic carries all three alert paths, so notification setup is a single question: which endpoints, on which topic
- A rejected object version → the worker publishes it once, when the run settles terminal
- A blocked startup → the bootstrap publishes it, once per distinct condition rather than once per crash-loop restart, and once more if the same condition returns after a startup has succeeded
- DLQ non-empty → CloudWatch alarm → SNS. This is the *only* alert for retry exhaustion, so an infrastructure failure is reported once
- Both alarms sit on the queue's own `AWS/SQS` metrics, so no problem is announced twice

## 5. IAM role for EC2
- s3:GetObject, s3:GetObjectVersion (on the fixed accepted keys only),
sqs:ReceiveMessage, DeleteMessage, ChangeMessageVisibility, SendMessage,
sns:Publish; logs:CreateLogStream, PutLogEvents
- No `s3:ListBucket` and no `sqs:GetQueueAttributes`: the worker addresses its keys
directly and reads the delivery count off the message it receives, so neither is granted
- The region is handed to the container as `AWS_DEFAULT_REGION` rather than discovered,
because a container cannot count on reaching the instance metadata service

## 6. Terraform
- Use Terraform for scripting
- `terraform/` provisions the bucket, queue + DLQ, topic, instance profile, log groups,
  metrics and alarms (#10, `docs/adr/0010-terraform-deployment-contract.md`); the operator
  steps are in `terraform/README.md`
- The delivery contract (six-hour visibility, five deliveries, fourteen-day DLQ retention) is
  not a variable: it is the contract `etl/ingestion.py` is written against, stated once in
  `locals` and pinned against the Python constants by `tests/test_terraform_config.py`
- The instance profile grants no `cloudwatch:PutMetricData` — the worker metrics are
  log-derived and the queue metrics are native, so nothing on the host publishes a metric
- The AWS provider has no resource for attaching an instance profile, so the attachment is one
  `aws ec2 modify-instance-attribute` call and a Terraform `check` block warns on later plans
  when the host is not running under the profile

## 7. What this design deliberately does not do
- **No S3 event filtering beyond the file extension.** S3 accepts one prefix/suffix filter per
  notification configuration, so the configuration filters `.gpkg` and the exact ten keys are
  enforced where they can be exact: the worker rejects anything else, and the instance
  profile grants a read on the accepted keys only
- **No manifest, no discovery, no per-run configuration.** The key layout *is* the interface;
  adding an Energy source is a code change (a fixed key plus its transform), not an upload
- **No partial boundary releases.** The four levels are one geography: a release is applied as
  a batch and one invalid level rejects the whole batch, so a bad file can never half-apply
- **No deletion of loaded data.** Core rows and raw versions are never removed by a later
  snapshot; a unit that leaves a Source dataset stays as history. Correcting data means
  publishing a new version
- **No retry of a file that failed its own validation.** An S3 version is immutable, so
  validating it again reaches the same verdict; the run is terminal and the operator uploads a
  new version (see §2, "Retries, rejection, DLQ, and alerting")
- **No write access for the visualization.** The app connects as `viz_reader`, which never
  receives `INSERT/UPDATE/DELETE`
- **No batch path in the server deployment.** `run-all` over a mounted data volume is the
  local workflow (`local_compose.yaml`); the server stack mounts no data volume at all
