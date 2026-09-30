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
- ETL-worker loads file from s3 to /tmp folder, after finishing ETL erases it.
- Energy source resolves through energy_source column in dataframe (not through filename) 
- Transform stage receives table name to transform. 
- All files are treated consequently via SQS 
- All extracted files are logged in loaded_files table and not extracted twice.

### The message contract (issue #6)
- The worker receives **one** SQS message at a time (long polling) and decodes every S3 record it carries. Separate messages are never combined, because one message is also one acknowledgement
- Within a message the order is fixed: Boundary records first as a single release, then Source records. Source rows are enriched with Boundary geography, so the release has to be applied first
- Records are processed independently: each one is its own Ingestion run with its own result, and a failing record never blocks the others in the same message
- The geography rebuild and the three materialized views are refreshed **once** per message, after the last record
- The message is deleted only when every record is successful, terminally skipped, or stale. If one record is still retryable the message stays, and redelivery processes only the records that are not settled yet
- The topic is read from `SNS_TOPIC_ARN`, the queue from `SQS_QUEUE_URL`, the bucket from `S3_BUCKET`, and the region from `AWS_DEFAULT_REGION` — the container cannot reach the instance metadata service, so the region is handed to it rather than discovered

### Startup and recovery (issue #8)
- The server container runs `python -m etl startup`: an explicit bootstrap — database preconditions, the fixed S3 key checks, the current Boundary releases applied with their downstream rebuild, the current Source object versions enqueued — and only then the worker. A missing or invalid Boundary is fatal: the container refuses the worker start and crash-loops until it is fixed; a missing Source is non-fatal
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
- A pattern only fires if it can match the line it was written for, so each one is checked
  against the output of `python -m etl startup`. A `?` on a line that starts its own text
  would publish nothing while looking configured

## 4. Notifications
- Errors in ETL trigger SNS-alert with email subscription
- CloudWatch Alarm on DLQ → SNS
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