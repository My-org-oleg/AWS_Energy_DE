# The AWS deployment is Terraform, and the delivery contract is not a variable

The AWS infrastructure the event-driven worker runs against — the S3 bucket, the
SQS queue and its dead-letter queue, the SNS topic, the EC2 instance profile and
the CloudWatch log groups, metrics and alarms — is provisioned by Terraform in
`terraform/`. Nothing about it is created by hand in the console.

## Context

Issue #10 asks for the infrastructure of `AWS.md` to exist as code: a
reproducible deployment, least-privilege credentials, and validation that does
not need an AWS account. The worker is already written against a specific
delivery contract (`etl/ingestion.py`: `VISIBILITY_TIMEOUT_SECONDS = 21600`,
`MAX_DELIVERY_ATTEMPTS = 5`), and issue #7's alerting contract says each
operational problem is reported by exactly one path.

Four things in this configuration are judgement calls rather than
transcriptions, and each one had a plausible alternative.

## Decision

- **The delivery contract is a `local`, not a variable.** Visibility timeout,
  `maxReceiveCount` and DLQ retention are what `etl/ingestion.py` is written
  against, so they are stated once in `terraform/main.tf` next to the queue and
  pinned against the Python constants by `tests/test_terraform_config.py`.
  Making them operator-tunable would let a deployment drift away from the code
  that has to survive it. Everything genuinely per-deployment — region, name
  prefix, the instance id, subscriptions, retention — stays a variable.
- **Two log groups, not one per Compose service.** A container's log file
  cannot be told apart from its path, so a CloudWatch agent tailing
  `/var/lib/docker/containers/*/*-json.log` sends every container's output to
  every group it is configured with. One group with one stream per container
  (`publish_multi_logs`) is honest about that; four groups that all collect the
  same files would be worse. The second group is the agent's own log, so that
  "the logs stopped arriving" has an answer. Thirty-day retention is set
  explicitly, because CloudWatch's own default never expires.
- **The instance profile grants no `cloudwatch:PutMetricData`.** The worker
  metrics are log-derived and the queue metrics are native `AWS/SQS` ones, so
  nothing on the host calls it; the grant would need a wildcard resource and
  would be a privilege nothing uses. A metric-publishing source added later
  brings the grant with it.
- **Terraform attaches nothing to the host; it checks.** The AWS provider has no
  resource for associating an instance profile (a profile is a property of the
  instance, not a separable object), so the attachment is one
  `aws ec2 modify-instance-attribute` call documented in `terraform/README.md`,
  and a `check` block warns on every later plan when the host is not running
  under the profile. A `check` warns rather than fails because the first apply
  cannot see its own profile attached.
- **The bucket filters `.gpkg`; the exact key layout is enforced by the worker
  and the instance profile.** A notification configuration filters one
  prefix/suffix pair, so the ten accepted keys cannot be named in the bucket —
  the widest filter available is the suffix, and an object the worker does not
  accept is dropped there and never granted a read. The layout is therefore
  declared once, in `local.accepted_keys`, and checked against
  `etl.ingestion.ACCEPTED_KEYS`.
- **Two queue alarms, because the DLQ alarm cannot see half the failures.** Issue
  #10 asks for the DLQ alarm, and it is the right alert for a message that used
  up its five deliveries. A worker that is alive but not draining, or not
  running, never puts anything in the DLQ, so the second alarm watches the
  queue's own `ApproximateAgeOfOldestMessage` with its threshold above the
  six-hour visibility window — a Boundary release legitimately holds a message
  invisible for as long as the worker asked for. It is an addition to the issue,
  kept because the gap it closes is an unalerted outage.
- **The region is handed to the container, not discovered.** `boto3` falls back
  to the instance metadata service when no region is configured, and a container
  on a Compose bridge network cannot count on reaching it. The region is
  therefore an output and a required `AWS_DEFAULT_REGION` on the pipeline
  service, alongside the three the worker already required.

Credentials are never configured: the provider takes the operator's ambient AWS
configuration, and the account id is read from `data.aws_caller_identity`, so
nothing account-specific or secret is committed.

## Consequences

- The deployment is reproducible and reviewable, and CI catches a renamed
  argument (`terraform validate`) or a formatting slip (`terraform fmt -check`)
  on every pull request. CI also runs `tests/test_terraform_config.py`, which
  needs no database, no data and no credentials — cheap enough to be a gate, and
  the only thing that pins the Terraform side to `etl/ingestion.py`.
- The accepted-key layout and the delivery contract are checked against the
  Python side on every test run, so the bucket cannot promise a layout the
  worker rejects, and the queue cannot be tuned past what the worker survives.
  Each log-derived metric is checked against the line it counts, so a renamed
  banner or a mistyped filter pattern fails the build instead of emptying a
  metric in production.
- One step of the deployment is manual by necessity, and it is the step that
  gives the worker its credentials; the `check` block makes it visible on the
  next plan rather than at the next incident.
- Changing a delivery number is a code change in two places, which is the
  point: the two must agree.
