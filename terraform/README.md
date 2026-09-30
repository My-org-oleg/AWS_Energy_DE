# Terraform: the event-driven deployment (issue #10)

This directory provisions everything the ETL worker talks to in AWS, and nothing
else: the S3 bucket it reads, the SQS queue that tells it what to read, the
dead-letter queue a failed ingest ends up in, the SNS topic it alerts on, the
EC2 instance profile it runs under, and the CloudWatch log groups, metrics and
alarms the operational contract in [`AWS.md`](../AWS.md) describes.

It creates no EC2 instance, no VPC and no RDS: the host already exists and the
database runs on it. The only value the operator must supply is which instance
that is, and the only deployment step Terraform cannot take is attaching the
instance profile to it (the provider has no resource for that — see below).

## What it provisions, and why it looks like this

| Resource | Deliberate choices |
|---|---|
| S3 bucket | Private, versioned, SSE, TLS-only, **no lifecycle rule** — object versions are the audit trail the Ingestion ledger points at, so nothing may expire them. |
| SQS ingestion queue | Six-hour visibility and five deliveries, stated as `locals` next to the queue rather than as variables: they are the contract `etl/ingestion.py` is written against, and `tests/test_terraform_config.py` fails if the two drift apart. |
| SQS dead-letter queue | Fourteen-day retention — the window an operator gets to act before a terminal failure is gone. |
| SQS queue policies | S3 may send to the ingestion queue and only from this bucket and this account; the DLQ accepts a redrive from the ingestion queue only. |
| SNS topic | One topic, no policy (the alarms and the worker are the same account). Subscription endpoints come from `alert_subscriptions`, so no address is committed. |
| IAM role + instance profile | Every statement names the call that needs it. No `cloudwatch:PutMetricData`: the worker metrics are log-derived and the queue metrics are native, so nothing on the host publishes a metric. No SQS access to the DLQ — draining it is an operator action. A `check` block warns when the host is not running under the profile. |
| CloudWatch log groups | Two: the Compose stack's container logs, and the agent's own log, so that "the logs stopped arriving" is answerable. Thirty-day retention, set here because CloudWatch's own default never expires. |
| CloudWatch alarms | DLQ not empty, and a queue whose oldest message outgrew the visibility window. Both on `AWS/SQS` metrics, so no problem is announced twice. |

One limit worth knowing: a container's log file cannot be told apart from its
path, so the agent ships every container to the one Compose group as one stream
per container, rather than claiming a group per service. The two post-apply
steps below are what put the agent's own log where it can be found.

## Apply

Credentials come from the operator's ambient AWS configuration (environment,
shared config, or a CI role). Nothing secret and no account id is in this
repository; the account is read from `data.aws_caller_identity`.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # set ec2_instance_id
terraform init
terraform plan
terraform apply
```

`ec2_instance_id` is the only required value. Names default to
`<name_prefix>-<account id>-data` for the bucket and `<name_prefix>-ingestion`,
`<name_prefix>-ingestion-dlq`, `<name_prefix>-alerts` for the rest; set the
`*_name` variables only to adopt resources that already exist.

An email subscription is silent until the confirmation click, which is the
operator's action, not Terraform's.

State belongs in remote storage, not in this repository — `terraform/.gitignore`
keeps it out, and the account's resource ids are in it. The provider lock file is
committed, so CI and every operator resolve the same provider version.

## After the apply: two steps Terraform cannot do for you

**1. Attach the profile to the running host.** The AWS provider has no resource
for attaching an instance profile — a profile is a property of the instance,
not a thing of its own — so this is one CLI call, and a profile only reaches the
host's own processes on the next boot:

```bash
aws ec2 modify-instance-attribute \
  --instance-id i-0123456789abcdef0 \
  --iam-instance-profile "$(terraform output -raw instance_profile_name)"

sudo shutdown -r now     # or stop/start the instance
```

Every later `terraform plan` checks this and names the instance if it drifts, so
a host that was recreated without the profile is caught rather than discovered
by a worker that cannot read S3.

**2. Install the CloudWatch agent config.** The rendered config is an output, so
it always matches the log groups this apply created:

```bash
sudo tee /opt/aws/amazon-cloudwatch-agent/etc/cloudwatch-agent.json \
  > /dev/null < <(terraform output -raw cloudwatch_agent_config)
sudo systemctl restart amazon-cloudwatch-agent
```

Then point the Compose stack at the outputs, in the host's `.env`
(`docs/remote-deploy.md` has the full walk-through):

```bash
terraform output -raw aws_region            # → AWS_DEFAULT_REGION
terraform output -raw bucket_name           # → S3_BUCKET
terraform output -raw ingestion_queue_url   # → SQS_QUEUE_URL
terraform output -raw alert_topic_arn       # → SNS_TOPIC_ARN
```

`SQS_QUEUE_URL` and `SNS_TOPIC_ARN` are the two values the worker cannot start
without, and `S3_BUCKET` and `AWS_DEFAULT_REGION` the other two: the Compose
stack requires all four, because boto3 would otherwise look for a region on the
instance metadata service, which a container cannot count on reaching. The
outputs also print `dead_letter_queue_url` and `log_group_names` for an
investigation.

## When an alarm fires

Two alarms watch the queue, and each covers a failure the other cannot see.
**The DLQ is not empty** is the terminal alert for a message that used up its
five deliveries: the worker stays silent on exhaustion, so a message in the DLQ
is always exactly one problem. Inspect it, then redrive from the ledger, which is
the authoritative record of what failed and why:

```bash
aws sqs receive-message \
  --queue-url "$(terraform output -raw dead_letter_queue_url)" \
  --max-number-of-messages 10

python -m etl redrive            # re-enqueues failed or DLQ object versions
python -m etl redrive --key sources/bio.gpkg
```

`redrive` re-enqueues from the database, not from the DLQ, so the queue's
messages can be deleted once inspected.

**The oldest message is older than the visibility window** catches the other
half: a worker that is alive but not draining, or not running at all, never puts
anything in the DLQ. The threshold sits above the six-hour visibility timeout
because a Boundary release legitimately holds its message invisible for as long
as the worker asked for.

## Verifying a change

```bash
terraform fmt -check -diff
terraform validate          # needs the provider, not credentials
python -m pytest tests/test_terraform_config.py
```

CI runs `fmt`, `init -backend=false`, `validate` and the configuration tests on
every pull request. The configuration tests parse the same files and assert the
things an apply would otherwise only prove by accident — most importantly that
the accepted-key layout and the delivery contract still match
`etl/ingestion.py`.
