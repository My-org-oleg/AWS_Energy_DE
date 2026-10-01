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

## Install Terraform

The host does not need Terraform to *run* the stack — only to change it. The
`apply` can happen on any machine that holds AWS credentials, and the host
consumes nothing but the outputs below. When the commands do run on the host,
use the version this repository pins: CI validates with `1.9.5` and
`required_version` is `>= 1.6.0`, so a newer release is allowed and an older
one is not.

```bash
TF_VERSION=1.9.5
ARCH=amd64                      # arm64 on a Graviton instance
cd /tmp
curl -fsSLO "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_${ARCH}.zip"
curl -fsSLO "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_SHA256SUMS"
grep "terraform_${TF_VERSION}_linux_${ARCH}.zip" terraform_${TF_VERSION}_SHA256SUMS | sha256sum -c -
unzip -o terraform_${TF_VERSION}_linux_${ARCH}.zip
sudo install -m 0755 terraform /usr/local/bin/terraform
terraform -version
```

The checksum line is why the pinned binary is the default route: the archive is
verified against the release HashiCorp signs, and a mismatch aborts before
anything is unpacked. Two alternatives, neither of which pins a version:

- **apt** — `https://apt.releases.hashicorp.com` installs whatever the newest
  release is, and needs a supported Ubuntu codename; a distribution released
  recently has no `dists` entry yet.
- **The container image** — nothing to install at all, and the same version CI
  uses. Bind-mount this directory so state persists outside the container, and
  hand it credentials from the host:

  ```bash
  docker run --rm -v "$PWD/terraform:/tf" -w /tf \
    -e AWS_PROFILE -e AWS_REGION -v "$HOME/.aws:/root/.aws:ro" \
    hashicorp/terraform:1.9.5 <init|plan|apply|fmt|validate>
  ```

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

One bootstrap caveat if the first apply runs on the instance itself: the
instance profile it needs is what this apply creates, so a host without a
profile yet cannot authenticate. Run that first apply from a machine that has
credentials — the role and profile it creates are then attached to the host by
the first of the two steps below, and every later `plan`/`apply` can be run from
there.

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

**2. Install the CloudWatch agent, then its config.** The agent is the thing that
actually ships the container logs; the config below only tells it where to put
them, and the config is an output so it always matches the log groups this
apply created. The package is not in Ubuntu's own repositories, so it comes from
Amazon — as a signed `.deb`, which `apt` installs like any other local package:

```bash
ARCH=amd64                      # arm64 on a Graviton instance
cd /tmp
curl -fsSLO "https://amazoncloudwatch-agent.s3.amazonaws.com/ubuntu/${ARCH}/latest/amazon-cloudwatch-agent.deb"
curl -fsSLO "https://amazoncloudwatch-agent.s3.amazonaws.com/ubuntu/${ARCH}/latest/amazon-cloudwatch-agent.deb.sig"
curl -fsSL https://amazoncloudwatch-agent.s3.amazonaws.com/assets/amazon-cloudwatch-agent.gpg | gpg --import
gpg --verify amazon-cloudwatch-agent.deb.sig amazon-cloudwatch-agent.deb
sudo apt-get install -y ./amazon-cloudwatch-agent.deb
systemctl status amazon-cloudwatch-agent
```

`gpg --verify` has to report a good signature from the *Amazon CloudWatch Agent*
key (`3B789C72`) before the install step; the version this resolves to is at
`https://amazoncloudwatch-agent.s3.amazonaws.com/info/latest/CWAGENT_VERSION`.
The `*.deb.sig` and the GPG key are the verification material AWS documents for
this package.

Amazon also publishes an apt repository for the agent
(`https://packages.aws.amazon.com/amazoncloudwatch-agent/ubuntu/`) with an
`amazoncloudwatch-agent.list` entry signed by the AWS CLI archive key, which
gives you `apt upgrade` for the agent. It carries one `dists/` directory per
Ubuntu release, so a distribution too new for it fails the same way a too-old
Terraform repo does — check with `apt-cache policy amazon-cloudwatch-agent` and
fall back to the `.deb` above if there is no candidate.

The package starts a systemd service as root, which it has to be: it reads
`/var/lib/docker/containers/*/*.log` (the `docker_container_log_glob` this repo
passes through). Now the config — and the path is not free: the service runs
`start-amazon-cloudwatch-agent` with no `-c`, so the agent reads the default
`/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json` and nothing
else. A config saved under any other name is invisible to it, and the service
exits 1 with `No json config files found, please provide config, exit now`:

```bash
sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json \
  > /dev/null < <(terraform output -raw cloudwatch_agent_config)
sudo systemctl enable --now amazon-cloudwatch-agent
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a status
```

`-a status` prints the running config and whether the agent is started; if it
prints `stopped`, the agent's own log says why —
`/opt/aws/amazon-cloudwatch-agent/logs/amazon-cloudwatch-agent.log`.

No IAM change is needed for the agent. AWS's own instructions ask for the
managed policy `CloudWatchAgentServerPolicy`; the worker profile this apply
attaches already grants the narrower three calls on the two log groups
(`WriteOperationalLogs`, `terraform/iam.tf:80`), and it has to run under that
profile anyway — so attaching AWS's managed policy would add grants nothing
here calls.

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
