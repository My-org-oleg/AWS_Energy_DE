# Event-driven deployment for the AWS Energy DE ETL (issue #10).
#
# Everything the deployed worker needs and nothing else: one private, versioned
# data bucket holding the fixed accepted key layout, one standard SQS queue with
# its dead-letter queue, one alert topic, the least-privilege instance profile
# for the existing EC2 host, and the CloudWatch log groups, metrics and alarms
# the operational contract in `AWS.md` describes.
#
# The instance profile is created here but attached to the host by the operator:
# the AWS provider has no resource for attaching a profile to an instance that
# already exists. The `check` block in iam.tf warns while that step is pending.
#
# The delivery contract (six-hour visibility, five deliveries, fourteen-day DLQ
# retention) is deliberately not a variable: it is the contract `etl/ingestion.py`
# is written against, so it is stated once here, next to the queue it configures,
# and `tests/test_terraform_config.py` pins both sides of it.
#
# Credentials are never configured here. The provider takes the operator's
# ambient credentials (environment, shared config, or the CI role), so nothing
# secret and no account id is committed — see README.md.

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # The fixed accepted key layout: six Source datasets and four Boundary levels.
  # `etl.ingestion.ACCEPTED_KEYS` is the same list, enforced by the worker on
  # every message; the config test fails if the two drift apart.
  accepted_keys = [
    "sources/bio.gpkg",
    "sources/gas.gpkg",
    "sources/hydro.gpkg",
    "sources/solar.gpkg",
    "sources/storage.gpkg",
    "sources/wind.gpkg",
    "boundaries/level-0.gpkg",
    "boundaries/level-1.gpkg",
    "boundaries/level-2.gpkg",
    "boundaries/level-3.gpkg",
  ]

  # Bucket names are global and queue/topic names are per-account, so each name
  # is derived from the prefix and the caller's account id unless the operator
  # pins one explicitly.
  bucket_name = coalesce(var.bucket_name, "${var.name_prefix}-${local.account_id}-data")
  queue_name  = coalesce(var.queue_name, "${var.name_prefix}-ingestion")
  dlq_name    = coalesce(var.dlq_name, "${var.name_prefix}-ingestion-dlq")
  topic_name  = coalesce(var.topic_name, "${var.name_prefix}-alerts")

  # The delivery contract with `etl/ingestion.py`, which sets its visibility
  # timeout to the six-hour SQS maximum and counts deliveries itself. A shorter
  # timeout here would let a second delivery start on top of a running ingest.
  visibility_timeout_seconds = 21600

  # MAX_DELIVERY_ATTEMPTS: the fifth delivery leaves the run retryable and lets
  # the redrive policy move the message, which is the DLQ alarm's single alert.
  max_receive_count = 5

  # Four days on the queue is a redrive window; the DLQ keeps a terminal
  # failure for fourteen days, which is the time an operator gets to act on it.
  queue_message_retention_seconds = 345600
  dlq_message_retention_seconds   = 1209600

  # Two groups: the Compose stack's container logs, and the CloudWatch agent's
  # own log — so that "the logs stopped arriving" has an answer. A container's
  # log file cannot be told apart from its path, so the stack shares one group
  # with one stream per container rather than four groups that all collect the
  # same files.
  log_groups = {
    compose = "${var.log_group_prefix}/compose"
    agent   = "${var.log_group_prefix}/host/amazon-cloudwatch-agent"
  }
  # The two group names, stated once so the agent template and the outputs
  # cannot drift onto different groups than the resources below create.
  worker_log_group = local.log_groups["compose"]
  agent_log_group  = local.log_groups["agent"]

  # Worker activity derived from log lines the container already writes, each
  # pattern specific enough to only match the ETL logger or the CLI's own
  # output, because the group also carries the database, the app and the proxy.
  # Metrics only, deliberately: a rejected object version is announced by the
  # worker and retry exhaustion by the DLQ alarm, so a third alert path over the
  # same lines would report one problem twice.
  #
  # A pattern is the words of the line it counts, in the order they appear.
  # Two things are barred here, and both are invisible until CloudWatch acts:
  # PutMetricFilter rejects a filter whose unquoted term carries a character
  # outside `[A-Za-z0-9_.-]` — it fails the whole resource with `Invalid
  # character(s) in term`, and the colon in the two report lines above is
  # exactly that — while a leading `?` is not an anchor but CloudWatch's
  # JSON-field existence selector, which parses and then never fires, because
  # these events are plain text. tests/test_terraform_config.py models both
  # rules, so a pattern carrying either fails the build rather than the apply.
  worker_metric_patterns = {
    WorkerStarted   = "Ingestion worker reading"
    StartupBlocked  = "Worker start blocked"
    BoundaryApplied = "Boundary release applied"
    WorkerErrors    = "ERROR etl"
  }

  tags = merge(
    {
      Project   = var.name_prefix
      ManagedBy = "terraform"
    },
    var.tags,
  )
}
