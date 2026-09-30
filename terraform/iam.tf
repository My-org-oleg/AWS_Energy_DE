# The instance profile of the existing EC2 host (issue #10).
#
# Terraform creates the role, its policy and the profile, and re-asserts all
# three on every apply. Attaching the profile to the host is the one step it
# cannot do — the provider has no resource for it — so the check at the bottom
# warns and README.md gives the operator the single command. Nothing here may
# launch an instance: this is an EC2 profile, not a service role, so `ec2:*`
# stays out of the policy.

data "aws_instance" "server" {
  instance_id = var.ec2_instance_id
}

data "aws_iam_policy_document" "worker_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "worker" {
  name               = "${var.name_prefix}-worker"
  description        = "Least-privilege role for the AWS Energy DE ingestion worker and the CloudWatch agent on the EC2 host."
  assume_role_policy = data.aws_iam_policy_document.worker_assume_role.json

  tags = merge(local.tags, { Component = "worker" })
}

# Every action the worker and the CloudWatch agent actually call, and no others.
# The named statements exist so a reviewer can check the grant by reason rather
# than by reading a list of wildcards.
data "aws_iam_policy_document" "worker" {
  # The worker reads one object version of an accepted key, and `head_object`
  # during the bootstrap key checks needs the same read grant. Nothing lists the
  # bucket: the accepted key layout is known in advance, so the worker addresses
  # keys directly and drops an event for anything outside the layout.
  statement {
    sid       = "ReadAcceptedObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [for key in local.accepted_keys : "${aws_s3_bucket.data.arn}/${key}"]
  }

  # Receive, delete and re-time the one message the worker is working; send for
  # the bootstrap enqueue and for `etl redrive`. The delivery count arrives as a
  # message attribute on the receive call, so no queue-level read is granted. The
  # DLQ is absent on purpose: draining it is an operator action, and redrive
  # re-enqueues from the database rather than from the DLQ.
  statement {
    sid    = "OperateIngestionQueue"
    effect = "Allow"

    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:ReceiveMessage",
      "sqs:SendMessage",
    ]

    resources = [aws_sqs_queue.ingestion.arn]
  }

  statement {
    sid       = "PublishAlerts"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }

  # The log groups already exist, so the agent only needs to create streams in
  # them; retention stays Terraform's to set. `cloudwatch:PutMetricData` is
  # absent on purpose: the worker metrics are log-derived and the queue metrics
  # are native `AWS/SQS` ones, so nothing on this host publishes a metric of its
  # own. A grant the code never calls is a grant waiting to be abused.
  statement {
    sid    = "WriteOperationalLogs"
    effect = "Allow"

    actions = [
      "logs:CreateLogStream",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]

    resources = [
      "${aws_cloudwatch_log_group.compose.arn}:*",
      "${aws_cloudwatch_log_group.agent.arn}:*",
    ]
  }
}

resource "aws_iam_role_policy" "worker" {
  name   = "${var.name_prefix}-worker"
  role   = aws_iam_role.worker.id
  policy = data.aws_iam_policy_document.worker.json
}

resource "aws_iam_instance_profile" "worker" {
  name = "${var.name_prefix}-worker"
  role = aws_iam_role.worker.name

  tags = local.tags
}

# The provider has no resource for attaching a profile to an instance — the
# profile is a property of the instance, not a thing of its own — so the
# attachment is one `aws ec2 modify-instance-attribute` away, and the README
# says so. What Terraform can do is notice when it has not happened, which is
# what this check is for: a `check` warns instead of failing, because the first
# apply cannot possibly see its own profile attached. The next plan either
# passes or names the instance that drifted.
check "host_runs_the_worker_profile" {
  assert {
    condition     = data.aws_instance.server.iam_instance_profile == aws_iam_instance_profile.worker.name
    error_message = "The EC2 host is not running under the ${aws_iam_instance_profile.worker.name} profile, so the worker and the CloudWatch agent have no credentials. Attach it (see terraform/README.md) and stop/start the instance, because a profile only reaches a running host's processes on the next boot."
  }
}
