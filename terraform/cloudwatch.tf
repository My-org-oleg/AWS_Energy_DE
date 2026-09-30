# CloudWatch: log groups, the worker metrics derived from them, and the two
# alarms (issue #10).
#
# Three alert paths exist for the deployment and each problem is reported by
# exactly one of them:
#
#   - a rejected object version — the worker publishes it (issue #7),
#   - retry exhaustion — the DLQ alarm below,
#   - a stalled queue — the oldest-message alarm below.
#
# The metric filters below deliberately have no alarm: they would be a second
# voice for something already announced.

resource "aws_cloudwatch_log_group" "compose" {
  name              = local.log_groups["compose"]
  retention_in_days = var.log_retention_days

  tags = local.tags
}

resource "aws_cloudwatch_log_group" "agent" {
  name              = local.log_groups["agent"]
  retention_in_days = var.log_retention_days

  tags = local.tags
}

# The worker writes its progress to stdout, which the CloudWatch agent ships to
# the Compose log group. Counting the lines it already prints is the cheapest
# honest signal of worker activity: it needs no code change in the container.
resource "aws_cloudwatch_log_metric_filter" "worker" {
  for_each = local.worker_metric_patterns

  name           = each.key
  pattern        = each.value
  log_group_name = aws_cloudwatch_log_group.compose.name

  metric_transformation {
    name          = each.key
    namespace     = var.metrics_namespace
    value         = "1"
    default_value = 0
    unit          = "Count"
  }
}

# The terminal alert for infrastructure trouble. The worker deliberately stays
# silent on exhaustion so this alarm is the single notification for it.
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.name_prefix}-dlq-not-empty"
  alarm_description   = "The dead-letter queue is not empty: a message used up its five deliveries and is waiting for an explicit redrive."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = var.dlq_alarm_period_seconds
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }

  alarm_actions = concat([aws_sns_topic.alerts.arn], var.additional_alarm_actions)
  ok_actions    = var.alarm_ok_actions

  tags = merge(local.tags, { Component = "alarms" })
}

# A queue that stops draining is invisible without this: the worker can be alive
# and still not ingesting. The threshold sits above the six-hour visibility
# timeout on purpose — a Boundary release legitimately holds its message
# invisible for as long as the worker asked for, and paging on that would report
# healthy work as an incident.
resource "aws_cloudwatch_metric_alarm" "ingestion_stalled" {
  alarm_name          = "${var.name_prefix}-ingestion-stalled"
  alarm_description   = "The oldest message on the ingestion queue is older than the visibility window allows for healthy work, so the worker is not draining the queue."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  period              = var.ingestion_stalled_period_seconds
  evaluation_periods  = var.ingestion_stalled_evaluation_periods
  threshold           = var.ingestion_stalled_threshold_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { QueueName = aws_sqs_queue.ingestion.name }

  alarm_actions = concat([aws_sns_topic.alerts.arn], var.additional_alarm_actions)
  ok_actions    = var.alarm_ok_actions

  tags = merge(local.tags, { Component = "alarms" })
}
