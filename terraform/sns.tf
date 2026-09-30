# The alert topic (issue #10).
#
# One topic carries all three alert paths, so notification setup stays a single
# question an operator has to answer: which endpoints, on which topic.
#
#   - a rejected object version, published by the worker the moment the run is
#     settled terminal (issue #7),
#   - a fatal bootstrap condition, published before the worker is allowed to
#     start (issue #8),
#   - the DLQ alarm, which is the single alert for retry exhaustion, so an
#     infrastructure failure is reported once rather than twice.
#
# No topic policy is needed: the alarms that publish to it run in this account,
# and within an account the topic owner may publish without one.

resource "aws_sns_topic" "alerts" {
  name = local.topic_name

  tags = merge(local.tags, { Component = "alerts" })
}

# Endpoints come from `alert_subscriptions`, never from a committed address. An
# empty map provisions the topic alone, which is a valid deployment: an operator
# can read the alarms in the console.
resource "aws_sns_topic_subscription" "alerts" {
  for_each = var.alert_subscriptions

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = each.value.protocol
  endpoint  = each.value.endpoint
}
