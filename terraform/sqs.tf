# The ingestion queue and its dead-letter queue (issue #10).
#
# One standard queue, because the worker's unit of work is one message and a FIFO
# queue would also add message-group bookkeeping the worker has no use for. The
# timing constants come from `locals`, where they are tied to the constants in
# `etl/ingestion.py`.

resource "aws_sqs_queue" "dlq" {
  name                      = local.dlq_name
  message_retention_seconds = local.dlq_message_retention_seconds

  tags = merge(local.tags, { Component = "ingestion-dlq" })
}

resource "aws_sqs_queue" "ingestion" {
  name                       = local.queue_name
  visibility_timeout_seconds = local.visibility_timeout_seconds
  message_retention_seconds  = local.queue_message_retention_seconds

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = local.max_receive_count
  })

  tags = merge(local.tags, { Component = "ingestion-queue" })
}

# The S3-to-SQS grant belongs to the queue, not to the instance: it is the
# delivery permission for the bucket, and the instance never sends a message
# except when it re-enqueues or redrives. The account condition is what keeps
# another account's bucket from using this queue as a relay.
data "aws_iam_policy_document" "ingestion_queue" {
  statement {
    sid       = "AllowS3ObjectCreatedDelivery"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.ingestion.arn]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.data.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "ingestion" {
  queue_url = aws_sqs_queue.ingestion.id
  policy    = data.aws_iam_policy_document.ingestion_queue.json
}

# A dead-letter queue needs its own grant for SQS to move a message into it,
# and the only action that move needs is sending: nothing else in the account may
# use this queue as a sink, and the principal is already narrowed to the
# ingestion queue's ARN.
data "aws_iam_policy_document" "dlq" {
  statement {
    sid       = "AllowIngestionQueueRedrive"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sqs_queue.ingestion.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  policy    = data.aws_iam_policy_document.dlq.json
}
