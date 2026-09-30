# The data bucket (issue #10).
#
# Private, versioned, and without a lifecycle rule: every object version is
# immutable input the Ingestion run ledger points at, so expiring one would
# delete history the pipeline still claims to reference. The worker identifies
# input by `(bucket, key, version id)`, which only means something while the
# version can still be read back.

resource "aws_s3_bucket" "data" {
  bucket        = local.bucket_name
  force_destroy = false

  tags = merge(local.tags, { Component = "data-bucket" })
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Private is a policy, not just an ACL setting: this denies any request that
# does not arrive over TLS, so the published source data is never on the wire in
# the clear even for a caller the bucket policy would otherwise allow.
data "aws_iam_policy_document" "bucket" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.data.arn,
      "${aws_s3_bucket.data.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "data" {
  bucket = aws_s3_bucket.data.id
  policy = data.aws_iam_policy_document.bucket.json
}

# Uploads become queue messages; the worker has no poller, so an upload it is
# not told about is never ingested. S3 accepts one prefix/suffix filter per
# notification configuration, so `.gpkg` is the widest filter available here —
# the exact-key rule is enforced where it is exact, by the worker and by the
# instance profile's object grant.
resource "aws_s3_bucket_notification" "ingestion" {
  bucket = aws_s3_bucket.data.id

  queue {
    queue_arn     = aws_sqs_queue.ingestion.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".gpkg"
  }

  # S3 validates the destination queue's policy when the notification is
  # created, so the grant has to exist first.
  depends_on = [aws_sqs_queue_policy.ingestion]
}
