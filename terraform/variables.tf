# Everything an operator may set. The deployment contract itself (queue
# visibility, delivery attempts, DLQ retention) is in `locals` instead, because
# the worker is written against those numbers.

variable "aws_region" {
  description = "Region the deployment lives in. The EC2 host must be in the same region as the bucket, queue and topic, and it is handed to the worker container as AWS_DEFAULT_REGION."
  type        = string
  default     = "eu-central-1"
}

variable "name_prefix" {
  description = "Prefix for every resource name, so the deployment is recognisable in the console and in the logs."
  type        = string
  default     = "aws-energy-de"
}

variable "ec2_instance_id" {
  description = "Existing EC2 instance running the Compose stack. Terraform creates the instance profile it needs; attaching the profile to the instance is the operator step in README.md, because the AWS provider has no resource for it."
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name. Bucket names are globally unique, so the default embeds the account id; set it to adopt an existing bucket."
  type        = string
  default     = null
}

variable "queue_name" {
  description = "Name of the ingestion queue carrying the S3 ObjectCreated events. Leave empty to derive it from name_prefix."
  type        = string
  default     = null
}

variable "dlq_name" {
  description = "Name of the dead-letter queue. Leave empty to derive it from name_prefix."
  type        = string
  default     = null
}

variable "topic_name" {
  description = "Name of the alert topic carrying deterministic-input alerts, bootstrap failures and the DLQ alarm. Leave empty to derive it from name_prefix."
  type        = string
  default     = null
}

variable "log_group_prefix" {
  description = "Prefix of the two CloudWatch log groups: the Compose stack's container logs and the CloudWatch agent's own log."
  type        = string
  default     = "/aws-energy-de"
}

variable "docker_container_log_glob" {
  description = "Glob the CloudWatch agent tails for the Compose stack's container logs. A container's log file cannot be told apart from its path, so all containers land in one group as one stream each."
  type        = string
  default     = "/var/lib/docker/containers/*/*-json.log"
}

variable "log_retention_days" {
  description = "How long operational logs are kept. Thirty days is the window an investigation gets once the EC2 console is gone."
  type        = number
  default     = 30

  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653,
    ], var.log_retention_days)
    error_message = "log_retention_days must be one of the retention periods CloudWatch Logs accepts."
  }
}

variable "metrics_namespace" {
  description = "CloudWatch namespace for the worker metrics derived from the log group. The queue metrics are the native AWS/SQS ones and keep their own namespace."
  type        = string
  default     = "AWSEnergyDE/Worker"
}

variable "alert_subscriptions" {
  description = <<-EOT
    Notifications on the alert topic, as `{name = {protocol, endpoint}}`.

    Email subscriptions need a confirmation click after the first apply — that
    is the operator's action, not Terraform's. Leave it empty to provision the
    topic alone.
  EOT

  type = map(object({
    protocol = string
    endpoint = string
  }))

  default = {}

  validation {
    condition = alltrue([
      for subscription in values(var.alert_subscriptions) :
      contains(["application", "email", "https", "lambda", "sqs"], subscription.protocol)
    ])
    error_message = "alert_subscriptions protocols must be one of application, email, https, lambda, sqs."
  }
}

variable "additional_alarm_actions" {
  description = "Extra targets every alarm also notifies, for an on-call rotation that does not live in this repository."
  type        = list(string)
  default     = []
}

variable "alarm_ok_actions" {
  description = "Targets notified when an alarm returns to OK. Empty by default, so a resolved alarm stays silent."
  type        = list(string)
  default     = []
}

variable "dlq_alarm_period_seconds" {
  description = "Evaluation window of the DLQ alarm. A message only reaches the DLQ after five deliveries, so this is a slow signal."
  type        = number
  default     = 300
}

variable "ingestion_stalled_threshold_seconds" {
  description = "Age at which the oldest message on the ingestion queue counts as abnormal. Must exceed the six-hour visibility timeout, because a long Boundary release legitimately holds a message that long."
  type        = number
  default     = 25200
}

variable "ingestion_stalled_period_seconds" {
  description = "Evaluation window of the stalled-queue alarm."
  type        = number
  default     = 3600
}

variable "ingestion_stalled_evaluation_periods" {
  description = "Consecutive windows the queue must be stalled before the alarm fires."
  type        = number
  default     = 2
}

variable "tags" {
  description = "Extra tags merged onto every taggable resource, on top of the project tags."
  type        = map(string)
  default     = {}
}
