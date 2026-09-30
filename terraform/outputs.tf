# The values the operator hands to the host once the infrastructure exists
# (issue #10). They are printed rather than written anywhere: the deployment has
# no secrets to store and no remote state to read back, and the Compose stack is
# started with them on the command line.

output "aws_region" {
  description = "Region the deployment lives in. Compose's AWS_DEFAULT_REGION, without which the worker container cannot build an AWS client of its own."
  value       = var.aws_region
}

output "bucket_name" {
  description = "S3 bucket the worker reads. Compose's S3_BUCKET."
  value       = aws_s3_bucket.data.id
}

output "ingestion_queue_url" {
  description = "URL of the queue carrying the S3 ObjectCreated events. Compose's SQS_QUEUE_URL, and the queue the worker receives from."
  value       = aws_sqs_queue.ingestion.url
}

output "dead_letter_queue_url" {
  description = "URL of the dead-letter queue, for an operator inspecting a terminal failure. The worker is not given access to it."
  value       = aws_sqs_queue.dlq.url
}

output "alert_topic_arn" {
  description = "ARN of the alert topic. Compose's SNS_TOPIC_ARN, and the target both alarms notify."
  value       = aws_sns_topic.alerts.arn
}

output "log_group_names" {
  description = "The CloudWatch log groups, keyed by what they hold: the Compose stack's container logs, and the CloudWatch agent's own log."
  value       = local.log_groups
}

output "instance_profile_name" {
  description = "The instance profile to attach to the EC2 host. Attaching it is the one deployment step Terraform cannot take, because the AWS provider has no resource for it."
  value       = aws_iam_instance_profile.worker.name
}

output "cloudwatch_agent_config" {
  description = "Rendered CloudWatch agent configuration. Write it to the agent's config path on the host, then restart the agent."
  value = templatefile("${path.module}/templates/cloudwatch-agent.json.tftpl", {
    docker_container_log_glob = var.docker_container_log_glob
    worker_log_group          = local.worker_log_group
    agent_log_group           = local.agent_log_group
  })
}
