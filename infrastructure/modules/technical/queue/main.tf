# Copyright (C) 2022, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

variable "name" {
  type = string
}
variable "max_receive_count" {
  default = 5
  type    = number
}
variable "visibility_timeout_seconds" {
  default = 60
  type    = number
}
variable "policy" {
  default = null
  type    = string
}
variable "alerts_topic_arn" {
  type = string
}
variable "message_age_to_alert" {
  type    = number
  default = 60 * 60 * 24 * 3
}
variable "kms_master_key_id" {
  type = string
}
variable "needs_dlq_monitoring" {
  default = true
  type    = bool
}

resource "aws_sqs_queue" "q" {
  name                       = var.name
  message_retention_seconds  = 60 * 60 * 24 * 14
  visibility_timeout_seconds = var.visibility_timeout_seconds
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
  policy            = var.policy
  kms_master_key_id = var.kms_master_key_id
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = 60 * 60 * 24 * 14
  kms_master_key_id         = var.kms_master_key_id
}

resource "aws_cloudwatch_metric_alarm" "queue-contains-old-messages" {
  alarm_name        = "${var.name}-queue-contains-old-messages"
  alarm_description = "${var.name} contains old messages"

  comparison_operator = "GreaterThanThreshold"
  threshold           = var.message_age_to_alert
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  metric_name = "ApproximateAgeOfOldestMessage"
  namespace   = "AWS/SQS"
  period      = "300"
  statistic   = "Maximum"

  dimensions = {
    QueueName = aws_sqs_queue.q.name
  }

  alarm_actions = [var.alerts_topic_arn]
}

module "dlq-monitoring" {
  source              = "../dlq-monitoring"
  enabled             = var.needs_dlq_monitoring
  dlq-name            = aws_sqs_queue.dlq.name
  sns_alarm_topic_arn = var.alerts_topic_arn
}

output "name" {
  value = aws_sqs_queue.q.name
}

output "arn" {
  value = aws_sqs_queue.q.arn
}

output "id" {
  value = aws_sqs_queue.q.id
}
