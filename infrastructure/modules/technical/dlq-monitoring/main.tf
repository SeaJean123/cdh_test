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

variable "enabled" {
  default = true
  type    = bool
}
variable "dlq-name" {
  type = string
}

variable "sns_alarm_topic_arn" {
  type = string
}

resource "aws_cloudwatch_metric_alarm" "dlq-contains-old-messages" {
  count             = var.enabled ? 1 : 0
  alarm_name        = "${var.dlq-name}-queue-contains-old-messages"
  alarm_description = "${var.dlq-name} contains old messages"

  comparison_operator = "GreaterThanThreshold"
  threshold           = 60 * 60 * 24 * 3
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  metric_name = "ApproximateAgeOfOldestMessage"
  namespace   = "AWS/SQS"
  period      = 300
  statistic   = "Maximum"

  dimensions = {
    QueueName = var.dlq-name
  }

  alarm_actions = [var.sns_alarm_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "dlq-got-message" {
  count             = var.enabled ? 1 : 0
  alarm_name        = "${var.dlq-name}-got-message"
  alarm_description = "The dlq ${var.dlq-name} received messages"

  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  metric_name = "ApproximateNumberOfMessagesVisible"
  namespace   = "AWS/SQS"
  period      = 300
  statistic   = "Maximum"

  dimensions = {
    QueueName = var.dlq-name
  }

  alarm_actions = [var.sns_alarm_topic_arn]
}
