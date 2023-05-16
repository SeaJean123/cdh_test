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

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.63"
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "resource-account-bucket-count" {
  log_group_name = "/aws/lambda/${var.core_api_function_name}"
  name           = "Buckets in Account"
  pattern        = "{ $.total_account_buckets > 0}"
  metric_transformation {
    name      = "${var.core_api_function_name}-bucket-count"
    namespace = "CDH/Monitoring"
    dimensions = {
      Account = "$.account_id"
    }
    value = "$.total_account_buckets"
    unit  = "Count"
  }
}

resource "aws_cloudwatch_metric_alarm" "resource-account-bucket-count-threshold-reached-alarm" {
  # Note: On fast filling accounts, there might only be one alert!
  # Reason: The alarm only goes back from status "In Alarm" to "Ok" iff no new bucket was created in the period.
  # The alert is only generated when the alarm goes from "Ok" to "In Alarm", not if it stays "In Alarm".

  for_each = toset(var.resource_account_ids)

  alarm_name = "${var.core_api_function_name}-account-${each.value}-bucket-count-alarm"

  comparison_operator = "GreaterThanThreshold"
  threshold           = var.max_buckets_in_account * var.bucket_count_alarm_threshold / 100
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  metric_name = "${var.core_api_function_name}-bucket-count"
  namespace   = "CDH/Monitoring"
  period      = 60 * 60 * 12
  statistic   = "Maximum"

  dimensions = {
    Account = each.value
  }

  alarm_actions = [var.alerts_topic_arn]
}
