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

module "deps_layer" {
  source = "../../modules/technical/lambda_layer"

  bucket_name            = var.layers_bucket_name
  context                = "coreapi"
  layer_name             = "${var.resource_name_prefix}core-api-deps-layer"
  requirements_file_path = abspath("${path.module}/../../../src/lambdas/cdh_core_api/requirements.txt")
}

resource "aws_sns_topic" "dataset_creation_and_change_notification_topic" {
  name              = "${var.resource_name_prefix}cdh-dataset-notification-topic"
  kms_master_key_id = var.kms_master_key_arn
  policy            = data.aws_iam_policy_document.access_policy.json
}

resource "aws_sns_topic" "notification_topic" {
  name                        = "${var.resource_name_prefix}cdh-api-notification-topic.fifo"
  kms_master_key_id           = var.kms_master_key_arn
  policy                      = data.aws_iam_policy_document.access_policy.json
  fifo_topic                  = true
  content_based_deduplication = true
}

data "aws_iam_policy_document" "access_policy" {
  version = "2012-10-17"
  statement {
    sid = "CoreApiPublish"
    principals {
      type        = "AWS"
      identifiers = [module.core-api-lambda.role_arn]
    }
    actions   = ["sns:Publish"]
    resources = ["*"]
  }
  statement {
    sid = "AllowSubscribe"
    principals {
      type        = "AWS"
      identifiers = local.update_subscribers
    }
    actions = [
      "SNS:Subscribe",
      "SNS:Receive",
      "SNS:ListSubscriptionsByTopic",
      "SNS:GetTopicAttributes",
    ]
    resources = ["*"]
  }
}


module "core-api-lambda" {
  source = "../technical/lambda"

  handler     = "cdh_core_api.app.entry_point"
  name        = local.core_api_lambda_name
  source_path = "${path.module}/../../../src/lambdas/cdh_core_api"
  timeout     = 180
  memory      = 2048
  layers      = [module.deps_layer.lambda_layer_arn, var.cdh_core_layer_arn]
  needs_dlq   = false
  environment_vars = {
    RESOURCE_NAME_PREFIX          = var.resource_name_prefix
    DATASET_NOTIFICATION_TOPIC    = aws_sns_topic.dataset_creation_and_change_notification_topic.arn
    NOTIFICATION_TOPIC            = aws_sns_topic.notification_topic.arn
    AWS_XRAY_TRACING_NAME         = "${var.resource_name_prefix}${local.x_ray_tracing_name_suffix}"
    AUTHORIZATION_API_URL         = var.authorization_api_url
    USERS_API_URL                 = var.users_api_url
    AUTHORIZATION_API_COOKIE_NAME = var.authorization_api_cookie_name
    PYTHONFAULTHANDLER            = "1"
    ENCRYPTION_KEY_NAME           = aws_ssm_parameter.encryption_key.name
    RESULT_PAGE_SIZE              = local.result_page_size
  }
  environment               = var.environment
  alerts_topic_arn          = var.alerts_topic_arn
  role_name                 = local.core_api_lambda_name
  logs_subscription_arn     = var.logs_subscription_arn
  needs_log_subscription    = false # TODO: set to true
  kms_master_key_id         = var.kms_master_key_arn
  log_retention             = var.resource_name_prefix == "" ? 30 : 14
  xray_enabled              = true
  publish_lambda            = true
  cdh_core_config_file_path = var.cdh_core_config_file_path
  admin_role_name           = var.admin_role_name
}

resource "aws_lambda_alias" "core_api_lambda_alias" {
  name             = local.core_api_lambda_alias_name
  function_name    = module.core-api-lambda.function_name
  function_version = module.core-api-lambda.version
}


resource "aws_xray_sampling_rule" "lambda" {
  count          = var.resource_name_prefix == "" ? 1 : 0
  rule_name      = module.core-api-lambda.function_name
  priority       = 1000
  version        = 1
  reservoir_size = 1
  fixed_rate     = 1.0
  url_path       = "*"
  host           = "*"
  http_method    = "*"
  service_type   = "*"
  service_name   = "*${local.x_ray_tracing_name_suffix}"
  resource_arn   = "*"
}

resource "aws_cloudwatch_log_metric_filter" "request_latency_filter" {
  log_group_name = "/aws/lambda/${module.core-api-lambda.function_name}"
  name           = "Request Latency Filter"
  pattern        = "{ $.elapsed_total_ms > 0 }"
  metric_transformation {
    name      = "${module.core-api-lambda.function_name}-request-latency"
    namespace = "CDH/Requests"
    dimensions = {
      Route  = "$.route"
      Method = "$.http_method"
    }
    unit  = "Milliseconds"
    value = "$.elapsed_total_ms"
  }
}

resource "aws_cloudwatch_log_metric_filter" "lambda_latency_filter" {
  log_group_name = "/aws/lambda/${module.core-api-lambda.function_name}"
  name           = "Lambda Latency Filter"
  pattern        = "{ $.elapsed_lambda_ms > 0 }"
  metric_transformation {
    name      = "${module.core-api-lambda.function_name}-lambda-latency"
    namespace = "CDH/Requests"
    dimensions = {
      Route  = "$.route"
      Method = "$.http_method"
    }
    unit  = "Milliseconds"
    value = "$.elapsed_lambda_ms"
  }
}

resource "aws_cloudwatch_log_metric_filter" "error_5xx_filter" {
  log_group_name = "/aws/lambda/${module.core-api-lambda.function_name}"
  name           = "Lambda 5XX Error Filter"
  pattern        = "{ $.status_code > 499 }"
  metric_transformation {
    name      = "${module.core-api-lambda.function_name}-lambda-5XX-errors"
    namespace = "CDH/Requests"
    dimensions = {
      Route  = "$.route"
      Method = "$.http_method"
    }
    value = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "error_4xx_filter" {
  log_group_name = "/aws/lambda/${module.core-api-lambda.function_name}"
  name           = "Lambda 4XX Error Filter"
  pattern        = "{ $.status_code > 399 && $.status_code < 500}"
  metric_transformation {
    name      = "${module.core-api-lambda.function_name}-lambda-4XX-errors"
    namespace = "CDH/Requests"
    dimensions = {
      Route  = "$.route"
      Method = "$.http_method"
    }
    value = "1"
  }
}

resource "aws_cloudwatch_log_group" "audit-log-group" {
  name              = "${var.resource_name_prefix}cdh-audit-log"
  retention_in_days = var.resource_name_prefix == "" ? 0 : 30
  kms_key_id        = var.kms_master_key_arn
}

resource "aws_cloudwatch_log_metric_filter" "audit-log-metric" {
  name           = "${var.resource_name_prefix}cdh-audit-log-count"
  pattern        = ""
  log_group_name = aws_cloudwatch_log_group.audit-log-group.name

  metric_transformation {
    name      = "${var.resource_name_prefix}EventCount"
    namespace = "audit-log"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "audit-log-has-no-messages" {
  alarm_name        = "${var.resource_name_prefix}audit-log-has-no-messages"
  alarm_description = "${var.resource_name_prefix}cdh-audit-log does not contain messages"

  comparison_operator = "LessThanThreshold"
  threshold           = 1
  evaluation_periods  = 1

  metric_name        = aws_cloudwatch_log_metric_filter.audit-log-metric.metric_transformation[0].name
  namespace          = aws_cloudwatch_log_metric_filter.audit-log-metric.metric_transformation[0].namespace
  treat_missing_data = "breaching"
  period             = "86400" # one day is maximum
  statistic          = "Average"

  alarm_actions = [var.alerts_topic_arn]
}


output "lambda_function_arn" {
  value = aws_lambda_alias.core_api_lambda_alias.arn
}

output "role_arn" {
  value = module.core-api-lambda.role_arn
}

output "dataset_notification_topic_arn" {
  value = aws_sns_topic.dataset_creation_and_change_notification_topic.arn
}

output "notification_topic_arn" {
  value = aws_sns_topic.notification_topic.arn
}
