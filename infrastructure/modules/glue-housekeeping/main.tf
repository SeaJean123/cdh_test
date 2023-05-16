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

variable "resource_name_prefix" {
  type = string
}
variable "cdh_core_config_file_path" {
  type = string
}
variable "alerts_topic_arn" {
  type = string
}
variable "environment" {
  type = string
}
variable "kms_master_key_id" {
  type = string
}
variable "cdh_core_layer_arn" {
  type = string
}
variable "max_table_versions" {
  type    = number
  default = 5
}
variable "admin_role_name" {
  default = "CDHX-DevOps"
  type    = string
}

locals {
  name = "${var.resource_name_prefix}cdh-core-glue-housekeeping"
}

resource "aws_cloudwatch_event_rule" "trigger_schedule" {
  name                = "${local.name}-lambda-trigger"
  schedule_expression = "rate(7 days)"
  description         = "Triggers the housekeeping lambda weekly"
}

resource "aws_cloudwatch_event_target" "trigger_schedule" {
  arn  = module.cleanup_lambda.function_arn
  rule = aws_cloudwatch_event_rule.trigger_schedule.name
}

resource "aws_lambda_permission" "trigger_schedule_permission" {
  statement_id  = "AllowInvocationFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = module.cleanup_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trigger_schedule.arn
}

module "cleanup_queue" {
  source = "../technical/queue"

  alerts_topic_arn           = var.alerts_topic_arn
  kms_master_key_id          = var.kms_master_key_id
  name                       = "${local.name}-queue"
  visibility_timeout_seconds = "900"
  needs_dlq_monitoring       = false
}

resource "aws_lambda_event_source_mapping" "trigger_sqs" {
  function_name    = module.cleanup_lambda.function_arn
  event_source_arn = module.cleanup_queue.arn
}

module "cleanup_lambda" {
  source = "../technical/lambda"

  name           = "${local.name}-lambda"
  handler        = "glue_housekeeping.handler"
  source_path    = "${abspath(path.module)}/../../../src/lambdas/glue_housekeeping"
  source_is_file = false
  timeout        = 900
  layers         = [var.cdh_core_layer_arn]

  environment_vars = {
    QUEUE_URL            = module.cleanup_queue.id
    MAX_TABLE_VERSIONS   = var.max_table_versions
    RESOURCE_NAME_PREFIX = var.resource_name_prefix
  }

  environment               = var.environment
  alerts_topic_arn          = var.alerts_topic_arn
  kms_master_key_id         = var.kms_master_key_id
  cdh_core_config_file_path = var.cdh_core_config_file_path
  use_cwd_for_source_dir    = false
  admin_role_name           = var.admin_role_name
}

data "aws_iam_policy_document" "access_sqs" {
  statement {
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    resources = [
      module.cleanup_queue.arn
    ]
  }
}

resource "aws_iam_role_policy" "access_sqs" {
  name   = "${local.name}-lambda-access-sqs"
  policy = data.aws_iam_policy_document.access_sqs.json
  role   = module.cleanup_lambda.role_name
}

data "aws_iam_policy_document" "access_glue" {
  statement {
    actions = [
      "glue:GetTables",
      "glue:GetTableVersions",
      "glue:GetDatabases",
      "glue:DeleteTableVersion",
      "glue:BatchDeleteTableVersion"
    ]
    resources = [
      "*"
    ]
  }
}

resource "aws_iam_role_policy" "access_glue" {
  name   = "${local.name}-lambda-access-glue"
  policy = data.aws_iam_policy_document.access_glue.json
  role   = module.cleanup_lambda.role_name
}
