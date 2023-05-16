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

variable "environment" {
  type = string
}
variable "teams_webhook_url" {
  type = string
}
variable "resource_name_prefix" {
  type = string
}
variable "kms_master_key_id" {
  type = string
}
variable "reporting_failed_topic_arn" {
  type = string
}
variable "core_api_layer" {
  type = string
}
variable "core_api_deps_layer" {
  type = string
}
variable "cdh_utils_layer_arn" {}
variable "cdh_core_config_file_path" {
  type = string
}
variable "deps_layer_bucket" {
  type    = string
  default = "cdh-core-lambda-layers"
}
variable "admin_role_name" {
  default = "CDHX-DevOps"
  type    = string
}
variable "fetch_custom_credentials" {
  type    = string
  default = "notset"
}
variable "bucket_name" {
  type = string
}
variable "cdh_core_layer_arn" {
  type        = string
  description = "ARN of the Lambda Layer providing the 'cdh-core' package (including dependencies)."
}

locals {
  name             = "${var.resource_name_prefix}cdh-notify-teams-single"
  code_folder_path = "${path.module}/../../../src/lambdas/notify_teams/"
}

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}


module "notify-queue" {
  source = "../technical/queue"

  alerts_topic_arn  = var.reporting_failed_topic_arn
  name              = local.name
  kms_master_key_id = var.kms_master_key_id
}


module "notify-lambda" {
  source = "../technical/lambda"

  name           = local.name
  handler        = "notify_teams.handler"
  source_path    = "${path.module}/../../../src/lambdas/notify_teams"
  source_is_file = false
  timeout        = 10
  needs_dlq      = false # must be false
  layers         = [var.cdh_core_layer_arn]
  environment_vars = {
    WEBHOOK_URL           = var.teams_webhook_url,
    ENABLED               = var.resource_name_prefix == ""
    RESOURCE_NAME_PREFIX  = var.resource_name_prefix
    SILENCED_ALERTS_TABLE = aws_dynamodb_table.silenced_alerts.name
  }
  environment               = var.environment
  alerts_topic_arn          = var.reporting_failed_topic_arn
  needs_log_subscription    = false
  logs_subscription_arn     = ""
  kms_master_key_id         = var.kms_master_key_id
  publish_lambda            = true
  cdh_core_config_file_path = var.cdh_core_config_file_path
  admin_role_name           = var.admin_role_name
  use_cwd_for_source_dir    = false
}

resource "aws_lambda_event_source_mapping" "queue_trigger" {
  event_source_arn = module.notify-queue.arn
  function_name    = module.notify-lambda.function_name
  batch_size       = 1
}


resource "aws_iam_policy" "sqs_and_dynamo" {
  name   = "${module.notify-lambda.role_name}-sqs-receive-dynamo-get"
  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid":"sqsReceive",
        "Effect": "Allow",
        "Action": [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ],
        "Resource": "${module.notify-queue.arn}"
      },
      {
        "Sid":"dynamoGet",
        "Effect": "Allow",
        "Action": [
            "dynamodb:BatchGetItem",
            "dynamodb:DescribeTable",
            "dynamodb:GetItem",
            "dynamodb:Scan",
            "dynamodb:Query",
            "dynamodb:GetRecords"
        ],
        "Resource": [
            "arn:${data.aws_partition.current.partition}:dynamodb:*:${data.aws_caller_identity.current.account_id}:table/${var.resource_name_prefix}cdh-accounts",
            "${aws_dynamodb_table.silenced_alerts.arn}"
        ]
      }
    ]
  }
  EOF
}

resource "aws_iam_role_policy_attachment" "sqs_receive" {
  policy_arn = aws_iam_policy.sqs_and_dynamo.arn
  role       = module.notify-lambda.role_name
}

resource "aws_sqs_queue_policy" "allow_sns_to_sqs" {
  queue_url = module.notify-queue.id
  policy    = <<-EOF
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Action": [
            "sqs:SendMessage"
          ],
          "Resource": [
            "${module.notify-queue.arn}"
          ],
          "Effect": "Allow",
          "Principal": {
            "Service": "sns.amazonaws.com"
          },
          "Condition": {
            "ArnEquals": {
              "aws:SourceArn": "arn:${data.aws_partition.current.partition}:sns:*:${data.aws_caller_identity.current.account_id}:*"
            }
          }
        }
      ]
    }
EOF
}

resource "aws_dynamodb_table" "silenced_alerts" {
  name         = "${var.resource_name_prefix}cdh-silenced-alerts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "hash"
  attribute {
    name = "hash"
    type = "S"
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}


output "central_alerting_queue_arn" {
  value = module.notify-queue.arn
}
