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
variable "environment" {
  type = string
}
variable "resource_name_prefix" {
  type = string
}
variable "alerts_topic_arn" {
  type = string
}
variable "kms_master_key_id" {
  type = string
}
variable "api_kms_master_key_id" {
  type = string
}
variable "cdh_core_config_file_path" {
  type = string
}
variable "bucket_name" {
  type = string
}
variable "fetch_custom_credentials" {
  type    = string
  default = "notset"
}
variable "cdh_core_layer_arn" {
  type        = string
  description = "ARN of the Lambda Layer providing the 'cdh-core' package (including dependencies)."
}
variable "admin_role_name" {
  default = "CDHX-DevOps"
  type    = string
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  name             = "${var.resource_name_prefix}cdh-logs-subscription"
  code_folder_path = "${abspath(path.module)}/../../../src/lambdas/logs_subscription/"
  github_ref_regex = "git\\+ssh://git@github.com/bmw-cdh/cdh-core.git@[a-f0-9]{40}"
}

module "layer" {
  source = "../technical/lambda_layer"

  bucket_name              = var.bucket_name
  context                  = "logs-subscription"
  layer_name               = "${var.resource_name_prefix}logs-subscription-layer"
  requirements_file_path   = "${local.code_folder_path}requirements.txt"
  fetch_custom_credentials = var.fetch_custom_credentials
}

module "logs-subscription-lambda" {
  source = "../technical/lambda"

  name           = local.name
  source_path    = local.code_folder_path
  handler        = "logs_subscription_handler.handler"
  source_is_file = false
  timeout        = 20
  needs_dlq      = true
  layers         = [module.layer.lambda_layer_arn, var.cdh_core_layer_arn]
  environment_vars = {
    RESOURCE_NAME_PREFIX = var.resource_name_prefix
    ALERTS_TOPIC_ARN     = var.alerts_topic_arn
  }
  environment               = var.environment
  alerts_topic_arn          = var.alerts_topic_arn
  kms_master_key_id         = var.kms_master_key_id
  publish_lambda            = true
  cdh_core_config_file_path = var.cdh_core_config_file_path
  use_cwd_for_source_dir    = false
  admin_role_name           = var.admin_role_name
}

resource "aws_lambda_permission" "cloudwatch_logs_permission" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.logs-subscription-lambda.function_name
  principal     = "logs.amazonaws.com"
  source_arn    = "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"
  # source_account should be set to prevent falsely reported errors in aws console
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_iam_role_policy" "allow_publish" {
  name   = "${local.name}-AllowDynamoAndSNS"
  role   = module.logs-subscription-lambda.role_name
  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "SnsPublish",
        "Effect": "Allow",
        "Action": [
          "SNS:Publish"
        ],
        "Resource": "${var.alerts_topic_arn}"
      },
      {
        "Sid":  "EventsDBAccess",
        "Effect": "Allow",
        "Action": [
          "dynamodb:List*",
          "dynamodb:Describe*",
          "dynamodb:Get*",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:ConditionCheckItem",
          "dynamodb:PutItem",
          "dynamodb:DeleteItem",
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:UpdateItem",
          "dynamodb:GetRecords"
        ],
        "Resource": "${aws_dynamodb_table.events-history.arn}"
      },
      {
            "Sid": "KMSDecryptOfApiKey",
            "Effect": "Allow",
            "Action": [
                "kms:GenerateDataKey",
                "kms:Decrypt"
            ],
            "Resource": "${var.api_kms_master_key_id}"
      }
    ]
  }
  EOF
}

resource "aws_dynamodb_table" "events-history" {
  name         = "${var.resource_name_prefix}cdh-events-history"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "eventHash"
  attribute {
    name = "eventHash"
    type = "S"
  }
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  point_in_time_recovery {
    enabled = var.resource_name_prefix == "" ? true : false
  }
}

output "logs_subscription_function_arn" {
  value = module.logs-subscription-lambda.function_arn
}
