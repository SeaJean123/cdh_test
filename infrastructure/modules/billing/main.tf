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

#####################################################################################
# provider
#####################################################################################
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.63"
    }
  }
}

#####################################################################################
# variables
#####################################################################################
variable "assumable_role_name" {
  type = string
}
variable "assumable_role_path" {
  type = string
}
variable "bucket_name" {
  type = string
}
variable "resource_name_prefix" {
  type = string
}
variable "core_api_url" {
  type = string
}
variable "core_api_api_gateway_id" {
  type = string
}
variable "alerts_topic_arn" {
  type = string
}
variable "logs_subscription_arn" {
  type = string
}
variable "kms_master_key_id" {
  type = string
}
variable "environment" {
  type = string
}
variable "additional_layer_arns" {
  type    = list(string)
  default = []
}
variable "cdh_core_config_file_path" {
  type = string
}
variable "code_folder_path" {
  type    = string
  default = "notset"
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

variable "timeout" {
  type        = number
  description = "The maximum allowed duration for your Lambda's execution."
  default     = 300
}

variable "schedule_expression" {
  type        = string
  description = "Schedule expression for the Cloudwatch event triggering the lambda. e.g. cron(0 3 ? * * *) for daily at 3am UTC"
  default     = "cron(0 3 ? * * *)"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  local_code_folder_path = "${path.module}/../../../src/lambdas/cdh_billing/cdh_billing/"
  set_code_folder_path   = var.code_folder_path != "notset" ? var.code_folder_path : local.local_code_folder_path
}

#####################################################################################
# Non connected Resources
#####################################################################################
moved {
  from = aws_s3_bucket.billing-bucket
  to   = module.billing-bucket.aws_s3_bucket.bucket
}

moved {
  from = aws_s3_bucket_server_side_encryption_configuration.billing-bucket
  to   = module.billing-bucket.aws_s3_bucket_server_side_encryption_configuration.bucket
}

moved {
  from = aws_s3_bucket_public_access_block.billing-bucket
  to   = module.billing-bucket.aws_s3_bucket_public_access_block.bucket
}

moved {
  from = aws_s3_bucket_lifecycle_configuration.billing-bucket
  to   = module.billing-bucket.aws_s3_bucket_lifecycle_configuration.bucket[0]
}

moved {
  from = aws_s3_bucket_policy.allow-ssl-requests-only
  to   = module.billing-bucket.aws_s3_bucket_policy.bucket-policies
}

module "billing-bucket" {
  source                   = "../technical/s3"
  kms_key_id               = var.kms_master_key_id
  name                     = var.bucket_name
  default_lifecycle_config = true
  force_destroy            = var.resource_name_prefix == "" ? false : true
}

module "queue" {
  source = "../technical/queue"

  name                       = "${var.resource_name_prefix}cdh-core-billing-lambda-accounts"
  alerts_topic_arn           = var.alerts_topic_arn
  message_age_to_alert       = 60 * 60 * 2
  kms_master_key_id          = var.kms_master_key_id
  max_receive_count          = 3 # Invocations may fail due to throttling of the receiving Lambda function
  visibility_timeout_seconds = 300
}

#####################################################################################
# IAM for cost export lambda
#####################################################################################
resource "aws_iam_policy" "cdh-core-assume-billing-assumable-role" {
  name        = "${var.resource_name_prefix}cdh-core-assume-billing-assumable-role"
  path        = "/"
  description = "Allows assuming a role named ${var.assumable_role_name} in any account"
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::*:role${var.assumable_role_path}${var.assumable_role_name}"
        }
    ]
  }
  EOF
}

resource "aws_iam_policy" "cdh-core-billing-s3" {
  name        = "${var.resource_name_prefix}cdh-core-billing-s3"
  path        = "/"
  description = "Allows reading and writing for bucket ${module.billing-bucket.name}"
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ConsoleAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetAccountPublicAccessBlock",
                "s3:GetBucketAcl",
                "s3:GetBucketLocation",
                "s3:GetBucketPolicyStatus",
                "s3:GetBucketPublicAccessBlock",
                "s3:ListAllMyBuckets"
            ],
            "Resource": "*"
        },
        {
            "Sid": "ListObjectsInBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetEncryptionConfiguration"
            ],
            "Resource": ["arn:aws:s3:::${module.billing-bucket.name}"]
        },
        {
            "Sid": "AllObjectActions",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": ["arn:aws:s3:::${module.billing-bucket.name}/*"]
        }
    ]
  }
  EOF
}

resource "aws_iam_policy" "cdh-core-billing-sqs-consume" {
  name        = "${var.resource_name_prefix}cdh-core-billing-sqs-consume"
  path        = "/"
  description = "Allows consuming message from sqs queue ${module.queue.name}"
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReceiveAndDeleteMessages",
            "Effect": "Allow",
            "Action": [
                "sqs:DeleteMessage",
                "sqs:ReceiveMessage",
                "sqs:GetQueueAttributes"
            ],
            "Resource": "${module.queue.arn}"
        },
        {
            "Sid": "AllowListQueues",
            "Effect": "Allow",
            "Action": "sqs:ListQueues",
            "Resource": "*"
        }
    ]
  }
  EOF
}

resource "aws_iam_policy" "cdh-core-billing-grant-access-to-cn-credentials-ssm" {
  name   = "${var.resource_name_prefix}cdh-core-billing-grant-access-to-cn-credentials-ssm"
  path   = "/"
  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GetSsm",
            "Effect": "Allow",
            "Action": [
              "ssm:GetParameter"
            ],
            "Resource": [
              "arn:aws:ssm:*:*:parameter/*cn_*_access_key",
              "arn:aws:ssm:*:*:parameter/*cn_*_secret_key"
            ]
        }
    ]
  }
  EOF
}

resource "aws_iam_role_policy_attachment" "cdh-core-api-cn-credential-access" {
  role       = module.billing-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-billing-grant-access-to-cn-credentials-ssm.arn
}

resource "aws_iam_role_policy_attachment" "attach-s3-policy" {
  role       = module.billing-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-billing-s3.arn
}

resource "aws_iam_role_policy_attachment" "attach-assume-policy" {
  role       = module.billing-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-assume-billing-assumable-role.arn
}

resource "aws_iam_role_policy_attachment" "attach-basic-lambda-policy-1" {
  role       = module.billing-lambda.role_name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "attach-sqs-policy" {
  role       = module.billing-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-billing-sqs-consume.arn
}

resource "aws_iam_policy" "cdh-core-billing-sqs-send" {
  name        = "${var.resource_name_prefix}cdh-core-billing-sqs-send"
  path        = "/"
  description = "Allows sending messages to sqs queue ${module.queue.name}"
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowSend",
            "Effect": "Allow",
            "Action": "sqs:SendMessage",
            "Resource": "${module.queue.arn}"
        }
    ]
  }
  EOF
}

resource "aws_iam_role_policy_attachment" "billing-invoke-core-api-policy-attachment" {
  role       = module.billing-lambda.role_name
  policy_arn = aws_iam_policy.billing-invoke-core-api-policy.arn
}

resource "aws_iam_policy" "billing-invoke-core-api-policy" {
  name   = "${var.resource_name_prefix}cdh-core-billing-invoke-core-api-policy"
  path   = "/"
  policy = data.aws_iam_policy_document.billing-invoke-core-api-policy-document.json
}

data "aws_iam_policy_document" "billing-invoke-core-api-policy-document" {
  statement {
    effect  = "Allow"
    actions = ["execute-api:Invoke"]
    sid     = "CoreApiInvoke"

    resources = [
      "arn:aws:execute-api:*:${data.aws_caller_identity.current.account_id}:${var.core_api_api_gateway_id}/default/GET/accounts/*",
      "arn:aws:execute-api:*:${data.aws_caller_identity.current.account_id}:${var.core_api_api_gateway_id}/default/PUT/accounts/*/billing",
    ]
  }
}

#####################################################################################
# IAM for fan out Lambda
#####################################################################################

resource "aws_iam_role_policy_attachment" "attach-basic-lambda-policy" {
  role       = module.fan-out-lambda.role_name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "attach-sqs-send-policy" {
  role       = module.fan-out-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-billing-sqs-send.arn
}

resource "aws_iam_role_policy_attachment" "fan-out-invoke-core-api-policy-attachment" {
  role       = module.fan-out-lambda.role_name
  policy_arn = aws_iam_policy.fan-out-invoke-core-api-policy.arn
}

resource "aws_iam_policy" "fan-out-invoke-core-api-policy" {
  name   = "${var.resource_name_prefix}cdh-core-fan-out-invoke-core-api-policy"
  path   = "/"
  policy = data.aws_iam_policy_document.fan-out-invoke-core-api-policy-document.json
}

data "aws_iam_policy_document" "fan-out-invoke-core-api-policy-document" {
  statement {
    effect  = "Allow"
    actions = ["execute-api:Invoke"]
    sid     = "CoreApiInvoke"

    resources = [
      "arn:aws:execute-api:*:${data.aws_caller_identity.current.account_id}:${var.core_api_api_gateway_id}/default/GET/accounts"
    ]
  }
}

#####################################################################################
# Lambda Functions
#####################################################################################

module "layer" {
  source = "../technical/lambda_layer"

  bucket_name              = "cdh-core-lambda-layers-${data.aws_region.current.name}"
  context                  = "billing"
  layer_name               = "${var.resource_name_prefix}billing-layer"
  requirements_file_path   = abspath("${local.set_code_folder_path}/requirements.txt")
  fetch_custom_credentials = var.fetch_custom_credentials
}


module "billing-lambda" {
  source = "../technical/lambda"

  handler              = "export_cost.lambda_handler"
  name                 = "${var.resource_name_prefix}cdh-core-billing-export-cost"
  source_path          = local.set_code_folder_path
  source_is_file       = false
  timeout              = var.timeout
  needs_dlq            = false
  memory               = 512
  layers               = concat([module.layer.lambda_layer_arn, var.cdh_core_layer_arn], var.additional_layer_arns)
  reserved_concurrency = var.resource_name_prefix == "" ? 15 : -1
  environment_vars = {
    BUCKET               = module.billing-bucket.name
    RESOURCE_NAME_PREFIX = var.resource_name_prefix
    CORE_API_URL         = var.core_api_url
  }
  environment                = var.environment
  alerts_topic_arn           = var.alerts_topic_arn
  needs_log_subscription     = false # the queue feeding this lambda has a DLQ which we alert on
  logs_subscription_arn      = var.logs_subscription_arn
  role_name                  = "${var.resource_name_prefix}cdh-core-billing-export-cost"
  kms_master_key_id          = var.kms_master_key_id
  alert_on_invocation_errors = false
  cdh_core_config_file_path  = var.cdh_core_config_file_path
  admin_role_name            = var.admin_role_name
}

module "fan-out-lambda" {
  source = "../technical/lambda"

  handler              = "fan_out.lambda_handler"
  name                 = "${var.resource_name_prefix}cdh-core-billing-fan-out"
  source_path          = local.set_code_folder_path
  source_is_file       = false
  timeout              = 30
  needs_dlq            = false
  memory               = 256
  layers               = concat([module.layer.lambda_layer_arn, var.cdh_core_layer_arn], var.additional_layer_arns)
  reserved_concurrency = var.resource_name_prefix == "" ? 5 : -1
  environment_vars = {
    QUEUE_URL            = module.queue.id
    RESOURCE_NAME_PREFIX = var.resource_name_prefix
    CORE_API_URL         = var.core_api_url
  }
  environment               = var.environment
  alerts_topic_arn          = var.alerts_topic_arn
  needs_log_subscription    = true
  logs_subscription_arn     = var.logs_subscription_arn
  role_name                 = "${var.resource_name_prefix}cdh-core-billing-fan-out"
  kms_master_key_id         = var.kms_master_key_id
  cdh_core_config_file_path = var.cdh_core_config_file_path
  admin_role_name           = var.admin_role_name
}

#####################################################################################
# Triggers for Lambdas
#####################################################################################
resource "aws_lambda_permission" "cloudwatch_trigger" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.fan-out-lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda-trigger.arn
}

resource "aws_cloudwatch_event_rule" "lambda-trigger" {
  name                = "${var.resource_name_prefix}cdh-core-billing-trigger-lambda-daily"
  description         = "Schedule trigger for lambda execution with schedule ${var.schedule_expression}"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "lambda-target" {
  rule = aws_cloudwatch_event_rule.lambda-trigger.name
  arn  = module.fan-out-lambda.function_arn
}

resource "aws_lambda_event_source_mapping" "sqs-event-mapping" {
  event_source_arn = module.queue.arn
  function_name    = module.billing-lambda.function_name
  batch_size       = 1
}
