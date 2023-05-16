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
# Variables
#####################################################################################
variable "environment" {
  type = string
}
variable "resource_name_prefix" {
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
variable "cdh_core_config_file_path" {
  type = string
}
variable "security_account_id" {
  type = string
}
variable "s3_attribute_extractor_name" {
  type    = string
  default = "s3-attribute-extractor"
}
variable "cdh_core_layer_arn" {
  type        = string
  description = "ARN of the Lambda Layer providing the 'cdh-core' package (including dependencies)."
}
variable "reserved_concurrency" {
  type    = number
  default = -1
}
variable "admin_role_name" {
  default = "CDHX-DevOps"
  type    = string
}

#####################################################################################
# Data
#####################################################################################
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

data "external" "hub" {
  program = ["python", "${path.module}/../../bin/get_hub.py"]
  query = {
    environment : var.environment
    account_id : data.aws_caller_identity.current.account_id
  }
}


#####################################################################################
# locals
#####################################################################################
locals {
  trigger_topic_name = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-s3-trigger-${data.aws_region.current.name}"
}

#####################################################################################
# SQS Queues/SNS Topics
#####################################################################################
module "queue" {
  source                     = "../technical/queue"
  name                       = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-event-forwarder-${data.aws_region.current.name}"
  visibility_timeout_seconds = 60
  alerts_topic_arn           = var.alerts_topic_arn
  message_age_to_alert       = 15 * 60
  kms_master_key_id          = var.kms_master_key_id
  policy                     = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": {
      "Effect": "Allow",
      "Principal": {"AWS": "*"},
      "Action": "SQS:SendMessage",
      "Resource": "*",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:${data.aws_partition.current.partition}:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${local.trigger_topic_name}"
        }
      }
    }
  }
  EOF
}

resource "aws_sns_topic" "sns_topic" {
  name              = local.trigger_topic_name
  kms_master_key_id = var.kms_master_key_id
  policy            = <<-EOF
  {
    "Version": "2012-10-17",
    "Id": "SNSResourcePolicy",
    "Statement": [
      {
        "Sid": "S3AllowPublish",
        "Effect": "Allow",
        "Principal": {
          "Service": "s3.amazonaws.com"
        },
        "Action": [
          "SNS:Publish"
        ],
        "Resource": "*",
        "Condition":{
          "StringEquals":{
            "AWS:SourceAccount":"${data.aws_caller_identity.current.account_id}"
          }
        }
      },
      {
        "Sid": "SQSallowSubscribe",
        "Effect": "Allow",
        "Principal": {
          "AWS": "*"
        },
        "Action": [
          "SNS:Subscribe",
          "SNS:Receive",
          "SNS:ListSubscriptionsByTopic",
          "SNS:GetTopicAttributes"
        ],
        "Resource": "*",
        "Condition": {
          "ArnEquals": {
            "aws:SourceArn": "${module.queue.arn}"
          }
        }
      }
    ]
  }
  EOF
}

resource "aws_sns_topic_subscription" "sqs_sns_subscription" {
  topic_arn = aws_sns_topic.sns_topic.arn
  protocol  = "sqs"
  endpoint  = module.queue.arn
}

#####################################################################################
# IAM for lambda
#####################################################################################
resource "aws_iam_policy" "cdh-core-s3-permissions" {
  name        = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-s3-permissions-${data.aws_region.current.name}"
  path        = "/"
  description = "Grants s3:GetBucketTagging permission to the lambda."
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGetBucketTagging",
            "Action": [
                "s3:GetBucketTagging"
            ],
            "Effect": "Allow",
            "Resource": "arn:${data.aws_partition.current.partition}:s3:::*"
        }
    ]
  }
  EOF
}

resource "aws_iam_policy" "cdh-core-sqs-consume" {
  name        = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-sqs-consume-${data.aws_region.current.name}"
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

resource "aws_iam_policy" "cdh-core-sns-send" {
  name        = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-sns-send-${data.aws_region.current.name}"
  path        = "/"
  description = "Grants the permission to send messages through sns."
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowSend",
            "Effect": "Allow",
            "Action": [
              "sns:SendMessage",
              "sns:Publish"
            ],
            "Resource": "*"
        }
    ]
  }
  EOF
}

resource "aws_iam_role_policy_attachment" "attach-s3-policy" {
  role       = module.s3-attribute-extractor-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-s3-permissions.arn
}

resource "aws_iam_role_policy_attachment" "attach-basic-lambda-policy-1" {
  role       = module.s3-attribute-extractor-lambda.role_name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "attach-sqs-policy" {
  role       = module.s3-attribute-extractor-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-sqs-consume.arn
}

resource "aws_iam_role_policy_attachment" "attach-sns-policy" {
  role       = module.s3-attribute-extractor-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-sns-send.arn
}

resource "aws_iam_role_policy" "kms-shared-key" {
  role   = module.s3-attribute-extractor-lambda.role_name
  name   = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-shared-kms"
  policy = data.aws_iam_policy_document.kms-shared-key.json
}

data "aws_iam_policy_document" "kms-shared-key" {
  statement {
    sid       = "AllowSharedKeys"
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = ["arn:${data.aws_partition.current.partition}:kms:*:${var.security_account_id}:key/*"]
    condition {
      test     = "ForAnyValue:StringLike"
      values   = ["alias/${var.resource_name_prefix}cdh-${var.environment}-${data.external.hub.result.hub}-${data.aws_caller_identity.current.account_id}"]
      variable = "kms:ResourceAliases"
    }
  }
}

#####################################################################################
# Lambda Functions
#####################################################################################


module "s3-attribute-extractor-lambda" {
  source = "../technical/lambda"

  handler              = "s3_attribute_extractor.lambda_handler"
  name                 = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-lambda-${data.aws_region.current.name}"
  role_name            = "${var.resource_name_prefix}${var.s3_attribute_extractor_name}-lambda-${data.aws_region.current.name}"
  source_path          = "${abspath(path.module)}/../../../src/lambdas/s3_attribute_extractor/"
  source_is_file       = false
  timeout              = 60
  memory               = 256
  layers               = [var.cdh_core_layer_arn]
  reserved_concurrency = var.reserved_concurrency
  environment_vars = {
    ACCOUNT_ID = data.aws_caller_identity.current.account_id
    SQS_URL    = module.queue.id
  }
  environment                = var.environment
  alerts_topic_arn           = var.alerts_topic_arn
  needs_dlq                  = false
  needs_log_subscription     = false
  logs_subscription_arn      = var.logs_subscription_arn
  alert_on_invocation_errors = false
  use_cwd_for_source_dir     = false
  kms_master_key_id          = var.kms_master_key_id
  publish_lambda             = true
  cdh_core_config_file_path  = var.cdh_core_config_file_path
  admin_role_name            = var.admin_role_name
}

#####################################################################################
# Triggers for Lambdas
#####################################################################################
resource "aws_lambda_event_source_mapping" "sqs-event-mapping" {
  event_source_arn = module.queue.arn
  function_name    = module.s3-attribute-extractor-lambda.function_name
  batch_size       = 10
}
