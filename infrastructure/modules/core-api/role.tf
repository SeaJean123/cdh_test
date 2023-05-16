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

data "aws_iam_policy_document" "cdh-core-api-dynamodb" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:ConditionCheckItem",
      "dynamodb:PutItem",
      "dynamodb:DescribeTable",
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:Scan",
      "dynamodb:Query",
      "dynamodb:UpdateItem",
      "dynamodb:GetRecords"
    ]
    resources = [
      "arn:aws:dynamodb:*:${data.aws_caller_identity.current.account_id}:table/${var.resource_name_prefix}*/stream/",
      "arn:aws:dynamodb:*:${data.aws_caller_identity.current.account_id}:table/${var.resource_name_prefix}*/index/",
      "arn:aws:dynamodb:*:${data.aws_caller_identity.current.account_id}:table/${var.resource_name_prefix}*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:ListTables",
      "dynamodb:DescribeLimits"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "cdh-core-api-dynamodb" {
  name        = "${var.resource_name_prefix}cdh-core-api-dynamodb"
  path        = "/"
  description = "Policy grants read and write access to the dynamo DB table or tables of the data catalog."
  policy      = data.aws_iam_policy_document.cdh-core-api-dynamodb.json
}

resource "aws_iam_role_policy_attachment" "cdh-core-api-dynamodb" {
  role       = module.core-api-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-api-dynamodb.arn
}

locals {
  kms_resources = flatten(
    [for account in keys(data.external.security-accounts.result) :
      [
        "arn:${data.aws_partition.current.partition}:kms:*:${account}:key/*",
        "arn:${data.aws_partition.current.partition}:kms:*:${account}:alias/*"
      ]
    ]
  )
}

data "aws_iam_policy_document" "cdh-core-api-kms" {
  statement {
    effect = "Allow"
    actions = [
      "kms:EnableKeyRotation",
      "kms:EnableKey",
      "kms:UntagResource",
      "kms:UpdateKeyDescription",
      "kms:PutKeyPolicy",
      "kms:GetKeyPolicy",
      "kms:DisableKey",
      "kms:UpdateAlias",
      "kms:TagResource",
      "kms:GetKeyRotationStatus",
      "kms:ScheduleKeyDeletion",
      "kms:CreateAlias",
      "kms:DescribeKey",
      "kms:DeleteAlias",
      "kms:Decrypt",
      "kms:GenerateDataKey"
    ]
    resources = local.kms_resources
  }
  statement {
    effect = "Allow"
    actions = [
      "kms:ListKeys",
      "kms:ListAliases",
      "kms:CreateKey"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "cdh-core-api-kms" {
  name        = "${var.resource_name_prefix}cdh-core-api-kms"
  path        = "/"
  description = "Allows to create, list, put key-policy and describe keys - only - in cdh security accounts."
  policy      = data.aws_iam_policy_document.cdh-core-api-kms.json
}

resource "aws_iam_role_policy_attachment" "cdh-core-api-kms" {
  role       = module.core-api-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-api-kms.arn
}

resource "aws_iam_policy" "cdh-core-api-gateway" {
  name        = "${var.resource_name_prefix}cdh-core-api-gateway"
  path        = "/"
  description = "Allows to make calls against api gateways"
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "apigateway:GET"
        ],
        "Resource": [
          "arn:aws:apigateway:*"
        ]
      }
    ]
  }
  EOF
}

#resource "aws_iam_role_policy_attachment" "cdh-core-api-gateway" {
#  role       = module.core-api-lambda.role_name
#  policy_arn = aws_iam_policy.cdh-core-api-gateway.arn
#}

data "external" "assumable-role-arns" {
  program = ["python", "${path.module}/get_assumable_role_arns.py"]
  query = {
    environment = var.environment
    prefix      = var.resource_name_prefix
  }
}

data "aws_iam_policy_document" "cdh-core-api-sts" {
  statement {
    effect = "Allow"
    actions = [
      "sts:AssumeRole"
    ]
    resources = values(data.external.assumable-role-arns.result)
  }
}

resource "aws_iam_policy" "cdh-core-api-sts" {
  name        = "${var.resource_name_prefix}cdh-core-api-sts"
  path        = "/"
  description = "Allows assuming dedicated roles in other accounts"
  policy      = data.aws_iam_policy_document.cdh-core-api-sts.json
}

resource "aws_iam_role_policy_attachment" "cdh-core-api-sts" {
  role       = module.core-api-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-api-sts.arn
}


resource "aws_iam_policy" "cdh-core-api-misc" {
  name   = "${var.resource_name_prefix}cdh-core-api-misc"
  path   = "/"
  policy = data.aws_iam_policy_document.cdh-core-api-misc.json
}


data "aws_iam_policy_document" "cdh-core-api-misc" {
  statement {
    sid = "XrayAccess"
    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
      "xray:GetSamplingTargets",
      "xray:GetSamplingStatisticSummaries"
    ]
    resources = ["*"]
  }

  statement {
    sid       = "SnsPublish"
    actions   = ["sns:SendMessage", "sns:Publish"]
    resources = [aws_sns_topic.dataset_creation_and_change_notification_topic.arn, aws_sns_topic.notification_topic.arn]
  }

  statement {
    sid     = "GetSsm"
    actions = ["ssm:GetParameter"]
    resources = [
      aws_ssm_parameter.encryption_key.arn,
    ]
  }

  statement {
    sid       = "AuthAPI"
    actions   = ["execute-api:Invoke"]
    resources = [for account_id in keys(data.external.authorization-accounts.result) : "arn:aws:execute-api:*:${account_id}:*/*/*/*"]
  }
}

resource "aws_iam_role_policy_attachment" "cdh-core-api-misc" {
  role       = module.core-api-lambda.role_name
  policy_arn = aws_iam_policy.cdh-core-api-misc.arn
}


resource "aws_iam_role_policy" "allow_access_to_audit_log_group" {
  name   = "${var.resource_name_prefix}core-api-audit-log-access"
  role   = module.core-api-lambda.role_name
  policy = data.aws_iam_policy_document.allow_access_to_audit_log_group.json
}

data "aws_iam_policy_document" "allow_access_to_audit_log_group" {
  statement {
    sid    = "Logging"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["${aws_cloudwatch_log_group.audit-log-group.arn}:*"]
  }

  statement {
    sid    = "KMSDecrypt"
    effect = "Allow"
    actions = [
      "kms:GenerateDataKey",
    ]
    resources = [var.kms_master_key_arn]
  }
}

resource "aws_iam_role_policy" "allow_access_to_cloudwatch_metrics" {
  name   = "${var.resource_name_prefix}core-api-cloudwatch-metrics-access"
  role   = module.core-api-lambda.role_name
  policy = data.aws_iam_policy_document.allow_access_to_cloudwatch_metrics.json
}

data "aws_iam_policy_document" "allow_access_to_cloudwatch_metrics" {
  statement {
    sid    = "putCloudwatchMetric"
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricData"
    ]
    resources = ["*"]
  }
}


resource "aws_iam_role_policy" "assume_metadata_role" {
  name   = "assume_user_roles"
  role   = module.core-api-lambda.role_name
  policy = data.aws_iam_policy_document.assume_metadata_role.json
}

data "aws_iam_policy_document" "assume_metadata_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    resources = [
      "arn:${data.aws_partition.current.partition}:iam::*:role${local.assumable_metadata_role_path}${local.assumable_metadata_role_name}"
    ]
  }
}
