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

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}


variable "resource_name_prefix" {
  type        = string
  default     = ""
  description = "Prefix for all module resources and generated database names"
}

variable "principals" {
  type    = list(string)
  default = []
}


data "aws_iam_policy_document" "assume-role" {
  statement {
    effect = "Allow"
    principals {
      identifiers = var.principals
      type        = "AWS"
    }
    actions = ["sts:AssumeRole"]
  }
}


resource "aws_iam_role" "cdh-core-cleanup" {
  name               = "${var.resource_name_prefix}cdh-core-cleanup"
  description        = "Runs the cleanup script after a prefix deployment"
  assume_role_policy = data.aws_iam_policy_document.assume-role.json
}

resource "aws_iam_role_policy_attachment" "cleanup-list-resources" {
  role       = aws_iam_role.cdh-core-cleanup.name
  policy_arn = aws_iam_policy.cleanup-list-resources.arn
}

resource "aws_iam_policy" "cleanup-list-resources" {
  name   = "${var.resource_name_prefix}cdh-core-cleanup-list-resources"
  policy = data.aws_iam_policy_document.cleanup-list-resources.json
}

data "aws_iam_policy_document" "cleanup-list-resources" {
  statement {
    sid    = "ListWildcards"
    effect = "Allow"
    actions = [
      "athena:ListWorkGroups",
      "dynamodb:ListTables",
      "glue:Get*",
      "lakeformation:ListPermissions",
      "lakeformation:ListResources",
      "s3:List*",
      "events:List*",
      "ram:GetResourceShares"
    ]
    resources = ["*"]
  }
  statement {
    sid     = "Iam"
    effect  = "Allow"
    actions = ["iam:List*"]
    resources = [
      "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/",
      "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/",
      "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${var.resource_name_prefix}*",
      "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/${var.resource_name_prefix}*"
    ]
  }
  statement {
    sid     = "Quicksight"
    effect  = "Allow"
    actions = ["quicksight:ListGroups"]
    resources = [
      "arn:${data.aws_partition.current.partition}:quicksight:*:${data.aws_caller_identity.current.account_id}:group/default/*"
    ]
  }
  statement {
    sid       = "SNS"
    effect    = "Allow"
    actions   = ["sns:ListTopics"]
    resources = ["arn:${data.aws_partition.current.partition}:sns:*:${data.aws_caller_identity.current.account_id}:*"]
  }
  statement {
    sid       = "SQS"
    effect    = "Allow"
    actions   = ["sqs:ListQueues"]
    resources = ["arn:${data.aws_partition.current.partition}:sqs:*:${data.aws_caller_identity.current.account_id}:*"]
  }
}


resource "aws_iam_role_policy_attachment" "cleanup-delete-resources" {
  role       = aws_iam_role.cdh-core-cleanup.name
  policy_arn = aws_iam_policy.cleanup-delete-resources.arn
}

resource "aws_iam_policy" "cleanup-delete-resources" {
  name   = "${var.resource_name_prefix}cdh-core-cleanup-delete-resources"
  policy = data.aws_iam_policy_document.cleanup-delete-resources.json
}

data "aws_iam_policy_document" "cleanup-delete-resources" {
  statement {
    sid    = "Events"
    effect = "Allow"
    actions = [
      "events:RemoveTargets",
      "events:DeleteRule"
    ]
    resources = [
    "arn:${data.aws_partition.current.partition}:events:*:${data.aws_caller_identity.current.account_id}:rule/${var.resource_name_prefix}*"]
  }
  statement {
    sid    = "Glue"
    effect = "Allow"
    actions = [
      "glue:DeleteDatabase",
      "glue:UpdateCrawler"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:database/${var.resource_name_prefix}*",
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:table/${var.resource_name_prefix}*/*",
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:userDefinedFunction/${var.resource_name_prefix}*/*",
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:crawler/${var.resource_name_prefix}*"
    ]
  }
  statement {
    sid     = "Athena"
    effect  = "Allow"
    actions = ["athena:DeleteWorkGroup"]
    resources = [
      "arn:${data.aws_partition.current.partition}:athena:*:${data.aws_caller_identity.current.account_id}:workgroup/${var.resource_name_prefix}*"
    ]
  }
  statement {
    sid    = "DynamoDB"
    effect = "Allow"
    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:Scan",
      "dynamodb:DeleteItem"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:dynamodb:*:${data.aws_caller_identity.current.account_id}:table/${var.resource_name_prefix}*"
    ]
  }
  statement {
    sid    = "Iam"
    effect = "Allow"
    actions = [
      "iam:DeleteRole",
      "iam:DetachRolePolicy",
      "iam:DeletePolicyVersion",
      "iam:DeletePolicy"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${var.resource_name_prefix}*",
      "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/${var.resource_name_prefix}*"
    ]
  }
  statement {
    sid    = "LakeFormation"
    effect = "Allow"
    actions = [
      "lakeformation:DeregisterResource",
      "lakeformation:GrantPermissions",
      "lakeformation:RevokePermissions"
    ]
    resources = ["*"]
  }
  statement {
    sid     = "Quicksight"
    effect  = "Allow"
    actions = ["quicksight:DeleteGroup"]
    resources = [
      "arn:${data.aws_partition.current.partition}:quicksight:*:${data.aws_caller_identity.current.account_id}:group/default/*"
    ]
  }
  statement {
    sid    = "S3"
    effect = "Allow"
    actions = [
      "s3:DeleteObject",
      "s3:DeleteBucket"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.resource_name_prefix}*"]
  }
  statement {
    sid     = "Sns"
    effect  = "Allow"
    actions = ["sns:DeleteTopic"]
    resources = [
      "arn:${data.aws_partition.current.partition}:sns:*:${data.aws_caller_identity.current.account_id}:${var.resource_name_prefix}*"
    ]
  }
  statement {
    sid     = "Sqs"
    effect  = "Allow"
    actions = ["sqs:DeleteQueue"]
    resources = [
      "arn:${data.aws_partition.current.partition}:sqs:*:${data.aws_caller_identity.current.account_id}:${var.resource_name_prefix}*"
    ]
  }
  statement {
    sid    = "Ram"
    effect = "Allow"
    actions = [
      "ram:DeleteResourceShare",
      "ram:DisassociateResourceShare"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:ram:*:${data.aws_caller_identity.current.account_id}:resource-share/*"
    ]
  }
  statement {
    sid    = "GlueForRam"
    effect = "Allow"
    actions = [
      "glue:GetResourcePolicy",
      "glue:PutResourcePolicy",
      "glue:DeleteResourcePolicy"
    ]
    resources = ["*"]
  }
}


output "cdh-core-cleanup-role" {
  value = aws_iam_role.cdh-core-cleanup
}
