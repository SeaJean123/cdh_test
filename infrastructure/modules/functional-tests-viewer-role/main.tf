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
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
  }
}

variable "resource_name_prefix" {
  type = string
}

variable "environment" {
  type = string
}

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}

data "external" "test-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "test"
    partition : data.aws_partition.current.partition
  }
}

locals {
  test_account_ids                        = keys(data.external.test-accounts.result)
  test_account_functional_test_user_roles = [for account_id in local.test_account_ids : "arn:${data.aws_partition.current.partition}:iam::${account_id}:role/${var.resource_name_prefix}cdh-core-functional-tests"]
}

resource "aws_iam_role" "cdh_core_functional_tests_role" {
  name               = "${var.resource_name_prefix}cdh-core-functional-tests-viewer"
  assume_role_policy = data.aws_iam_policy_document.cdh_core_functional_tests_assume_role_policy_document.json
  lifecycle {
    create_before_destroy = true
  }
}


data "aws_iam_policy_document" "cdh_core_functional_tests_assume_role_policy_document" {
  statement {
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = local.test_account_ids
    }
    condition {
      test     = "ArnEquals"
      values   = local.test_account_functional_test_user_roles
      variable = "aws:PrincipalArn"
    }
    actions = ["sts:AssumeRole"]
  }
}


resource "aws_iam_role_policy_attachment" "cdh_core_functional_tests_cloudwatch_access" {
  role       = aws_iam_role.cdh_core_functional_tests_role.name
  policy_arn = aws_iam_policy.cdh_core_functional_tests_cloudwatch_access.arn
}

resource "aws_iam_policy" "cdh_core_functional_tests_cloudwatch_access" {
  name        = "${var.resource_name_prefix}cdh-core-functional-tests-cloudwatch-access"
  path        = "/"
  description = "Allows access to cloudwatch within prefix scope"
  policy      = data.aws_iam_policy_document.cdh_core_functional_tests_cloudwatch_access.json
}

data "aws_iam_policy_document" "cdh_core_functional_tests_cloudwatch_access" {
  statement {
    sid       = "CloudwatchLogsGroupList"
    effect    = "Allow"
    actions   = ["logs:DescribeLogGroups"]
    resources = ["arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:*"]
  }
  statement {
    sid    = "CloudWatchLogsAccessToAuditLog"
    effect = "Allow"
    actions = [
      "logs:DescribeLogStreams",
      "logs:FilterLogEvents"
    ]
    resources = ["arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:log-group:${var.resource_name_prefix}cdh-audit-log:log-stream:"]
  }
}

data "aws_iam_policy_document" "sts" {
  statement {
    sid       = "Sts"
    actions   = ["sts:AssumeRole"]
    resources = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${var.resource_name_prefix}cdh-data-explorer"]
  }
}

resource "aws_iam_policy" "sts-access" {
  name   = "${var.resource_name_prefix}cdh-core-functional-tests-sts"
  policy = data.aws_iam_policy_document.sts.json
}

resource "aws_iam_role_policy_attachment" "attach-sts" {
  role       = aws_iam_role.cdh_core_functional_tests_role.name
  policy_arn = aws_iam_policy.sts-access.arn
}
