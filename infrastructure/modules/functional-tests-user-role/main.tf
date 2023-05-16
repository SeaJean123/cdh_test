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

variable "bucket-kms-keys" {
  type = list(string)
}

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}

data "external" "api-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "api"
    partition : data.aws_partition.current.partition
  }
}

data "external" "test-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "test"
    partition : data.aws_partition.current.partition
  }
}

variable "athena_workgroup_name" {
  type = string
}

variable "athena_query_result_bucket_names" {
  type = list(string)
}

locals {
  api_account_id                          = keys(data.external.api-accounts.result)[0] # there is only one
  test_account_ids                        = keys(data.external.test-accounts.result)
  test_account_functional_test_user_roles = [for account_id in local.test_account_ids : "arn:${data.aws_partition.current.partition}:iam::${account_id}:role/${var.resource_name_prefix}cdh-core-functional-tests" if account_id != data.aws_caller_identity.current.account_id]
}

resource "aws_iam_role" "cdh_core_functional_tests_role" {
  name               = "${var.resource_name_prefix}cdh-core-functional-tests"
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
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/cdh-deployer"]
    }
    actions = ["sts:AssumeRole"]
  }
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

resource "aws_iam_role_policy_attachment" "cdh_core_functional_tests_execute_api_policy_attachment" {
  role       = aws_iam_role.cdh_core_functional_tests_role.name
  policy_arn = aws_iam_policy.cdh_core_functional_tests_execute_api_policy.arn
}

resource "aws_iam_policy" "cdh_core_functional_tests_execute_api_policy" {
  name        = "${var.resource_name_prefix}cdh-core-functional-tests-execute-api-policy"
  path        = "/"
  description = "Allow execution of the cdh core api."
  policy      = data.aws_iam_policy_document.cdh_core_functional_tests_execute_api_policy_document.json
}

data "aws_iam_policy_document" "cdh_core_functional_tests_execute_api_policy_document" {
  statement {
    effect    = "Allow"
    actions   = ["execute-api:Invoke"]
    sid       = "CoreApiInvoke"
    resources = ["arn:aws:execute-api:*:${local.api_account_id}:*/*/*/*"]
  }
}

resource "aws_iam_role_policy" "manage_resources_for_business_cases_tests" {
  name   = "manage-resources-for-business-cases-tests"
  role   = aws_iam_role.cdh_core_functional_tests_role.name
  policy = data.aws_iam_policy_document.manage_resources_for_business_cases_tests_document.json
}

data "aws_iam_policy_document" "manage_resources_for_business_cases_tests_document" {
  statement {
    sid    = "S3"
    effect = "Allow"
    actions = [
      "s3:Put*",
      "s3:List*",
      "s3:Get*",
      "s3:DeleteObject"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.resource_name_prefix}*"]
  }

  statement {
    sid    = "Glue"
    effect = "Allow"
    actions = [
      "glue:Get*",
      "glue:CreateDatabase",
      "glue:CreateTable",
      "glue:DeleteDatabase"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:*:*:catalog",
      "arn:${data.aws_partition.current.partition}:glue:*:*:database/${var.resource_name_prefix}*",
      "arn:${data.aws_partition.current.partition}:glue:*:*:table/${var.resource_name_prefix}*/*",
      "arn:${data.aws_partition.current.partition}:glue:*:*:userDefinedFunction/${var.resource_name_prefix}*/*"
    ]
  }

  statement {
    sid    = "KMS"
    effect = "Allow"
    actions = [
      "kms:Encrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
      "kms:Decrypt"
    ]
    resources = var.bucket-kms-keys
  }

  statement {
    sid       = "SnsForAttributeExtractorTest"
    effect    = "Allow"
    actions   = ["sns:Subscribe"]
    resources = ["*"]
  }

  statement {
    sid    = "SqsForAttributeExtractorTest"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:Get*",
      "sqs:Delete*",
      "sqs:Create*"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:sqs:*:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    sid    = "AthenaExecutionAccess"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:athena:*:${data.aws_caller_identity.current.account_id}:workgroup/${var.athena_workgroup_name}"]
  }

  statement {
    sid    = "ListAthenaResultBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [for bucket in var.athena_query_result_bucket_names : "arn:${data.aws_partition.current.partition}:s3:::${bucket}"]
    condition {
      test     = "StringEquals"
      values   = ["", "${var.athena_workgroup_name}/"]
      variable = "s3:prefix"
    }
    condition {
      test     = "StringEquals"
      values   = ["/"]
      variable = "s3:delimiter"
    }
  }

  statement {
    sid    = "CreateAndVerifyAthenaResultBucket"
    effect = "Allow"
    actions = [
      "s3:CreateBucket",
      "s3:GetBucketLocation"
    ]
    resources = [for bucket in var.athena_query_result_bucket_names : "arn:${data.aws_partition.current.partition}:s3:::${bucket}"]
  }

  statement {
    sid    = "WriteAthenaResultBucket"
    effect = "Allow"
    actions = [
      "s3:Get*",
      "s3:List*",
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:DeleteObjectTagging",
      "s3:DeleteObjectVersion",
      "s3:DeleteObjectVersionTagging",
      "s3:ObjectOwnerOverrideToBucketOwner",
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:PutObjectVersionTagging"
    ]
    resources = [for bucket in var.athena_query_result_bucket_names : "arn:${data.aws_partition.current.partition}:s3:::${bucket}/${var.athena_workgroup_name}/*"]
  }

  statement {
    sid       = "LakeformationAccess"
    effect    = "Allow"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "assume_api_viewer_role" {
  name   = "assume_user_roles"
  role   = aws_iam_role.cdh_core_functional_tests_role.name
  policy = data.aws_iam_policy_document.assume_api_viewer_role_document.json
}

data "aws_iam_policy_document" "assume_api_viewer_role_document" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    resources = concat([
      "arn:aws:iam::${local.api_account_id}:role/${var.resource_name_prefix}cdh-core-functional-tests-viewer"
    ], local.test_account_functional_test_user_roles)
  }
}

output "cdh-core-functional-test-role-arn" {
  value = aws_iam_role.cdh_core_functional_tests_role.arn
}
