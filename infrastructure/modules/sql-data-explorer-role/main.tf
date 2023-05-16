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
variable "regions" {
  type = list(string)
}
variable "assume_role_account_ids" {
  type = list(string)
}
variable "sql-data-explorer-workgroup" {
  type = map(object({
    arn                         = string,
    encryption_key              = string,
    name                        = string,
    location                    = string,
    athena_query_results_bucket = string,
  }))
}
variable "functional_test_role_name" {
  type    = string
  default = ""
}
variable "security_account_id" {
  type = string
}

data "aws_caller_identity" "current" {}

data "external" "hub" {
  program = ["python", "${path.module}/../../bin/get_hub.py"]
  query = {
    environment : var.environment
    account_id : data.aws_caller_identity.current.account_id
  }
}

locals {
  region_camel_case = {
    for region in var.regions : region => replace(region, "-", "")
  }

  workgroup_arns     = [for region in var.regions : var.sql-data-explorer-workgroup[region].arn]
  workgroup_kms_keys = [for region in var.regions : var.sql-data-explorer-workgroup[region].encryption_key]
  glue_resources     = flatten([for region in var.regions : [for exp in ["catalog", "database/*", "table/*/*"] : "arn:aws:glue:${region}:${data.aws_caller_identity.current.account_id}:${exp}"]])
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"
    principals {
      identifiers = [for account in var.assume_role_account_ids : "arn:aws:iam::${account}:root"]
      type        = "AWS"
    }
    condition {
      test     = "StringLike"
      variable = "aws:PrincipalArn"
      values   = ["arn:aws:iam::*:role/*sql-api*", "arn:aws:iam::*:role/*data-portal-api"]
    }
    actions = ["sts:AssumeRole"]
  }

  dynamic "statement" {
    for_each = var.functional_test_role_name == "" ? [] : [1] #content of the list is arbitrary, but must be of length 1 to enable this block

    content {
      effect = "Allow"
      principals {
        identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
        type        = "AWS"
      }
      condition {
        test     = "StringLike"
        variable = "aws:PrincipalArn"
        values   = ["arn:aws:iam::*:role/${var.functional_test_role_name}"]
      }
      actions = ["sts:AssumeRole"]
    }
  }
}

resource "aws_iam_role" "data-explorer" {
  name               = "${var.resource_name_prefix}cdh-data-explorer"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_iam_role_policy" "data-explorer-lakeformation-access" {
  name   = "${var.resource_name_prefix}cdh-data-explorer-lakeformation-access"
  role   = aws_iam_role.data-explorer.name
  policy = data.aws_iam_policy_document.data-explorer-lakeformation-access.json
}

data "aws_iam_policy_document" "data-explorer-lakeformation-access" {
  statement {
    sid       = "LakeformationAccess"
    effect    = "Allow"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "kms-access" {
  name   = "${var.resource_name_prefix}cdh-allow-data-explorer-kms-access-results"
  policy = data.aws_iam_policy_document.kms-access.json
}

data "aws_iam_policy_document" "kms-access" {
  statement {
    sid       = "AllowAthenaQueryResultsKeyAccess"
    effect    = "Allow"
    resources = local.workgroup_kms_keys
    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey"
    ]
  }
}

resource "aws_iam_policy" "database-access" {
  name = "${var.resource_name_prefix}cdh-allow-data-explorer-glue-access"

  policy = data.aws_iam_policy_document.database-access.json
}

data "aws_iam_policy_document" "database-access" {
  statement {
    effect    = "Allow"
    resources = local.glue_resources
    actions = [
      "glue:Get*",
      "glue:BatchGet*"
    ]
  }
}

resource "aws_iam_policy" "athena-access" {
  name = "${var.resource_name_prefix}cdh-allow-data-explorer-athena-access"

  policy = data.aws_iam_policy_document.athena-access.json
}

data "aws_iam_policy_document" "athena-access" {
  statement {
    sid       = "AthenaRead"
    effect    = "Allow"
    resources = ["*"]
    actions = [
      "athena:GetCatalogs",
      "athena:GetExecutionEngine",
      "athena:GetExecutionEngines",
      "athena:GetNamespace",
      "athena:GetNamespaces",
      "athena:GetTable",
      "athena:GetTables",
      "athena:ListWorkGroups"
    ]
  }
  statement {
    sid       = "AthenaWorkgroupAccess"
    effect    = "Allow"
    resources = local.workgroup_arns
    actions = [
      "athena:BatchGetNamedQuery",
      "athena:BatchGetQueryExecution",
      "athena:CreateNamedQuery",
      "athena:DeleteNamedQuery",
      "athena:GetNamedQuery",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:GetWorkGroup",
      "athena:ListNamedQueries",
      "athena:ListQueryExecutions",
      "athena:StartQueryExecution",
      "athena:StopQueryExecution"
    ]
  }
}

data "aws_iam_policy_document" "athena-results-access" {
  dynamic "statement" {
    for_each = toset(var.regions)
    iterator = region
    content {
      sid       = "ListAthenaResultBucket${local.region_camel_case[region.value]}"
      effect    = "Allow"
      resources = ["arn:aws:s3:::${var.sql-data-explorer-workgroup[region.value].athena_query_results_bucket}"]
      actions   = ["s3:ListBucket"]
      condition {
        test     = "StringEquals"
        values   = ["", "${var.sql-data-explorer-workgroup[region.value].name}/"]
        variable = "s3:prefix"
      }
      condition {
        test     = "StringEquals"
        values   = ["/"]
        variable = "s3:delimiter"
      }
    }
  }
  statement {
    sid       = "VerifyBucket"
    effect    = "Allow"
    resources = [for region in var.regions : "arn:aws:s3:::${var.sql-data-explorer-workgroup[region].athena_query_results_bucket}"]
    actions   = ["s3:GetBucketLocation"]
  }
  statement {
    sid       = "WriteAthenaResultBucket"
    effect    = "Allow"
    resources = [for region in var.regions : "arn:aws:s3:::${var.sql-data-explorer-workgroup[region].location}*"]
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
  }
}

# athena needs to be able to "verify" the result bucket via 'GetBucketLocation'
resource "aws_iam_policy" "athena-results-access" {
  name   = "${var.resource_name_prefix}cdh-allow-data-explorer-athena-results-access"
  policy = data.aws_iam_policy_document.athena-results-access.json
}

data "aws_iam_policy_document" "kms-shared-key-access" {
  statement {
    sid       = "AllowSharedKeysAccess"
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = ["arn:aws:kms:*:${var.security_account_id}:key/*"]
    condition {
      test     = "ForAnyValue:StringLike"
      values   = ["alias/${var.resource_name_prefix}cdh-${var.environment}-${data.external.hub.result.hub}-${data.aws_caller_identity.current.account_id}"]
      variable = "kms:ResourceAliases"
    }
  }
}

resource "aws_iam_policy" "kms-shared-key-access" {
  name   = "${var.resource_name_prefix}cdh-allow-data-explorer-shared-kms-access"
  policy = data.aws_iam_policy_document.kms-shared-key-access.json
}

resource "aws_iam_role_policy_attachment" "kms-access" {
  policy_arn = aws_iam_policy.kms-access.arn
  role       = aws_iam_role.data-explorer.name
}

resource "aws_iam_role_policy_attachment" "database-access" {
  policy_arn = aws_iam_policy.database-access.arn
  role       = aws_iam_role.data-explorer.name
}

resource "aws_iam_role_policy_attachment" "athena-access" {
  policy_arn = aws_iam_policy.athena-access.arn
  role       = aws_iam_role.data-explorer.name
}

resource "aws_iam_role_policy_attachment" "athena-results-access" {
  policy_arn = aws_iam_policy.athena-results-access.arn
  role       = aws_iam_role.data-explorer.name
}

resource "aws_iam_role_policy_attachment" "kms-shared-key-access" {
  policy_arn = aws_iam_policy.kms-shared-key-access.arn
  role       = aws_iam_role.data-explorer.name
}


output "data-explorer-role-name" {
  value = aws_iam_role.data-explorer.name
}
