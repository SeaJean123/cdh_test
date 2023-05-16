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

variable "assume_role_policy_document" {
  type        = string
  description = "The AssumeRolePolicyDocument encoded as a JSON string"
}

variable "environment" {
  type = string
}

variable "permission_boundary" {
  type        = string
  default     = null
  description = "Apply a permission boundary to the created role. If left empty, no boundary will be set."
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  created_by_tag_key   = "createdBy"
  created_by_tag_value = "core-api"
}


resource "aws_iam_role" "cdh-core-api-assumable-security" {
  name                 = "cdh-core-api-assumable-security-${var.environment}"
  description          = "Used by Core API to provision resources"
  assume_role_policy   = var.assume_role_policy_document
  permissions_boundary = var.permission_boundary
}

data "aws_iam_policy_document" "kms-keys" {
  statement {
    sid    = "CreateAndTagKeyWithExpectedTags"
    effect = "Allow"
    actions = [
      "kms:CreateKey",
      "kms:TagResource"
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      values   = [local.created_by_tag_value]
      variable = "aws:RequestTag/${local.created_by_tag_key}"
    }
    condition {
      test     = "StringEquals"
      values   = [local.created_by_tag_value]
      variable = "aws:ResourceTag/${local.created_by_tag_key}"
    }
    condition {
      test     = "StringEquals"
      values   = [var.environment]
      variable = "aws:RequestTag/environment"
    }
    condition {
      test     = "StringEquals"
      values   = [var.environment]
      variable = "aws:ResourceTag/environment"
    }
  }
  statement {
    sid    = "ModifyKeyWithExpectedTags"
    effect = "Allow"
    actions = [
      "kms:CreateGrant",
      "kms:Describe*",
      "kms:DisableKey",
      "kms:Enable*",
      "kms:Get*",
      "kms:PutKeyPolicy",
      "kms:UpdateKeyDescription",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:kms:*:${data.aws_caller_identity.current.account_id}:key/*"
    ]
    condition {
      test     = "StringEquals"
      values   = [local.created_by_tag_value]
      variable = "aws:ResourceTag/${local.created_by_tag_key}"
    }
    condition {
      test     = "StringEquals"
      values   = [var.environment]
      variable = "aws:ResourceTag/environment"
    }
  }
  statement {
    sid    = "ManageAliases"
    effect = "Allow"
    actions = [
      "kms:CreateAlias",
      "kms:UpdateAlias"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:kms:*:${data.aws_caller_identity.current.account_id}:key/*",
      "arn:${data.aws_partition.current.partition}:kms:*:${data.aws_caller_identity.current.account_id}:alias/*cdh-${var.environment}-*"
    ]
    condition {
      test     = "StringEqualsIfExists"
      values   = [local.created_by_tag_value]
      variable = "aws:ResourceTag/${local.created_by_tag_key}"
    }
    condition {
      test     = "StringEqualsIfExists"
      values   = [var.environment]
      variable = "aws:ResourceTag/environment"
    }
  }
  statement {
    sid    = "ListAll"
    effect = "Allow"
    actions = [
      "kms:List*"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "kms-keys" {
  name        = "cdh-core-api-assumable-security-${var.environment}"
  path        = "/"
  description = "Allows key creation, management and tagging. No usage and deletion"
  policy      = data.aws_iam_policy_document.kms-keys.json
}

resource "aws_iam_role_policy_attachment" "cdh-core-api-assumable-security" {
  role       = aws_iam_role.cdh-core-api-assumable-security.name
  policy_arn = aws_iam_policy.kms-keys.arn
}

output "assumable_role_name" {
  value = aws_iam_role.cdh-core-api-assumable-security.name
}

output "assumable_role_arn" {
  value = aws_iam_role.cdh-core-api-assumable-security.arn
}
