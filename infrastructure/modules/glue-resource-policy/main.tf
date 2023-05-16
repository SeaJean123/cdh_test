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

variable "trusted_account_ids" {
  type = list(string)
}
variable "org_ids" {
  type = list(string)
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}


locals {
  enable_access_via_org_id = length(var.org_ids) > 0
  accounts                 = formatlist("arn:${data.aws_partition.current.partition}:iam::%s:root", var.trusted_account_ids)
}

resource "aws_glue_resource_policy" "resource-policy" {
  policy        = data.aws_iam_policy_document.policy.json
  enable_hybrid = "TRUE"
}

data "aws_iam_policy_document" "policy" {
  dynamic "statement" {
    # include the statement only if an OrgId is provided, since otherwise the statement is invalid
    for_each = local.enable_access_via_org_id ? [1] : []
    content {
      effect = "Allow"
      principals {
        type        = "AWS"
        identifiers = ["*"]
      }
      actions = [
        "glue:Get*",
        "glue:BatchGet*"
      ]
      resources = [
        "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
        "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
        "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*"
      ]
      condition {
        test     = "StringEquals"
        values   = var.org_ids
        variable = "aws:PrincipalOrgID"
      }
    }
  }
  statement {
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = local.accounts
    }
    actions = [
      "glue:Get*",
      "glue:BatchGet*"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*"
    ]
  }
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ram.amazonaws.com"]
    }
    actions = [
      "glue:ShareResource"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*"
    ]
  }
}
