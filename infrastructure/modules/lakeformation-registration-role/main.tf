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

variable "resource_name_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "security_account_id" {
  type = string
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

data "external" "hub" {
  program = ["python", "${path.module}/../../bin/get_hub.py"]
  query = {
    environment : var.environment
    account_id : data.aws_caller_identity.current.account_id
  }
}


resource "aws_iam_role" "lakeformation_registration" {
  name        = "${var.resource_name_prefix}cdh-lakeformation-registration"
  description = "Used by Lake Formation to access data of registered buckets"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lakeformation.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lakeformation_registration" {
  name = "${var.resource_name_prefix}cdh-lakeformation-registration"
  role = aws_iam_role.lakeformation_registration.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "S3Access",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ],
        Effect   = "Allow",
        Resource = "arn:${data.aws_partition.current.partition}:s3:::${var.resource_name_prefix}cdh-*"
      },
      {
        Sid = "KMSAccess",
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ],
        Effect   = "Allow",
        Resource = "arn:${data.aws_partition.current.partition}:kms:*:${var.security_account_id}:key/*",
        Condition = {
          "ForAnyValue:StringLike" = {
            "kms:ResourceAliases" = "alias/${var.resource_name_prefix}cdh-${var.environment}-${data.external.hub.result.hub}-${data.aws_caller_identity.current.account_id}"
          }
        }
      },
    ]
  })
}


output "role_arn" {
  value = aws_iam_role.lakeformation_registration.arn
}
