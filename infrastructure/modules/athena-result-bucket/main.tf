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

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

variable "clean_up_all_objects" {
  type    = bool
  default = false
}

variable "devops_role_name" {
  type    = string
  default = ""
}

variable "bucket_name" {
  type    = string
  default = ""
}

variable "lifecycle_rules" {
  default = []
  type = list(object({
    id              = string
    status          = string
    filter_prefix   = string
    expiration_days = number
  }))
  description = "Adds custom life cycle configuration rules"
}

locals {
  bucket_name = var.bucket_name == "" ? "aws-athena-query-results-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}" : var.bucket_name
}

resource "aws_s3_bucket" "athena_results" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "cdh-core-athena-query-results-eu-west-1" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "query-cache-cleanup"
    status = var.clean_up_all_objects ? "Enabled" : "Disabled"
    expiration {
      days = 10
    }
  }
  rule {
    id     = "query-cache-cleanup-DevOps"
    status = var.devops_role_name == "" ? "Disabled" : "Enabled"
    filter {
      prefix = var.devops_role_name
    }
    expiration {
      days = 10
    }
  }
  dynamic "rule" {
    for_each = var.lifecycle_rules
    content {
      id     = rule.value.id
      status = rule.value.status
      filter {
        prefix = rule.value.filter_prefix
      }
      expiration {
        days = rule.value.expiration_days
      }
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results_encryption" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "allow-ssl-requests-only" {
  bucket = aws_s3_bucket.athena_results.id
  policy = data.aws_iam_policy_document.allow-ssl-requests-only.json
}

data "aws_iam_policy_document" "allow-ssl-requests-only" {
  statement {
    sid    = "AllowSSLRequestsOnly"
    effect = "Deny"
    principals {
      identifiers = ["*"]
      type        = "*"
    }
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.athena_results.arn, "${aws_s3_bucket.athena_results.arn}/*"]
    condition {
      test     = "Bool"
      values   = ["false"]
      variable = "aws:SecureTransport"
    }
  }
}

output "bucket" {
  value = aws_s3_bucket.athena_results.bucket
}
