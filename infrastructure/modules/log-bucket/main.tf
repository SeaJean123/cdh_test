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

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  read_roles_and_deployer = var.terraform_deployer_arn != null ? concat(var.read_roles, [var.terraform_deployer_arn]) : var.read_roles
}

moved {
  from = aws_s3_bucket.log_bucket
  to   = module.log_bucket.aws_s3_bucket.bucket
}

moved {
  from = aws_s3_bucket_public_access_block.log_bucket
  to   = module.log_bucket.aws_s3_bucket_public_access_block.bucket
}

moved {
  from = aws_s3_bucket_server_side_encryption_configuration.log_bucket
  to   = module.log_bucket.aws_s3_bucket_server_side_encryption_configuration.bucket
}

moved {
  from = aws_s3_bucket_ownership_controls.log_bucket
  to   = module.log_bucket.aws_s3_bucket_ownership_controls.bucket[0]
}

moved {
  from = aws_s3_bucket_policy.log_bucket
  to   = module.log_bucket.aws_s3_bucket_policy.bucket-policies
}

module "log_bucket" {
  source                = "../technical/s3"
  kms_key_id            = var.kms_master_key_id
  name                  = "${var.resource_name_prefix}cdh-core-s3-logging-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}"
  bucket_owner_enforced = true
  policies              = data.aws_iam_policy_document.log_bucket.json
  bucket_key_enabled    = true
}

resource "aws_s3_bucket_lifecycle_configuration" "log_bucket" {
  bucket = module.log_bucket.name

  rule {
    id     = "log-bucket-lifecycle"
    status = "Enabled"
    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
    expiration {
      days = var.resource_name_prefix == "" ? var.log_retention_days : 7
    }
  }
}

data "aws_iam_policy_document" "log_bucket" {
  dynamic "statement" {
    # Only restrict access if no read roles are given, even if the terraform deployer is set (same below)
    for_each = length(var.read_roles) > 0 ? [1] : []
    content {
      sid    = "DenyLogAccessInSameAccountUnlessReader"
      effect = "Deny"

      principals {
        type        = "*"
        identifiers = ["*"]
      }

      actions = [
        "s3:GetObject",
        "s3:ListBucket",
      ]

      resources = [
        module.log_bucket.arn,
        "${module.log_bucket.arn}/*"
      ]

      condition {
        test     = "ArnNotEquals"
        variable = "aws:PrincipalArn"
        values   = local.read_roles_and_deployer
      }
    }
  }

  dynamic "statement" {
    for_each = length(var.read_roles) > 0 ? [1] : []
    content {
      sid    = "AllowLogAccessForReaderAndDeployer"
      effect = "Allow"

      principals {
        type        = "AWS"
        identifiers = local.read_roles_and_deployer
      }

      actions = [
        "s3:GetObject",
        "s3:ListBucket",
      ]

      resources = [
        module.log_bucket.arn,
        "${module.log_bucket.arn}/*"
      ]
    }
  }

  statement {
    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }

    actions = [
      "s3:PutObject"
    ]

    resources = [
      module.log_bucket.arn,
      "${module.log_bucket.arn}/*"
    ]

    condition {
      test     = "ArnLike"
      values   = ["arn:aws:s3:::*cdh*"]
      variable = "aws:SourceArn"
    }

    condition {
      test     = "StringEquals"
      values   = [data.aws_caller_identity.current.account_id]
      variable = "aws:SourceAccount"
    }
  }
}
