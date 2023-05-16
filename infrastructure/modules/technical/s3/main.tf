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

variable "name" {
  type = string
}

variable "force_destroy" {
  type        = bool
  default     = false
  description = "A boolean that indicates all objects should be deleted from the bucket so that the bucket can be destroyed without error. These objects are not recoverable."
}

variable "bucket_owner_enforced" {
  type        = bool
  default     = false
  description = "If true, the bucket will have an ownership configuration that will enforce the bucket owner as the object owner. If false, the bucket will not have an ownership configuration."
}

variable "default_lifecycle_config" {
  type        = bool
  default     = false
  description = "If true, the bucket will have a default lifecycle configuration that will transition objects to intelligent tiering after 90 days. If false, the bucket will not have a default lifecycle configuration."
}

variable "policies" {
  type        = string
  default     = ""
  description = "A JSON formatted string containing the bucket policies to be applied to the bucket."
}

variable "kms_key_id" {
  type        = string
  default     = null
  description = "The ID of the KMS key to use for server-side encryption. If not specified, it will use AES256 encryption."
}

variable "bucket_key_enabled" {
  type        = bool
  default     = false
  description = "Whether or not to use Amazon S3 Bucket Keys for SSE-KMS."
}

variable "logging_target_bucket" {
  type        = string
  default     = ""
  description = "Target bucket for access logs to this bucket. If this is left empty, no aws_s3_bucket_logging resource will be created."
}

variable "versioning" {
  type        = string
  default     = "Disabled"
  description = "Whether or not to enable versioning on the bucket. Valid values are Enabled, Suspended and Disabled."
}

variable "acl" {
  type        = string
  default     = ""
  description = "The canned ACL to apply. Valid values are private, public-read, public-read-write, aws-exec-read, authenticated-read, log-delivery-write, bucket-owner-read, bucket-owner-full-control, and bucket-owner-full-control."
}

resource "aws_s3_bucket" "bucket" {
  bucket        = var.name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_acl" "bucket" {
  count  = var.acl == "" ? 0 : 1
  bucket = aws_s3_bucket.bucket.id
  acl    = var.acl
}

resource "aws_s3_bucket_versioning" "bucket" {
  bucket = aws_s3_bucket.bucket.id
  versioning_configuration {
    status = var.versioning
  }
}

resource "aws_s3_bucket_public_access_block" "bucket" {
  bucket                  = aws_s3_bucket.bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "bucket-policies" {
  bucket = aws_s3_bucket.bucket.id
  policy = data.aws_iam_policy_document.combined-bucket-policies.json
}

data "aws_iam_policy_document" "combined-bucket-policies" {
  source_policy_documents = [data.aws_iam_policy_document.allow-ssl-requests-only.json, var.policies]
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
    resources = [aws_s3_bucket.bucket.arn, "${aws_s3_bucket.bucket.arn}/*"]
    condition {
      test     = "Bool"
      values   = ["false"]
      variable = "aws:SecureTransport"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket" {
  count  = var.default_lifecycle_config ? 1 : 0
  bucket = aws_s3_bucket.bucket.id

  rule {
    id     = "transition-to-intelligent-tiering"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket" {
  bucket = aws_s3_bucket.bucket.id
  lifecycle {
    precondition {
      condition     = !(var.kms_key_id == "" && var.bucket_key_enabled)
      error_message = "If no KMS key is specified, setting bucket_key_enabled to true would be inconsequential."
    }
  }

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_id == null ? "AES256" : "aws:kms"
      kms_master_key_id = var.kms_key_id
    }
    bucket_key_enabled = var.bucket_key_enabled
  }
}

resource "aws_s3_bucket_ownership_controls" "bucket" {
  count  = var.bucket_owner_enforced ? 1 : 0
  bucket = aws_s3_bucket.bucket.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_logging" "bucket" {
  count         = var.logging_target_bucket == "" ? 0 : 1
  bucket        = aws_s3_bucket.bucket.id
  target_bucket = var.logging_target_bucket
  target_prefix = "log/"
}

output "name" {
  value = aws_s3_bucket.bucket.id
}

output "arn" {
  value = aws_s3_bucket.bucket.arn
}
