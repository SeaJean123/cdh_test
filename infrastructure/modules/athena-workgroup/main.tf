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
variable "bucket_name" {
  type = string
}
variable "publish_cloudwatch_metrics_enabled" {
  type    = bool
  default = true
}

variable "kms_key_arn" {
  type    = string
  default = ""
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

resource "aws_athena_workgroup" "wg" {
  name          = var.name
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = 1024 * 1024 * 1024 # 1024 MB can be scanned, 10 MB might be too little if there are joins involved to find a matching join
    publish_cloudwatch_metrics_enabled = var.publish_cloudwatch_metrics_enabled
    result_configuration {
      output_location = "s3://${var.bucket_name}/${var.name}/"
      encryption_configuration {
        encryption_option = var.kms_key_arn == "" ? "SSE_S3" : "SSE_KMS"
        kms_key_arn       = var.kms_key_arn
      }
    }
  }
}


output "encryption-key" {
  value = var.kms_key_arn
}

output "workgroup-location" {
  value = trimprefix(aws_athena_workgroup.wg.configuration[0].result_configuration[0].output_location, "s3://")
}

output "workgroup-name" {
  value = aws_athena_workgroup.wg.name
}

output "workgroup-arn" {
  value = aws_athena_workgroup.wg.arn
}

output "bucket-name" {
  value = var.bucket_name
}
