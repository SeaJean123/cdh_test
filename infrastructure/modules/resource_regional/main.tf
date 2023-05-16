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
variable "cdh_core_config_file_path" {
  type = string
}
variable "layers_bucket_name" {
  type = string
}
variable "alerts_topic_arn" {
  type = string
}
variable "kms_master_key_arn" {
  type = string
}
variable "api_kms_master_key_arn" {
  type = string
}
variable "trusted_account_ids" {
  type = list(string)
}
variable "org_ids" {
  type = list(string)
}
variable "attribute_extractor_reserved_concurrency" {
  type    = number
  default = -1
}
variable "set_glue_catalog_policy" {
  type = bool
}
variable "should_create_athena_query_results_bucket" {
  type = bool
}
variable "athena_query_results_buckets_name" {
  type = string
}
variable "admin_role_name" {
  default = "CDHX-DevOps"
  type    = string
}
variable "security_account_id" {
  type = string
}
variable "assumable_role_arn" {
  type = string
}
variable "cleanup_role_arn_list" {
  type = list(string)
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

data "aws_s3_bucket" "athena-result-bucket" {
  count  = var.should_create_athena_query_results_bucket ? 0 : 1
  bucket = var.athena_query_results_buckets_name
}

module "athena-result-bucket" {
  source           = "../athena-result-bucket"
  count            = var.should_create_athena_query_results_bucket ? 1 : 0
  devops_role_name = "CDHDevOps"
  bucket_name      = var.athena_query_results_buckets_name
}

locals {
  athena_query_results_bucket = var.should_create_athena_query_results_bucket ? module.athena-result-bucket[0].bucket : data.aws_s3_bucket.athena-result-bucket[0].bucket
  admin_role_arn              = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${var.admin_role_name}"
}

module "cdh-core-layer" {
  source = "../cdh-core-layer"

  layers_bucket_name = var.layers_bucket_name
  layer_name         = "${var.resource_name_prefix}cdh-core4resources"
}

module "logs-subscription" {
  source = "../logs-subscription"

  resource_name_prefix      = var.resource_name_prefix
  alerts_topic_arn          = var.alerts_topic_arn
  kms_master_key_id         = var.kms_master_key_arn
  api_kms_master_key_id     = var.api_kms_master_key_arn
  environment               = var.environment
  cdh_core_config_file_path = var.cdh_core_config_file_path
  bucket_name               = var.layers_bucket_name
  cdh_core_layer_arn        = module.cdh-core-layer.lambda_layer_arn
  admin_role_name           = var.admin_role_name
}

module "s3-attribute-extractor" {
  source                    = "../s3-attribute-extractor"
  environment               = var.environment
  resource_name_prefix      = var.resource_name_prefix
  alerts_topic_arn          = var.alerts_topic_arn
  logs_subscription_arn     = module.logs-subscription.logs_subscription_function_arn
  kms_master_key_id         = var.kms_master_key_arn
  cdh_core_config_file_path = var.cdh_core_config_file_path
  cdh_core_layer_arn        = module.cdh-core-layer.lambda_layer_arn
  reserved_concurrency      = var.attribute_extractor_reserved_concurrency
  admin_role_name           = var.admin_role_name
  security_account_id       = var.security_account_id
}

module "glue-catalog-policy" {
  source              = "../glue-resource-policy"
  count               = var.set_glue_catalog_policy ? 1 : 0
  trusted_account_ids = var.trusted_account_ids
  org_ids             = var.org_ids
}

module "glue_housekeeping" {
  source = "../../modules/glue-housekeeping"

  resource_name_prefix      = var.resource_name_prefix
  cdh_core_config_file_path = var.cdh_core_config_file_path
  environment               = var.environment
  kms_master_key_id         = var.kms_master_key_arn
  alerts_topic_arn          = var.alerts_topic_arn
  cdh_core_layer_arn        = module.cdh-core-layer.lambda_layer_arn
  admin_role_name           = var.admin_role_name
}

module "sql-data-explorer-workgroup" {
  source                             = "../athena-workgroup"
  name                               = "${var.resource_name_prefix}cdh-data-explorer"
  bucket_name                        = local.athena_query_results_bucket
  kms_key_arn                        = var.kms_master_key_arn
  publish_cloudwatch_metrics_enabled = true
}

module "log_bucket" {
  source               = "../log-bucket"
  resource_name_prefix = var.resource_name_prefix
  kms_master_key_id    = var.kms_master_key_arn
}

module "data-lake-settings" {
  source                   = "../data-lake-settings"
  lake_admin_role_arn_list = concat([local.admin_role_arn, var.assumable_role_arn], var.cleanup_role_arn_list)
}

output "sql-data-explorer-output" {
  value = {
    arn : module.sql-data-explorer-workgroup.workgroup-arn
    encryption_key : module.sql-data-explorer-workgroup.encryption-key
    name : module.sql-data-explorer-workgroup.workgroup-name
    location : module.sql-data-explorer-workgroup.workgroup-location
    athena_query_results_bucket : local.athena_query_results_bucket
  }
}
