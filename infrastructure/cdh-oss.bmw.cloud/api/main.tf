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
data "aws_partition" "current" {}

data "external" "domain" {
  program = ["python", "${path.module}/../../bin/get_domain.py"]
  query = {
    environment : var.environment
    partition : data.aws_partition.current.partition
  }
}

data "external" "accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    partition : data.aws_partition.current.partition
  }
}

data "external" "resource_account_ids" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "resources"
    partition : data.aws_partition.current.partition
  }
}

data "external" "admin_role_name" {
  program = ["python", "${path.module}/../../bin/get_admin_role_name.py"]
}

data "external" "config" {
  program = ["python", "${path.module}/../../bin/get_config_value.py"]
  query = {
    json_path : "aws_service.s3.configured_limits.resource_account_bucket_limit"
  }
}

locals {
  admin_role_name = data.external.admin_role_name.result.admin_role_name
  principals = sort(
    distinct(
      [
        "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${local.admin_role_name}",
        "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/cdh-deployer"
      ]
    )
  )
  cleanup_enabled = (contains(keys(data.external.test-environments.result), var.environment) && var.resource_name_prefix != "")
  metrics_enabled = var.resource_name_prefix == ""
}

resource "aws_sns_topic" "alerts" {
  provider = aws.primary
}

moved {
  from = aws_s3_bucket.layers[0]
  to   = module.layers[0].aws_s3_bucket.bucket
}

moved {
  from = aws_s3_bucket_public_access_block.layers[0]
  to   = module.layers[0].aws_s3_bucket_public_access_block.bucket
}

module "layers" {
  count     = local.should_create_layers_bucket ? 1 : 0
  providers = { aws = aws.primary }

  source                   = "../../modules/technical/s3"
  name                     = local.layers_bucket_name
  default_lifecycle_config = true
}

data "aws_kms_key" "internal" {
  key_id = "arn:aws:kms:${data.aws_region.current.name}:${local.security_account_id}:alias/${var.deployment_prefix}cdh-internal-${data.aws_caller_identity.current.account_id}"
}

module "api-gw-cloudwatch" {
  count     = local.new_cloudwatch_role ? 1 : 0
  source    = "../../modules/api-gw-cloudwatch"
  providers = { aws = aws.primary }
  role_name = var.cloudwatch_role_name
}

module "cdh-core-layer" {
  source = "../../modules/cdh-core-layer"

  layers_bucket_name = local.layers_bucket_name
  layer_name         = "${var.resource_name_prefix}cdh-core4api"
}

module "core-api" {
  source    = "../../modules/core-api"
  providers = { aws = aws.primary }

  alerts_topic_arn              = aws_sns_topic.alerts.arn
  api_name                      = "cdh-core-api"
  authorization_api_cookie_name = var.jwt_cookie_name
  authorization_api_url         = var.integrated_deployment ? var.authorization_api_url : ""
  users_api_url                 = var.integrated_deployment ? var.users_api_url : ""
  cdh_core_config_file_path     = abspath("${path.root}/${var.cdh_core_config_file_path}")
  domain                        = data.external.domain.result.domain
  environment                   = var.environment
  kms_master_key_arn            = data.aws_kms_key.internal.arn
  layers_bucket_name            = local.layers_bucket_name
  resource_name_prefix          = var.resource_name_prefix
  trusted_accounts              = concat(values(data.external.accounts.result), local.user_account_arns)
  trusted_org_ids               = local.trusted_org_ids
  logs_subscription_arn         = ""
  cdh_core_layer_arn            = module.cdh-core-layer.lambda_layer_arn
  api_gw_cloudwatch_setting_id  = local.new_cloudwatch_role ? module.api-gw-cloudwatch[0].api_gw_cloudwatch_setting_id : null
}

module "functional-tests-viewer-role" {
  count  = local.cleanup_enabled ? 1 : 0
  source = "../../modules/functional-tests-viewer-role"

  resource_name_prefix = var.resource_name_prefix
  environment          = var.environment
}

module "cleanup" {
  count  = local.cleanup_enabled ? 1 : 0
  source = "../../modules/cleanup"

  resource_name_prefix = var.resource_name_prefix
  principals           = local.principals

}

module "monitoring" {
  count      = local.metrics_enabled ? 1 : 0
  depends_on = [module.core-api]
  source     = "../../modules/monitoring"

  core_api_function_name = module.core-api.core_api_function_name

  alerts_topic_arn             = aws_sns_topic.alerts.arn
  bucket_count_alarm_threshold = 85
  max_buckets_in_account       = lookup(data.external.config.result, "resource_account_bucket_limit")
  resource_account_ids         = keys(data.external.resource_account_ids.result)
}

output "cdh_api_updates_topic_arn" {
  value = module.core-api.dataset_notification_topic_arn
}

output "cdh_api_updates_fifo_topic_arn" {
  value = module.core-api.notification_topic_arn
}
