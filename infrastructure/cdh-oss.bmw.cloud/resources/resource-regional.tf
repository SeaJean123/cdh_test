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


data "aws_kms_key" "internal_eu_west_1" {
  provider = aws.eu_west_1
  key_id   = "arn:aws:kms:eu-west-1:${local.security_account_id}:alias/${var.deployment_prefix}cdh-internal-${data.aws_caller_identity.current.account_id}"
}

data "aws_kms_key" "api_internal_eu_west_1" {
  provider = aws.eu_west_1
  key_id   = "arn:aws:kms:eu-west-1:${local.security_account_id}:alias/${var.deployment_prefix}cdh-internal-${local.api_account_id}"
}

moved {
  from = aws_s3_bucket.layers_eu_west_1[0]
  to   = module.layers_eu_west_1[0].aws_s3_bucket.bucket
}

moved {
  from = aws_s3_bucket_public_access_block.layers_eu_west_1[0]
  to   = module.layers_eu_west_1[0].aws_s3_bucket_public_access_block.bucket
}

module "layers_eu_west_1" {
  count = local.should_create_layers_buckets["eu-west-1"] ? 1 : 0
  providers = {
    aws = aws.eu_west_1
  }

  source                   = "../../modules/technical/s3"
  name                     = local.layers_buckets_names["eu-west-1"]
  default_lifecycle_config = true
}

resource "aws_sns_topic" "alerts_eu_west_1" {
  provider = aws.eu_west_1
}

module "resource_regional_eu_west_1" {
  source = "../../modules/resource_regional"
  providers = {
    aws = aws.eu_west_1
  }

  alerts_topic_arn                          = aws_sns_topic.alerts_eu_west_1.arn
  api_kms_master_key_arn                    = data.aws_kms_key.api_internal_eu_west_1.arn
  cdh_core_config_file_path                 = abspath("${path.root}/${var.cdh_core_config_file_path}")
  cleanup_role_arn_list                     = local.cleanup_role_arn_list
  environment                               = var.environment
  kms_master_key_arn                        = data.aws_kms_key.internal_eu_west_1.arn
  layers_bucket_name                        = local.layers_buckets_names["eu-west-1"]
  org_ids                                   = local.trusted_org_ids
  resource_name_prefix                      = var.resource_name_prefix
  trusted_account_ids                       = concat(keys(data.external.accounts.result), var.user_account_ids)
  attribute_extractor_reserved_concurrency  = var.attribute_extractor_reserved_concurrency
  set_glue_catalog_policy                   = local.set_glue_catalog_policy
  should_create_athena_query_results_bucket = local.should_create_layers_buckets["eu-west-1"]
  athena_query_results_buckets_name         = local.athena_query_results_buckets_names["eu-west-1"]
  admin_role_name                           = data.external.admin_role_name.result.admin_role_name
  security_account_id                       = local.security_account_id
  assumable_role_arn                        = module.assumable_resources_role.assumable_role_arn

  depends_on = [module.layers_eu_west_1]
}

data "aws_kms_key" "internal_us_east_1" {
  provider = aws.us_east_1
  key_id   = "arn:aws:kms:us-east-1:${local.security_account_id}:alias/${var.deployment_prefix}cdh-internal-${data.aws_caller_identity.current.account_id}"
}

data "aws_kms_key" "api_internal_us_east_1" {
  provider = aws.us_east_1
  key_id   = "arn:aws:kms:us-east-1:${local.security_account_id}:alias/${var.deployment_prefix}cdh-internal-${local.api_account_id}"
}

moved {
  from = aws_s3_bucket.layers_us_east_1[0]
  to   = module.layers_us_east_1[0].aws_s3_bucket.bucket
}

moved {
  from = aws_s3_bucket_public_access_block.layers_us_east_1[0]
  to   = module.layers_us_east_1[0].aws_s3_bucket_public_access_block.bucket
}

module "layers_us_east_1" {
  count = local.should_create_layers_buckets["us-east-1"] ? 1 : 0
  providers = {
    aws = aws.us_east_1
  }

  source                   = "../../modules/technical/s3"
  name                     = local.layers_buckets_names["us-east-1"]
  default_lifecycle_config = true
}

resource "aws_sns_topic" "alerts_us_east_1" {
  provider = aws.us_east_1
}

module "resource_regional_us_east_1" {
  source = "../../modules/resource_regional"
  providers = {
    aws = aws.us_east_1
  }

  alerts_topic_arn                          = aws_sns_topic.alerts_us_east_1.arn
  api_kms_master_key_arn                    = data.aws_kms_key.api_internal_us_east_1.arn
  cdh_core_config_file_path                 = abspath("${path.root}/${var.cdh_core_config_file_path}")
  cleanup_role_arn_list                     = local.cleanup_role_arn_list
  environment                               = var.environment
  kms_master_key_arn                        = data.aws_kms_key.internal_us_east_1.arn
  layers_bucket_name                        = local.layers_buckets_names["us-east-1"]
  org_ids                                   = local.trusted_org_ids
  resource_name_prefix                      = var.resource_name_prefix
  trusted_account_ids                       = concat(keys(data.external.accounts.result), var.user_account_ids)
  attribute_extractor_reserved_concurrency  = var.attribute_extractor_reserved_concurrency
  set_glue_catalog_policy                   = local.set_glue_catalog_policy
  should_create_athena_query_results_bucket = local.should_create_layers_buckets["us-east-1"]
  athena_query_results_buckets_name         = local.athena_query_results_buckets_names["us-east-1"]
  admin_role_name                           = data.external.admin_role_name.result.admin_role_name
  security_account_id                       = local.security_account_id
  assumable_role_arn                        = module.assumable_resources_role.assumable_role_arn

  depends_on = [module.layers_us_east_1]
}
