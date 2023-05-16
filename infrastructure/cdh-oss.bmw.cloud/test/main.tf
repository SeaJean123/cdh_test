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

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}

data "external" "api-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "api"
    partition : data.aws_partition.current.partition
  }
}

data "external" "resource-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "resources"
    partition : data.aws_partition.current.partition
  }
}

data "external" "test-environments" {
  program = ["python", "${path.module}/../../bin/get_environments.py"]
  query = {
    test_environments_only : "true"
  }
}

data "external" "default-security-account" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    purpose : "security"
    partition : data.aws_partition.current.partition
    default_hub_only : "true"
  }
}

data "external" "regions" {
  program = ["python", "${path.module}/../../bin/get_regions.py"]
  query = {
    partition : data.aws_partition.current.partition
  }
}

data "external" "admin_role_name" {
  program = ["python", "${path.module}/../../bin/get_admin_role_name.py"]
}

locals {
  resource_account_ids = keys(data.external.resource-accounts.result)
  api_account_id       = keys(data.external.api-accounts.result)[0]             # there is only one
  security_account_id  = keys(data.external.default-security-account.result)[0] # there is only one security account in the default hub
  admin_role_name      = data.external.admin_role_name.result.admin_role_name
  admin_role_arn       = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${local.admin_role_name}"
  principals = sort(
    distinct(
      [
        local.admin_role_arn,
        "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/cdh-deployer"
      ]
    )
  )
  bucket_kms_keys                  = [for region in keys(data.external.regions.result) : "arn:aws:kms:${region}:${local.security_account_id}:key/*"]
  athena_query_result_bucket_base  = "cdh-core-aws-athena-query-results-${data.aws_caller_identity.current.account_id}"
  athena_query_result_bucket_names = [for region in keys(data.external.regions.result) : "${local.athena_query_result_bucket_base}-${region}"]
  athena_workgroup_name            = "${var.resource_name_prefix}cdh-functional-tests"
  admins                           = concat([local.admin_role_arn, module.functional-tests-user-role.cdh-core-functional-test-role-arn], module.cleanup[*].cdh-core-cleanup-role.arn)
}

module "functional-tests-user-role" {
  source = "../../modules/functional-tests-user-role"

  resource_name_prefix             = var.resource_name_prefix
  environment                      = var.environment
  bucket-kms-keys                  = local.bucket_kms_keys
  athena_workgroup_name            = local.athena_workgroup_name
  athena_query_result_bucket_names = local.athena_query_result_bucket_names
}

module "cleanup" {
  source = "../../modules/cleanup"

  count = (contains(keys(data.external.test-environments.result), var.environment)
    && var.resource_name_prefix != ""
    && !contains(local.resource_account_ids, data.aws_caller_identity.current.account_id)
    && local.api_account_id != data.aws_caller_identity.current.account_id
  ) ? 1 : 0

  resource_name_prefix = var.resource_name_prefix
  principals           = local.principals

}

data "aws_s3_bucket" "cdh-core-athena-query-results-eu-west-1" {
  count    = var.resource_name_prefix == "" ? 0 : 1
  bucket   = "${local.athena_query_result_bucket_base}-eu-west-1"
  provider = aws.eu_west_1
}

module "workgroup-eu_west_1" {
  source                             = "../../modules/athena-workgroup"
  name                               = local.athena_workgroup_name
  bucket_name                        = var.resource_name_prefix == "" ? module.athena-result-bucket_eu_west_1[0].bucket : data.aws_s3_bucket.cdh-core-athena-query-results-eu-west-1[0].bucket
  kms_key_arn                        = ""
  publish_cloudwatch_metrics_enabled = false
  providers = {
    aws = aws.eu_west_1
  }
}

module "data-lake-settings-eu_west_1" {
  source                   = "../../modules/data-lake-settings"
  lake_admin_role_arn_list = local.admins
  providers = {
    aws = aws.eu_west_1
  }
}

data "aws_s3_bucket" "cdh-core-athena-query-results-us-east-1" {
  count    = var.resource_name_prefix == "" ? 0 : 1
  bucket   = "${local.athena_query_result_bucket_base}-us-east-1"
  provider = aws.us_east_1
}

module "workgroup-us_east_1" {
  source                             = "../../modules/athena-workgroup"
  name                               = local.athena_workgroup_name
  bucket_name                        = var.resource_name_prefix == "" ? module.athena-result-bucket_us_east_1[0].bucket : data.aws_s3_bucket.cdh-core-athena-query-results-us-east-1[0].bucket
  kms_key_arn                        = ""
  publish_cloudwatch_metrics_enabled = false
  providers = {
    aws = aws.us_east_1
  }
}

module "data-lake-settings-us_east_1" {
  source                   = "../../modules/data-lake-settings"
  lake_admin_role_arn_list = local.admins
  providers = {
    aws = aws.us_east_1
  }
}
