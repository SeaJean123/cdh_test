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

data "external" "admin_role_name" {
  program = ["python", "${path.module}/../../bin/get_admin_role_name.py"]
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

  is_api_account  = (local.api_account_id == data.aws_caller_identity.current.account_id)
  is_prefixed_dev = (contains(keys(data.external.test-environments.result), var.environment) && var.resource_name_prefix != "")

  cleanup_enabled = (local.is_prefixed_dev && !local.is_api_account)

  cleanup_role_arn_list = (local.is_prefixed_dev && local.is_api_account ?
    ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${var.resource_name_prefix}cdh-core-cleanup"]
  : module.cleanup[*].cdh-core-cleanup-role.arn)
}

module "assumable_resources_role" {
  source                          = "../../modules/assumable-resources-role"
  allowed_arn_list                = local.assumable_role_allowed_arn_list
  resource_name_prefix            = var.resource_name_prefix
  lakeformation_registration_role = module.lakeformation-registration-role.role_arn
  providers = {
    aws = aws.primary
  }
}

module "lakeformation-registration-role" {
  source               = "../../modules/lakeformation-registration-role"
  resource_name_prefix = var.resource_name_prefix
  environment          = var.environment
  security_account_id  = local.security_account_id
  providers = {
    aws = aws.primary
  }
}

module "cleanup" {
  source = "../../modules/cleanup"

  count = local.cleanup_enabled ? 1 : 0

  resource_name_prefix = var.resource_name_prefix
  principals           = local.principals

}

module "sql-data-explorer-role" {
  source = "../../modules/sql-data-explorer-role"

  assume_role_account_ids = local.portal_account_ids
  regions                 = ["eu-west-1", "us-east-1"]
  environment             = var.environment
  security_account_id     = local.security_account_id
  resource_name_prefix    = var.resource_name_prefix
  sql-data-explorer-workgroup = {
    (data.aws_region.eu_west_1.name) : module.resource_regional_eu_west_1.sql-data-explorer-output
    (data.aws_region.us_east_1.name) : module.resource_regional_us_east_1.sql-data-explorer-output
  }
  functional_test_role_name = "${var.resource_name_prefix}cdh-core-functional-tests-viewer"
}

module "functional-tests-viewer-role" {
  count  = local.cleanup_enabled ? 1 : 0
  source = "../../modules/functional-tests-viewer-role"

  resource_name_prefix = var.resource_name_prefix
  environment          = var.environment
}
