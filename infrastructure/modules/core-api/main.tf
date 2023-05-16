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

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

data "external" "openapi-spec-template-file" {
  program = compact(["bash", "${path.module}/create_openapi_spec.sh",
    local.enable_access_via_org_id ? "--org-ids ${join("\",\"", var.trusted_org_ids)}" : "",
    "--partition", data.aws_partition.current.partition,
    "--region", data.aws_region.current.name,
    # lambda_arn  = aws_lambda_alias.core_api_lambda_alias.arn does not work, since it must be known during the plan step
    "--lambda-arn", "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${local.core_api_lambda_name}:${local.core_api_lambda_alias_name}",
    "--accounts", join("\",\"", var.trusted_accounts),
    "--options-arn", local.options_arn,
  ])
}

data "external" "portal-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "portal"
    partition : data.aws_partition.current.partition
  }
}

data "external" "authorization-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "iam"
    partition : data.aws_partition.current.partition
  }
}

data "external" "security-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    purpose : "security"
    partition : data.aws_partition.current.partition
  }
}

data "external" "assumable_metadata_role" {
  program = ["python", "${path.module}/../../bin/get_assumable_role.py"]
  query   = { aws_role : "metadata" }
}

locals {
  api_stage_name             = "default"
  core_api_lambda_name       = "${var.resource_name_prefix}cdh-core-api"
  core_api_lambda_alias_name = "current"

  enable_access_via_org_id = length(var.trusted_org_ids) > 0

  # we need to compute the hash of the generated api spec to make sure we redeploy the API at the right time
  api_spec_hash             = sha1(local.open_api_spec_template)
  full_domain               = var.resource_name_prefix == "" ? var.domain : "${var.resource_name_prefix}.${var.domain}"
  x_ray_tracing_name_suffix = "core-api"
  update_subscribers        = concat(values(data.external.portal-accounts.result), values(data.external.authorization-accounts.result))

  result_page_size = var.resource_name_prefix == "" ? 1000 : 5 # low value to facilitate testing the pagination mechanism in prefix functional tests

  assumable_metadata_role_name = data.external.assumable_metadata_role.result.name
  assumable_metadata_role_path = data.external.assumable_metadata_role.result.path

  options_arn = "arn:${data.aws_partition.current.partition}:execute-api:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*/${local.api_stage_name}/OPTIONS/*"

  open_api_spec_template = data.external.openapi-spec-template-file.result.openapiSpec
}

output "core_api_function_name" {
  value = local.core_api_lambda_name
}
