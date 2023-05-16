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

variable "environment" {
  type = string
}
variable "resource_name_prefix" {
  type    = string
  default = ""
}
variable "deployment_prefix" {
  type        = string
  default     = ""
  description = "Supplement our prefixing for cases where we don't use it internally. If set, should be the same as the resource_name_prefix."
}
variable "new_cloudwatch_role" {
  type    = bool
  default = null
}
variable "cloudwatch_role_name" {
  type    = string
  default = "cdh-core-api-gw-cloudwatch"
}
variable "users_api_url" {
  type    = string
  default = ""
}
variable "authorization_api_url" {
  type    = string
  default = ""
}
variable "cdh_core_config_file_path" {
  type    = string
  default = "../cdh-core-config-test-deployment.yaml"
}
variable "jwt_cookie_name" {
  type    = string
  default = "cdh_oss"
}
variable "trusted_org_ids" {
  type    = list(string)
  default = ["not-set"]
}
variable "user_account_ids" {
  type    = list(string)
  default = []
}
variable "layers_bucket_name" {
  type    = string
  default = ""
}
variable "integrated_deployment" {
  type        = bool
  default     = true
  description = "Indicates that there is an instance of the auth stack dedicated to this deployment. If set, the users and authorization api urls should be set as well."
}

data "external" "default-security-account" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    purpose : "security"
    partition : data.aws_partition.current.partition
    default_hub_only : "true"
  }
}

data "external" "test-environments" {
  program = ["python", "${path.module}/../../bin/get_environments.py"]
  query = {
    test_environments_only : "true"
  }
}

locals {
  security_account_id         = keys(data.external.default-security-account.result)[0] # there is only one security account in the default hub
  should_create_layers_bucket = var.layers_bucket_name == "" ? var.resource_name_prefix == "" : false
  default_layers_bucket_name  = "${var.deployment_prefix}cdh-core-layers-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}"
  layers_bucket_name          = var.layers_bucket_name == "" ? local.default_layers_bucket_name : var.layers_bucket_name
  trusted_org_ids             = var.trusted_org_ids == tolist(["not-set"]) ? [] : var.trusted_org_ids
  user_account_arns           = [for account_id in var.user_account_ids : "arn:${data.aws_partition.current.partition}:iam::${account_id}:root"]
  new_cloudwatch_role         = var.new_cloudwatch_role == null ? var.resource_name_prefix == "" : var.new_cloudwatch_role
}
