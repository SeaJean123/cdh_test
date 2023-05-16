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

variable "domain" {
  type = string
}
variable "kms_master_key_arn" {
  type = string
}
variable "trusted_accounts" {
  type = list(string)
}
variable "trusted_org_ids" {
  type = list(string)
}
variable "api_name" {
  type = string
}
variable "environment" {
  type = string
}
variable "resource_name_prefix" {
  type = string
}
variable "alerts_topic_arn" {
  type = string
}
variable "authorization_api_url" {
  type = string
}
variable "users_api_url" {
  type = string
}
variable "authorization_api_cookie_name" {
  type = string
}
variable "cdh_core_config_file_path" {
  type = string
}
variable "layers_bucket_name" {
  type = string
}
variable "logs_subscription_arn" {
  type = string
}
variable "cdh_core_layer_arn" {
  type        = string
  description = "ARN of the Lambda Layer providing the 'cdh-core' package (including dependencies)."
}
variable "api_gw_cloudwatch_setting_id" {
  type = string
}
variable "admin_role_name" {
  default = "CDHX-DevOps"
  type    = string
}
