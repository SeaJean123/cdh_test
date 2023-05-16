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

variable "api_account_id" {
  type = string
}
variable "assumable_billing_role_name" {
  type = string
}
variable "assumable_billing_role_path" {
  type = string
}
variable "billing_trustees" {
  type = list(string)
}
variable "assumable_metadata_role_name" {
  type = string
}
variable "assumable_metadata_role_path" {
  type = string
}
variable "saml_file_path" {
  type = string
}
variable "auth_domain" {
  type = string
}
variable "create_idp" {
  type = bool
}
