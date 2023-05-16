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
data "aws_partition" "current" {}

locals {
  saml_provider_name = "idp.${var.auth_domain}"
  provider_url       = "https://${var.auth_domain}/saml"
  devops_role_name   = "CDHDevOps"
  readonly_role_name = "CDHReadOnly"
  sso_role_names     = toset([local.devops_role_name, local.readonly_role_name])
}
