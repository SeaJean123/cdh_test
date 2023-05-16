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

data "external" "api-accounts" {
  program = ["python", "${path.module}/../../bin/get_accounts.py"]
  query = {
    environment : var.environment
    purpose : "api"
    partition : data.aws_partition.current.partition
  }
}

data "external" "assumable_billing_role" {
  program = ["python", "${path.module}/../../bin/get_assumable_role.py"]
  query   = { aws_role : "billing" }
}

data "external" "assumable_metadata_role" {
  program = ["python", "${path.module}/../../bin/get_assumable_role.py"]
  query   = { aws_role : "metadata" }
}

locals {
  api_account_id   = keys(data.external.api-accounts.result)[0] # there is only one
  billing_trustees = ["arn:aws:iam::${local.api_account_id}:root"]
}

module "bootstrapping" {
  source = "../../modules/bootstrapping"

  api_account_id               = local.api_account_id
  assumable_billing_role_name  = data.external.assumable_billing_role.result.name
  assumable_billing_role_path  = data.external.assumable_billing_role.result.path
  billing_trustees             = local.billing_trustees
  assumable_metadata_role_name = data.external.assumable_metadata_role.result.name
  assumable_metadata_role_path = data.external.assumable_metadata_role.result.path
  saml_file_path               = var.saml_file_path
  auth_domain                  = var.auth_domain
  create_idp                   = var.create_idp
}
