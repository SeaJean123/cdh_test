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

variable "lake_admin_role_arn_list" {
  type = list(string)
}
variable "terraform_deployer_arn" {
  type    = string
  default = ""
}
variable "credentials" {
  type    = map(string)
  default = {}
}

data "aws_region" "current" {}

locals {
  formatted_admins       = join(" ", formatlist("--admin %s", var.lake_admin_role_arn_list))
  raw_command            = "python ${path.module}/../../bin/set_lakeformation_settings.py"
  credential_parameters  = length(var.credentials) > 0 ? "--credentials-access-key ${var.credentials["access-key"]} --credentials-secret-key ${var.credentials["secret-key"]}" : ""
  assume_role_parameters = var.terraform_deployer_arn == "" ? "" : "--assume-role ${var.terraform_deployer_arn}"
  command                = "${local.raw_command} --region ${data.aws_region.current.name} ${local.credential_parameters} ${local.assume_role_parameters} ${local.formatted_admins}"
}

resource "null_resource" "data_lake_settings" {
  triggers = {
    admins = join("", var.lake_admin_role_arn_list)
  }

  provisioner "local-exec" {
    command = local.command
  }
}
