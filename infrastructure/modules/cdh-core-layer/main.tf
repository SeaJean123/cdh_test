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

#####################################################################################
# provider
#####################################################################################
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.63"
    }
  }
}

variable "layer_name" {
  type = string
}
variable "layers_bucket_name" {
  type = string
}
variable "context" {
  type        = string
  default     = "cdh-core"
  description = "Describe the context in which the layer is to be used. The cached S3 object can be shared across stacks if the same 'context' is used."
}
variable "fetch_custom_credentials" {
  type        = string
  default     = "notset"
  description = "Absolute path to a file that creates custom credentials."
}

data "aws_region" "current" {}

data "external" "code_zip" {
  program     = ["bash", "package.sh", "--layer", var.context, abspath("${path.module}/../../../src/cdh_core")]
  working_dir = "${path.module}/../../bin/"
}

module "layer" {
  source = "../technical/lambda_layer"

  bucket_name              = var.layers_bucket_name
  context                  = var.context
  layer_name               = var.layer_name
  requirements_file_path   = abspath("${path.module}/../../../src/cdh_core/requirements.txt")
  fetch_custom_credentials = var.fetch_custom_credentials
  include_zip              = data.external.code_zip.result.file
}

output "lambda_layer_arn" {
  value = module.layer.lambda_layer_arn
}
