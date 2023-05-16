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

variable "bucket_name" {
  type        = string
  description = "Name of the bucket used to store the lambda layer."
}
variable "layer_name" {
  type        = string
  description = "Name of the layer to be created."
}
variable "context" {
  type        = string
  description = "A short-hand for the purpose of the layer. Will be part of the created S3 object's key. Can be used to share the same artifact across multiple deployments."
}
variable "fetch_custom_credentials" {
  type        = string
  default     = "notset"
  description = "Absolute path to a file that creates custom credentials."
}
variable "requirements_file_path" {
  type        = string
  description = "Absolute path to the requirements file."
}
variable "include_zip" {
  type        = string
  default     = "notset"
  description = "Path to an additional (local) zip file that should be included. Must contain all Python code under `/python`."
}
variable "docker_build" {
  type        = bool
  default     = true
  description = "If true, this will use Docker to build the dependencies layer. Set to true when having binary dependencies"
}

data "aws_region" "current" {}

data "external" "build_layer_object" {
  program = [
    "python3", "src/cdh_core_dev_tools/cdh_core_dev_tools/dependencies/create_deps_layer.py",
    "--context", var.context,
    "--requirements-file-path", var.requirements_file_path,
    "--region", data.aws_region.current.name,
    "--bucket-name", var.bucket_name,
    "--fetch-custom-credentials", var.fetch_custom_credentials,
    "--include-zip", var.include_zip,
    "--docker-build", var.docker_build
  ]
  working_dir = "${path.module}/../../../../"
}

resource "aws_lambda_layer_version" "deps_layer" {
  layer_name          = var.layer_name
  s3_bucket           = data.external.build_layer_object.result.bucket
  s3_key              = data.external.build_layer_object.result.key
  description         = "Provides Lambda dependencies for ${var.context}"
  compatible_runtimes = ["python3.9"]
}

output "lambda_layer_arn" {
  value = aws_lambda_layer_version.deps_layer.arn
}
