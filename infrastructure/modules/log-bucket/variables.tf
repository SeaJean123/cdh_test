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

variable "resource_name_prefix" {
  type        = string
  default     = ""
  description = "Prefix of the log bucket"
}
variable "read_roles" {
  type        = list(string)
  description = "List of role ARNs allowed to read the log bucket. If empty (default), all roles and other IAM principles in the account will have access."
  default     = []
}
variable "log_retention_days" {
  type        = number
  description = "Number of days log data will be retained"
  default     = 90
}
variable "terraform_deployer_arn" {
  type        = string
  description = "ARN of the terraform deployer, will be granted access in addition to read_roles."
  default     = null
}
variable "kms_master_key_id" {
  type        = string
  description = "Arn of the key to use to encrypt the log bucket"
}
