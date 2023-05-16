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

variable "alerts_topic_arn" {
  type = string
}

variable "bucket_count_alarm_threshold" {
  type        = number
  default     = 85
  description = "Threshold value in percentage, when the alarm should be raised. A value between 0 and 100"

  validation {
    condition     = (var.bucket_count_alarm_threshold >= 0 && var.bucket_count_alarm_threshold <= 100)
    error_message = "Please choose a value between 0 and 100 for the threshold value."
  }
}

variable "core_api_function_name" {
  type        = string
  description = "The name of the core api lambda."
}

variable "max_buckets_in_account" {
  type        = number
  description = "The bucket limit on the AWS Account. See more: https://docs.aws.amazon.com/AmazonS3/latest/userguide/BucketRestrictions.html"
}

variable "resource_account_ids" {
  type = list(string)
}
