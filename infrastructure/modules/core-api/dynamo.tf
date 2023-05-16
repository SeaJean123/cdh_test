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

resource "aws_dynamodb_table" "datasets" {
  name         = "${var.resource_name_prefix}cdh-datasets"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"
  attribute {
    name = "id"
    type = "S"
  }
  point_in_time_recovery {
    enabled = var.resource_name_prefix == "" ? true : false
  }
}

resource "aws_dynamodb_table" "locks" {
  name         = "${var.resource_name_prefix}cdh-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "lock_id"
  attribute {
    name = "lock_id"
    type = "S"
  }
  point_in_time_recovery {
    enabled = var.resource_name_prefix == "" ? true : false
  }
}

resource "aws_dynamodb_table" "resources" {
  name         = "${var.resource_name_prefix}cdh-resources"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dataset_id"
  range_key    = "id"
  attribute {
    name = "dataset_id"
    type = "S"
  }
  attribute {
    name = "id"
    type = "S"
  }
  point_in_time_recovery {
    enabled = var.resource_name_prefix == "" ? true : false
  }
}

resource "aws_dynamodb_table" "accounts" {
  name         = "${var.resource_name_prefix}cdh-accounts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "account_id"
  attribute {
    name = "account_id"
    type = "S"
  }
  point_in_time_recovery {
    enabled = var.resource_name_prefix == "" ? true : false
  }
}

resource "aws_dynamodb_table" "filter_packages" {
  name         = "${var.resource_name_prefix}cdh-filter-packages"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "datasetid_stage_region"
  range_key    = "id"
  attribute {
    name = "datasetid_stage_region"
    type = "S"
  }
  attribute {
    name = "id"
    type = "S"
  }
  point_in_time_recovery {
    enabled = var.resource_name_prefix == "" ? true : false
  }
}

output "dynamo_accounts_table_name" {
  value = aws_dynamodb_table.accounts.name
}
