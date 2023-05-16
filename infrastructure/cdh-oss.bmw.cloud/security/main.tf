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
data "aws_region" "current" {}
data "aws_partition" "current" {}


data "external" "regions" {
  program = ["python", "${path.module}/../../bin/get_regions.py"]
  query = {
    partition : data.aws_partition.current.partition
  }
}

locals {
  kms_key_template = {
    for account in toset(local.accounts) : account => templatefile("${path.module}/../../cdh-oss.bmw.cloud/security/kms-policy.tftpl", {
      security_account   = data.aws_caller_identity.current.account_id
      key_user_account   = account
      key_user_partition = data.aws_partition.current.partition
      logs_service_list  = join("\",\"", formatlist("logs.%s.amazonaws.com", keys(data.external.regions.result)))
    })
  }
}

resource "aws_kms_key" "internal_eu_west_1" {
  provider    = aws.eu_west_1
  for_each    = toset(local.accounts)
  description = "cdh internal encryption key for services in account ${each.value}"
  policy      = local.kms_key_template[each.value]
}

resource "aws_kms_alias" "internal_alias_eu_west_1" {
  provider      = aws.eu_west_1
  for_each      = toset(local.accounts)
  name          = "alias/${var.deployment_prefix}cdh-internal-${each.value}"
  target_key_id = aws_kms_key.internal_eu_west_1[each.value].key_id
}

resource "aws_kms_key" "internal_test_key_eu_west_1" {
  provider    = aws.eu_west_1
  for_each    = toset(local.test_accounts)
  description = "key to test fail case in functional test in account ${each.value}"
  policy      = local.kms_key_template[each.value]
}

resource "aws_kms_alias" "internal_test_key_eu_west_1_alias" {
  for_each      = toset(local.test_accounts)
  provider      = aws.eu_west_1
  name          = "alias/${var.deployment_prefix}cdh-internal-test-key-${each.value}"
  target_key_id = aws_kms_key.internal_test_key_eu_west_1[each.value].key_id
}

resource "aws_kms_key" "internal_us_east_1" {
  provider    = aws.us_east_1
  for_each    = toset(local.accounts)
  description = "cdh internal encryption key for services in account ${each.value}"
  policy      = local.kms_key_template[each.value]
}

resource "aws_kms_alias" "internal_alias_us_east_1" {
  provider      = aws.us_east_1
  for_each      = toset(local.accounts)
  name          = "alias/${var.deployment_prefix}cdh-internal-${each.value}"
  target_key_id = aws_kms_key.internal_us_east_1[each.value].key_id
}

resource "aws_kms_key" "internal_test_key_us_east_1" {
  provider    = aws.us_east_1
  for_each    = toset(local.test_accounts)
  description = "key to test fail case in functional test in account ${each.value}"
  policy      = local.kms_key_template[each.value]
}

resource "aws_kms_alias" "internal_test_key_us_east_1_alias" {
  for_each      = toset(local.test_accounts)
  provider      = aws.us_east_1
  name          = "alias/${var.deployment_prefix}cdh-internal-test-key-${each.value}"
  target_key_id = aws_kms_key.internal_test_key_us_east_1[each.value].key_id
}
