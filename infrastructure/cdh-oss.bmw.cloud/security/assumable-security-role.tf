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

data "aws_iam_policy_document" "assume-security-role" {
  provider = aws.primary
  for_each = data.external.environments.result
  statement {
    sid     = "AllowApiAccount"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${local.api_accounts[each.key]}:root"]
      type        = "AWS"
    }
    condition {
      test     = "StringLike"
      values   = ["arn:${data.aws_partition.current.partition}:iam::${local.api_accounts[each.key]}:role/*cdh-core-api"]
      variable = "aws:PrincipalArn"
    }
  }
}

module "assumable-role" {
  providers = {
    aws = aws.primary
  }
  for_each = data.external.environments.result
  source   = "../../modules/assumable-security-role"


  assume_role_policy_document = data.aws_iam_policy_document.assume-security-role[each.key].json
  environment                 = each.key
}
