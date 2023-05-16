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

resource "aws_iam_role" "cdh-assumable-billing" {
  name        = var.assumable_billing_role_name
  description = "The lambda function running in the cdh api account is supposed to assume this role to get the cost and usage from this account"
  path        = var.assumable_billing_role_path

  assume_role_policy = data.aws_iam_policy_document.cdh-assumable-billing.json
}

data "aws_iam_policy_document" "cdh-assumable-billing" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = var.billing_trustees
    }
  }
}

resource "aws_iam_role_policy_attachment" "cdh-assumable-billing" {
  role       = aws_iam_role.cdh-assumable-billing.name
  policy_arn = aws_iam_policy.cost-explorer-usage.arn
}

resource "aws_iam_policy" "cost-explorer-usage" {
  name        = "cdh-core-ce-getCostAndUsage"
  description = "Allow usage of cost explorer"
  path        = var.assumable_billing_role_path
  policy      = data.aws_iam_policy_document.cost-explorer-usage.json
}

data "aws_iam_policy_document" "cost-explorer-usage" {
  statement {
    effect    = "Allow"
    actions   = ["ce:GetCostAndUsage", "ce:GetCostForecast"]
    resources = ["*"]
  }
}
