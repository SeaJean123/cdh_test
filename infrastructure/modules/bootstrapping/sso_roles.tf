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

locals {
  expected_saml_provider_arn = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:saml-provider/${local.saml_provider_name}"
  saml_provider_arn          = var.create_idp ? aws_iam_saml_provider.saml_provider[0].arn : data.aws_iam_saml_provider.saml_provider[0].arn
}

# either create the idp or assert it is present with a data block
resource "aws_iam_saml_provider" "saml_provider" {
  count                  = var.create_idp ? 1 : 0
  name                   = local.saml_provider_name
  saml_metadata_document = file(var.saml_file_path)
}
data "aws_iam_saml_provider" "saml_provider" {
  count = var.create_idp ? 0 : 1
  arn   = local.expected_saml_provider_arn
}

resource "aws_iam_role" "sso-role" {
  for_each             = local.sso_role_names
  name                 = each.key
  description          = "SSO-enabled ${each.key} role"
  max_session_duration = 8 * 60 * 60

  assume_role_policy = data.aws_iam_policy_document.sso-role.json
}

data "aws_iam_policy_document" "sso-role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithSAML", "sts:TagSession", "sts:SetSourceIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.saml_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "SAML:aud"
      values   = ["https://signin.aws.amazon.com/saml", local.provider_url]
    }
  }

  statement {
    sid     = "DenyAssume"
    effect  = "Deny"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "cdh-devops-admin-access-attachment" {
  role       = aws_iam_role.sso-role[local.devops_role_name].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AdministratorAccess"
}

resource "aws_iam_role_policy_attachment" "cdh-readonly-access-attachment" {
  role       = aws_iam_role.sso-role[local.readonly_role_name].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "cdh-readonly-execute-api" {
  role       = aws_iam_role.sso-role[local.readonly_role_name].name
  policy_arn = aws_iam_policy.cdh-readonly-execute-api.arn
}

resource "aws_iam_policy" "cdh-readonly-execute-api" {
  name        = "${local.readonly_role_name}-allow-execute-api"
  description = "Role ${local.devops_role_name} should be able to call Core API GET endpoints"
  path        = "/cdh/"
  policy      = data.aws_iam_policy_document.cdh-readonly-execute-api.json
}

data "aws_iam_policy_document" "cdh-readonly-execute-api" {
  statement {
    sid     = "AllowGETRequests"
    effect  = "Allow"
    actions = ["execute-api:*"]
    resources = [
      "arn:${data.aws_partition.current.partition}:execute-api:*:${var.api_account_id}:*/default/OPTIONS/*",
      "arn:${data.aws_partition.current.partition}:execute-api:*:${var.api_account_id}:*/default/GET/*",
    ]
  }
}
