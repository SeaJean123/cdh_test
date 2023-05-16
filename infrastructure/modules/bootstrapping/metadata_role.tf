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

resource "aws_iam_role" "cdh-assumable-metadata" {
  name        = var.assumable_metadata_role_name
  description = "Role is used to manage glue metadata."
  path        = var.assumable_metadata_role_path

  assume_role_policy = data.aws_iam_policy_document.cdh-assumable-metadata.json
}

data "aws_iam_policy_document" "cdh-assumable-metadata" {
  statement {
    actions = [
      "sts:AssumeRole"
    ]

    principals {
      type = "AWS"
      identifiers = [
        "arn:${data.aws_partition.current.partition}:iam::${var.api_account_id}:root"
      ]
    }
  }
}

resource "aws_iam_role_policy_attachment" "cdh-assumable-metadata" {
  role       = aws_iam_role.cdh-assumable-metadata.name
  policy_arn = aws_iam_policy.glue-access.arn
}

resource "aws_iam_policy" "glue-access" {
  name        = var.assumable_metadata_role_name
  description = "Allow managing glue metadata"
  path        = var.assumable_metadata_role_path
  policy      = data.aws_iam_policy_document.glue-access.json
}

data "aws_iam_policy_document" "glue-access" {
  statement {
    sid    = "AllowGlue"
    effect = "Allow"
    actions = [
      # read *
      "glue:Get*",
      "glue:BatchGet*",
      "glue:List*",
      # policy
      "glue:PutResourcePolicy",
      "glue:DeleteResourcePolicy",
      # database
      "glue:CreateDatabase",
      "glue:UpdateDatabase",
      "glue:DeleteDatabase",
      # table
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      # table version
      "glue:BatchDeleteTableVersion",
      # partition
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "AllowKms"
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = ["arn:${data.aws_partition.current.partition}:kms:*:${data.aws_caller_identity.current.account_id}:key/*"]
    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values   = ["glue.*.amazonaws.com"]
    }
  }
}
