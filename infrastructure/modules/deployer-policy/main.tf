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

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.63"
    }
  }
}

variable "role_name" {
  type        = string
  description = "Role to attach to the policy"
}


resource "aws_iam_role_policy" "oss_deployment" {
  name   = "platform-deployment-policy"
  role   = var.role_name
  policy = data.aws_iam_policy_document.deployer_policy.json
}


data "aws_iam_policy_document" "deployer_policy" {
  statement {
    sid = "IAM"
    actions = [
      "iam:Get*",
      "iam:List*",
      "iam:AttachRolePolicy",
      "iam:CreatePolicy",
      "iam:CreatePolicyVersion",
      "iam:CreateRole",
      "iam:CreateServiceLinkedRole",
      "iam:DeletePolicy",
      "iam:DeleteRole",
      "iam:DeleteRolePolicy",
      "iam:DeletePolicyVersion",
      "iam:DetachRolePolicy",
      "iam:PassRole",
      "iam:PutRolePolicy",
      "iam:UpdateAssumeRolePolicy",
      "iam:UpdateRole",
      "iam:UpdateRoleDescription",
    ]
    resources = ["*"]
  }

  statement {
    sid     = "DenyTerraformDeployer"
    effect  = "Deny"
    actions = ["iam:*"]
    resources = [
      "arn:aws:iam::*:role/${var.role_name}",
      "arn:aws:iam::*:policy/${var.role_name}",
    ]
  }

  statement {
    sid = "KMS"
    actions = [
      "kms:List*",
      "kms:Get*",
      "kms:Describe*",
      # Encrypt + Decrypt are necessary to let Terraform upload files to S3 (e.g. lambda-storage-bucket)
      "kms:GenerateDataKey*",
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:CreateAlias",
      "kms:DeleteAlias",
      "kms:CreateKey",
      "kms:PutKeyPolicy",
      "kms:ScheduleKeyDeletion"
    ]
    resources = ["*"]
  }

  statement {
    sid = "other"
    actions = [
      "acm:*",
      "apigateway:*",
      "athena:*",
      "autoscaling:*",
      "cloudwatch:*",
      "cloudfront:*",
      "globalaccelerator:*",
      "elasticloadbalancing:*",
      "ecs:*",
      "dynamodb:*",
      "ec2:*",
      "events:*",
      "glue:*",
      "lambda:*",
      "lakeformation:*",
      "logs:*",
      "route53:*",
      "s3:*",
      "ssm:*",
      "secretsmanager:*",
      "sns:*",
      "sqs:*",
      "states:*",
      "xray:*",
    ]
    resources = ["*"]
  }
}
