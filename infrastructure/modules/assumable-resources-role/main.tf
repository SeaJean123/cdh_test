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

variable "allowed_arn_list" {
  type = list(string)
}

variable "resource_name_prefix" {
  type = string
}

variable "lakeformation_registration_role" {
  type = string
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  name = "${var.resource_name_prefix}cdh-core-api-assumable-resources"
}


resource "aws_iam_role" "cdh_core_api_assumable_resources" {
  name        = local.name
  description = "Used by Core API to provision resources"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "AWS" : var.allowed_arn_list
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "cdh_core_api_assumable_resources" {
  policy_arn = aws_iam_policy.cdh_core_api_assumable_resources.arn
  role       = aws_iam_role.cdh_core_api_assumable_resources.name
}

resource "aws_iam_policy" "cdh_core_api_assumable_resources" {
  name   = local.name
  policy = data.aws_iam_policy_document.cdh_core_api_assumable_resources.json
}

data "aws_iam_policy_document" "cdh_core_api_assumable_resources" {
  statement {
    sid    = "S3"
    effect = "Allow"
    actions = [
      "s3:DeleteBucket",
      "s3:PutAnalyticsConfiguration",
      "s3:GetObjectVersionTagging",
      "s3:CreateBucket",
      "s3:GetObjectAcl",
      "s3:PutLifecycleConfiguration",
      "s3:GetObjectVersionAcl",
      "s3:PutBucketAcl",
      "s3:HeadBucket",
      "s3:GetBucketPolicyStatus",
      "s3:PutAccountPublicAccessBlock",
      "s3:GetBucketWebsite",
      "s3:PutReplicationConfiguration",
      "s3:GetBucketNotification",
      "s3:PutBucketCORS",
      "s3:DeleteBucketPolicy",
      "s3:GetReplicationConfiguration",
      "s3:ListMultipartUploadParts",
      "s3:GetObject",
      "s3:PutBucketNotification",
      "s3:PutBucketLogging",
      "s3:PutObjectVersionAcl",
      "s3:GetAnalyticsConfiguration",
      "s3:GetObjectVersionForReplication",
      "s3:GetLifecycleConfiguration",
      "s3:ListBucketByTags",
      "s3:GetInventoryConfiguration",
      "s3:GetBucketTagging",
      "s3:PutAccelerateConfiguration",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:GetBucketLogging",
      "s3:ListBucketVersions",
      "s3:RestoreObject",
      "s3:PutBucketTagging",
      "s3:GetAccelerateConfiguration",
      "s3:GetBucketPolicy",
      "s3:PutEncryptionConfiguration",
      "s3:GetEncryptionConfiguration",
      "s3:GetObjectVersionTorrent",
      "s3:GetBucketRequestPayment",
      "s3:GetObjectTagging",
      "s3:GetMetricsConfiguration",
      "s3:PutBucketVersioning",
      "s3:PutObjectAcl",
      "s3:GetBucketPublicAccessBlock",
      "s3:ListBucketMultipartUploads",
      "s3:PutBucketPublicAccessBlock",
      "s3:PutMetricsConfiguration",
      "s3:GetBucketVersioning",
      "s3:GetBucketAcl",
      "s3:PutInventoryConfiguration",
      "s3:GetObjectTorrent",
      "s3:ObjectOwnerOverrideToBucketOwner",
      "s3:GetAccountPublicAccessBlock",
      "s3:PutBucketRequestPayment",
      "s3:GetBucketCORS",
      "s3:PutBucketPolicy",
      "s3:GetBucketLocation",
      "s3:GetObjectVersion",
      "s3:PutBucketOwnershipControls",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.resource_name_prefix}cdh-*", "arn:${data.aws_partition.current.partition}:s3:::${var.resource_name_prefix}cdh-*/*"]
  }
  statement {
    sid       = "s3listall"
    effect    = "Allow"
    actions   = ["s3:ListAllMyBuckets", "s3:ListBucket"]
    resources = ["*"]
  }
  statement {
    sid    = "Sns"
    effect = "Allow"
    // Allows to create, list, put sns topics and policies to topics
    actions = [
      "sns:ListSubscriptionsByTopic",
      "sns:GetTopicAttributes",
      "sns:DeleteTopic",
      "sns:CreateTopic",
      "sns:ListTopics",
      "sns:Unsubscribe",
      "sns:SetTopicAttributes",
      "sns:GetSubscriptionAttributes",
      "sns:ListSubscriptions",
      "sns:AddPermission",
      "sns:GetEndpointAttributes",
      "sns:SetSubscriptionAttributes",
      "sns:ConfirmSubscription",
      "sns:RemovePermission",
      "sns:TagResource",
      "sns:UntagResource"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:sns:*:${data.aws_caller_identity.current.account_id}:${var.resource_name_prefix}cdh-*"]
  }
  statement {
    sid    = "GlueSyncsGlue"
    effect = "Allow"
    actions = [
      "glue:DeleteDatabase",
      "glue:CreateDatabase"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:database/${var.resource_name_prefix}*",
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:table/${var.resource_name_prefix}*/*",
      "arn:${data.aws_partition.current.partition}:glue:*:${data.aws_caller_identity.current.account_id}:userDefinedFunction/${var.resource_name_prefix}*/*"
    ]
  }
  statement {
    sid    = "GlueSyncsRAM"
    effect = "Allow"
    actions = [
      "ram:CreateResourceShare",
      "ram:TagResource",
      "ram:GetResourceShares",
      "ram:DeleteResourceShare",
      "ram:GetResourceShareAssociations",
      "ram:AssociateResourceShare",
      "ram:AssociateResourceSharePermission",
      "glue:PutResourcePolicy",
      "glue:DeleteResourcePolicy"
    ]
    resources = ["*"]
  }
  statement {
    sid    = "Metrics"
    effect = "Allow"
    actions = [
      "cloudwatch:GetMetricData",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:ListMetrics"
    ]
    resources = ["*"]
  }
  statement {
    sid    = "Config"
    effect = "Allow"
    actions = [
      "config:GetResourceConfigHistory"
    ]
    resources = ["*"]
  }
}


resource "aws_iam_policy" "cdh_core_api_assumable_resources_iam" {
  name   = "${local.name}-iam"
  policy = data.aws_iam_policy_document.cdh_core_api_assumable_resources_iam.json
}

resource "aws_iam_role_policy_attachment" "cdh_core_api_assumable_resources_iam" {
  policy_arn = aws_iam_policy.cdh_core_api_assumable_resources_iam.arn
  role       = aws_iam_role.cdh_core_api_assumable_resources.name
}

data "aws_iam_policy_document" "cdh_core_api_assumable_resources_iam" {
  statement {
    sid    = "PolicyAccess"
    effect = "Allow"
    actions = [
      "iam:CreatePolicy",
      "iam:GetPolicy",
      "iam:DeletePolicy",
      "iam:ListPolicyVersions",
      "iam:GetPolicyVersion",
      "iam:CreatePolicyVersion",
      "iam:DeletePolicyVersion",
      "iam:SetDefaultPolicyVersion"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/${var.resource_name_prefix}*"]
  }

  statement {
    sid       = "LakeformationRegistration"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [var.lakeformation_registration_role]
  }
}

resource "aws_iam_role_policy_attachment" "cdh_core_api_assumable_resources_lakeformation_data_admin" {
  role       = aws_iam_role.cdh_core_api_assumable_resources.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AWSLakeFormationDataAdmin"
}

resource "aws_iam_role_policy_attachment" "cdh_core_api_assumable_resources_lakeformation_cross_account" {
  role       = aws_iam_role.cdh_core_api_assumable_resources.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AWSLakeFormationCrossAccountManager"
}

output "assumable_role_name" {
  value = aws_iam_role.cdh_core_api_assumable_resources.name
}

output "assumable_role_arn" {
  value = aws_iam_role.cdh_core_api_assumable_resources.arn
}
