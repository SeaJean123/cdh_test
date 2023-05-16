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
import json
from contextlib import contextmanager
from contextlib import suppress
from dataclasses import dataclass
from logging import getLogger
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Iterator

from cdh_core_api.config import Config

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.s3_client import BucketAlreadyExists
from cdh_core.aws_clients.s3_client import BucketNotEmpty
from cdh_core.aws_clients.s3_client import S3Client
from cdh_core.aws_clients.sns_client import SnsTopic
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn import build_arn_string
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.resource import S3Resource
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.constants import CREATED_BY_CORE_API_TAG

READ_ACCESS_STATEMENT_SID = "GrantGetBucket"
DEFAULT_S3_TAGS = CREATED_BY_CORE_API_TAG
LOG = getLogger(__name__)


@dataclass(frozen=True)
class S3ResourceSpecification:
    """Represents the specification of an s3 resource in the CDH."""

    dataset: Dataset
    stage: Stage
    region: Region
    resource_account_id: AccountId
    owner_id: AccountId


class S3BucketManager:
    """Handles s3 buckets."""

    def __init__(self, config: Config, aws: AwsClientFactory):
        self._config = config
        self._aws = aws

    @staticmethod
    def _get_account_bucket_count(client: S3Client) -> int:
        buckets = client.list_buckets()
        return len(buckets)

    def create_bucket(self, spec: S3ResourceSpecification, kms_key: KmsKey) -> Arn:
        """Create an encrypted s3 bucket with the given specification and kms encryption key."""
        client = self._aws.s3_client(
            account_id=spec.resource_account_id, account_purpose=AccountPurpose("resources"), region=spec.region
        )
        bucket_arn = self._create_available_encrypted_bucket(client, spec, kms_key)
        bucket_name = bucket_arn.identifier
        bucket_policy = self._create_initial_bucket_policy(
            bucket_arn=bucket_arn, owner_id=spec.owner_id, kms_key_arn=kms_key.arn
        )
        client.set_bucket_policy(bucket=bucket_name, policy=bucket_policy)
        client.block_public_access(bucket_name)
        client.set_bucket_cors(bucket_name)
        client.set_lifecycle_configuration(bucket_name)

        access_log_bucket_name = self._get_access_log_bucket_name(spec)
        access_log_prefix = self._build_access_log_prefix(bucket_name)
        client.enable_bucket_access_logging(
            bucket=bucket_name, log_bucket=access_log_bucket_name, log_prefix=access_log_prefix
        )

        if spec.stage in self._config.environment.stages_with_extended_metrics:
            client.enable_extended_metrics(bucket_name)

        total_account_buckets = self._get_account_bucket_count(client)
        LOG.info(f"Successfully created bucket: {bucket_arn}")
        LOG.info(
            json.dumps(
                {
                    "account_id": spec.resource_account_id,
                    "total_account_buckets": total_account_buckets,
                }
            )
        )

        return bucket_arn

    def delete_bucket(self, account_id: AccountId, region: Region, bucket_name: str) -> None:
        """Delete the given s3 bucket if it is empty."""
        client = self._aws.s3_client(account_id=account_id, account_purpose=AccountPurpose("resources"), region=region)
        if not client.is_empty(bucket_name=bucket_name):
            raise BucketNotEmpty(bucket=bucket_name)
        client.delete_bucket(bucket_name=bucket_name)

    def link_to_s3_attribute_extractor_lambda(self, bucket_name: str, topic: SnsTopic) -> None:
        """Enable notifications for object creation events in the s3 bucket via the given sns topic."""
        account_id = topic.arn.account_id
        region = topic.region
        client = self._aws.s3_client(account_id=account_id, account_purpose=AccountPurpose("resources"), region=region)

        # The s3 attribute extractor lambda needs this tag to find the topic for a given bucket.
        client.set_bucket_tags(bucket_name, tags={"snsTopicArn": str(topic.arn), **DEFAULT_S3_TAGS})

        s3_attribute_extractor_topic_arn = self._config.get_s3_attribute_extractor_topic_arn(account_id, region)
        client.send_creation_events_to_sns(bucket_name, s3_attribute_extractor_topic_arn)

    @contextmanager
    def update_bucket_policy_read_access_statement_transaction(
        self, s3_resource: S3Resource, account_ids_with_read_access: FrozenSet[AccountId]
    ) -> Iterator[None]:
        """Update the policy of the s3 bucket to enable read access for the given accounts."""
        s3_client = self._aws.s3_client(
            account_id=s3_resource.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=s3_resource.region,
        )

        old_policy = s3_client.get_bucket_policy(bucket=s3_resource.name)

        new_policy = self._create_new_policy(old_policy, s3_resource, account_ids_with_read_access)
        with s3_client.set_bucket_policy_transaction(
            bucket_name=s3_resource.name, old_policy=old_policy, new_policy=new_policy
        ):
            yield

    def _create_new_policy(
        self, old_policy: PolicyDocument, s3_resource: S3Resource, account_ids_with_read_access: FrozenSet[AccountId]
    ) -> PolicyDocument:
        if account_ids_with_read_access:
            statement = self._create_read_access_statement(s3_resource.arn, account_ids_with_read_access)
            return old_policy.add_or_update_statement(statement)
        return old_policy.delete_statement_if_present(sid=READ_ACCESS_STATEMENT_SID)

    def _create_available_encrypted_bucket(
        self, client: S3Client, spec: S3ResourceSpecification, kms_key: KmsKey
    ) -> Arn:
        for _ in range(10):
            with suppress(BucketAlreadyExists):  # Retry if bucket already exists
                bucket_name = spec.dataset.build_cdh_bucket_name(self._config.prefix)
                return client.create_encrypted_bucket(
                    name=bucket_name,
                    region=spec.region,
                    kms_key_arn=kms_key.arn,
                    tags=DEFAULT_S3_TAGS,
                )
        raise RuntimeError("Couldn't find any available S3 bucket name. Something is wrong here.")

    @staticmethod
    def _create_initial_bucket_policy(bucket_arn: Arn, owner_id: AccountId, kms_key_arn: Arn) -> PolicyDocument:
        statements = [
            {
                "Sid": "AllowWriteForOwner",
                "Effect": "Allow",
                "Principal": {
                    "AWS": build_arn_string(
                        service="iam", region=None, account=owner_id, resource="root", partition=bucket_arn.partition
                    )
                },
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:DeleteObject",
                    "s3:DeleteObjectTagging",
                    "s3:DeleteObjectVersion",
                    "s3:DeleteObjectVersionTagging",
                    "s3:ObjectOwnerOverrideToBucketOwner",
                    "s3:PutObjectTagging",
                    "s3:PutObjectVersionTagging",
                    "s3:PutLifecycleConfiguration",
                ],
                "Resource": [f"{bucket_arn}/*", f"{bucket_arn}"],
            },
            {
                "Sid": "AllowPutObjectForOwner",
                "Effect": "Allow",
                "Principal": {
                    "AWS": build_arn_string(
                        service="iam", region=None, account=owner_id, resource="root", partition=bucket_arn.partition
                    )
                },
                "Action": "s3:PutObject",
                "Resource": f"{bucket_arn}/*",
            },
            {
                "Sid": "AllowGetBucketForOwner",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        build_arn_string(
                            service="iam",
                            region=None,
                            account=owner_id,
                            resource="root",
                            partition=bucket_arn.partition,
                        )
                    ]
                },
                "Action": ["s3:Get*", "s3:List*"],
                "Resource": [f"{bucket_arn}", f"{bucket_arn}/*"],
            },
            {
                "Sid": "DenyNonHTTPS",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [f"{bucket_arn}", f"{bucket_arn}/*"],
                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
            },
            S3BucketManager._get_kms_encryption_restriction_statement(bucket_arn),
            S3BucketManager._get_kms_correct_key_restriction_statement(bucket_arn, kms_key_arn),
        ]
        return PolicyDocument.create_bucket_policy(statements)

    @staticmethod
    def _get_kms_correct_key_restriction_statement(bucket_arn: Arn, kms_key_arn: Arn) -> Dict[str, Any]:
        return {
            "Sid": "RestrictToCorrectKmsKeyIfSseEnabled",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObject",
            "Resource": f"{bucket_arn}/*",
            "Condition": {
                "StringNotEqualsIfExists": {
                    "s3:x-amz-server-side-encryption-aws-kms-key-id": str(kms_key_arn)  # true if key is not
                    # supplied (which means AWS managed CMK) or supplied key is not bucket default key
                },
                "StringEquals": {"s3:x-amz-server-side-encryption": "aws:kms"},  # true if encryption method is aws:kms
            },
        }

    @staticmethod
    def _get_kms_encryption_restriction_statement(bucket_arn: Arn) -> Dict[str, Any]:
        return {
            "Sid": "RestrictToDefaultOrKmsEncryption",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutObject",
            "Resource": f"{bucket_arn}/*",
            "Condition": {
                "Null": {"s3:x-amz-server-side-encryption": "false"},  # true if key exists and value is not null
                "StringNotEquals": {
                    "s3:x-amz-server-side-encryption": "aws:kms"  # true if encryption method is not aws:kms
                },
            },
        }

    @staticmethod
    def _create_read_access_statement(
        bucket_arn: Arn, account_ids_with_read_access: FrozenSet[AccountId]
    ) -> Dict[str, Any]:
        return {
            "Sid": READ_ACCESS_STATEMENT_SID,
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    build_arn_string(
                        service="iam", region=None, account=account_id, resource="root", partition=bucket_arn.partition
                    )
                    for account_id in account_ids_with_read_access
                ]
            },
            "Action": ["s3:Get*", "s3:List*"],
            "Resource": [f"{bucket_arn}", f"{bucket_arn}/*"],
        }

    def _get_access_log_bucket_name(self, spec: S3ResourceSpecification) -> str:
        """Get the name of the bucket used for storing access logs, has to exist."""
        return "-".join([self._config.prefix + "cdh-core-s3-logging", spec.resource_account_id, spec.region.value])

    def _build_access_log_prefix(self, bucket_name: str) -> str:
        """Build the S3 prefix(path) to prepend for access log objects."""
        return "/".join([self._config.prefix + "logs", bucket_name, ""])  # end with slash
