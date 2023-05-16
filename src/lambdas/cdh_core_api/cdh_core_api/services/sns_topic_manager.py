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
from typing import ContextManager
from typing import List

from cdh_core_api.config import Config
from cdh_core_api.services.s3_bucket_manager import S3ResourceSpecification

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.sns_client import SnsTopic
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn import build_arn_string
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.iterables import unique
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.constants import CREATED_BY_CORE_API_TAG


class SnsTopicManager:
    """Manages SNS Topics created by Core API."""

    def __init__(self, config: Config, aws: AwsClientFactory):
        self._aws = aws
        self._config = config

    def create_topic(self, specification: S3ResourceSpecification, topic_name: str, kms_key_arn: Arn) -> SnsTopic:
        """
        Create an SNS topic as specified.

        The topic's policy gives the owner read access and allows the Attribute Extractor Lambda to publish messages.
        """
        client = self._aws.sns_client(
            account_id=specification.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=specification.region,
        )
        topic_arn = client.create_sns(topic_name, tags=CREATED_BY_CORE_API_TAG, kms_key_arn=kms_key_arn)
        topic = SnsTopic(
            name=topic_name,
            arn=topic_arn,
            region=specification.region,
        )
        policy = self._create_topic_policy(
            topic,
            account_ids_with_read_access=[specification.owner_id],
            attribute_extractor_lambda_arn=self._config.get_s3_attribute_extractor_lambda_role_arn(
                topic.arn.account_id, topic.region
            ),
        )
        client.set_sns_policy(topic.arn, policy)
        return topic

    def delete_topic(self, topic_arn: Arn) -> None:
        """Delete an SNS topic."""
        client = self._aws.sns_client(
            account_id=topic_arn.account_id,
            account_purpose=AccountPurpose("resources"),
            region=Region(topic_arn.region),
        )
        client.delete_sns_topic(sns_arn=topic_arn)

    def update_policy_transaction(
        self, topic: SnsTopic, owner_account_id: AccountId, account_ids_with_read_access: List[AccountId]
    ) -> ContextManager[None]:
        """
        Update an SNS topic's policy.

        The transactional style enables rollback in the case of failure of subsequent operations.
        """
        resource_account_id = topic.arn.account_id
        policy = self._create_topic_policy(
            topic=topic,
            account_ids_with_read_access=unique([owner_account_id] + account_ids_with_read_access),
            attribute_extractor_lambda_arn=self._config.get_s3_attribute_extractor_lambda_role_arn(
                topic.arn.account_id, topic.region
            ),
        )
        client = self._aws.sns_client(
            account_id=resource_account_id, account_purpose=AccountPurpose("resources"), region=topic.region
        )
        return client.set_sns_policy_transaction(topic.arn, policy)

    @staticmethod
    def _create_topic_policy(
        topic: SnsTopic, account_ids_with_read_access: List[AccountId], attribute_extractor_lambda_arn: Arn
    ) -> PolicyDocument:
        return PolicyDocument.create_sns_policy(
            [
                {
                    "Sid": "AllowLambdaToPublishToSNSTopic",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": str(attribute_extractor_lambda_arn),
                    },
                    "Action": "sns:Publish",
                    "Resource": str(topic.arn),
                },
                {
                    "Sid": "AllowSubscribe",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            build_arn_string(
                                service="iam",
                                partition=topic.arn.partition,
                                region=None,
                                account=account_id,
                                resource="root",
                            )
                            for account_id in account_ids_with_read_access
                        ]
                    },
                    "Action": ["sns:Subscribe"],
                    "Resource": str(topic.arn),
                    "Condition": {"StringNotEquals": {"sns:Protocol": ["email", "email-json", "sms"]}},
                },
                {
                    "Sid": "AllowGet",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            build_arn_string(
                                service="iam",
                                partition=topic.arn.partition,
                                region=None,
                                account=account_id,
                                resource="root",
                            )
                            for account_id in account_ids_with_read_access
                        ]
                    },
                    "Action": ["sns:ListSubscriptionsByTopic", "sns:GetTopicAttributes"],
                    "Resource": str(topic.arn),
                },
            ]
        )
