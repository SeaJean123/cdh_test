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
from typing import Any
from typing import List
from unittest.mock import Mock

import boto3
import pytest
from cdh_core_api.config_test import build_config
from cdh_core_api.services.s3_bucket_manager import S3ResourceSpecification
from cdh_core_api.services.sns_topic_manager import SnsTopicManager

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.sns_client import SnsClient
from cdh_core.aws_clients.sns_client import TopicNotFound
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_kms_key_arn
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core.primitives.constants import CREATED_BY_CORE_API_TAG
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.utils import build_and_set_moto_account_id

MOTO_ACCOUNT_ID = AccountId(build_and_set_moto_account_id())


class TestSnsTopicManager:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_sns: Any) -> None:  # pylint: disable=unused-argument
        self.resource_account = build_resource_account()
        self.region = build_region(partition=self.resource_account.partition)
        self.config = build_config()
        self.sns_client = SnsClient(boto_sns_client=boto3.client("sns", region_name=self.region.value))
        self.aws = Mock(AwsClientFactory)
        self.aws.sns_client.return_value = self.sns_client
        self.dataset = build_dataset()
        self.specification = S3ResourceSpecification(
            dataset=self.dataset,
            stage=self.resource_account.stage,
            region=self.region,
            resource_account_id=self.resource_account.id,
            owner_id=build_account_id(),
        )
        self.topic_name = Builder.build_random_string()
        self.kms_key_arn = build_kms_key_arn()

    def test_create_topic_returns_correct_attributes(self) -> None:
        manager = SnsTopicManager(self.config, self.aws)
        topic = manager.create_topic(self.specification, self.topic_name, self.kms_key_arn)

        assert topic.name == self.topic_name
        # moto automatically creates the SNS topic in $MOTO_ACCOUNT_ID, so the ARN contains this accountId
        assert topic.arn == Arn(
            f"arn:{self.specification.region.partition.value}:sns:{self.specification.region.value}"
            f":{MOTO_ACCOUNT_ID}:{topic.name}"
        )
        assert topic.region is self.specification.region

        sns_client = boto3.client("sns", self.specification.region.value)
        assert sns_client.list_topics()["Topics"] == [{"TopicArn": str(topic.arn)}]
        self.aws.sns_client.assert_called_once_with(
            account_id=self.resource_account.id, account_purpose=AccountPurpose("resources"), region=self.region
        )

    def test_topic_has_correct_tags(self) -> None:
        sns_client = Mock(SnsClient)
        self.aws.sns_client.return_value = sns_client
        manager = SnsTopicManager(self.config, self.aws)
        topic = manager.create_topic(self.specification, self.topic_name, self.kms_key_arn)
        sns_client.create_sns.assert_called_once_with(
            topic.name, tags=CREATED_BY_CORE_API_TAG, kms_key_arn=self.kms_key_arn
        )

    def assert_has_readers(self, policy: PolicyDocument, expected_account_ids: List[AccountId]) -> None:
        expected_arns = [f"arn:aws:iam::{account_id}:root" for account_id in expected_account_ids]
        assert policy.get_principals_with_action("sns:Subscribe") == {"AWS": expected_arns}

    def test_create_topic_sets_correct_policy(self) -> None:
        manager = SnsTopicManager(self.config, self.aws)
        topic = manager.create_topic(self.specification, self.topic_name, self.kms_key_arn)

        client = self.aws.sns_client(MOTO_ACCOUNT_ID, Mock(), self.specification.region)
        policy = client.get_sns_policy(topic.arn)

        attribute_extractor_lambda_role_arn = self.config.get_s3_attribute_extractor_lambda_role_arn(
            AccountId(MOTO_ACCOUNT_ID), self.specification.region
        )

        assert policy.get_principals_with_action("sns:Publish") == {"AWS": [str(attribute_extractor_lambda_role_arn)]}

        self.assert_has_readers(policy, [self.specification.owner_id])

    def test_update_policy_transaction(self) -> None:
        manager = SnsTopicManager(self.config, self.aws)
        topic = manager.create_topic(self.specification, self.topic_name, self.kms_key_arn)
        readers = [build_account_id(), build_account_id()]
        client = self.aws.sns_client(MOTO_ACCOUNT_ID, Mock(), self.specification.region)

        with manager.update_policy_transaction(
            topic, owner_account_id=self.specification.owner_id, account_ids_with_read_access=readers
        ):
            policy = client.get_sns_policy(topic.arn)
            self.assert_has_readers(policy, [self.specification.owner_id] + readers)


class TestDeleteTopic:
    def setup_method(self) -> None:
        self.sns_client = Mock(SnsClient)
        self.aws = Mock(AwsClientFactory)
        self.aws.sns_client.return_value = self.sns_client
        self.topic_arn = build_arn(service="sns")
        self.sns_topic_manager = SnsTopicManager(build_config(), self.aws)

    def test_delete_successful(self) -> None:
        self.sns_topic_manager.delete_topic(topic_arn=self.topic_arn)
        self.sns_client.delete_sns_topic.assert_called_once_with(sns_arn=self.topic_arn)

    def test_topic_does_not_exist(self) -> None:
        self.sns_client.delete_sns_topic.side_effect = TopicNotFound(self.topic_arn)
        with pytest.raises(TopicNotFound):
            self.sns_topic_manager.delete_topic(topic_arn=self.topic_arn)
