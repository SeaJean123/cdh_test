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
from hashlib import sha256
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError

from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.sns_client import SnsClient
from cdh_core.aws_clients.utils import get_error_code
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_kms_key_arn
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.utils import build_and_set_moto_account_id

MOTO_ACCOUNT_ID = AccountId(build_and_set_moto_account_id())


class TestSnsClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_sns: Any) -> None:  # pylint: disable=unused-argument
        self.region = build_region()
        self.boto_sns_client = boto3.client("sns", region_name=self.region.value)
        self.sns_client = SnsClient(self.boto_sns_client)
        self.sns_name = Builder.build_random_string()
        self.kms_key_arn = build_kms_key_arn()

    def build_policy_document(self, sns_arn: Arn) -> PolicyDocument:
        return PolicyDocument.create_sns_policy(
            [
                {
                    "Sid": "SampleSid",
                    "Effect": "Allow",
                    "Action": "sns:Publish",
                    "Resource": str(sns_arn),
                    "Principal": {"AWS": str(build_role_arn())},
                }
            ]
        )

    def check_sns_topic_exists(self, topic_arn: str) -> bool:
        try:
            self.boto_sns_client.get_topic_attributes(TopicArn=topic_arn)
            return True
        except ClientError as error:
            if get_error_code(error) == "NotFound":
                return False
            raise error

    def test_create_sns(self) -> None:
        sns_arn = self.sns_client.create_sns(name=self.sns_name, kms_key_arn=self.kms_key_arn)

        sns_arn_expected = build_arn("sns", self.sns_name, account_id=MOTO_ACCOUNT_ID, region=self.region)

        assert sns_arn == sns_arn_expected

        kms_key = self.boto_sns_client.get_topic_attributes(TopicArn=str(sns_arn))["Attributes"]["KmsMasterKeyId"]

        assert kms_key == str(self.kms_key_arn)

    def test_create_sns_without_encryption_key(self) -> None:
        sns_arn = self.sns_client.create_sns(name=self.sns_name)

        sns_arn_expected = build_arn("sns", self.sns_name, account_id=MOTO_ACCOUNT_ID, region=self.region)
        assert sns_arn == sns_arn_expected

    def test_get_sns_policy(self) -> None:
        sns_arn = Arn(self.boto_sns_client.create_topic(Name=self.sns_name)["TopicArn"])
        sns_policy_expected = self.build_policy_document(sns_arn)
        self.boto_sns_client.set_topic_attributes(
            TopicArn=str(sns_arn),
            AttributeName="Policy",
            AttributeValue=sns_policy_expected.encode(),
        )

        sns_policy = self.sns_client.get_sns_policy(sns_arn)
        assert sns_policy == sns_policy_expected

    def test_set_sns_policy(self) -> None:
        sns_arn = Arn(self.boto_sns_client.create_topic(Name=self.sns_name)["TopicArn"])
        sns_policy_expected = self.build_policy_document(sns_arn)
        self.sns_client.set_sns_policy(sns_arn, sns_policy_expected)

        sns_policy = PolicyDocument.create_sns_policy(
            json.loads(self.boto_sns_client.get_topic_attributes(TopicArn=str(sns_arn))["Attributes"]["Policy"])[
                "Statement"
            ]
        )
        assert sns_policy == sns_policy_expected

    def test_set_sns_policy_transaction_successful(self) -> None:
        sns_arn = Arn(self.boto_sns_client.create_topic(Name=self.sns_name)["TopicArn"])
        sns_policy_initial = self.build_policy_document(sns_arn)
        self.boto_sns_client.set_topic_attributes(
            TopicArn=str(sns_arn),
            AttributeName="Policy",
            AttributeValue=sns_policy_initial.encode(),
        )

        sns_policy_expected = self.build_policy_document(sns_arn)
        with self.sns_client.set_sns_policy_transaction(sns_arn, sns_policy_expected):
            assert self.sns_client.get_sns_policy(sns_arn) == sns_policy_expected
        assert self.sns_client.get_sns_policy(sns_arn) == sns_policy_expected

    def test_set_sns_policy_transaction_rollback(self) -> None:
        sns_arn = Arn(self.boto_sns_client.create_topic(Name=self.sns_name)["TopicArn"])
        sns_policy_initial = self.build_policy_document(sns_arn)
        self.boto_sns_client.set_topic_attributes(
            TopicArn=str(sns_arn),
            AttributeName="Policy",
            AttributeValue=sns_policy_initial.encode(),
        )

        sns_policy_expected = self.build_policy_document(sns_arn)
        error = Exception("my error")
        with pytest.raises(Exception) as exc_info:
            with self.sns_client.set_sns_policy_transaction(sns_arn, sns_policy_expected):
                assert self.sns_client.get_sns_policy(sns_arn) == sns_policy_expected
                raise error
        assert exc_info.value == error
        assert self.sns_client.get_sns_policy(sns_arn) == sns_policy_initial

    def test_delete_sns_topic(self) -> None:
        sns_arn = Arn(self.boto_sns_client.create_topic(Name=self.sns_name)["TopicArn"])
        assert self.check_sns_topic_exists(str(sns_arn))
        self.sns_client.delete_sns_topic(sns_arn)
        assert self.check_sns_topic_exists(str(sns_arn)) is False

    def test_publish_message_non_fifo(self) -> None:
        boto_sns_client = Mock()
        sns_client = SnsClient(boto_sns_client)
        sns_arn = build_arn("sns")
        subject = Builder.build_random_string()
        body = Builder.build_random_string()
        randict = {"id": "some_id", "random_value": "some_text_value"}

        sns_client.publish_message(sns_arn=sns_arn, message_subject=subject, message_body=body, attributes=randict)

        boto_sns_client.publish.assert_called_once_with(
            TopicArn=str(sns_arn),
            Message=body,
            Subject=subject,
            MessageAttributes={
                "id": {"DataType": "String", "StringValue": "some_id"},
                "random_value": {"DataType": "String", "StringValue": "some_text_value"},
            },
        )

    def test_publish_message_fifo(self) -> None:
        boto_sns_client = Mock()
        sns_client = SnsClient(boto_sns_client)
        sns_arn = build_arn("sns", resource=Builder.build_random_string() + ".fifo")
        subject = Builder.build_random_string()
        body = Builder.build_random_string()
        randict = {"id": "some_id", "random_value": "some_text_value"}

        sns_client.publish_message(sns_arn=sns_arn, message_subject=subject, message_body=body, attributes=randict)

        boto_sns_client.publish.assert_called_once_with(
            TopicArn=str(sns_arn),
            Message=body,
            Subject=subject,
            MessageAttributes={
                "id": {"DataType": "String", "StringValue": "some_id"},
                "random_value": {"DataType": "String", "StringValue": "some_text_value"},
            },
            MessageGroupId="primary",
            MessageDeduplicationId=sha256((subject + body).encode("utf-8")).hexdigest(),
        )

    def test_publish_message_omits_with_empty_attributes(self) -> None:
        boto_sns_client = Mock()
        sns_client = SnsClient(boto_sns_client)
        sns_arn = Arn(self.boto_sns_client.create_topic(Name=Builder.build_random_string())["TopicArn"])
        subject = Builder.build_random_string()
        body = Builder.build_random_string()

        sns_client.publish_message(
            sns_arn=sns_arn, message_subject=subject, message_body=body, attributes={"empty": ""}
        )

        boto_sns_client.publish.assert_called_once_with(
            TopicArn=str(sns_arn),
            Message=body,
            Subject=subject,
            MessageAttributes={},
        )

    def test_is_not_fifo(self) -> None:
        assert not self.sns_client.is_fifo_topic(build_arn("sns"))

    def test_is_fifo(self) -> None:
        arn = build_arn("sns", resource=Builder.build_random_string() + ".fifo")
        assert self.sns_client.is_fifo_topic(arn)
