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
# pylint: disable=unused-argument
import json
import os
import time
from typing import Any
from typing import Dict
from typing import Generator
from typing import Optional
from unittest.mock import Mock

import boto3
import pytest
from aws_lambda_typing.context.context import Context
from logs_subscription import logs_subscription_handler
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient

from cdh_core.enums.aws_test import build_region
from cdh_core.log.log_safe import GenericLambdaException
from cdh_core_dev_tools.testing.fixtures import mock_dynamodb  # pylint: disable=unused-import
from cdh_core_dev_tools.testing.fixtures import mock_sns  # pylint: disable=unused-import
from cdh_core_dev_tools.testing.fixtures import mock_sqs  # pylint: disable=unused-import
from cdh_core_dev_tools.testing.utils import build_and_set_moto_account_id


@pytest.mark.usefixtures("mock_sns", "mock_sqs", "mock_dynamodb")
class TestLogsSubscription:
    CONTEXT = Context()
    CONTEXT.invoked_function_arn = "1:2:3:4:5"
    MOTO_ACCOUNT_ID = build_and_set_moto_account_id()
    REGION = build_region().value
    DYNAMO_EVENTS_TABLE = "cdh-events-history"
    ALERTS_TOPIC_NAME = "my-alerts-topic"
    ALERTS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{MOTO_ACCOUNT_ID}:{ALERTS_TOPIC_NAME}"
    SQS_QUEUE_NAME = "my-sqs-queue"
    SQS_QUEUE_ARN = f"arn:aws:sqs:{REGION}:{MOTO_ACCOUNT_ID}:{SQS_QUEUE_NAME}"

    @pytest.fixture()
    def setup_environment_variables(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("RESOURCE_NAME_PREFIX", "")
        monkeypatch.setenv("ALERTS_TOPIC_ARN", self.ALERTS_TOPIC_ARN)
        monkeypatch.setenv("AWS_REGION", self.REGION)
        monkeypatch.setenv("SQS_QUEUE_NAME", self.SQS_QUEUE_NAME)

    @pytest.fixture()
    def moto_sqs(self, setup_environment_variables: Any) -> Generator[SQSClient, None, None]:
        sqs = boto3.client("sqs", region_name=self.REGION)
        sqs.create_queue(QueueName=os.environ["SQS_QUEUE_NAME"])
        yield sqs

    @pytest.fixture()
    def moto_sns(self, setup_environment_variables: Any, moto_sqs: Any) -> Generator[SNSClient, None, None]:
        sns = boto3.client("sns", region_name=self.REGION)
        sns.create_topic(Name=self.ALERTS_TOPIC_NAME)
        sns.subscribe(TopicArn=self.ALERTS_TOPIC_ARN, Protocol="sqs", Endpoint=self.SQS_QUEUE_ARN)
        yield sns

    @pytest.fixture()
    def moto_dynamodb(self, setup_environment_variables: Any) -> Generator[DynamoDBClient, None, None]:
        dynamodb = boto3.client("dynamodb", region_name=self.REGION)
        dynamodb.create_table(
            AttributeDefinitions=[
                {"AttributeName": "eventHash", "AttributeType": "S"},
            ],
            TableName=self.DYNAMO_EVENTS_TABLE,
            KeySchema=[
                {"AttributeName": "eventHash", "KeyType": "HASH"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.update_time_to_live(
            TableName="cdh-events-history", TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"}
        )
        yield dynamodb

    @staticmethod
    def create_logs_event_ok() -> Dict[str, Any]:
        return {
            "awslogs": {
                "data": "H4sIAAAAAAAAAL2QT2vjMBDFv0oQe4ywRrJkKbfAur20LCQ5bRKC/m1riO0gKW1DyHffcUqhhz0vCI0Y6b356V1JH3O2L3FzOUWyID+Xm+XhuV2vl48tmZPxfYgJ28qwpjFSS801to/jy2Mazye8qex7ro62d8FWPrx+MKaxUOv9eB5KpsHRfBn8p2hdUrQ9qjjjrGK8AlZtfzwtN+16s4fggpSmid7JWgVjRMNlCJxJVStwAS3y2WWfulPpxuGhO5aYMllsydN9/Kf54RsETsz0u4bs7xjtW0Q0VF5JF5BGSK5wATBdK6MlB8Hx1LBGCGgareR9B4ZPQHEpoQatNUOi0mF+xfYYBUgNotbGGCnY/CtXtN+2q9Wv1X5Xtl+pHKZI/tWYcqGMU2AzkAuhFsLM0e33rqgaAijlaTCKUcQU1ITgaW3/RJDOSQd6V/rLLKY0plmJH2U3ICDWZH2J4aGLx4CfvpIc32LqygXR7mTTqymRCfW/E95u+9tfdDspeoUCAAA="  # noqa: E501
            },
            "eventHash_expected": "ERROR cdhx008cdh-accounts-db-sync accounts_sync.accounts_sync",
        }

    def get_event_from_dynamo(self, dynamodb_client: DynamoDBClient, event_hash: str) -> Dict[str, Any]:
        item = dynamodb_client.get_item(TableName=self.DYNAMO_EVENTS_TABLE, Key={"eventHash": {"S": event_hash}}).get(
            "Item"
        )
        assert isinstance(item, dict)
        return item

    def get_event_from_sqs(self) -> Optional[Dict[str, Any]]:
        sqs_obj = boto3.resource("sqs", region_name=self.REGION)
        sqs_queue = sqs_obj.get_queue_by_name(QueueName=self.SQS_QUEUE_NAME)
        sqs_msgs = sqs_queue.receive_messages(
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            VisibilityTimeout=10,
            WaitTimeSeconds=1,
            MaxNumberOfMessages=5,
        )
        return json.loads(json.loads(sqs_msgs[0].body)["Message"]) if len(sqs_msgs) == 1 else None

    def test_handler_on_new_event(
        self, setup_environment_variables: Any, moto_sqs: Any, moto_sns: Any, moto_dynamodb: Any
    ) -> None:
        logs_subscription_handler.LOG.info = Mock()  # type: ignore
        logs_subscription_handler.LOG.error = Mock()  # type: ignore
        logs_subscription_handler.LOG.critical = Mock()  # type: ignore

        logs_subscription_handler.handler(self.create_logs_event_ok(), self.CONTEXT)

        event_saved = self.get_event_from_dynamo(moto_dynamodb, self.create_logs_event_ok()["eventHash_expected"])
        assert logs_subscription_handler.LOG.error.call_count == 0
        assert logs_subscription_handler.LOG.critical.call_count == 0
        assert event_saved["eventHash"]["S"] == self.create_logs_event_ok()["eventHash_expected"]
        event_sqs_current = self.get_event_from_sqs()
        assert event_sqs_current
        assert event_sqs_current["hash"] == self.create_logs_event_ok()["eventHash_expected"]

    def test_handler_on_duplicate_event(
        self, setup_environment_variables: Any, moto_sqs: Any, moto_sns: Any, moto_dynamodb: Any
    ) -> None:
        logs_subscription_handler.LOG.info = Mock()  # type: ignore
        logs_subscription_handler.LOG.error = Mock()  # type: ignore
        logs_subscription_handler.LOG.critical = Mock()  # type: ignore
        logs_subscription_handler.handler(self.create_logs_event_ok(), self.CONTEXT)
        event_dynamo_previous = self.get_event_from_dynamo(
            moto_dynamodb, self.create_logs_event_ok()["eventHash_expected"]
        )
        event_sqs_previous = self.get_event_from_sqs()
        time.sleep(1)  # to be able to test ttl updates, we need to sleep a bit

        logs_subscription_handler.handler(self.create_logs_event_ok(), self.CONTEXT)

        event_dynamo_current = self.get_event_from_dynamo(
            moto_dynamodb, self.create_logs_event_ok()["eventHash_expected"]
        )
        event_sqs_current = self.get_event_from_sqs()
        assert logs_subscription_handler.LOG.error.call_count == 0
        assert logs_subscription_handler.LOG.critical.call_count == 0
        assert event_sqs_previous
        assert event_sqs_previous["hash"] == self.create_logs_event_ok()["eventHash_expected"]
        assert event_dynamo_current["ttl"]["N"] > event_dynamo_previous["ttl"]["N"]
        assert event_dynamo_current["eventHash"]["S"] == self.create_logs_event_ok()["eventHash_expected"]
        assert not event_sqs_current  # check if sqs is empty

    def test_handler_on_error(self, setup_environment_variables: Any, moto_sns: Any, moto_dynamodb: Any) -> None:
        logs_subscription_handler.LOG.info = Mock()  # type: ignore
        logs_subscription_handler.LOG.error = Mock()  # type: ignore
        logs_subscription_handler.LOG.critical = Mock()  # type: ignore
        logs_subscription_handler.forward_event_to_sns = Mock()
        moto_dynamodb.delete_table(TableName=self.DYNAMO_EVENTS_TABLE)

        with pytest.raises(GenericLambdaException):
            logs_subscription_handler.handler(self.create_logs_event_ok(), self.CONTEXT)
        logs_subscription_handler.LOG.critical.assert_called()
        logs_subscription_handler.forward_event_to_sns.assert_not_called()

        assert self.get_event_from_sqs() is None
