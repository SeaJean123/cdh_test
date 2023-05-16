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
import random
from dataclasses import dataclass
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from unittest.mock import Mock

import boto3
import pytest
from mypy_boto3_sqs.type_defs import MessageTypeDef
from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef

from cdh_core.aws_clients.sqs_client import MessageDeliveryFailed
from cdh_core.aws_clients.sqs_client import SqsClient
from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.utils import build_and_set_moto_account_id

MOTO_ACCOUNT_ID = AccountId(build_and_set_moto_account_id())
MOCK_SQS_ERROR = {"Code": "some error", "Message": "for reasons unknown"}


class TestSqsClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_sqs: Any) -> None:  # pylint: disable=unused-argument
        self.boto_sqs_client = boto3.client("sqs", region_name=build_region().value)
        self.queue_name = "test-queue"
        self.queue_url = self.boto_sqs_client.create_queue(QueueName=self.queue_name)["QueueUrl"]

    def test_get_queue_url(self) -> None:
        sqs_client = SqsClient(self.boto_sqs_client)
        assert sqs_client.get_queue_url(self.queue_name, MOTO_ACCOUNT_ID) == self.queue_url

    def test_send_single_message(self) -> None:
        message = {"hello": "world"}
        sqs_client = SqsClient(self.boto_sqs_client)

        sqs_client.send_messages(self.queue_url, [message])

        received = self.fetch_messages_from_sqs()
        assert len(received) == 1
        assert received[0]["Body"] == json.dumps(message)

    @pytest.mark.parametrize("number_of_messages", [10, 31, 49])
    @pytest.mark.parametrize("with_attributes", [True, False])
    def test_message_batching(self, number_of_messages: int, with_attributes: bool) -> None:
        messages = [{"text": f"{i:05d}"} for i in range(number_of_messages)]
        attributes = {"some": {"StringValue": "attribute", "DataType": "String"}}
        sqs_client = SqsClient(self.boto_sqs_client)

        if with_attributes:
            sqs_client.send_messages(self.queue_url, messages, attributes)
        else:
            sqs_client.send_messages(self.queue_url, messages)

        received = self.fetch_messages_from_sqs()
        assert len(received) == len(messages)
        assert {msg["Body"] for msg in received} == {json.dumps(message) for message in messages}
        if with_attributes:
            assert {msg["MessageAttributes"] == attributes for msg in received}
        else:
            assert {"MessageAttributes" not in msg.keys() for msg in received}

    @pytest.mark.parametrize("number_of_messages", [10, 31, 49])
    @pytest.mark.parametrize("with_attributes", [True, False])
    def test_message_json_batching(self, number_of_messages: int, with_attributes: bool) -> None:
        @dataclass(frozen=True)
        class Thing(DataClassJsonCDHMixin):
            property_a: str
            property_b: int

        json_messages = [
            Thing(property_a=Builder.build_random_string(), property_b=random.randint(0, 9)).to_json()
            for _ in range(number_of_messages)
        ]
        attributes = {"some": {"StringValue": "attribute", "DataType": "String"}}
        sqs_client = SqsClient(self.boto_sqs_client)

        if with_attributes:
            sqs_client.send_messages(self.queue_url, json_messages, attributes)
        else:
            sqs_client.send_messages(self.queue_url, json_messages)

        received = self.fetch_messages_from_sqs()
        assert len(received) == len(json_messages)
        assert {msg["Body"] for msg in received} == set(json_messages)
        if with_attributes:
            assert {msg["MessageAttributes"] == attributes for msg in received}
        else:
            assert {"MessageAttributes" not in msg.keys() for msg in received}

    def test_boto_client_error_raises(self) -> None:
        mock_boto = Mock()
        mock_boto.send_message_batch.side_effect = self.boto_sqs_client.exceptions.ClientError(
            error_response={"Error": MOCK_SQS_ERROR}, operation_name="foo"
        )
        sqs_client = SqsClient(mock_boto)

        with pytest.raises(MessageDeliveryFailed):
            sqs_client.send_messages(self.queue_url, [{"hello": "world"}])

    def test_failed_messages(self) -> None:
        def mock_failing_send_message_batch(  # pylint: disable=unused-argument, invalid-name
            QueueUrl: str, Entries: List[Dict[str, Any]]
        ) -> Dict[str, Any]:
            return {
                "Successful": [],
                "Failed": [{"Id": entry["Id"], **MOCK_SQS_ERROR} for entry in Entries],
            }

        mock_boto = Mock()
        mock_boto.send_message_batch.side_effect = mock_failing_send_message_batch
        sqs_client = SqsClient(mock_boto)

        with pytest.raises(MessageDeliveryFailed):
            sqs_client.send_messages(self.queue_url, [{"hello": "world"}])

    def test_failed_and_successful_messages(self) -> None:
        accepted_messages = [{"value": k} for k in range(50)]
        failed_messages = [{"value": k} for k in range(50, 100)]

        def mock_send_message_batch(  # pylint: disable=invalid-name
            QueueUrl: str, Entries: List[Dict[str, Any]]
        ) -> Dict[str, Any]:
            accepted_entries = [
                cast(SendMessageBatchRequestEntryTypeDef, entry)
                for entry in Entries
                if json.loads(entry["MessageBody"]) in accepted_messages
            ]
            self.boto_sqs_client.send_message_batch(QueueUrl=QueueUrl, Entries=accepted_entries)
            return {
                "Successful": [
                    {
                        "Id": entry["Id"],
                    }
                    for entry in accepted_entries
                ],
                "Failed": [{"Id": entry["Id"], **MOCK_SQS_ERROR} for entry in Entries if entry not in accepted_entries],
            }

        mock_boto = Mock()
        mock_boto.send_message_batch.side_effect = mock_send_message_batch
        sqs_client = SqsClient(mock_boto)
        all_messages = accepted_messages + failed_messages
        random.shuffle(all_messages)

        with pytest.raises(MessageDeliveryFailed):
            sqs_client.send_messages(self.queue_url, all_messages)

        received = self.fetch_messages_from_sqs()
        assert len(received) == len(accepted_messages)
        assert {msg["Body"] for msg in received} == {json.dumps(message) for message in accepted_messages}

    def fetch_messages_from_sqs(self) -> List[MessageTypeDef]:
        messages = []
        wait_for_more_messages = True
        while wait_for_more_messages:
            response = self.boto_sqs_client.receive_message(
                QueueUrl=self.queue_url, MaxNumberOfMessages=10, VisibilityTimeout=300, MessageAttributeNames=["All"]
            )
            new_messages = response.get("Messages", [])
            messages.extend(new_messages)
            wait_for_more_messages = bool(new_messages)
        return messages
