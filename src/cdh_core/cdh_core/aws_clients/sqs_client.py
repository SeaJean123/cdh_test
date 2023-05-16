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
import uuid
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from botocore.exceptions import ClientError

from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient
    from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef
else:
    SQSClient = object
    SendMessageBatchRequestEntryTypeDef = object


SQS_MAXIMUM_BATCH_SIZE = 10


class SqsClient:
    """Abstracts the boto3 SQS client."""

    def __init__(self, boto_sqs_client: SQSClient):
        self._client = boto_sqs_client

    def send_messages(
        self,
        queue_url: str,
        messages: Union[List[Dict[str, Any]], List[str]],
        attributes: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """
        Send multiple messages to an SQS queue in batches size 10.

        :param messages: can be a list of dicts which will be converted to json via. json.dumps(),
         or a list of JSON strings
        :param queue_url: the URL of the target SQS queue
        :param attributes: message attributes for all messages
        :raises MessageDeliveryFailed: if the messages cannot be send
        """
        failures: List[FailedDelivery] = []
        for batch in SqsClient._get_sendable_batches(messages, attributes):
            try:
                response = self._client.send_message_batch(QueueUrl=queue_url, Entries=batch)
                for entry in response.get("Failed", []):
                    failed_message = next(message for message in batch if message["Id"] == entry["Id"])
                    failures.append(
                        FailedDelivery(
                            message_body=failed_message["MessageBody"],
                            error_code=entry["Code"],
                            error_message=entry["Message"],
                        )
                    )
            except ClientError as error:
                failures.extend(
                    [
                        FailedDelivery(
                            message_body=message["MessageBody"],
                            error_code=error.response["Error"]["Code"],
                            error_message=error.response["Error"]["Message"],
                        )
                        for message in batch
                    ]
                )

        if failures:
            raise MessageDeliveryFailed(queue_url=queue_url, failures=failures)

    def get_queue_url(self, queue_name: str, account_id: Optional[AccountId]) -> str:
        """Get the URL of the sqs queue."""
        return str(
            self._client.get_queue_url(
                QueueName=queue_name,
                QueueOwnerAWSAccountId=account_id,  # type: ignore
            )["QueueUrl"]
        )

    @staticmethod
    def _get_sendable_batches(
        messages: Union[List[Dict[str, Any]], List[str]], attributes: Optional[Mapping[str, Any]]
    ) -> Iterator[List[SendMessageBatchRequestEntryTypeDef]]:
        for batch_index in range(0, len(messages), SQS_MAXIMUM_BATCH_SIZE):
            yield [
                SqsClient._get_sendable_message(message, attributes)
                for message in messages[batch_index : batch_index + SQS_MAXIMUM_BATCH_SIZE]
            ]

    @staticmethod
    def _get_sendable_message(
        message: Union[Dict[str, Any], str], attributes: Optional[Mapping[str, Any]]
    ) -> SendMessageBatchRequestEntryTypeDef:
        sendable_message: SendMessageBatchRequestEntryTypeDef = {
            "Id": str(uuid.uuid4()),
            "MessageBody": json.dumps(message) if isinstance(message, dict) else message,
        }
        if attributes is not None:
            sendable_message["MessageAttributes"] = attributes
        return sendable_message


@dataclass(frozen=True)
class FailedDelivery:
    """Simple container class for exceptions."""

    message_body: str
    error_code: str
    error_message: str


class MessageDeliveryFailed(Exception):
    """Signals a message could not be delivered."""

    def __init__(self, queue_url: str, failures: List[FailedDelivery]):
        super().__init__(f"Failed to send {len(failures)} message(s) to {queue_url}: {failures}")
