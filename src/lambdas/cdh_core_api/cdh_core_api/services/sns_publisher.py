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
from enum import Enum
from typing import Any
from typing import Collection
from typing import Dict
from typing import Literal
from typing import overload
from typing import Union

from cdh_core.aws_clients.sns_client import SnsClient
from cdh_core.entities.accounts import ResponseAccount
from cdh_core.entities.arn import Arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.resource import ResourcePayload


class EntityType(Enum):
    """The kind of entity that was affected by a change."""

    DATASET = "DATASET"
    RESOURCE = "RESOURCE"
    ACCOUNT = "ACCOUNT"


class Operation(Enum):
    """The kind of operation that was performed."""

    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class MessageConsistency(Enum):
    """Indicates whether the state described in the notification is permanent."""

    PRELIMINARY = "preliminary"
    CONFIRMED = "confirmed"


class SnsPublisher:
    """Publishes notifications to SNS topics."""

    def __init__(self, sns_client: SnsClient, topic_arns: Collection[Arn], requester_identity: RequesterIdentity):
        self._client = sns_client
        self._topic_arns = topic_arns
        self._requester_identity = requester_identity

    @overload
    def publish(
        self,
        entity_type: Literal[EntityType.DATASET],
        operation: Operation,
        payload: Dataset,
        message_consistency: MessageConsistency = MessageConsistency.CONFIRMED,
    ) -> None:
        ...

    @overload
    def publish(
        self,
        entity_type: Literal[EntityType.RESOURCE],
        operation: Operation,
        payload: ResourcePayload,
    ) -> None:
        ...

    @overload
    def publish(self, entity_type: Literal[EntityType.ACCOUNT], operation: Operation, payload: ResponseAccount) -> None:
        ...

    def publish(  # pylint: disable=too-many-arguments
        self,
        entity_type: EntityType,
        operation: Operation,
        payload: Union[Dataset, ResourcePayload, ResponseAccount],
        message_consistency: MessageConsistency = MessageConsistency.CONFIRMED,
    ) -> None:
        """Publish a change notification to all configured SNS topics."""
        self._publish_message(
            subject=self.build_subject(entity_type, operation),
            data=self._get_data_to_publish(payload=payload, message_consistency=message_consistency),
            sanitize_attributes=entity_type is EntityType.RESOURCE,
        )

    @staticmethod
    def build_subject(entity_type: EntityType, operation: Operation) -> str:
        """Build the subject header for an SNS message."""
        return f"{operation.value.upper()} {entity_type.value.upper()}"

    @staticmethod
    def _sanitize_attributes(data: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in data.items() if type(v) in (str, int, float)}

    def _get_data_to_publish(
        self,
        payload: Union[ResourcePayload, ResponseAccount, Dataset],
        message_consistency: MessageConsistency,
    ) -> Dict[str, Any]:
        data = payload.to_plain_dict()
        if self._requester_identity.jwt_user_id:
            data["requesterUserId"] = self._requester_identity.jwt_user_id
        data["requesterAccountId"] = self._requester_identity.arn.account_id
        data["messageConsistency"] = message_consistency.value
        return data

    def _publish_message(
        self,
        subject: str,
        data: Dict[str, Any],
        sanitize_attributes: bool,
    ) -> None:
        for topic_arn in self._topic_arns:
            self._client.publish_message(
                sns_arn=topic_arn,
                message_subject=subject,
                message_body=json.dumps(data),
                attributes=self._sanitize_attributes(data) if sanitize_attributes else data,
            )
