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
import copy
import json
from typing import Any
from typing import Dict
from typing import Optional
from unittest.mock import call
from unittest.mock import Mock

import pytest
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher

from cdh_core.aws_clients.sns_client import SnsClient
from cdh_core.entities.accounts import ResponseAccount
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core_dev_tools.testing.builder import Builder


@pytest.mark.parametrize("jwt_user_id", [None, "my-jwt"])
class TestSnsPublisher:
    def setup_method(self) -> None:
        self.sns_client = Mock(SnsClient)
        self.topic_arns = [build_arn("sns") for _ in range(3)]
        self.requester_arn = build_role_arn()

    def get_expected_body(
        self,
        data: Dict[str, Any],
        jwt_user_id: Optional[str],
        message_consistency: MessageConsistency = MessageConsistency.CONFIRMED,
    ) -> Dict[str, Any]:
        if jwt_user_id:
            data["requesterUserId"] = jwt_user_id
        data["requesterAccountId"] = self.requester_arn.account_id
        data["messageConsistency"] = message_consistency.value
        return data

    def build_publisher(self, jwt_user_id: Optional[str]) -> SnsPublisher:
        requester_identity = build_requester_identity(arn=self.requester_arn, jwt_user_id=jwt_user_id)
        return SnsPublisher(
            sns_client=self.sns_client, topic_arns=self.topic_arns, requester_identity=requester_identity
        )

    def test_publish_account(self, jwt_user_id: Optional[str]) -> None:
        operation = Builder.get_random_element(set(Operation))
        account = build_account().to_response_account(ResponseAccount)
        sns_publisher = self.build_publisher(jwt_user_id)

        sns_publisher.publish(
            entity_type=EntityType.ACCOUNT,
            operation=operation,
            payload=account,
        )

        expected_body = self.get_expected_body(
            account.to_plain_dict(),
            jwt_user_id,
        )
        self.sns_client.publish_message.assert_has_calls(
            [
                call(
                    sns_arn=topic_arn,
                    message_subject=SnsPublisher.build_subject(EntityType.ACCOUNT, operation),
                    message_body=json.dumps(expected_body),
                    attributes=expected_body,
                )
                for topic_arn in self.topic_arns
            ],
            any_order=True,
        )

    @pytest.mark.parametrize("message_consistency", MessageConsistency)
    def test_publish_dataset(self, jwt_user_id: Optional[str], message_consistency: MessageConsistency) -> None:
        operation = Builder.get_random_element(set(Operation))
        dataset = build_dataset()
        sns_publisher = self.build_publisher(jwt_user_id)

        sns_publisher.publish(
            entity_type=EntityType.DATASET,
            operation=operation,
            payload=dataset,
            message_consistency=message_consistency,
        )

        expected_body = self.get_expected_body(dataset.to_plain_dict(), jwt_user_id, message_consistency)
        self.sns_client.publish_message.assert_has_calls(
            [
                call(
                    sns_arn=topic_arn,
                    message_subject=SnsPublisher.build_subject(EntityType.DATASET, operation),
                    message_body=json.dumps(expected_body),
                    attributes=expected_body,
                )
                for topic_arn in self.topic_arns
            ],
            any_order=True,
        )

    def test_publish_resource(self, jwt_user_id: Optional[str]) -> None:
        operation = Builder.get_random_element(set(Operation))
        resource = build_s3_resource().to_payload()
        sns_publisher = self.build_publisher(jwt_user_id)

        sns_publisher.publish(
            entity_type=EntityType.RESOURCE,
            operation=operation,
            payload=resource,
        )

        expected_body = self.get_expected_body(resource.to_plain_dict(), jwt_user_id)
        sns_attributes = copy.deepcopy(expected_body)
        del sns_attributes["attributes"]
        self.sns_client.publish_message.assert_has_calls(
            [
                call(
                    sns_arn=topic_arn,
                    message_subject=SnsPublisher.build_subject(EntityType.RESOURCE, operation),
                    message_body=json.dumps(expected_body),
                    attributes=sns_attributes,
                )
                for topic_arn in self.topic_arns
            ],
            any_order=True,
        )


def test_build_subject() -> None:
    assert SnsPublisher.build_subject(EntityType.DATASET, Operation.UPDATE) == "UPDATE DATASET"
