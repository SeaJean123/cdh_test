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
from datetime import date
from typing import Any
from typing import Dict
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest
from cdh_billing.fan_out import lambda_handler

from cdh_core.aws_clients.sqs_client import SqsClient
from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.entities.accounts_test import build_response_account_without_costs
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.enums.accounts import Affiliation

ACC_1 = build_response_account_without_costs(affiliation=Affiliation("cdh"))
ACC_2 = build_response_account_without_costs(affiliation=Affiliation("cdh"))
ACC_3 = build_response_account_without_costs(affiliation=Affiliation("external"))


@pytest.mark.usefixtures("mock_sqs")
class TestLambdaFanOut:
    @pytest.fixture(autouse=True)
    def setup_environment_variables(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("CORE_API_URL", "")

    @pytest.fixture(autouse=True)
    def service_setup(self, mock_sqs: Any) -> None:  # pylint: disable=unused-argument
        sqs_client = boto3.client("sqs")
        self.queue_url = sqs_client.create_queue(QueueName="SomeQueueName")["QueueUrl"]

    @pytest.mark.parametrize("event", [{}, {"nominal_date": "2010-01-01"}])
    @patch.object(CoreApiClient, "get_accounts")
    @patch.object(SqsClient, "send_messages")
    def test_lambda_handler(
        self, send_messages: Mock, get_accounts: Mock, monkeypatch: Any, event: Dict[str, Any]
    ) -> None:
        context = LambdaContext()
        get_accounts.return_value = [ACC_1, ACC_2, ACC_3]
        monkeypatch.setenv("QUEUE_URL", self.queue_url)

        lambda_handler(event, context)

        get_accounts.assert_called_once()
        assert len(send_messages.call_args) == 2
        assert send_messages.call_args[0] == ()

        assert "queue_url" in send_messages.call_args[1]
        assert send_messages.call_args[1]["queue_url"] == self.queue_url

        assert "messages" in send_messages.call_args[1]
        received_messages = [json.loads(entry) for entry in send_messages.call_args[1]["messages"]]
        expected_messages = [{"account_id": a.id, "hub": a.hub.value} for a in [ACC_1, ACC_2]]
        for message in received_messages:
            assert message in expected_messages

        assert "attributes" in send_messages.call_args[1]
        received_attributes = send_messages.call_args[1]["attributes"]
        if event:
            assert received_attributes["nominal_date"]["StringValue"] == event["nominal_date"]
        else:
            assert received_attributes["nominal_date"]["StringValue"] == str(date.today())
