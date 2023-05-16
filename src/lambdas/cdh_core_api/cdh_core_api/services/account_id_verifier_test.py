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
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError
from cdh_core_api.config_test import build_config
from cdh_core_api.services.account_id_verifier import AccountIdVerifier

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.sns_client import SnsClient
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class TestAccountIdVerifier:
    def setup_method(self) -> None:
        self.partition = build_partition()
        self.environment = build_environment()
        accounts = [build_resource_account(stage_priority=i) for i in range(10)]
        # To ensure that there is at least one matching resource account for the environment
        accounts.append(
            build_resource_account(
                environment=self.environment, partition=self.partition, hub=Hub.default(self.partition), stage=Stage.dev
            )
        )
        self.config = build_config(account_store=AccountStore(accounts=accounts), environment=self.environment)
        self.region = build_region()
        self.account_id = build_account_id()
        self.resource_account_id = build_account_id()
        self.sns_arn = Builder.build_random_string()
        self.expected_topic_name = f"{self.config.prefix}cdh-verify-account-{self.account_id}"
        self.sns_client = Mock(SnsClient)
        self.sns_client.create_sns.return_value = self.sns_arn
        self.sns_client.get_sns_policy.return_value = PolicyDocument.create_sns_policy(
            [
                {
                    "Sid": "__default_statement_ID",
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": ["SNS:*"],
                    "Resource": f"arn:aws:sns:{self.region.value}:{self.resource_account_id}:topic",
                }
            ]
        )
        self.aws = Mock(AwsClientFactory)
        self.aws.sns_client.return_value = self.sns_client
        self.account_id_verifier = AccountIdVerifier(self.config, self.aws)
        self.policy_document_expected = PolicyDocument.create_sns_policy(
            [
                {
                    "Sid": "__default_statement_ID",
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*", str(self.account_id)]},
                    "Action": ["SNS:*"],
                    "Resource": f"arn:aws:sns:{self.region.value}:{self.resource_account_id}:topic",
                }
            ]
        )

    def test_verify_existing_account_id(self) -> None:
        self.account_id_verifier.verify(self.account_id, self.partition)

        self.sns_client.create_sns.assert_called_once()
        assert self.expected_topic_name in self.sns_client.create_sns.call_args.kwargs["name"]
        self.sns_client.set_sns_policy.assert_called_once_with(
            sns_arn=self.sns_arn, sns_policy=self.policy_document_expected
        )
        self.sns_client.delete_sns_topic.assert_called_once_with(sns_arn=self.sns_arn)

    def test_verify_not_existing_account_id(self) -> None:
        self.sns_client.set_sns_policy.side_effect = ClientError(
            error_response={
                "Error": {"Code": "InvalidParameter", "Message": "Invalid parameter: Policy Error: PrincipalNotFound"}
            },
            operation_name="",
        )

        with pytest.raises(
            ForbiddenError, match=f"Account id {self.account_id} does not exist in AWS partition {self.partition.value}"
        ):
            self.account_id_verifier.verify(self.account_id, self.partition)

        self.sns_client.create_sns.assert_called_once()
        assert self.expected_topic_name in self.sns_client.create_sns.call_args.kwargs["name"]
        self.sns_client.set_sns_policy.assert_called_once_with(
            sns_arn=self.sns_arn, sns_policy=self.policy_document_expected
        )
        self.sns_client.delete_sns_topic.assert_called_once_with(sns_arn=self.sns_arn)
