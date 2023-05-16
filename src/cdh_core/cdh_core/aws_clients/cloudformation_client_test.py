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
from copy import deepcopy
from typing import Any
from typing import Dict
from typing import List
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from mypy_boto3_cloudformation import CloudFormationClient

from cdh_core.aws_clients.cloudformation_client import _StackStatus
from cdh_core.aws_clients.cloudformation_client import CloudformationClient
from cdh_core.aws_clients.cloudformation_client import StackAlreadyExists
from cdh_core.aws_clients.cloudformation_client import StackCreationFailed
from cdh_core.aws_clients.cloudformation_client import StackStatusError
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder

TEST_ROLE_NAME = "test-role"
TEMPLATE = {
    "Parameters": {
        "enabledAccount": {"Type": "String", "Description": "The ID of the count enabled to assume the test role"}
    },
    "Resources": {
        "testrole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": {
                                    "Fn::Join": [
                                        "",
                                        [
                                            "arn:",
                                            {"Ref": "AWS::Partition"},
                                            ":iam::",
                                            {"Ref": "enabledAccount"},
                                            ":root",
                                        ],
                                    ]
                                }
                            },
                        }
                    ],
                    "Version": "2012-10-17",
                },
                "Path": "/",
                "RoleName": TEST_ROLE_NAME,
            },
        }
    },
}


@pytest.mark.usefixtures("mock_iam")
class TestCloudformationClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_cloudformation: Any) -> None:  # pylint: disable=unused-argument
        self.boto_cf_client = boto3.client("cloudformation", region_name=build_region().value)
        self.client = CloudformationClient(self.boto_cf_client)
        self.stack_name = Builder.build_random_string()
        self.enabled_account_id = build_account_id()

    def test_stack_exists_true(self) -> None:
        self.boto_cf_client.create_stack(
            StackName=self.stack_name,
            TemplateBody=json.dumps(TEMPLATE),
            Capabilities=["CAPABILITY_NAMED_IAM"],
            Parameters=[
                {
                    "ParameterKey": "enabledAccount",
                    "ParameterValue": self.enabled_account_id,
                }
            ],
        )

        assert self.client.stack_exists(stack_name=self.stack_name)

    def test_stack_exists_false(self) -> None:
        assert not self.client.stack_exists(stack_name=self.stack_name)

    def test_create_successful(self) -> None:
        self.client.create_stack(
            stack_name=self.stack_name,
            template=TEMPLATE,
            parameters={"enabledAccount": self.enabled_account_id},
        )

        stacks = self.boto_cf_client.describe_stacks(StackName=self.stack_name)["Stacks"]
        assert len(stacks) == 1
        assert stacks[0]["StackName"] == self.stack_name
        assert stacks[0]["Parameters"][0]["ParameterValue"] == self.enabled_account_id
        assert stacks[0]["StackStatus"] == _StackStatus.CREATE_COMPLETE.value
        assert self.boto_cf_client.get_template(StackName=self.stack_name)["TemplateBody"] == TEMPLATE

    # moto does not validate capabilities
    def test_create_sets_capabilities(self) -> None:
        mock_cloudformation = Mock()
        summary = self.boto_cf_client.get_template_summary(TemplateBody=json.dumps(TEMPLATE))
        # moto does not generate full summary
        mock_cloudformation.get_template_summary.return_value = {**summary, "Capabilities": ["FAKE_CAPABILITY"]}

        def mock_describe_stacks(**kwargs: Any) -> Dict[str, Any]:  # pylint: disable=unused-argument
            stacks: List[Dict[str, Any]] = []
            if mock_cloudformation.create_stack.call_count > 0:
                stacks.append({"StackName": self.stack_name, "StackStatus": _StackStatus.CREATE_COMPLETE.value})
            return {"Stacks": stacks}

        mock_cloudformation.describe_stacks.side_effect = mock_describe_stacks
        client = CloudformationClient(mock_cloudformation)

        client.create_stack(
            stack_name=self.stack_name, template=TEMPLATE, parameters={"enabledAccount": self.enabled_account_id}
        )

        mock_cloudformation.create_stack.assert_called_once_with(
            StackName=self.stack_name,
            TemplateBody=json.dumps(TEMPLATE),
            Capabilities=["FAKE_CAPABILITY"],
            Parameters=[
                {
                    "ParameterKey": "enabledAccount",
                    "ParameterValue": self.enabled_account_id,
                }
            ],
        )

    def test_failed_create_raises(self) -> None:
        with pytest.raises(StackCreationFailed):
            self.client.create_stack(
                stack_name=self.stack_name,
                template=TEMPLATE,
                parameters={"incorrectParameter": "dummy"},
            )

    def test_create_already_exists_raises(self) -> None:
        self.boto_cf_client.create_stack(
            StackName=self.stack_name,
            TemplateBody=json.dumps(TEMPLATE),
            Capabilities=["CAPABILITY_NAMED_IAM"],
            Parameters=[
                {
                    "ParameterKey": "enabledAccount",
                    "ParameterValue": self.enabled_account_id,
                }
            ],
        )

        with pytest.raises(StackAlreadyExists):
            self.client.create_stack(
                stack_name=self.stack_name,
                template=TEMPLATE,
                parameters={"enabledAccount": self.enabled_account_id},
            )

    def test_create_fails_cleanup(self) -> None:
        boto_client = Mock(CloudFormationClient)
        boto_client.describe_stacks.return_value = {"Stacks": [{"StackStatus": _StackStatus.CREATE_FAILED.value}]}
        client = CloudformationClient(boto_client)
        with pytest.raises(StackCreationFailed):
            client.create_stack(
                stack_name=self.stack_name,
                template=TEMPLATE,
                parameters={"enabledAccount": self.enabled_account_id},
            )

        for stack in self.boto_cf_client.list_stacks()["StackSummaries"]:
            assert stack["StackStatus"] == _StackStatus.DELETE_COMPLETE.value

    def test_delete_stack(self) -> None:
        self.boto_cf_client.create_stack(
            StackName=self.stack_name,
            TemplateBody=json.dumps(TEMPLATE),
            Capabilities=["CAPABILITY_NAMED_IAM"],
            Parameters=[
                {
                    "ParameterKey": "enabledAccount",
                    "ParameterValue": self.enabled_account_id,
                }
            ],
        )

        self.client.delete_stack(self.stack_name)
        with pytest.raises(ClientError):
            self.boto_cf_client.describe_stacks(StackName=self.stack_name)

    def test_delete_checks_status(self) -> None:
        boto_client = Mock(CloudFormationClient)
        boto_client.describe_stacks.return_value = {"Stacks": [{"StackStatus": _StackStatus.DELETE_FAILED.value}]}
        client = CloudformationClient(boto_client)

        with pytest.raises(StackStatusError):
            client.delete_stack(self.stack_name)

    def test_update_stack(self) -> None:
        other_template: Dict[str, Any] = deepcopy(TEMPLATE)
        other_template["Resources"]["testrole"]["Properties"]["RoleName"] = "other-role-name"
        self.boto_cf_client.create_stack(
            StackName=self.stack_name,
            TemplateBody=json.dumps(other_template),
            Capabilities=["CAPABILITY_NAMED_IAM"],
            Parameters=[
                {
                    "ParameterKey": "enabledAccount",
                    "ParameterValue": build_account_id(),
                }
            ],
        )

        self.client.update_stack(
            stack_name=self.stack_name,
            template=TEMPLATE,
            parameters={"enabledAccount": self.enabled_account_id},
        )

        assert (
            self.boto_cf_client.describe_stacks(StackName=self.stack_name)["Stacks"][0]["Parameters"][0][
                "ParameterValue"
            ]
            == self.enabled_account_id
        )
        assert self.boto_cf_client.get_template(StackName=self.stack_name)["TemplateBody"] == TEMPLATE

    def test_update_no_changes(self) -> None:
        # boto raises a validation error if the stack is already up to date; but moto does not
        boto_client = Mock(CloudFormationClient)
        boto_client.update_stack.side_effect = ClientError(
            error_response={"Error": {"Code": "ValidationError", "Message": "No updates are to be performed."}},
            operation_name="update_stack",
        )
        client = CloudformationClient(boto_client)

        client.update_stack(
            stack_name=self.stack_name,
            template=TEMPLATE,
            parameters={"enabledAccount": self.enabled_account_id},
        )
        boto_client.update_stack.assert_called_once()

    def test_update_checks_status(self) -> None:
        boto_client = Mock(CloudFormationClient)
        boto_client.describe_stacks.return_value = {"Stacks": [{"StackStatus": _StackStatus.UPDATE_FAILED.value}]}
        client = CloudformationClient(boto_client)

        with pytest.raises(StackStatusError):
            client.update_stack(
                stack_name=self.stack_name,
                template=TEMPLATE,
                parameters={"enabledAccount": self.enabled_account_id},
            )
