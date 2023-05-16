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
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest

from cdh_core.aws_clients.iam_client import AttachedPolicy
from cdh_core.aws_clients.iam_client import IamClient
from cdh_core.aws_clients.iam_client import ManageRolePolicyException
from cdh_core.aws_clients.iam_client import ManageRolePolicyOperationType
from cdh_core.aws_clients.iam_client import MAX_VERSIONS_PER_POLICY
from cdh_core.aws_clients.iam_client import PolicyNotFound
from cdh_core.aws_clients.iam_client import Role
from cdh_core.aws_clients.iam_client import RoleAlreadyExists
from cdh_core.aws_clients.iam_client import RoleNotFound
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws_clients import PolicyDocumentType
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.utils import build_and_set_moto_account_id

MOTO_ACCOUNT_ID = AccountId(build_and_set_moto_account_id())


@pytest.mark.usefixtures("mock_config_file")
class TestIamClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_iam: Any) -> None:  # pylint: disable=unused-argument
        self.boto_iam_client = boto3.client("iam", region_name=build_region().value)
        self.partition = build_partition()
        self.iam_client = IamClient(self.boto_iam_client, MOTO_ACCOUNT_ID, self.partition)
        self.role_name = Builder.build_random_string()
        self.assume_role_policy = PolicyDocument(
            version="2012-10-17",
            statements=[
                {
                    "Sid": "Access",
                    "Effect": "Allow",
                    "Principal": {"AWS": f"arn:{self.partition.value}:iam::{MOTO_ACCOUNT_ID}:root"},
                    "Action": "sts:AssumeRole",
                }
            ],
            policy_document_type=PolicyDocumentType.MANAGED,
        )
        self.description = Builder.build_random_string()
        self.policy_name = Builder.build_random_string()

    def build_policy_document(self, action: str = "sns:Publish") -> PolicyDocument:
        # Warning: this document must pass moto's validation
        return PolicyDocument.create_managed_policy(
            [{"Effect": "Allow", "Action": action, "Resource": "*"}],
        )

    def assert_policy(
        self, policy_arn: Arn, policy_name: str, policy_document: PolicyDocument, description: str = ""
    ) -> None:
        policy = self.boto_iam_client.get_policy(PolicyArn=str(policy_arn))["Policy"]
        assert policy["PolicyName"] == policy_name
        # Warning: AWS removes the key 'Description' if it is empty. Moto does not.
        assert policy["Description"] == description

        version_id = policy["DefaultVersionId"]
        policy_version_document = self.boto_iam_client.get_policy_version(
            PolicyArn=str(policy_arn), VersionId=version_id
        )["PolicyVersion"]["Document"]
        assert (
            PolicyDocument(
                version=policy_version_document["Version"],  # type: ignore
                statements=policy_version_document["Statement"],  # type: ignore
            )
            == policy_document
        )

    def test_get_role(self) -> None:
        arn_string = self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
            Description=self.description,
        )["Role"]["Arn"]

        role = self.iam_client.get_role(self.role_name)

        assert role == Role(name=self.role_name, arn=Arn(arn_string), description=self.description)

    def test_get_missing_role(self) -> None:
        with assert_raises(RoleNotFound(self.role_name)):
            self.iam_client.get_role(self.role_name)

    def test_get_attached_policy_arn_with_non_attached_policy(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        self.boto_iam_client.create_policy(
            PolicyName=self.policy_name,
            PolicyDocument=self.build_policy_document().encode(),
        )

        assert self.iam_client.get_attached_policy_arn(self.role_name, self.policy_name) is None

    def test_create_role(self) -> None:
        with self.iam_client.create_role_transaction(
            self.role_name, self.assume_role_policy, tags={"tagkey": "tagvalue"}, description=self.description
        ) as role:
            role_in_iam = self.boto_iam_client.get_role(RoleName=self.role_name)["Role"]
            assert role_in_iam["AssumeRolePolicyDocument"] == self.assume_role_policy.as_dict()  # type: ignore
            # We can't check the tags nor the description, because moto does not return them.
            # We keep them nevertheless, because moto will check the parameter format.

        assert role == Role(
            name=self.role_name,
            arn=Arn(f"arn:{self.partition.value}:iam::{MOTO_ACCOUNT_ID}:role/{self.role_name}"),
            description=self.description,
        )
        # Make sure role is not rolled back
        assert self.boto_iam_client.get_role(RoleName=self.role_name)["Role"]

    def test_rollback_create_role(self) -> None:
        with pytest.raises(ValueError):
            with self.iam_client.create_role_transaction(self.role_name, self.assume_role_policy):
                raise ValueError()

        self.assert_role_does_not_exist()

    def test_create_role_that_already_exists(self) -> None:
        # Unfortunately, moto does not raise any exception when creating a second role with the same name.
        exception = self.boto_iam_client.exceptions.EntityAlreadyExistsException({}, "create_role")

        with patch.object(self.boto_iam_client, "create_role", side_effect=exception):
            with assert_raises(RoleAlreadyExists(self.role_name)):
                self.iam_client.create_role(self.role_name, self.assume_role_policy)

    def test_manage_role_policy_that_does_not_exist_yet(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        policy_document = self.build_policy_document()

        manage_role_policy = self.iam_client.manage_role_policy(
            self.role_name, self.policy_name, policy_document, self.description
        )

        assert manage_role_policy.type == ManageRolePolicyOperationType.CREATE
        self.assert_policy(manage_role_policy.arn, self.policy_name, policy_document, self.description)
        attached_policies = self.boto_iam_client.list_attached_role_policies(RoleName=self.role_name)[
            "AttachedPolicies"
        ]
        assert attached_policies == [{"PolicyName": self.policy_name, "PolicyArn": str(manage_role_policy.arn)}]

    def test_manage_role_policy_that_overwrites_existing_not_yet_attached_policy(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        self.boto_iam_client.create_policy(
            PolicyName=self.policy_name,
            PolicyDocument=self.build_policy_document(action="sns:RemovePermission").encode(),
            Description=self.description,
        )
        policy_document = self.build_policy_document()

        manage_role_policy = self.iam_client.manage_role_policy(self.role_name, self.policy_name, policy_document)

        assert manage_role_policy.type == ManageRolePolicyOperationType.UPDATE
        self.assert_policy(manage_role_policy.arn, self.policy_name, policy_document, self.description)
        attached_policies = self.boto_iam_client.list_attached_role_policies(RoleName=self.role_name)[
            "AttachedPolicies"
        ]
        assert attached_policies == [{"PolicyName": self.policy_name, "PolicyArn": str(manage_role_policy.arn)}]

    def test_manage_role_policy_that_overwrites_existing_attached_policy(self) -> None:
        policy_document = self.build_policy_document(action="sns:RemovePermission")
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        policy_arn = Arn(
            self.boto_iam_client.create_policy(
                PolicyName=self.policy_name,
                PolicyDocument=self.build_policy_document(action="sns:Publish").encode(),
            )["Policy"]["Arn"]
        )
        self.boto_iam_client.attach_role_policy(RoleName=self.role_name, PolicyArn=str(policy_arn))

        manage_role_policy = self.iam_client.manage_role_policy(self.role_name, self.policy_name, policy_document)

        assert manage_role_policy.type == ManageRolePolicyOperationType.UPDATE
        self.assert_policy(manage_role_policy.arn, self.policy_name, policy_document)
        attached_policies = self.boto_iam_client.list_attached_role_policies(RoleName=self.role_name)[
            "AttachedPolicies"
        ]
        assert attached_policies == [{"PolicyName": self.policy_name, "PolicyArn": str(manage_role_policy.arn)}]

    def test_manage_role_policy_that_rollback_create(self) -> None:
        boto_iam_client = Mock()  # delete_policy is not yet implemented by moto
        iam_client = IamClient(boto_iam_client, MOTO_ACCOUNT_ID, self.partition)
        policy_arn = iam_client.build_policy_arn(self.policy_name)
        boto_iam_client.create_policy.return_value = {"Policy": {"Arn": str(policy_arn)}}
        boto_iam_client.attach_role_policy.side_effect = Builder.build_client_error("")
        boto_iam_client.exceptions.EntityAlreadyExistsException = (
            boto_iam_client.exceptions.EntityAlreadyExistsException
        )
        policy_document = self.build_policy_document()

        with pytest.raises(ManageRolePolicyException) as exc_info:
            iam_client.manage_role_policy(self.role_name, self.policy_name, policy_document)

        assert exc_info.value.manage_role_policy.type == ManageRolePolicyOperationType.CREATE
        boto_iam_client.delete_policy.assert_called_once_with(PolicyArn=str(policy_arn))

    def test_get_attached_policy_arn(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        policy_document = self.build_policy_document()
        policy_arn = self.iam_client.manage_role_policy(self.role_name, self.policy_name, policy_document).arn

        assert self.iam_client.get_attached_policy_arn(self.role_name, self.policy_name) == policy_arn

    def test_get_attached_policies(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        policy_document = self.build_policy_document()
        manage_role_policy = self.iam_client.manage_role_policy(self.role_name, self.policy_name, policy_document)

        assert self.iam_client.get_attached_policies(self.role_name) == [
            AttachedPolicy(
                name=self.policy_name,
                arn=manage_role_policy.arn,
                document=policy_document,
                version="v1",
            )
        ]

    def test_delete_oldest_policy_version_if_necessary(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        old_policy_document = self.build_policy_document("sns:Publish")
        policy_arn = self.iam_client.manage_role_policy(self.role_name, self.policy_name, old_policy_document).arn

        for i in range(MAX_VERSIONS_PER_POLICY):
            new_policy_document = self.build_policy_document(f"sns:action-{i}")
            self.iam_client.update_policy(policy_arn, new_policy_document)

        versions = self.boto_iam_client.list_policy_versions(PolicyArn=str(policy_arn))["Versions"]
        assert {version["VersionId"] for version in versions} == {f"v{i+2}" for i in range(MAX_VERSIONS_PER_POLICY)}
        assert any(
            version["VersionId"] == f"v{MAX_VERSIONS_PER_POLICY+1}" and version["IsDefaultVersion"]
            for version in versions
        )

    def test_detach_and_delete_policy(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        old_policy_document = self.build_policy_document("sns:Publish")
        policy_arn = self.iam_client.manage_role_policy(self.role_name, self.policy_name, old_policy_document).arn

        for i in range(MAX_VERSIONS_PER_POLICY - 1):
            new_policy_document = self.build_policy_document(f"sns:action-{i}")
            self.iam_client.update_policy(policy_arn, new_policy_document)

        self.iam_client.detach_and_delete_policy(self.role_name, policy_arn)

        with pytest.raises(PolicyNotFound):
            self.iam_client.get_attached_policy(self.role_name, self.policy_name)

    def test_delete_role(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        self.iam_client.delete_policies_and_role(self.role_name)

        self.assert_role_does_not_exist()

    def test_delete_nonexistent_role(self) -> None:
        with pytest.raises(RoleNotFound):
            self.iam_client.delete_policies_and_role(self.role_name)

    def test_delete_role_with_attached_managed_policy(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )

        self.iam_client.manage_role_policy(self.role_name, self.policy_name, self.build_policy_document())
        self.iam_client.delete_policies_and_role(self.role_name)

        self.assert_role_does_not_exist()

    def test_delete_role_with_inline_role_policy(self) -> None:
        self.boto_iam_client.create_role(
            RoleName=self.role_name,
            AssumeRolePolicyDocument=self.assume_role_policy.encode(),
        )
        self.boto_iam_client.put_role_policy(
            RoleName=self.role_name,
            PolicyName=self.policy_name,
            PolicyDocument=self.build_policy_document().encode(),
        )

        self.iam_client.delete_policies_and_role(self.role_name)

        self.assert_role_does_not_exist()

    def assert_role_does_not_exist(self) -> None:
        assert self.boto_iam_client.list_roles()["Roles"] == []
        with pytest.raises(self.boto_iam_client.exceptions.NoSuchEntityException):
            self.boto_iam_client.get_role(RoleName=self.role_name)
