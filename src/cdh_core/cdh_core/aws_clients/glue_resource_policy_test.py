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
from datetime import datetime

import pytest

from cdh_core.aws_clients.glue_resource_policy import GlueResourcePolicy
from cdh_core.aws_clients.glue_resource_policy import PROTECT_RESOURCE_LINKS_SID
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.policy_test import build_policy_statement
from cdh_core.entities.arn_test import build_arn
from cdh_core_dev_tools.testing.builder import Builder


class GlueResourcePolicyTest:
    def setup_method(self) -> None:
        self.principal = Builder.build_random_string()
        self.resources = {build_arn(service="glue") for _ in range(3)}


class TestGlueResourcePolicyAddProtection(GlueResourcePolicyTest):
    def test_add_protection_other_statements_not_overwritten(self) -> None:
        statements = [build_policy_statement() for _ in range(3)]

        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy(statements), policy_hash=Builder.build_random_string()
        )

        new_policy = policy.add_resource_protection(self.principal, self.resources)
        assert all(
            statement in new_policy._document.statements for statement in statements  # pylint: disable=protected-access
        )

    def test_add_protection_statement(self) -> None:
        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy([]), policy_hash=Builder.build_random_string()
        )

        new_policy = policy.add_resource_protection(self.principal, self.resources)

        assert new_policy.protected_resources == self.resources

    def test_extend_protection_statement(self) -> None:
        protected_resources = {build_arn(service="glue") for _ in range(3)}
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources=protected_resources
        )
        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy([protection_statement]),
            policy_hash=Builder.build_random_string(),
        )

        new_policy = policy.add_resource_protection(principal=self.principal, resources_to_add=self.resources)

        assert new_policy.protected_resources == protected_resources | self.resources

    def test_resources_already_protected_return_same_object(self) -> None:
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources=self.resources
        )
        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy([protection_statement]),
            policy_hash=Builder.build_random_string(),
        )

        new_policy = policy.add_resource_protection(principal=self.principal, resources_to_add=self.resources)

        assert new_policy is policy


class TestGlueResourcePolicyRemoveProtection(GlueResourcePolicyTest):
    def test_remove_protection_other_statements_not_overwritten(self) -> None:
        statements = [build_policy_statement() for _ in range(3)]
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources=self.resources
        )
        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy(statements + [protection_statement]),
            policy_hash=Builder.build_random_string(),
        )

        new_policy = policy.remove_resource_protection(self.principal, self.resources)
        assert all(
            statement in new_policy._document.statements for statement in statements  # pylint: disable=protected-access
        )

    def test_remove_protection_statement(self) -> None:
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources=self.resources
        )
        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy([protection_statement]),
            policy_hash=Builder.build_random_string(),
        )

        new_policy = policy.remove_resource_protection(self.principal, self.resources)

        assert not new_policy._document.has_statement_with_sid(  # pylint: disable=protected-access
            PROTECT_RESOURCE_LINKS_SID
        )

    def test_reduce_protection_statement(self) -> None:
        protected_resources = {build_arn(service="glue") for _ in range(3)}
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources=self.resources | protected_resources
        )
        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy([protection_statement]),
            policy_hash=Builder.build_random_string(),
        )

        new_policy = policy.remove_resource_protection(self.principal, self.resources)

        assert new_policy.protected_resources == protected_resources

    def test_resources_not_protected_return_same_object(self) -> None:
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources={build_arn(service="glue") for _ in range(3)}
        )
        policy = GlueResourcePolicy(
            document=PolicyDocument.create_glue_resource_policy([protection_statement]),
            policy_hash=Builder.build_random_string(),
        )

        new_policy = policy.remove_resource_protection(principal=self.principal, resources_to_remove=self.resources)

        assert new_policy is policy


def test_encode() -> None:
    document = PolicyDocument.create_glue_resource_policy(statements=[build_policy_statement() for _ in range(3)])
    policy = GlueResourcePolicy(document, Builder.build_random_string())

    assert policy.to_boto() == document.encode()


def test_encode_empty_raises_value_error() -> None:
    document = PolicyDocument.create_glue_resource_policy(statements=[])
    policy = GlueResourcePolicy(document, Builder.build_random_string())

    with pytest.raises(ValueError):
        policy.to_boto()


def test_from_boto() -> None:
    document = PolicyDocument.create_glue_resource_policy(statements=[build_policy_statement() for _ in range(3)])
    policy_hash = Builder.build_random_string()

    policy = GlueResourcePolicy.from_boto(
        {
            "PolicyInJson": document.encode(),
            "PolicyHash": policy_hash,
            "CreateTime": datetime.now(),
            "UpdateTime": datetime.now(),
            "ResponseMetadata": {},  # type: ignore
        }
    )

    assert policy._document == document  # pylint: disable=protected-access
    assert policy.policy_hash == policy_hash


class TestProtectedResources:
    def test_no_protection_statement(self) -> None:
        document = PolicyDocument.create_glue_resource_policy(statements=[build_policy_statement() for _ in range(3)])
        policy = GlueResourcePolicy(document, Builder.build_random_string())

        assert policy.protected_resources == set()

    def test_protection_statement_single_resource(self) -> None:
        resource = build_arn(service="glue")
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=Builder.build_random_string(), resources={resource}
        )
        protection_statement["Resource"] = str(resource)  # AWS converts singleton lists to simple strings

        document = PolicyDocument.create_glue_resource_policy(statements=[protection_statement])
        policy = GlueResourcePolicy(document, Builder.build_random_string())

        assert policy.protected_resources == {resource}

    def test_protection_statement_multiple_resources(self) -> None:
        resources = {build_arn(service="glue") for _ in range(3)}
        protection_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=Builder.build_random_string(), resources=resources
        )

        document = PolicyDocument.create_glue_resource_policy(statements=[protection_statement])
        policy = GlueResourcePolicy(document, Builder.build_random_string())

        assert policy.protected_resources == resources
