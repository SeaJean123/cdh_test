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
from typing import Dict

import pytest

from cdh_core.aws_clients.policy import InvalidPolicyStatement
from cdh_core.aws_clients.policy import PolicyCountExceeded
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.policy import PolicySizeExceeded
from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.aws_clients import PolicyDocumentType
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class TestPolicyDocument:
    def test_exceed_managed_policy_size(self) -> None:
        statements = [
            {
                "Sid": "WeAreMany",
                "Effect": "Allow",
                "Action": "s3:ObjectOwnerOverrideToBucketOwner",
                "Resource": "arn:aws:s3:::loooooooooooooooooongbucketname/*",
            }
        ] * 100
        with pytest.raises(PolicySizeExceeded):
            PolicyDocument.create_managed_policy(statements)

    def test_exceed_bucket_policy_size(self) -> None:
        statement = {
            "Sid": "WeAreMany",
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Principal": "*",
            "Resource": "arn:aws:sns:::" + "a" * PolicyDocumentType.BUCKET.get_max_policy_length(),
        }
        with pytest.raises(PolicySizeExceeded):
            PolicyDocument.create_bucket_policy([statement])

    def test_exceed_kms_policy_size(self) -> None:
        statement = {
            "Sid": "WeAreMany",
            "Effect": "Allow",
            "Action": "kms:Encrypt",
            "Principal": {},
            "Resource": "arn:aws:sns:::" + "a" * PolicyDocumentType.KMS.get_max_policy_length(),
        }
        with pytest.raises(PolicySizeExceeded):
            PolicyDocument.create_key_policy([statement])

    def test_exceed_sns_policy_size(self) -> None:
        statement = {
            "Sid": "WeAreMany",
            "Effect": "Allow",
            "Action": "sns:Publish",
            "Principal": {"AWS": "arn:aws:sns:::" + "a" * PolicyDocumentType.SNS.get_max_policy_length()},
            "Resource": "*",
        }
        with pytest.raises(PolicySizeExceeded):
            PolicyDocument.create_sns_policy([statement])

    def test_get_policy_statement_by_sid_not_exists(self) -> None:
        policy_document = PolicyDocument.create_bucket_policy(
            [
                {
                    "Sid": "WeAreMany",
                    "Effect": "Allow",
                    "Action": "sns:Publish",
                    "Principal": "*",
                    "Resource": "*",
                }
            ]
        )
        with pytest.raises(KeyError):
            policy_document.get_policy_statement_by_sid("NotExists")

    def test_get_policy_statement_by_sid_exists(self) -> None:
        sid = "GroundSloth"
        statement = {
            "Sid": sid,
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Principal": "*",
            "Resource": "*",
        }
        policy_document = PolicyDocument.create_bucket_policy([{**statement, "Sid": "other"}, statement])
        assert policy_document.get_policy_statement_by_sid(sid) == statement

    def test_add_or_update_nonexisting_sid(self) -> None:
        sid1 = "onetwothree"
        sid2 = "mississippi"
        statement_without_sid = {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Principal": "*",
            "Resource": "*",
        }
        statement = {**statement_without_sid, "Sid": sid1}
        policy_document = PolicyDocument.create_bucket_policy([statement_without_sid, statement])

        new_statement = {**statement_without_sid, "Sid": sid2}
        new_policy_document = policy_document.add_or_update_statement(new_statement)

        assert not policy_document.has_statement_with_sid(sid2)
        assert policy_document.get_policy_statement_by_sid(sid1) == statement
        assert statement_without_sid in policy_document.statements
        assert new_policy_document.get_policy_statement_by_sid(sid1) == statement
        assert new_policy_document.get_policy_statement_by_sid(sid2) == new_statement
        assert statement_without_sid in policy_document.statements

    def test_add_or_update_existing_sid(self) -> None:
        sid = "severely-interesting-details"
        statement_without_sid = {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Principal": "*",
            "Resource": "*",
        }
        statement = {**statement_without_sid, "Sid": sid}
        policy_document = PolicyDocument.create_bucket_policy([statement_without_sid, statement])

        new_statement = {**statement_without_sid, "Sid": sid, "Effect": "Deny"}
        new_policy_document = policy_document.add_or_update_statement(new_statement)

        assert policy_document.get_policy_statement_by_sid(sid) == statement
        assert statement_without_sid in policy_document.statements
        assert new_policy_document.get_policy_statement_by_sid(sid) == new_statement
        assert statement_without_sid in new_policy_document.statements

    def test_delete_statement(self) -> None:
        sid = "severely-interesting-details"
        statement_without_sid = {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Principal": "*",
            "Resource": "*",
        }
        statement = {**statement_without_sid, "Sid": sid}
        policy_document = PolicyDocument.create_bucket_policy([statement_without_sid, statement])

        assert policy_document.delete_statement_if_present(sid="NotPresent") == policy_document
        assert policy_document.delete_statement_if_present(sid=sid) == PolicyDocument.create_bucket_policy(
            [statement_without_sid]
        )


class TestValidateResourcePolicyStatements:
    VALID_STATEMENT: Dict[str, Any] = {
        "Sid": "LikesAcorns",
        "Effect": "Allow",
        "Principal": {"AWS": [str(build_arn("iam"))]},
        "Action": "*",
        "Resource": "*",
    }

    def test_valid_statement(self) -> None:
        PolicyDocument.create_bucket_policy([self.VALID_STATEMENT]).validate_resource_policy_statements()

    def test_missing_required_key(self) -> None:
        statement = self.VALID_STATEMENT.copy()
        del statement["Resource"]
        with assert_raises(InvalidPolicyStatement("missing required key", statement)):
            PolicyDocument.create_bucket_policy([statement]).validate_resource_policy_statements()

    def test_unsupported_keys(self) -> None:
        statement = {**self.VALID_STATEMENT, "Description": "bogus"}
        with assert_raises(InvalidPolicyStatement("unsupported keys", statement)):
            PolicyDocument.create_bucket_policy([statement]).validate_resource_policy_statements()

    def test_invalid_effect(self) -> None:
        statement = {**self.VALID_STATEMENT, "Effect": "maybe"}
        with assert_raises(InvalidPolicyStatement("invalid effect", statement)):
            PolicyDocument.create_bucket_policy([statement]).validate_resource_policy_statements()

    def test_invalid_principal(self) -> None:
        statement = {**self.VALID_STATEMENT, "Principal": {"Owner": [str(build_arn("iam"))]}}
        with assert_raises(InvalidPolicyStatement("invalid principal", statement)):
            PolicyDocument.create_bucket_policy([statement]).validate_resource_policy_statements()


@pytest.mark.usefixtures("mock_config_file")
class TestSplitStatementsToManagedPolicies:
    def test_empty_list(self) -> None:
        assert PolicyDocument.split_statements_to_managed_policies([]) == []

    def test_statements_fit_into_one_policy(self) -> None:
        statement1 = {"Sid": "Anarchy", "Action": "*", "Resource": "*", "Principal": "*"}
        statement2 = {"Sid": "AllowRead", "Action": "Read", "Resource": "Books", "Principal": "*"}
        assert PolicyDocument.split_statements_to_managed_policies([statement1, statement2]) == [
            PolicyDocument.create_managed_policy([statement1, statement2])
        ]

    def test_must_split_policies(self) -> None:
        statements = [
            {"Sid": f"Allow{i}", "Action": "s3:*", "Resource": f"bucket{i}", "Principal": "*"} for i in range(100)
        ]
        policies = PolicyDocument.split_statements_to_managed_policies(statements)
        assert len(policies) > 1
        # Don't split too early:
        assert len(policies[0].encode()) > 0.9 * PolicyDocumentType.MANAGED.get_max_policy_length()

    def test_too_many_statements(self) -> None:
        statements = [
            {"Sid": f"Loooong{i}", "Action": "*", "Resource": "a" * 6000, "Principal": "*"}
            for i in range(
                ConfigFileLoader.get_config().aws_service.iam.configured_limits.max_managed_policies_per_role + 1
            )
        ]
        with pytest.raises(PolicyCountExceeded):
            PolicyDocument.split_statements_to_managed_policies(statements)


def build_policy_statement() -> Dict[str, Any]:
    return {
        "Sid": Builder.build_random_string(),
        "Effect": Builder.build_random_string(),
        "Action": [f"{Builder.build_random_string()}:{Builder.build_random_string()}"],
        "Principal": {"AWS": [str(build_arn(service=Builder.build_random_string()))]},
        "Resource": [Builder.build_random_string()],
    }
