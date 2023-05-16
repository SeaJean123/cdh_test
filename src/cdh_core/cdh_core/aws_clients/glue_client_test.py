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
from contextlib import ExitStack
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from unittest.mock import call
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError
from freezegun import freeze_time

from cdh_core.aws_clients.glue_client import GlueClient
from cdh_core.aws_clients.glue_client import GlueDatabaseAlreadyExists
from cdh_core.aws_clients.glue_client import GlueDatabaseNotFound
from cdh_core.aws_clients.glue_client import GlueTable
from cdh_core.aws_clients.glue_client import GlueTableNotFound
from cdh_core.aws_clients.glue_client import LOG
from cdh_core.aws_clients.glue_resource_policy import GlueResourcePolicy
from cdh_core.aws_clients.glue_resource_policy import PROTECT_RESOURCE_LINKS_SID
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.policy import PolicySizeExceeded
from cdh_core.aws_clients.policy_test import build_policy_statement
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.entities.glue_database_test import build_database_name
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class TestGlueClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_glue: Any) -> None:  # pylint: disable=unused-argument
        self.region = build_region()
        self.account = build_account()
        self.database_name = Builder.build_random_string()
        self.table_name = Builder.build_random_string()
        self.boto_client = boto3.client("glue", region_name=self.region.value)
        self.glue_client = GlueClient(self.boto_client)

    def test_get_all_database_names(self) -> None:
        databases = [{"Name": self.database_name}]
        with patch("cdh_core.aws_clients.glue_client.repeat_continuation_call", return_value=databases) as repeat_call:
            result = self.glue_client.get_all_database_names()
            repeat_call.assert_called_once_with(self.boto_client.get_databases, "DatabaseList")
            assert result == [self.database_name]

    @pytest.mark.parametrize("specify_account_id", [True, False])
    def test_get_tables(self, specify_account_id: bool) -> None:
        tables = [{"Name": "table", "DatabaseName": self.database_name, "StorageDescriptor": {"Location": "location"}}]
        account_id = build_account_id() if specify_account_id else None
        with patch("cdh_core.aws_clients.glue_client.repeat_continuation_call", return_value=tables) as repeat_call:
            result = self.glue_client.get_tables(self.database_name, account_id)
            if account_id:
                repeat_call.assert_called_once_with(
                    self.boto_client.get_tables, "TableList", DatabaseName=self.database_name, CatalogId=account_id
                )
            else:
                repeat_call.assert_called_once_with(
                    self.boto_client.get_tables, "TableList", DatabaseName=self.database_name
                )
            assert result == [GlueTable(name="table", database_name=self.database_name, location="location")]

    def test_get_tables_error(self) -> None:
        with patch(
            "cdh_core.aws_clients.glue_client.repeat_continuation_call",
            side_effect=ClientError(error_response={"Error": {"Code": "EntityNotFoundException"}}, operation_name=""),
        ):
            with pytest.raises(GlueDatabaseNotFound):
                self.glue_client.get_tables(self.database_name)

    def test_get_table_version_ids(self) -> None:
        versions = [{"VersionId": "0"}]
        with patch("cdh_core.aws_clients.glue_client.repeat_continuation_call", return_value=versions) as repeat_call:
            result = self.glue_client.get_table_version_ids(database=self.database_name, table=self.table_name)
            repeat_call.assert_called_once_with(
                self.boto_client.get_table_versions,
                "TableVersions",
                DatabaseName=self.database_name,
                TableName=self.table_name,
            )
            assert result == ["0"]

    def test_get_table_version_ids_error(self) -> None:
        with patch(
            "cdh_core.aws_clients.glue_client.repeat_continuation_call",
            side_effect=ClientError(error_response={"Error": {"Code": "EntityNotFoundException"}}, operation_name=""),
        ), pytest.raises(GlueTableNotFound):
            self.glue_client.get_table_version_ids(database=self.database_name, table=self.table_name)

    def test_delete_table_versions(self) -> None:
        versions = [Builder.build_random_string()]
        with patch.object(self.boto_client, "batch_delete_table_version") as batch_delete_table_versions:
            self.glue_client.delete_table_versions(self.database_name, self.table_name, versions)
            batch_delete_table_versions.assert_called_once_with(
                DatabaseName=self.database_name, TableName=self.table_name, VersionIds=versions
            )

    def test_handle_database_event_deletes_in_chunks(self) -> None:
        versions = ["0", "1", "2", "3", "4"]
        versions_to_delete_batches = [["0", "1", "2"], ["3", "4"]]

        with patch("cdh_core.aws_clients.glue_client.MAX_VERSIONS_PER_BATCH_DELETE", 3), patch.object(
            self.boto_client, "batch_delete_table_version"
        ) as batch_delete_table_versions:
            self.glue_client.delete_table_versions(self.database_name, self.table_name, versions)
            batch_delete_table_versions.assert_has_calls(
                [
                    call(DatabaseName=self.database_name, TableName=self.table_name, VersionIds=batch)
                    for batch in versions_to_delete_batches
                ]
            )

    def test_create_already_existing_database_raises(self) -> None:
        self.boto_client.create_database(
            DatabaseInput={
                "Name": self.database_name,
            }
        )

        with pytest.raises(GlueDatabaseAlreadyExists):
            self.glue_client.create_database(self.database_name)

    def test_create_database_successful(self) -> None:
        now = datetime.now(tz=timezone.utc)

        with freeze_time(now):
            self.glue_client.create_database(self.database_name)

        assert self.boto_client.get_database(Name=self.database_name)["Database"] == {
            "Name": self.database_name,
            "CreateTime": now,
        }

    def test_create_database_successful_remove_default_permissions(self) -> None:
        now = datetime.now(tz=timezone.utc)

        with freeze_time(now):
            self.glue_client.create_database(self.database_name, remove_default_permissions=True)

        assert self.boto_client.get_database(Name=self.database_name)["Database"] == {
            "Name": self.database_name,
            "CreateTime": now,
            "CreateTableDefaultPermissions": [],
        }

    def test_database_exists_false(self) -> None:
        assert not self.glue_client.database_exists(self.database_name)

    def test_database_exists_true(self) -> None:
        self.boto_client.create_database(
            DatabaseInput={
                "Name": self.database_name,
            }
        )

        assert self.glue_client.database_exists(self.database_name)

    def test_delete_non_existent_database_does_not_raise_an_exception(self) -> None:
        self.glue_client.delete_database_if_present(self.database_name)

    def test_delete_database(self) -> None:
        self.boto_client.create_database(
            DatabaseInput={
                "Name": self.database_name,
            }
        )

        self.glue_client.delete_database_if_present(self.database_name)

        with pytest.raises(self.boto_client.exceptions.EntityNotFoundException):
            self.boto_client.get_database(Name=self.database_name)


class TestDeleteProtectedDatabase:
    class _AccessDenied(ClientError):
        def __init__(self) -> None:
            super().__init__(
                error_response={"Error": {"Code": "AccessDeniedException", "Message": "nope!"}}, operation_name="foo"
            )

    class _NotFound(Exception):
        ...

    def setup_method(self) -> None:
        self.account_id = build_account_id()
        self.region = build_region()
        unprotected_databases: List[str] = []
        self.database_deletion = Mock()
        self.boto_client = self._create_mock_boto_client(
            region_name=self.region.value,
            unprotected_databases=unprotected_databases,
            database_deletion=self.database_deletion,
        )
        self.sleep = Mock()
        self.glue_client = GlueClient(self.boto_client, sleep=self.sleep)
        self.glue_client.remove_deletion_protection = Mock(  # type: ignore
            side_effect=lambda x, _: unprotected_databases.append(x)
        )
        self.database_name = build_database_name()

    def _create_mock_boto_client(
        self, region_name: str, unprotected_databases: List[str], database_deletion: Callable[[str], None]
    ) -> Mock:
        boto_client = Mock()
        boto_client.exceptions.AccessDeniedException = TestDeleteProtectedDatabase._AccessDenied
        boto_client.exceptions.EntityNotFoundException = TestDeleteProtectedDatabase._NotFound
        boto_client.meta.region_name = region_name

        def delete(Name: str) -> None:  # pylint: disable=invalid-name
            if Name not in unprotected_databases:
                raise TestDeleteProtectedDatabase._AccessDenied()
            database_deletion(Name)

        boto_client.delete_database = Mock(side_effect=delete)
        return boto_client

    def test_cannot_delete_without_removing_protection(self) -> None:
        # this test just verifies the test setup works as intended
        with pytest.raises(TestDeleteProtectedDatabase._AccessDenied):
            self.glue_client.delete_database_if_present(self.database_name)
        self.database_deletion.assert_not_called()

    def test_delete_protected_database(self) -> None:
        self.glue_client.delete_protected_database(self.database_name, account_id=self.account_id)

        self.glue_client.remove_deletion_protection.assert_called_once_with(  # type: ignore
            self.database_name, self.account_id
        )
        self.database_deletion.assert_called_once_with(self.database_name)

    def test_delete_protected_database_retry_access_denied(self) -> None:
        calls: Dict[str, bool] = {}

        def delete(db_name: str) -> None:
            if not calls.get(db_name):
                calls[db_name] = True
                raise TestDeleteProtectedDatabase._AccessDenied()

        self.database_deletion.side_effect = delete

        self.glue_client.delete_protected_database(self.database_name, account_id=self.account_id)
        assert len(self.database_deletion.call_args_list) == 2
        self.sleep.assert_called_once()
        assert self.sleep.call_args.args[0] > 0

    def test_delete_protected_database_reraise_access_denied_eventually(self) -> None:
        def delete(db_name: str) -> None:
            raise TestDeleteProtectedDatabase._AccessDenied()

        self.database_deletion.side_effect = delete

        with pytest.raises(TestDeleteProtectedDatabase._AccessDenied):
            self.glue_client.delete_protected_database(self.database_name, account_id=self.account_id)


class TestSetupGlueClient:
    class NotFound(Exception):
        """Custom Not Found Exception for Testing purposes only."""

    class AccessDeniedException(Exception):
        """Custom AccessDenied Exception for Testing purposes only."""

    class InvalidInputException(Exception):
        """Custom InvalidInput Exception for Testing purposes only."""

    def setup_method(self) -> None:
        self.account_id = build_account_id()
        self.region = build_region()
        self.boto_client = Mock()
        self.boto_client.exceptions.EntityNotFoundException = TestSetupGlueClient.NotFound
        self.boto_client.exceptions.AccessDeniedException = TestSetupGlueClient.AccessDeniedException
        self.boto_client.exceptions.InvalidInputException = TestSetupGlueClient.InvalidInputException
        self.boto_client.meta.region_name = self.region.value
        self.glue_client = GlueClient(self.boto_client)
        self.database_name = build_database_name()
        self.database_arn = build_arn(
            service="glue",
            resource=f"database/{self.database_name}",
            account_id=self.account_id,
            region=self.region,
        )
        self.principal = self.glue_client._get_resource_link_protection_principal(  # pylint: disable=protected-access
            account_id=self.account_id
        )

    def set_current_policy(self, statements: List[Dict[str, Any]]) -> Tuple[PolicyDocument, str]:
        mocked_policy_document = PolicyDocument.create_glue_resource_policy(statements)
        mocked_hash_condition = Builder.build_random_string()
        self.boto_client.get_resource_policy.return_value = {
            "PolicyInJson": mocked_policy_document.encode(),
            "PolicyHash": mocked_hash_condition,
        }
        return mocked_policy_document, mocked_hash_condition


class TestGlueClientProtect(TestSetupGlueClient):
    def test_protect_policy_if_not_exist(self) -> None:
        self.boto_client.get_resource_policy.side_effect = TestSetupGlueClient.NotFound("No resource policy")
        policy_document = PolicyDocument.create_glue_resource_policy(
            statements=[
                GlueResourcePolicy.create_resource_link_protect_policy_statement(
                    principal=self.principal, resources={self.database_arn}
                )
            ]
        )

        self.glue_client.add_deletion_protection(database_name=self.database_name, account_id=self.account_id)

        self.boto_client.put_resource_policy.assert_called_once_with(
            PolicyInJson=policy_document.encode(),
            EnableHybrid="TRUE",
            PolicyExistsCondition="NOT_EXIST",
        )

    @patch.object(LOG, "warning")
    def test_protect_policy_database_ignore_access_denied(self, mock_log_warning: Mock) -> None:
        self.boto_client.put_resource_policy.side_effect = TestSetupGlueClient.AccessDeniedException("Access Denied")
        statements = [build_policy_statement() for _ in range(3)]
        self.set_current_policy(statements)

        self.glue_client.add_deletion_protection(database_name=self.database_name, account_id=self.account_id)

        self.boto_client.put_resource_policy.assert_called_once()
        mock_log_warning.assert_called_once_with(
            f"No permission to update glue resource policy to protect {self.database_arn}"
        )

    def test_protect_policy_other_policies_not_overwritten(self) -> None:
        statements = [build_policy_statement() for _ in range(3)]
        mocked_policy_document, mocked_hash_condition = self.set_current_policy(statements)
        self.glue_client.add_deletion_protection(database_name=self.database_name, account_id=self.account_id)

        mocked_policy_document.statements.append(
            GlueResourcePolicy.create_resource_link_protect_policy_statement(
                principal=self.principal, resources={self.database_arn}
            )
        )
        assert len(mocked_policy_document.statements) == 4
        self.boto_client.put_resource_policy.assert_called_once_with(
            PolicyInJson=mocked_policy_document.encode(),
            PolicyHashCondition=mocked_hash_condition,
            EnableHybrid="TRUE",
            PolicyExistsCondition="MUST_EXIST",
        )

    def test_protect_policy_database_already_added(self) -> None:
        statements = [build_policy_statement() for _ in range(3)] + [
            GlueResourcePolicy.create_resource_link_protect_policy_statement(
                principal=self.principal, resources={self.database_arn}
            )
        ]
        self.set_current_policy(statements)

        self.glue_client.add_deletion_protection(database_name=self.database_name, account_id=self.account_id)

        self.boto_client.put_resource_policy.assert_not_called()

    def test_protect_a_second_database(self) -> None:
        statements = [build_policy_statement() for _ in range(3)] + [
            GlueResourcePolicy.create_resource_link_protect_policy_statement(
                principal=self.principal, resources={self.database_arn}
            )
        ]
        mocked_policy_document, mocked_hash_condition = self.set_current_policy(statements)

        second_database_name = build_database_name()
        second_database_arn = build_arn(
            service="glue", resource=f"database/{second_database_name}", account_id=self.account_id, region=self.region
        )
        self.glue_client.add_deletion_protection(database_name=second_database_name, account_id=self.account_id)

        update_statement = mocked_policy_document.get_policy_statement_by_sid(PROTECT_RESOURCE_LINKS_SID)
        update_statement["Resource"] = sorted([str(second_database_arn), str(self.database_arn)])
        mocked_policy_document.add_or_update_statement(statement=update_statement)
        self.boto_client.put_resource_policy.assert_called_once_with(
            PolicyInJson=mocked_policy_document.encode(),
            PolicyHashCondition=mocked_hash_condition,
            EnableHybrid="TRUE",
            PolicyExistsCondition="MUST_EXIST",
        )

    def test_policy_size_exceeded(self) -> None:
        mock_policy_document, _ = self.set_current_policy([])
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(PolicyDocument, "create_glue_resource_policy", return_value=mock_policy_document)
            )
            stack.enter_context(
                patch.object(mock_policy_document, "add_or_update_statement", side_effect=PolicySizeExceeded)
            )
            self.glue_client.add_deletion_protection(database_name=self.database_name, account_id=self.account_id)
        self.boto_client.put_resource_policy.assert_not_called()
        self.boto_client.delete_resource_policy.assert_not_called()

    def test_policy_size_exceeded_boto(self) -> None:
        mock_policy_document, _ = self.set_current_policy([])
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(PolicyDocument, "create_glue_resource_policy", return_value=mock_policy_document)
            )
            stack.enter_context(
                patch(
                    "cdh_core.aws_clients.glue_client.get_error_message", return_value="Resource policy size is limited"
                )
            )
            stack.enter_context(
                patch.object(
                    mock_policy_document,
                    "add_or_update_statement",
                    side_effect=TestSetupGlueClient.InvalidInputException,
                )
            )
            self.glue_client.add_deletion_protection(database_name=self.database_name, account_id=self.account_id)
        self.boto_client.put_resource_policy.assert_not_called()
        self.boto_client.delete_resource_policy.assert_not_called()

    def test_replace_policy_statement(self) -> None:
        database_name = build_database_name()
        database_arn = GlueDatabase(name=database_name, account_id=self.account_id, region=self.region).arn
        other_database_arn = build_arn(service=Builder.build_random_string())
        _, mocked_hash_condition = self.set_current_policy(
            [
                {
                    "Sid": PROTECT_RESOURCE_LINKS_SID,
                    "Effect": Builder.build_random_string(),
                    "Action": [Builder.build_random_string()],
                    "Principal": {"AWS": [Builder.build_random_string()]},
                    "Resource": [str(other_database_arn)],
                }
            ]
        )

        self.glue_client.add_deletion_protection(database_name=database_name, account_id=self.account_id)
        expected_policy_document = PolicyDocument.create_glue_resource_policy(
            [
                {
                    "Sid": PROTECT_RESOURCE_LINKS_SID,
                    "Effect": "Deny",
                    "Action": ["glue:DeleteDatabase"],
                    "Principal": {"AWS": [f"arn:{self.region.partition.value}:iam::{self.account_id}:root"]},
                    "Resource": sorted([str(database_arn), str(other_database_arn)]),
                }
            ]
        )
        self.boto_client.put_resource_policy.assert_called_once_with(
            PolicyInJson=expected_policy_document.encode(),
            PolicyHashCondition=mocked_hash_condition,
            EnableHybrid="TRUE",
            PolicyExistsCondition="MUST_EXIST",
        )


class TestGlueClientUnprotect(TestSetupGlueClient):
    def test_unprotect_policy_if_not_exist(self) -> None:
        class NotFound(Exception):
            ...

        self.boto_client.exceptions.EntityNotFoundException = NotFound
        self.boto_client.get_resource_policy.side_effect = NotFound("No resource policy")

        self.glue_client.remove_deletion_protection(database_name=self.database_name, account_id=self.account_id)
        self.boto_client.put_resource_policy.assert_not_called()
        self.boto_client.delete_resource_policy.assert_not_called()

    @patch.object(LOG, "warning")
    def test_unprotect_policy_database_ignore_access_denied(self, mock_log_warning: Mock) -> None:
        self.boto_client.delete_resource_policy.side_effect = TestSetupGlueClient.AccessDeniedException("Access Denied")
        statements = [
            GlueResourcePolicy.create_resource_link_protect_policy_statement(
                principal=self.principal, resources={self.database_arn}
            )
        ]
        self.set_current_policy(statements)

        self.glue_client.remove_deletion_protection(database_name=self.database_name, account_id=self.account_id)

        self.boto_client.delete_resource_policy.assert_called_once()
        mock_log_warning.assert_called_once_with(
            f"No permission to update glue resource policy to unprotect {self.database_arn}"
        )

    def test_unprotect_and_delete_policy(self) -> None:
        statements = [
            GlueResourcePolicy.create_resource_link_protect_policy_statement(
                principal=self.principal, resources={self.database_arn}
            )
        ]
        _, mocked_hash_condition = self.set_current_policy(statements)
        self.glue_client.remove_deletion_protection(self.database_name, account_id=self.account_id)
        self.boto_client.delete_resource_policy.assert_called_once_with(PolicyHashCondition=mocked_hash_condition)

    def test_unprotect_policy_remove_full_statement(self) -> None:
        statements = [build_policy_statement() for _ in range(3)] + [
            GlueResourcePolicy.create_resource_link_protect_policy_statement(
                principal=self.principal, resources={self.database_arn}
            )
        ]
        _, mocked_hash_condition = self.set_current_policy(statements)
        expected_policy_document = PolicyDocument.create_glue_resource_policy(statements[:-1])
        self.glue_client.remove_deletion_protection(self.database_name, account_id=self.account_id)
        self.boto_client.put_resource_policy.assert_called_once_with(
            PolicyInJson=expected_policy_document.encode(),
            PolicyHashCondition=mocked_hash_condition,
            EnableHybrid="TRUE",
            PolicyExistsCondition="MUST_EXIST",
        )

    def test_delete_one_database_out_of_multiple(self) -> None:
        other_resources = {build_arn(service="glue") for _ in range(3)}

        statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources=other_resources | {self.database_arn}
        )
        _, mocked_hash_condition = self.set_current_policy([statement])
        expected_statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=self.principal, resources=other_resources
        )
        expected_policy_document = PolicyDocument.create_glue_resource_policy([expected_statement])
        self.glue_client.remove_deletion_protection(self.database_name, account_id=self.account_id)
        self.boto_client.put_resource_policy.assert_called_once_with(
            PolicyInJson=expected_policy_document.encode(),
            PolicyHashCondition=mocked_hash_condition,
            EnableHybrid="TRUE",
            PolicyExistsCondition="MUST_EXIST",
        )

    def test_replace_policy_statement(self) -> None:
        database_name = build_database_name()
        database_arn = GlueDatabase(name=database_name, account_id=self.account_id, region=self.region).arn
        other_database_arn = GlueDatabase(
            name=build_database_name(), account_id=self.account_id, region=self.region
        ).arn
        _, mocked_hash_condition = self.set_current_policy(
            [
                {
                    "Sid": PROTECT_RESOURCE_LINKS_SID,
                    "Effect": Builder.build_random_string(),
                    "Action": [Builder.build_random_string()],
                    "Principal": {"AWS": [Builder.build_random_string()]},
                    "Resource": [str(database_arn), str(other_database_arn)],
                }
            ]
        )

        self.glue_client.remove_deletion_protection(database_name=database_name, account_id=self.account_id)
        expected_policy_document = PolicyDocument.create_glue_resource_policy(
            [
                {
                    "Sid": PROTECT_RESOURCE_LINKS_SID,
                    "Effect": "Deny",
                    "Action": ["glue:DeleteDatabase"],
                    "Principal": {"AWS": [f"arn:{self.region.partition.value}:iam::{self.account_id}:root"]},
                    "Resource": [str(other_database_arn)],
                }
            ]
        )
        self.boto_client.put_resource_policy.assert_called_once_with(
            PolicyInJson=expected_policy_document.encode(),
            PolicyHashCondition=mocked_hash_condition,
            EnableHybrid="TRUE",
            PolicyExistsCondition="MUST_EXIST",
        )
