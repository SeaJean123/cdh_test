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
import random
from typing import Any
from typing import Callable
from typing import List
from typing import Union
from unittest.mock import call
from unittest.mock import Mock

import pytest

from cdh_core.aws_clients.lakeformation_client import LakeFormationClient
from cdh_core.aws_clients.lakeformation_client import TableFilterAlreadyExists
from cdh_core.aws_clients.lakeformation_client import TableFilterNotFound
from cdh_core.aws_clients.utils import FailedToDeleteResourcesStillAssociating
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.filter_package import ColumnName
from cdh_core.entities.filter_package import RowFilter
from cdh_core.entities.filter_package_test import build_table_filter
from cdh_core.entities.glue_database_test import build_database_name
from cdh_core.entities.glue_database_test import build_glue_database
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class TestLakeFormationClient:
    class InvalidInputException(Exception):
        """Custom InvalidInput Exception for Testing purposes only."""

        def __init__(self) -> None:
            self.response = {"Message": "No permissions revoked."}

    def setup_method(self) -> None:
        self.database_name = build_database_name()
        self.database_account_id = build_account_id()
        self.database = build_glue_database(name=self.database_name, account_id=self.database_account_id)
        self.boto_client_mock = Mock()
        self.boto_client_mock.exceptions.InvalidInputException = self.InvalidInputException
        self.client = LakeFormationClient(self.boto_client_mock)

    def test_revoke_iam_allowed_principals_permissions_for_database(self) -> None:
        self.client.revoke_iam_allowed_principals_permissions_for_database(self.database)

        self.boto_client_mock.revoke_permissions.assert_called_once_with(
            Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
            Resource={"Database": {"CatalogId": self.database_account_id, "Name": self.database_name}},
            Permissions=["ALL"],
            CatalogId=self.database_account_id,
            PermissionsWithGrantOption=[],
        )

    @pytest.mark.parametrize("principal_factory", [build_account_id, lambda: build_arn("iam")])
    def test_grant_write_access_for_database(self, principal_factory: Callable[[], Union[AccountId, Arn]]) -> None:
        principal = principal_factory()

        self.client.grant_write_access_for_database(principal, self.database)

        self.boto_client_mock.grant_permissions.assert_has_calls(
            self._get_write_access_permission_calls(str(principal))
        )

    @pytest.mark.parametrize("principal_factory", [build_account_id, lambda: build_arn("iam")])
    def test_revoke_write_access_for_database(self, principal_factory: Callable[[], Union[AccountId, Arn]]) -> None:
        principal = principal_factory()

        self.client.revoke_write_access_for_database(principal, self.database)

        self.boto_client_mock.revoke_permissions.assert_has_calls(
            self._get_write_access_permission_calls(str(principal))
        )

    @pytest.mark.parametrize("fail_first", [True, False])
    def test_revoke_write_access_for_database_permission_not_found(self, fail_first: bool) -> None:
        self.boto_client_mock.revoke_permissions.side_effect = (
            [self.InvalidInputException, None] if fail_first else [None, self.InvalidInputException]
        )
        principal = build_account_id()

        self.client.revoke_write_access_for_database(principal, self.database)

        self.boto_client_mock.revoke_permissions.assert_has_calls(
            self._get_write_access_permission_calls(str(principal))
        )

    @pytest.mark.parametrize("fail_first", [True, False])
    def test_revoke_write_access_for_database_resource_share_still_associating(self, fail_first: bool) -> None:
        error = Builder.build_client_error("ConcurrentModificationException")
        self.boto_client_mock.revoke_permissions.side_effect = [error, None] if fail_first else [None, error]

        principal = build_account_id()

        with pytest.raises(FailedToDeleteResourcesStillAssociating):
            self.client.revoke_write_access_for_database(principal, self.database)

        calls = self._get_write_access_permission_calls(str(principal))
        self.boto_client_mock.revoke_permissions.assert_has_calls(calls if not fail_first else calls[:1])

    def _get_write_access_permission_calls(self, principal_identifier: str) -> List[Any]:
        return [
            call(
                Principal={"DataLakePrincipalIdentifier": principal_identifier},
                Resource={"Database": {"CatalogId": self.database_account_id, "Name": self.database_name}},
                Permissions=["ALTER", "CREATE_TABLE", "DESCRIBE"],
                PermissionsWithGrantOption=["ALTER", "CREATE_TABLE", "DESCRIBE"],
            ),
            call(
                Principal={"DataLakePrincipalIdentifier": principal_identifier},
                Resource={
                    "Table": {
                        "CatalogId": self.database_account_id,
                        "DatabaseName": self.database_name,
                        "TableWildcard": {},
                    }
                },
                Permissions=["ALL"],
                PermissionsWithGrantOption=["ALL"],
            ),
        ]

    @pytest.mark.parametrize("principal_factory", [build_account_id, lambda: build_arn("iam")])
    def test_grant_write_access_for_s3_resource(self, principal_factory: Callable[[], Union[AccountId, Arn]]) -> None:
        principal = principal_factory()
        resource_account_id = build_account_id()
        bucket_arn = build_arn(service="s3", account_id=resource_account_id)
        s3_resource = build_s3_resource(resource_account_id=resource_account_id, arn=bucket_arn)

        self.client.grant_write_access_for_s3_resource(principal, s3_resource)

        self.boto_client_mock.grant_permissions.assert_called_once_with(
            Principal={"DataLakePrincipalIdentifier": str(principal)},
            Resource={"DataLocation": {"CatalogId": resource_account_id, "ResourceArn": str(bucket_arn)}},
            Permissions=["DATA_LOCATION_ACCESS"],
            PermissionsWithGrantOption=["DATA_LOCATION_ACCESS"],
        )

    @pytest.mark.parametrize("permission_missing", [True, False])
    @pytest.mark.parametrize("principal_factory", [build_account_id, lambda: build_arn("iam")])
    def test_revoke_write_access_for_s3_resource(
        self, principal_factory: Callable[[], Union[AccountId, Arn]], permission_missing: bool
    ) -> None:
        principal = principal_factory()
        resource_account_id = build_account_id()
        bucket_arn = build_arn(service="s3", account_id=resource_account_id)
        s3_resource = build_s3_resource(resource_account_id=resource_account_id, arn=bucket_arn)
        if permission_missing:
            self.boto_client_mock.revoke_permissions.side_effect = self.InvalidInputException

        self.client.revoke_write_access_for_s3_resource(principal, s3_resource)

        self.boto_client_mock.revoke_permissions.assert_called_once_with(
            Principal={"DataLakePrincipalIdentifier": str(principal)},
            Resource={"DataLocation": {"CatalogId": resource_account_id, "ResourceArn": str(bucket_arn)}},
            Permissions=["DATA_LOCATION_ACCESS"],
            PermissionsWithGrantOption=["DATA_LOCATION_ACCESS"],
        )

    @pytest.mark.parametrize("grantable", [True, False])
    @pytest.mark.parametrize("principal_factory", [build_account_id, lambda: build_arn("iam")])
    def test_grant_read_access_for_database(
        self, principal_factory: Callable[[], Union[AccountId, Arn]], grantable: bool
    ) -> None:
        principal = principal_factory()

        self.client.grant_read_access_for_database(principal, self.database, grantable)

        self.boto_client_mock.grant_permissions.assert_called_once_with(
            Principal={"DataLakePrincipalIdentifier": str(principal)},
            Resource={
                "Table": {
                    "CatalogId": self.database_account_id,
                    "DatabaseName": self.database_name,
                    "TableWildcard": {},
                }
            },
            Permissions=["DESCRIBE", "SELECT"],
            PermissionsWithGrantOption=["DESCRIBE", "SELECT"] if grantable else [],
        )

    @pytest.mark.parametrize("fail_if_missing", [True, False])
    @pytest.mark.parametrize("grantable", [True, False])
    @pytest.mark.parametrize("principal_factory", [build_account_id, lambda: build_arn("iam")])
    def test_revoke_read_access_for_database(
        self, principal_factory: Callable[[], Union[AccountId, Arn]], grantable: bool, fail_if_missing: bool
    ) -> None:
        principal = principal_factory()

        self.client.revoke_read_access_for_database(principal, self.database, grantable, fail_if_missing)

        self.boto_client_mock.revoke_permissions.assert_called_once_with(
            Principal={"DataLakePrincipalIdentifier": str(principal)},
            Resource={
                "Table": {
                    "CatalogId": self.database_account_id,
                    "DatabaseName": self.database_name,
                    "TableWildcard": {},
                }
            },
            Permissions=["DESCRIBE", "SELECT"],
            PermissionsWithGrantOption=["DESCRIBE", "SELECT"] if grantable else [],
        )

    @pytest.mark.parametrize("fail_if_missing", [True, False])
    @pytest.mark.parametrize("grantable", [True, False])
    @pytest.mark.parametrize("principal_factory", [build_account_id, lambda: build_arn("iam")])
    def test_revoke_read_access_for_database_no_permissions_revoked(
        self, principal_factory: Callable[[], Union[AccountId, Arn]], grantable: bool, fail_if_missing: bool
    ) -> None:
        self.boto_client_mock.revoke_permissions.side_effect = self.InvalidInputException
        principal = principal_factory()

        if fail_if_missing:
            with pytest.raises(self.InvalidInputException):
                self.client.revoke_read_access_for_database(principal, self.database, grantable, fail_if_missing)
        else:
            self.client.revoke_read_access_for_database(principal, self.database, grantable, fail_if_missing)

    def test_revoke_read_access_for_database_concurrent_modification(self) -> None:
        error = Builder.build_client_error("ConcurrentModificationException")
        self.boto_client_mock.revoke_permissions.side_effect = error
        grantable = random.choice([True, False])
        fail_if_missing = random.choice([True, False])
        principal = build_account_id()

        with pytest.raises(FailedToDeleteResourcesStillAssociating):
            self.client.revoke_read_access_for_database(principal, self.database, grantable, fail_if_missing)

    def test_create_table_filter_with_full_access(self) -> None:
        table_filter = build_table_filter(full_access=True)

        self.client.create_table_filter(table_filter)

        self.boto_client_mock.create_data_cells_filter.assert_called_once_with(
            TableData={
                "Name": table_filter.filter_id,
                "TableCatalogId": table_filter.resource_account_id,
                "DatabaseName": table_filter.database_name,
                "TableName": table_filter.table_name,
                "RowFilter": {
                    "AllRowsWildcard": {},
                },
                "ColumnNames": [],
            }
        )

    def test_create_table_filters_with_included_columns(self) -> None:
        column_name = ColumnName(Builder.build_random_string())
        table_filter = build_table_filter(full_access=False, row_filter=RowFilter(""), included_columns=[column_name])

        self.client.create_table_filter(table_filter)

        self.boto_client_mock.create_data_cells_filter.assert_called_once_with(
            TableData={
                "Name": table_filter.filter_id,
                "TableCatalogId": table_filter.resource_account_id,
                "DatabaseName": table_filter.database_name,
                "TableName": table_filter.table_name,
                "RowFilter": {
                    "AllRowsWildcard": {},
                },
                "ColumnNames": [column_name],
            }
        )

    def test_create_table_filters_with_excluded_columns(self) -> None:
        column_name = ColumnName(Builder.build_random_string())
        table_filter = build_table_filter(full_access=False, row_filter=RowFilter(""), excluded_columns=[column_name])
        self.client.create_table_filter(table_filter)

        self.boto_client_mock.create_data_cells_filter.assert_called_once_with(
            TableData={
                "Name": table_filter.filter_id,
                "TableCatalogId": table_filter.resource_account_id,
                "DatabaseName": table_filter.database_name,
                "TableName": table_filter.table_name,
                "RowFilter": {
                    "AllRowsWildcard": {},
                },
                "ColumnNames": [],
                "ColumnWildcard": {"ExcludedColumnNames": [column_name]},
            }
        )

    def test_create_table_filters_with_expression(self) -> None:
        expression = RowFilter(Builder.build_random_string())
        table_filter = build_table_filter(
            full_access=False, row_filter=RowFilter(expression), included_columns=[], excluded_columns=[]
        )

        self.client.create_table_filter(table_filter)

        self.boto_client_mock.create_data_cells_filter.assert_called_once_with(
            TableData={
                "Name": table_filter.filter_id,
                "TableCatalogId": table_filter.resource_account_id,
                "DatabaseName": table_filter.database_name,
                "TableName": table_filter.table_name,
                "RowFilter": {
                    "FilterExpression": expression,
                },
                "ColumnNames": [],
            }
        )

    def test_create_already_existing_table_filter(self) -> None:
        error = Builder.build_client_error("AlreadyExistsException")
        self.boto_client_mock.create_data_cells_filter.side_effect = error

        with pytest.raises(TableFilterAlreadyExists):
            self.client.create_table_filter(build_table_filter())

    def test_delete_table_filter(self) -> None:
        table_filter = build_table_filter()

        self.client.delete_table_filter(table_filter)

        self.boto_client_mock.delete_data_cells_filter.assert_called_once_with(
            Name=table_filter.filter_id,
            TableCatalogId=table_filter.resource_account_id,
            DatabaseName=table_filter.database_name,
            TableName=table_filter.table_name,
        )

    def test_missing_delete_table_filter(self) -> None:
        error = Builder.build_client_error("EntityNotFoundException")
        self.boto_client_mock.delete_data_cells_filter.side_effect = error

        with pytest.raises(TableFilterNotFound):
            self.client.delete_table_filter(build_table_filter())
