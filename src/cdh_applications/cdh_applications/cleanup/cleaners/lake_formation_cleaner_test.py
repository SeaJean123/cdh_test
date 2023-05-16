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
# pylint: disable=protected-access
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import TYPE_CHECKING
from unittest.mock import call
from unittest.mock import Mock
from unittest.mock import patch

from cdh_applications.cleanup.cleaners.lake_formation_cleaner import LakeFormationCleaner
from cdh_applications.cleanup.cleanup_utils_test import PREFIX
from cdh_core.entities.accounts_test import build_base_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.builder import Builder

if TYPE_CHECKING:
    from mypy_boto3_lakeformation.type_defs import PrincipalResourcePermissionsTypeDef
    from mypy_boto3_lakeformation.type_defs import ResourceTypeDef
else:
    PrincipalResourcePermissionsTypeDef = Dict[str, Any]
    ResourceTypeDef = Dict[str, Any]


class TestLakeFormationCleaner:
    def setup_method(self) -> None:
        self.lakeformation_client = Mock()
        self.glue_client = Mock()

        mock_session = Mock()
        mock_session.client.side_effect = [self.lakeformation_client, self.glue_client]
        with patch("cdh_applications.cleanup.cleaners.lake_formation_cleaner.boto3") as mock_boto3:
            mock_boto3.session.Session.return_value = mock_session
            self.lake_formation_cleaner = LakeFormationCleaner(
                region=build_region().value,
                prefix=PREFIX,
                clean_filter=Mock(return_value=True),
                credentials={},
                partition=build_partition(),
                account=build_base_account(),
                log=getLogger(),
            )

    def test_clean(self) -> None:
        self.lake_formation_cleaner._deregister_resources = mock_deregister_resources = Mock()  # type: ignore
        self.lake_formation_cleaner._give_control_back_to_iam = mock_give_control_back_to_iam = Mock()  # type: ignore
        self.lake_formation_cleaner._clean_cross_account_permissions = mock_clean_cap = Mock()  # type: ignore

        self.lake_formation_cleaner.clean()

        mock_deregister_resources.assert_called_once()
        mock_give_control_back_to_iam.assert_called_once()
        mock_clean_cap.assert_called_once()

    def test_deregister_resources(self) -> None:
        shared_resource_names = [PREFIX + Builder.build_random_string() for _ in range(3)]
        resources = {
            "ResourceInfoList": [
                {"ResourceArn": shared_resource_name} for shared_resource_name in shared_resource_names
            ]
        }
        self.lakeformation_client.list_resources.return_value = resources

        self.lake_formation_cleaner._deregister_resources()

        self.lakeformation_client.deregister_resource.assert_has_calls(
            [call(ResourceArn=shared_resource_name) for shared_resource_name in shared_resource_names]
        )

    def test_do_not_deregister_resources_if_should_not_clean(self) -> None:
        shared_resource_names = [PREFIX + Builder.build_random_string() for _ in range(3)]
        resources = {
            "ResourceInfoList": [
                {"ResourceArn": shared_resource_name} for shared_resource_name in shared_resource_names
            ]
        }
        self.lakeformation_client.list_resources.return_value = resources
        self.lake_formation_cleaner._should_clean = Mock(return_value=False)

        self.lake_formation_cleaner._deregister_resources()

        self.lakeformation_client.deregister_resource.assert_not_called()

    def test_give_control_back_to_iam(self) -> None:
        prefixed_database_names = [PREFIX + Builder.build_random_string() for _ in range(3)]
        non_prefixed_database_names = [PREFIX + Builder.build_random_string() for _ in range(3)]
        database_names = prefixed_database_names + non_prefixed_database_names
        mock_paginator = Mock()
        mock_paginator.paginate.return_value = [
            {
                "DatabaseList": [
                    {
                        "Name": name,
                        "CatalogId": Builder.build_random_digit_string(12),
                        "CreateTableDefaultPermissions": [
                            {Builder.build_random_string(): Builder.build_random_string()}
                        ],
                        "CreateTime": Builder.build_random_datetime().timestamp(),
                    }
                ]
            }
            for name in database_names
        ]
        self.glue_client.get_paginator.return_value = mock_paginator

        self.lake_formation_cleaner._give_control_back_to_iam()

        self.lakeformation_client.grant_permissions.assert_has_calls(
            [
                call(
                    Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                    Resource={"Database": {"Name": name}},
                    Permissions=["ALL"],
                )
                for name in prefixed_database_names
            ]
        )

    def test_do_not_give_control_back_if_should_not_clean(self) -> None:
        prefixed_database_names = [PREFIX + Builder.build_random_string() for _ in range(3)]
        non_prefixed_database_names = [PREFIX + Builder.build_random_string() for _ in range(3)]
        database_names = prefixed_database_names + non_prefixed_database_names
        mock_paginator = Mock()
        mock_paginator.paginate.return_value = [{"DatabaseList": [{"Name": name}]} for name in database_names]
        self.glue_client.get_paginator.return_value = mock_paginator
        self.lake_formation_cleaner._should_clean = Mock(return_value=False)

        self.lake_formation_cleaner._give_control_back_to_iam()

        self.lakeformation_client.grant_permissions.assert_not_called()

    def test_clean_cross_account_permissions(self) -> None:
        prefixed_name = PREFIX + Builder.build_random_string()
        prefixed_bucket_permission = self._build_permission(resource_type="DataLocation", name=prefixed_name)
        non_prefixed_bucket_permission = self._build_permission(resource_type="DataLocation")
        prefixed_database_permission = self._build_permission(resource_type="Database", database=prefixed_name)
        non_prefixed_database_permission = self._build_permission(resource_type="Database")
        prefixed_table_permission = self._build_permission(resource_type="Table", database=prefixed_name)
        non_prefixed_table_permission = self._build_permission(resource_type="Table")
        non_cross_account_permission = self._build_permission(principal="IAM_ALLOWED_PRINCIPALS")
        self.lakeformation_client.list_permissions.return_value = {
            "PrincipalResourcePermissions": [
                prefixed_bucket_permission,
                non_prefixed_bucket_permission,
                prefixed_database_permission,
                non_prefixed_database_permission,
                prefixed_table_permission,
                non_prefixed_table_permission,
                non_cross_account_permission,
            ]
        }

        self.lake_formation_cleaner._clean_cross_account_permissions()

        self.lakeformation_client.revoke_permissions.assert_has_calls(
            [
                call(
                    Principal=prefixed_bucket_permission["Principal"],
                    Resource=prefixed_bucket_permission["Resource"],
                    Permissions=prefixed_bucket_permission["Permissions"],
                    PermissionsWithGrantOption=prefixed_bucket_permission["PermissionsWithGrantOption"],
                ),
                call(
                    Principal=prefixed_database_permission["Principal"],
                    Resource=prefixed_database_permission["Resource"],
                    Permissions=prefixed_database_permission["Permissions"],
                    PermissionsWithGrantOption=prefixed_database_permission["PermissionsWithGrantOption"],
                ),
                call(
                    Principal=prefixed_table_permission["Principal"],
                    Resource=prefixed_table_permission["Resource"],
                    Permissions=prefixed_table_permission["Permissions"],
                    PermissionsWithGrantOption=prefixed_table_permission["PermissionsWithGrantOption"],
                ),
            ]
        )

    def test_clean_cross_account_permissions_wildcards(self) -> None:
        prefixed_name = PREFIX + Builder.build_random_string()
        prefixed_all_tables_permission = self._build_permission(resource_type="AllTables", database=prefixed_name)
        prefixed_column_permission = self._build_permission(resource_type="TableWithColumns", database=prefixed_name)
        prefixed_all_tables_column_permission = self._build_permission(
            resource_type="TableWithColumns", database=prefixed_name, name="ALL_TABLES"
        )
        self.lakeformation_client.list_permissions.return_value = {
            "PrincipalResourcePermissions": [
                prefixed_all_tables_permission,
                prefixed_column_permission,
                prefixed_all_tables_column_permission,
            ]
        }

        self.lake_formation_cleaner._clean_cross_account_permissions()

        self.lakeformation_client.revoke_permissions.assert_has_calls(
            [
                call(
                    Principal=prefixed_all_tables_permission["Principal"],
                    Resource={
                        "Table": {
                            "CatalogId": prefixed_all_tables_permission["Resource"]["Table"]["CatalogId"],
                            "DatabaseName": prefixed_name,
                            "TableWildcard": {},
                        }
                    },
                    Permissions=prefixed_all_tables_permission["Permissions"],
                    PermissionsWithGrantOption=prefixed_all_tables_permission["PermissionsWithGrantOption"],
                ),
                call(
                    Principal=prefixed_column_permission["Principal"],
                    Resource=prefixed_column_permission["Resource"],
                    Permissions=prefixed_column_permission["Permissions"],
                    PermissionsWithGrantOption=prefixed_column_permission["PermissionsWithGrantOption"],
                ),
                call(
                    Principal=prefixed_all_tables_column_permission["Principal"],
                    Resource={
                        "Table": {
                            "CatalogId": prefixed_all_tables_column_permission["Resource"]["Table"]["CatalogId"],
                            "DatabaseName": prefixed_name,
                            "TableWildcard": {},
                        }
                    },
                    Permissions=prefixed_all_tables_column_permission["Permissions"],
                    PermissionsWithGrantOption=prefixed_all_tables_column_permission["PermissionsWithGrantOption"],
                ),
            ]
        )

    def test_do_not_clean_cross_account_permissions_if_should_not_clean(self) -> None:
        prefixed_permission = self._build_permission(database=PREFIX + Builder.build_random_string())
        self.lakeformation_client.list_permissions.return_value = {
            "PrincipalResourcePermissions": [prefixed_permission]
        }
        self.lake_formation_cleaner._should_clean = Mock(return_value=False)

        self.lake_formation_cleaner._clean_cross_account_permissions()

        self.lakeformation_client.revoke_permissions.assert_not_called()

    def _build_permission(
        self,
        principal: Optional[str] = None,
        resource_type: Optional[str] = None,
        database: Optional[str] = None,
        name: Optional[str] = None,
    ) -> PrincipalResourcePermissionsTypeDef:
        resource_type = resource_type or "Database"
        permissions: List[
            Literal[
                "ALL",
                "ALTER",
                "ASSOCIATE",
                "CREATE_DATABASE",
                "CREATE_TABLE",
                "CREATE_TAG",
                "DATA_LOCATION_ACCESS",
                "DELETE",
                "DESCRIBE",
                "DROP",
                "INSERT",
                "SELECT",
            ]
        ] = ["ALL"]
        if resource_type == "DataLocation":
            permissions = ["DATA_LOCATION_ACCESS"]
        elif resource_type == "Database":
            permissions = ["ALTER", "CREATE_TABLE", "DESCRIBE"]
        elif resource_type == "TableWithColumns":
            permissions = ["SELECT"]

        return {
            "Principal": {"DataLakePrincipalIdentifier": principal or Builder.build_random_digit_string(12)},
            "Resource": self._build_resource(
                resource_type=resource_type,
                database=database or Builder.build_random_string(),
                name=name or Builder.build_random_string(),
            ),
            "Permissions": permissions,
            "PermissionsWithGrantOption": permissions,
        }

    @staticmethod
    def _build_resource(resource_type: str, database: str, name: str) -> ResourceTypeDef:
        if resource_type == "DataLocation":
            bucket_arn = build_arn("s3", name)
            return {
                "DataLocation": {
                    "CatalogId": Builder.build_random_digit_string(12),
                    "ResourceArn": str(bucket_arn),
                }
            }
        if resource_type == "Database":
            return {"Database": {"CatalogId": Builder.build_random_digit_string(12), "Name": database}}
        if resource_type == "Table":
            return {
                "Table": {
                    "CatalogId": Builder.build_random_digit_string(12),
                    "DatabaseName": database,
                    "Name": name,
                }
            }
        if resource_type == "AllTables":
            return {
                "Table": {
                    "CatalogId": Builder.build_random_digit_string(12),
                    "DatabaseName": database,
                    "Name": "ALL_TABLES",
                    "TableWildcard": {},
                }
            }
        if resource_type == "TableWithColumns":
            return {
                "TableWithColumns": {
                    "CatalogId": Builder.build_random_digit_string(12),
                    "DatabaseName": database,
                    "Name": name,
                    "ColumnWildcard": {},
                }
            }
        raise AssertionError("Unknown resource type!")
