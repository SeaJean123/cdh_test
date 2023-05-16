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
from logging import Logger
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Tuple
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

from cdh_applications.cleanup.cleanup_utils import has_prefix
from cdh_applications.cleanup.generic_cleaner import GenericCleaner
from cdh_core.aws_clients.utils import repeat_continuation_call
from cdh_core.entities.accounts import BaseAccount
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws import Partition

if TYPE_CHECKING:
    from mypy_boto3_lakeformation.type_defs import PrincipalResourcePermissionsTypeDef
    from mypy_boto3_lakeformation.type_defs import ResourceTypeDef
else:
    PrincipalResourcePermissionsTypeDef = Dict[str, Any]
    ResourceTypeDef = Dict[str, Any]


class LakeFormationCleaner(GenericCleaner):
    """Cleaner class for the functional tests for the AWS Lake Formation service."""

    def __init__(  # pylint: disable=too-many-arguments,super-init-not-called
        self,
        region: str,
        prefix: str,
        clean_filter: Callable[[str, str, Any], bool],
        credentials: Dict[str, Any],
        partition: Partition,
        account: BaseAccount,
        log: Logger,
    ):
        self._region = region
        self._prefix = prefix
        self._should_clean = clean_filter
        self._session = boto3.session.Session(**credentials)
        self._lakeformation = self._session.client("lakeformation", region_name=region)
        self._glue = self._session.client("glue", region_name=region)
        self._partition = partition
        self._account_id = account.id
        self.logger = log

    def clean(self) -> None:
        """Start the cdh_cleanup of the AWS Lake Formation service."""
        self._deregister_resources()
        self._give_control_back_to_iam()
        self._clean_cross_account_permissions()

    def _deregister_resources(self) -> None:
        self.logger.info(f"Looking for Lake Formation managed resources in {self._region}...")
        resource_generator = repeat_continuation_call(
            self._lakeformation.list_resources,
            "ResourceInfoList",
            FilterConditionList=[
                {
                    "Field": "RESOURCE_ARN",
                    "ComparisonOperator": "BEGINS_WITH",
                    "StringValueList": [f"arn:{self._partition.value}:s3:::{self._prefix}"],
                }
            ],
        )
        for resource in resource_generator:
            resource_arn = resource["ResourceArn"]
            if self._should_clean("Lake Formation registration for", resource_arn, self.logger):
                try:
                    self._lakeformation.deregister_resource(ResourceArn=resource_arn)
                except ClientError as error:
                    self.logger.warning(error)

    def _give_control_back_to_iam(self) -> None:
        self.logger.info(f"Handing back control over databases in {self._region} to IAM...")
        for database_name in self._list_all_databases():
            if has_prefix(database_name, self._prefix) and self._should_clean(
                "Lake Formation control for", database_name, self.logger
            ):
                try:
                    self._lakeformation.grant_permissions(
                        Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                        Resource={"Database": {"Name": database_name}},
                        Permissions=["ALL"],
                    )
                except ClientError as error:
                    self.logger.warning(error)

    def _list_all_databases(self) -> Generator[str, None, None]:
        paginator = self._glue.get_paginator("get_databases")
        for page in paginator.paginate():
            yield from [entry["Name"] for entry in page["DatabaseList"]]

    def _clean_cross_account_permissions(self) -> None:
        self.logger.info(f"Looking for cross account Lake Formation permissions in {self._region}...")
        for permission in self._list_all_permissions():
            principal = str(permission["Principal"]["DataLakePrincipalIdentifier"])

            # not a permission for an account: do nothing
            if not principal.isdigit() or len(principal) != 12:
                continue

            resource = permission["Resource"]
            resource_type, resource_name, resource_database_name = self._get_resource_type_name_database(resource)

            # not a table in a prefixed database, a prefixed database itself or a prefixed data location: do nothing
            if not has_prefix(resource_database_name, self._prefix) and not has_prefix(resource_name, self._prefix):
                continue

            if self._should_clean(f"Lake Formation permission for {resource_type}", resource_name, self.logger):
                self._adapt_resource_on_permission(permission, resource_type)
                self._revoke_permission(permission)

    def _list_all_permissions(self) -> Generator[PrincipalResourcePermissionsTypeDef, None, None]:
        yield from repeat_continuation_call(
            self._lakeformation.list_permissions,
            "PrincipalResourcePermissions",
            MaxResults=1000,
        )

    @staticmethod
    def _get_resource_type_name_database(resource: ResourceTypeDef) -> Tuple[str, str, str]:
        database = ""
        if "Catalog" in resource:
            resource_type = "Catalog"
            name = "catalog"
        elif "Database" in resource:
            resource_type = "Database"
            name = resource["Database"]["Name"]
            database = name
        elif "Table" in resource:
            resource_type = "Table"
            name = f"{resource['Table']['DatabaseName']}.{resource['Table']['Name']}"
            database = resource["Table"]["DatabaseName"]
        elif "TableWithColumns" in resource:
            resource_type = "TableWithColumns"
            name = f"{resource['TableWithColumns']['DatabaseName']}.{resource['TableWithColumns']['Name']}.columns"
            database = resource["TableWithColumns"]["DatabaseName"]
        elif "DataLocation" in resource:
            resource_type = "DataLocation"
            bucket_arn = Arn(resource["DataLocation"]["ResourceArn"])
            name = bucket_arn.identifier
        elif "DataCellsFilter" in resource:
            resource_type = "DataCellsFilter"
            name = f"{resource['DataCellsFilter']['DatabaseName']}.{resource['DataCellsFilter']['TableName']}.{resource['DataCellsFilter']['Name']}"  # noqa: E501
            database = resource["DataCellsFilter"]["DatabaseName"]
        elif "LFTag" in resource:
            resource_type = "LFTag"
            name = resource["LFTag"]["TagKey"]
        elif "LFTagPolicy" in resource:
            resource_type = "LFTagPolicy"
            name = str(resource["LFTagPolicy"]["Expression"])
        else:
            resource_type = "unknown"
            name = "unknown_resource"
        return resource_type, name, database

    @staticmethod
    def _adapt_resource_on_permission(permission: PrincipalResourcePermissionsTypeDef, resource_type: str) -> None:
        if resource_type == "Table" and "TableWildcard" in permission["Resource"]["Table"]:
            del permission["Resource"]["Table"]["Name"]

        if (
            resource_type == "TableWithColumns"
            and "ColumnWildcard" in permission["Resource"]["TableWithColumns"]
            and "Name" in permission["Resource"]["TableWithColumns"]
            and permission["Resource"]["TableWithColumns"]["Name"] == "ALL_TABLES"
        ):
            permission["Resource"]["Table"] = {
                "CatalogId": permission["Resource"]["TableWithColumns"]["CatalogId"],
                "DatabaseName": permission["Resource"]["TableWithColumns"]["DatabaseName"],
                "TableWildcard": {},
            }
            del permission["Resource"]["TableWithColumns"]

    def _revoke_permission(self, permission: PrincipalResourcePermissionsTypeDef) -> None:
        try:
            self._lakeformation.revoke_permissions(
                Principal=permission["Principal"],
                Resource=permission["Resource"],
                Permissions=permission["Permissions"],
                PermissionsWithGrantOption=permission["PermissionsWithGrantOption"],
            )
        except ClientError as error:
            self.logger.warning(error)
