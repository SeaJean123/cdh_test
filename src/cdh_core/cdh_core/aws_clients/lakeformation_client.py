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
from contextlib import contextmanager
from time import sleep
from typing import Generator
from typing import Literal
from typing import TYPE_CHECKING
from typing import Union

from botocore.exceptions import ClientError

from cdh_core.aws_clients.boto_retry_decorator import create_boto_retry_decorator
from cdh_core.aws_clients.utils import FailedToDeleteResourcesStillAssociating
from cdh_core.aws_clients.utils import get_error_code
from cdh_core.entities.arn import Arn
from cdh_core.entities.filter_package import TableFilter
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.entities.resource import S3Resource
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_lakeformation import LakeFormationClient as BotoLakeFormationClient
    from mypy_boto3_glue.type_defs import DataLakePrincipalTypeDef
else:
    BotoLakeFormationClient = object
    DataLakePrincipalTypeDef = object


def _principal_to_data_lake_principal(
    principal: Union[AccountId, Arn, Literal["IAM_ALLOWED_PRINCIPALS"]]
) -> DataLakePrincipalTypeDef:
    return {"DataLakePrincipalIdentifier": str(principal)}


class LakeFormationClient:
    """Abstracts the boto3 Lake Formation client."""

    retry = create_boto_retry_decorator("_sleep")

    def __init__(self, boto_lake_formation_client: BotoLakeFormationClient):
        self._client = boto_lake_formation_client
        self._sleep = sleep

    def register_resource(self, resource_arn: Arn, role_arn: Arn) -> None:
        """Register a resource with Lake Formation."""
        self._client.register_resource(
            ResourceArn=str(resource_arn),
            UseServiceLinkedRole=False,
            RoleArn=str(role_arn),
        )

    def deregister_resource(self, resource_arn: Arn) -> None:
        """Deregister a resource from Lake Formation."""
        self._client.deregister_resource(ResourceArn=str(resource_arn))

    def revoke_iam_allowed_principals_permissions_for_database(self, database: GlueDatabase) -> None:
        """Revoke all Lake Formation permissions from IAM allowed principals."""
        self._client.revoke_permissions(
            Principal=_principal_to_data_lake_principal("IAM_ALLOWED_PRINCIPALS"),
            Resource=database.to_lake_formation_database_resource,
            Permissions=["ALL"],
            CatalogId=database.account_id,
            PermissionsWithGrantOption=[],
        )

    @retry(num_attempts=5, wait_between_attempts=1, retryable_error_codes=["ConcurrentModificationException"])
    def grant_write_access_for_database(self, principal: Union[AccountId, Arn], database: GlueDatabase) -> None:
        """Give all permissions on a database, except for drop database, to a principal including grant option."""
        self._client.grant_permissions(
            Principal=_principal_to_data_lake_principal(principal),
            Resource=database.to_lake_formation_database_resource,
            Permissions=["ALTER", "CREATE_TABLE", "DESCRIBE"],
            PermissionsWithGrantOption=["ALTER", "CREATE_TABLE", "DESCRIBE"],
        )
        self._client.grant_permissions(
            Principal=_principal_to_data_lake_principal(principal),
            Resource=database.to_lake_formation_tables_resource,
            Permissions=["ALL"],
            PermissionsWithGrantOption=["ALL"],
        )

    def revoke_write_access_for_database(self, principal: Union[AccountId, Arn], database: GlueDatabase) -> None:
        """Revoke write permissions on a database, except for drop database, from a principal including grant option."""
        try:
            with self._handle_missing_permission(fail_if_missing=False):
                self._client.revoke_permissions(
                    Principal=_principal_to_data_lake_principal(principal),
                    Resource=database.to_lake_formation_database_resource,
                    Permissions=["ALTER", "CREATE_TABLE", "DESCRIBE"],
                    PermissionsWithGrantOption=["ALTER", "CREATE_TABLE", "DESCRIBE"],
                )
            with self._handle_missing_permission(fail_if_missing=False):
                self._client.revoke_permissions(
                    Principal=_principal_to_data_lake_principal(principal),
                    Resource=database.to_lake_formation_tables_resource,
                    Permissions=["ALL"],
                    PermissionsWithGrantOption=["ALL"],
                )
        except ClientError as client_error:
            if get_error_code(client_error) == "ConcurrentModificationException":
                raise FailedToDeleteResourcesStillAssociating from client_error
            raise

    @retry(num_attempts=5, wait_between_attempts=1, retryable_error_codes=["ConcurrentModificationException"])
    def grant_write_access_for_s3_resource(self, principal: Union[AccountId, Arn], s3_resource: S3Resource) -> None:
        """Give data location access on an s3 resource to a principal, including grant option."""
        self._client.grant_permissions(
            Principal=_principal_to_data_lake_principal(principal),
            Resource=s3_resource.to_lake_formation_data_location,
            Permissions=["DATA_LOCATION_ACCESS"],
            PermissionsWithGrantOption=["DATA_LOCATION_ACCESS"],
        )

    def revoke_write_access_for_s3_resource(self, principal: Union[AccountId, Arn], s3_resource: S3Resource) -> None:
        """Revoke data location access on an s3 resource from a principal, including grant option."""
        with self._handle_missing_permission(fail_if_missing=False):
            self._client.revoke_permissions(
                Principal=_principal_to_data_lake_principal(principal),
                Resource=s3_resource.to_lake_formation_data_location,
                Permissions=["DATA_LOCATION_ACCESS"],
                PermissionsWithGrantOption=["DATA_LOCATION_ACCESS"],
            )

    @retry(num_attempts=5, wait_between_attempts=1, retryable_error_codes=["ConcurrentModificationException"])
    def grant_read_access_for_database(
        self, principal: Union[AccountId, Arn], database: GlueDatabase, grantable: bool
    ) -> None:
        """Give read access on a database to a principal."""
        self._client.grant_permissions(
            Principal=_principal_to_data_lake_principal(principal),
            Resource=database.to_lake_formation_tables_resource,
            Permissions=["DESCRIBE", "SELECT"],
            PermissionsWithGrantOption=["DESCRIBE", "SELECT"] if grantable else [],
        )

    def revoke_read_access_for_database(
        self, principal: Union[AccountId, Arn], database: GlueDatabase, grantable: bool, fail_if_missing: bool = True
    ) -> None:
        """Revoke read access on a database from a principal."""
        try:
            with self._handle_missing_permission(fail_if_missing):
                self._client.revoke_permissions(
                    Principal=_principal_to_data_lake_principal(principal),
                    Resource=database.to_lake_formation_tables_resource,
                    Permissions=["DESCRIBE", "SELECT"],
                    PermissionsWithGrantOption=["DESCRIBE", "SELECT"] if grantable else [],
                )
        except ClientError as client_error:
            if get_error_code(client_error) == "ConcurrentModificationException":
                raise FailedToDeleteResourcesStillAssociating from client_error
            raise

    @contextmanager
    def _handle_missing_permission(self, fail_if_missing: bool = True) -> Generator[None, None, None]:
        try:
            yield
        except self._client.exceptions.InvalidInputException as invalid_input:
            if fail_if_missing or not invalid_input.response["Message"].startswith("No permissions revoked."):
                raise

    def create_table_filter(self, table_filter: TableFilter) -> None:
        """Create a table filter."""
        try:
            self._client.create_data_cells_filter(TableData=table_filter.to_lake_formation_table_filter_resource)
        except ClientError as client_error:
            if get_error_code(client_error) == "AlreadyExistsException":
                raise TableFilterAlreadyExists(table_filter.filter_id) from client_error

    def delete_table_filter(self, table_filter: TableFilter) -> None:
        """Delete a table filter."""
        try:
            self._client.delete_data_cells_filter(
                Name=table_filter.filter_id,
                TableCatalogId=table_filter.resource_account_id,
                DatabaseName=table_filter.database_name,
                TableName=table_filter.table_name,
            )
        except ClientError as client_error:
            if get_error_code(client_error) == "EntityNotFoundException":
                raise TableFilterNotFound(table_filter.filter_id) from client_error


class TableFilterAlreadyExists(Exception):
    """Signals the requested table filter already exists."""

    def __init__(self, table_filter_name: str):
        super().__init__(f"Table filter {table_filter_name} already exists")


class TableFilterNotFound(Exception):
    """Signals the requested table filter does not exist."""

    def __init__(self, table_filter_name: str):
        super().__init__(f"Table filter {table_filter_name} already exists")
