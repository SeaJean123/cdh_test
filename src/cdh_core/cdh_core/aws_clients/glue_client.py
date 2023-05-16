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
import time
from dataclasses import dataclass
from logging import getLogger
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from botocore.exceptions import ConnectTimeoutError
from botocore.exceptions import ProxyConnectionError

from cdh_core.aws_clients.boto_retry_decorator import create_boto_retry_decorator
from cdh_core.aws_clients.glue_resource_policy import GlueResourcePolicy
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.policy import PolicySizeExceeded
from cdh_core.aws_clients.utils import get_error_code
from cdh_core.aws_clients.utils import get_error_message
from cdh_core.aws_clients.utils import repeat_continuation_call
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_glue.type_defs import DatabaseInputTypeDef
    from mypy_boto3_glue import GlueClient as BotoGlueClient
else:
    BotoGlueClient = object
    DatabaseInputTypeDef = Dict[str, Any]


LOG = getLogger(__name__)
MAX_VERSIONS_PER_BATCH_DELETE = 100  # https://docs.aws.amazon.com/glue/latest/webapi/API_BatchDeleteTableVersion.html


@dataclass(frozen=True)
class GlueTable:
    """Represents a simple glue table."""

    name: str
    database_name: str
    location: str


class GlueClient:
    """Abstracts the boto3 glue client."""

    retry = create_boto_retry_decorator("_sleep")

    def __init__(
        self,
        boto3_glue_client: BotoGlueClient,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._client = boto3_glue_client
        self._region = Region(self._client.meta.region_name)
        self._sleep = sleep

    def get_all_database_names(self) -> List[str]:
        """Get the names of all databases."""
        return [database["Name"] for database in repeat_continuation_call(self._client.get_databases, "DatabaseList")]

    def get_table_version_ids(self, database: str, table: str) -> List[str]:
        """Get all version IDs for a specified database and table name."""
        try:
            return [
                version["VersionId"]
                for version in repeat_continuation_call(
                    self._client.get_table_versions, "TableVersions", DatabaseName=database, TableName=table
                )
            ]
        except ClientError as error:
            if get_error_code(error) == "EntityNotFoundException":
                raise GlueTableNotFound(table) from error
            raise error

    def delete_table_versions(self, database: str, table: str, version_ids: List[str]) -> None:
        """Delete a batch of table versions, identified via their version IDs."""
        for chunk_start in range(0, len(version_ids), MAX_VERSIONS_PER_BATCH_DELETE):
            self._client.batch_delete_table_version(
                DatabaseName=database,
                TableName=table,
                VersionIds=version_ids[chunk_start : chunk_start + MAX_VERSIONS_PER_BATCH_DELETE],
            )

    def get_tables(self, database: str, catalog_id: Optional[AccountId] = None) -> List[GlueTable]:
        """Get all tables from a database."""
        try:
            result = (
                repeat_continuation_call(self._client.get_tables, "TableList", DatabaseName=database)
                if catalog_id is None
                else repeat_continuation_call(
                    self._client.get_tables, "TableList", DatabaseName=database, CatalogId=catalog_id
                )
            )

            return [
                GlueTable(
                    name=table_data["Name"],
                    database_name=table_data["DatabaseName"],
                    location=table_data.get("StorageDescriptor", {}).get("Location", ""),
                )
                for table_data in result
            ]
        except ClientError as error:
            if get_error_code(error) == "EntityNotFoundException":
                raise GlueDatabaseNotFound(database) from error
            raise error

    def database_exists(self, database_name: str) -> bool:
        """Return True if glue database exists."""
        try:
            self._client.get_database(Name=database_name)
            return True
        except self._client.exceptions.EntityNotFoundException:
            return False

    @retry(num_attempts=5, wait_between_attempts=1, retryable_errors=[ProxyConnectionError, ConnectTimeoutError])
    def is_database_a_resource_link(self, database_name: str) -> bool:
        """Return True if a glue database is a resource-link."""
        try:
            response = self._client.get_database(Name=database_name)
        except self._client.exceptions.EntityNotFoundException as error:
            raise GlueDatabaseNotFound(database_name) from error
        return "TargetDatabase" in response["Database"]

    @retry(num_attempts=5, wait_between_attempts=1, retryable_error_codes=["ConcurrentModificationException"])
    def delete_database_if_present(self, database_name: str) -> None:
        """Delete a glue database if it exists."""
        try:
            self._client.delete_database(Name=database_name)
        except self._client.exceptions.EntityNotFoundException:
            LOG.info(f"Database {database_name} not present, nothing to delete.")

    def delete_protected_database(self, database_name: DatabaseName, account_id: AccountId) -> None:
        """Remove a databases' protection and delete the database."""
        self.remove_deletion_protection(database_name, account_id)
        self._delete_database_with_access_denied_retry(database_name)

    @retry(num_attempts=10, wait_between_attempts=0.5, retryable_error_codes=["AccessDeniedException"])
    def _delete_database_with_access_denied_retry(self, database_name: str) -> None:
        self.delete_database_if_present(database_name)

    def create_resource_link(self, database_name: str, source_account_id: AccountId) -> None:
        """Create a glue database via a resource link to the database in the given account."""
        try:
            self._client.create_database(
                DatabaseInput={
                    "Name": database_name,
                    "TargetDatabase": {
                        "CatalogId": source_account_id,
                        "DatabaseName": database_name,
                    },
                }
            )
        except ClientError as error:
            if get_error_code(error) == "AlreadyExistsException":
                raise GlueDatabaseAlreadyExists(database_name) from error
            if get_error_code(error) == "GlueEncryptionException":
                raise GlueEncryptionException(database_name) from error
            raise

    def create_database(self, database_name: str, remove_default_permissions: bool = False) -> None:
        """Create a glue database."""
        database_input: DatabaseInputTypeDef = {"Name": database_name}
        if remove_default_permissions:
            database_input["CreateTableDefaultPermissions"] = []
        try:
            self._client.create_database(DatabaseInput=database_input)
        except ClientError as error:
            if get_error_code(error) == "AlreadyExistsException":
                raise GlueDatabaseAlreadyExists(database_name) from error
            raise

    def _get_resource_policy_if_exists(self) -> Optional[GlueResourcePolicy]:
        try:
            response = self._client.get_resource_policy()
            return GlueResourcePolicy.from_boto(response)
        except self._client.exceptions.EntityNotFoundException:
            return None

    def _get_resource_link_protection_principal(self, account_id: AccountId) -> str:
        return f"arn:{self._region.partition.value}:iam::{account_id}:root"

    @retry(num_attempts=3, wait_between_attempts=1, retryable_error_codes=["ConditionCheckFailureException"])
    def add_deletion_protection(self, database_name: DatabaseName, account_id: AccountId) -> None:
        """Protect database resource links from accidental deletion."""
        database_arn = GlueDatabase(name=database_name, account_id=account_id, region=self._region).arn
        if (policy := self._get_resource_policy_if_exists()) is not None:
            assert policy.policy_hash
            try:
                new_policy = policy.add_resource_protection(
                    principal=self._get_resource_link_protection_principal(account_id),
                    resources_to_add={database_arn},
                )
                if new_policy is policy:
                    return
                self._client.put_resource_policy(
                    PolicyInJson=new_policy.to_boto(),
                    PolicyHashCondition=policy.policy_hash,
                    EnableHybrid="TRUE",
                    PolicyExistsCondition="MUST_EXIST",
                )
            except PolicySizeExceeded:
                LOG.warning(
                    f"The updated policy document would exceed the size limit. "
                    f"Not adding protection for {database_arn}"
                )
            except self._client.exceptions.InvalidInputException as err:
                if "Resource policy size is limited" not in get_error_message(err):  # type: ignore
                    raise
                LOG.warning(
                    f"The updated policy document would exceed the size limit. "
                    f"Not adding protection for {database_arn}"
                )
            except self._client.exceptions.AccessDeniedException:
                LOG.warning(f"No permission to update glue resource policy to protect {database_arn}")
        else:
            statement = GlueResourcePolicy.create_resource_link_protect_policy_statement(
                principal=self._get_resource_link_protection_principal(account_id),
                resources={database_arn},
            )
            policy = GlueResourcePolicy(document=PolicyDocument.create_glue_resource_policy([statement]))
            try:
                self._client.put_resource_policy(
                    PolicyInJson=policy.to_boto(),
                    EnableHybrid="TRUE",
                    PolicyExistsCondition="NOT_EXIST",
                )
            except self._client.exceptions.AccessDeniedException:
                LOG.warning(f"No permission to update glue resource policy to protect {database_arn}")

    @retry(num_attempts=3, wait_between_attempts=1, retryable_error_codes=["ConditionCheckFailureException"])
    def remove_deletion_protection(self, database_name: DatabaseName, account_id: AccountId) -> None:
        """Unprotect database resource links from accidental deletion."""
        database_arn = GlueDatabase(name=database_name, account_id=account_id, region=self._region).arn
        if (policy := self._get_resource_policy_if_exists()) is not None:
            assert policy.policy_hash
            new_policy = policy.remove_resource_protection(
                principal=self._get_resource_link_protection_principal(account_id), resources_to_remove={database_arn}
            )
            if new_policy is policy:
                return
            try:
                if new_policy:
                    self._client.put_resource_policy(
                        PolicyInJson=new_policy.to_boto(),
                        PolicyHashCondition=policy.policy_hash,
                        EnableHybrid="TRUE",
                        PolicyExistsCondition="MUST_EXIST",
                    )
                else:
                    self._client.delete_resource_policy(PolicyHashCondition=policy.policy_hash)
            except self._client.exceptions.AccessDeniedException:
                LOG.warning(f"No permission to update glue resource policy to unprotect {database_arn}")
        else:
            LOG.info(f"The database {database_name} is already unprotected")


class GlueDatabaseNotFound(Exception):
    """Signals the requested database does not exist."""

    def __init__(self, database: str):
        super().__init__(f"Glue database {database} was not found")


class GlueTableNotFound(Exception):
    """Signals the requested table does not exist."""

    def __init__(self, table: str):
        super().__init__(f"Glue table {table} was not found")


class GlueDatabaseAlreadyExists(Exception):
    """Signals the requested database already exists."""

    def __init__(self, database: str):
        super().__init__(f"Glue database {database} already exists")


class GlueEncryptionException(Exception):
    """Signals the glue encryption operation failed."""

    def __init__(self, database: str):
        super().__init__(f"Encrypting glue database {database} failed")


class DatabaseAlreadyProtected(Exception):
    """Signals the database is already protected."""

    def __init__(self, database_name: str):
        super().__init__(f"The database {database_name} is already protected")


class DatabaseAlreadyUnprotected(Exception):
    """Signals the database is already unprotected."""

    def __init__(self, database_name: str):
        super().__init__(f"The database {database_name} is already unprotected")
