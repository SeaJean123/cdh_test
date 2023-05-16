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
from contextlib import suppress
from functools import lru_cache
from logging import getLogger
from typing import Any
from typing import Optional

from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericUpdateAccountBody
from cdh_core_api.services.metadata_role_assumer import MetadataRoleAssumer

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.glue_client import GlueClient
from cdh_core.aws_clients.glue_client import GlueDatabaseNotFound
from cdh_core.aws_clients.glue_client import GlueEncryptionException
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)


class ResourceLink:
    """Handles resource links."""

    def __init__(
        self,
        aws: AwsClientFactory,
        accounts_table: GenericAccountsTable[GenericAccount],
        metadata_role_assumer: MetadataRoleAssumer[GenericAccount, GenericUpdateAccountBody],
    ):
        self._aws = aws
        self._accounts_table = accounts_table
        self._metadata_role_assumer = metadata_role_assumer

    def glue_db_exists(self, database: GlueDatabase) -> bool:
        """Return True if a glue database with the given name exists."""
        glue_client = self._get_glue_client(database.account_id, database.region)
        return glue_client.database_exists(database.name)

    def has_glue_db_sync_type_glue_sync_if_db_exists(self, database: GlueDatabase) -> Optional[bool]:
        """Return whether a glue database has sync type glue_sync if it exists."""
        glue_client = self._get_glue_client(database.account_id, database.region)
        with suppress(GlueDatabaseNotFound):
            return not glue_client.is_database_a_resource_link(database.name)
        return None

    def delete_resource_link(self, target_account_id: AccountId, source_database: GlueDatabase) -> None:
        """Delete a resource-link to the source database in the given target account."""
        glue_client = self._get_glue_client(target_account_id, source_database.region)
        glue_client.delete_protected_database(source_database.name, target_account_id)

    def create_resource_link(self, target_account_id: AccountId, source_database: GlueDatabase) -> None:
        """Create a resource-link to the source database in the given target account."""
        glue_client = self._get_glue_client(target_account_id, source_database.region)
        try:
            glue_client.create_resource_link(
                database_name=source_database.name,
                source_account_id=source_database.account_id,
            )
            glue_client.add_deletion_protection(source_database.name, target_account_id)
        except GlueEncryptionException as err:
            LOG.warning(
                f"Creating resource link for database {source_database.name} in account {target_account_id} "
                f"and region {source_database.region.value} failed: {str(err)}"
            )
            raise GlueEncryptionFailed(target_account_id, source_database.region, source_database.name) from err

    def _get_glue_client(self, target_account_id: AccountId, region: Region) -> GlueClient:
        return GlueClient(boto3_glue_client=self._get_boto_client(target_account_id=target_account_id, region=region))

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def _get_boto_client(
        self,
        target_account_id: AccountId,
        region: Region,
    ) -> Any:
        target_account = self._accounts_table.get(target_account_id)
        session = self._metadata_role_assumer.assume_account(target_account).boto3_session
        return self._aws.create_client(service="glue", region=region, session=session)


class GlueEncryptionFailed(Exception):
    """Signals the encryption of the glue database failed."""

    def __init__(self, account_id: AccountId, region: Region, database_name: str):
        self.account_id = account_id
        self.region = region
        self.database_name = database_name
        super().__init__(
            f"Encrypting glue database {database_name} in account {account_id} and region {region.value} failed"
        )

    def get_user_facing_message(self, failed_operation_description: str) -> str:
        """Get the error message that is displayed to the user."""
        return (
            f"{failed_operation_description}, because the Glue encryption operation failed. This is most probably "
            f"because of the Glue catalog in account {self.account_id} and region {self.region.value} is encrypted "
            f"with a key which was deleted or is not available anymore. The Glue catalog is not usable by anyone right"
            f" now. "
            f"Please restore the KMS key with which the Glue catalog is encrypted - if not possible anymore since it "
            f"is already deleted, please open up an AWS ticket in the AWS Console. Once this issue has been resolved, "
            f"please repeat this request."
        )
