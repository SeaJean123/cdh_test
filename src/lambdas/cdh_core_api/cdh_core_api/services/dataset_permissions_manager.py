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
from logging import getLogger
from typing import Generic

from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.dataset_permissions_validator import ValidatedDatasetAccessPermission
from cdh_core_api.services.lake_formation_service import ConflictingReadAccessModificationInProgress
from cdh_core_api.services.lake_formation_service import LakeFormationService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.metadata_role_assumer import CannotAssumeMetadataRole
from cdh_core_api.services.metadata_role_assumer import UnsupportedAssumeMetadataRole
from cdh_core_api.services.resource_link import GlueEncryptionFailed
from cdh_core_api.services.resource_link import ResourceLink
from cdh_core_api.services.s3_resource_manager import S3ResourceManager
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher

from cdh_core.aws_clients.glue_client import GlueDatabaseAlreadyExists
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetAccountPermissionAction
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import UnprocessableEntityError

LOG = getLogger(__name__)


class DatasetPermissionsManager(Generic[GenericAccount, GenericS3Resource, GenericGlueSyncResource]):
    """
    Adds and removes resource permissions for accounts.

    Can use an override of the metadata handling method to update dataset metadata after
    a successful update.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        config: Config,
        datasets_table: DatasetsTable,
        lock_service: LockService,
        s3_resource_manager: S3ResourceManager[GenericS3Resource],
        lake_formation_service: LakeFormationService,
        sns_publisher: SnsPublisher,
        accounts_table: GenericAccountsTable[GenericAccount],
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        resource_link: ResourceLink,
    ):
        self._config = config
        self._sns_publisher = sns_publisher
        self._datasets_table = datasets_table
        self._lock_service = lock_service
        self._s3_resource_manager = s3_resource_manager
        self._lake_formation_service = lake_formation_service
        self._accounts_table = accounts_table
        self._resources_table = resources_table
        self._resource_link = resource_link

    def add_or_remove_permission_handle_errors(
        self,
        validated_permission: ValidatedDatasetAccessPermission[GenericAccount, GenericS3Resource],
        action: DatasetAccountPermissionAction,
    ) -> None:
        """Call the add/remove permission and raise HTTP 4xx errors for known errors."""
        try:
            self.add_or_remove_permission(validated_permission, action)
        except ConflictingGlueDatabases as err:
            raise ConflictError(
                f"Glue database with the name {err.database_name} found in account "
                f"{validated_permission.account.id} and region {validated_permission.permission.region.value}. "
                f" Please try again once the database has been deleted."
            ) from err
        except GlueEncryptionFailed as err:
            raise UnprocessableEntityError(
                err.get_user_facing_message(f"Could not create a resource link for database {err.database_name}")
            ) from err
        except ConflictingReadAccessModificationInProgress as err:
            raise UnprocessableEntityError(
                f"Could not remove the read permissions for database {err.database_name}, because they have not "
                f"finished associating. Please try again later."
            ) from err

    def add_or_remove_permission(
        self,
        validated_permission: ValidatedDatasetAccessPermission[GenericAccount, GenericS3Resource],
        action: DatasetAccountPermissionAction,
        enforce_metadata_sync: bool = True,
    ) -> None:
        """
        Add/remove a resource permission to/from an account for a resource in a given dataset.

        Publishes the change in the dataset as SNS notification (only if any change
        was made).
        """
        lock = self._lock_service.acquire_lock(
            item_id=validated_permission.dataset.id,
            scope=LockingScope.s3_resource,
            region=validated_permission.s3_resource.region,
            stage=validated_permission.s3_resource.stage,
            data={"datasetId": validated_permission.dataset.id},
        )
        try:
            updated_dataset = self._update_read_access(
                action, validated_permission.dataset, validated_permission.permission, validated_permission.s3_resource
            )

            self._handle_metadata_update(
                validated_permission=validated_permission,
                enforce_metadata_sync=enforce_metadata_sync,
                action=action,
                updated_dataset=updated_dataset,
            )
        finally:
            # All updates to the bucket policy are done in a transactional manner, so an error should not leave an
            # inconsistent state. We only lock to avoid concurrent modification attempts, therefore we can always
            # safely release the lock here.
            self._lock_service.release_lock(lock)

    def _handle_metadata_update(
        self,
        validated_permission: ValidatedDatasetAccessPermission[GenericAccount, GenericS3Resource],
        action: DatasetAccountPermissionAction,
        enforce_metadata_sync: bool,
        updated_dataset: Dataset,
    ) -> None:
        dataset = validated_permission.dataset
        s3_resource = validated_permission.s3_resource
        permission = validated_permission.permission
        account = validated_permission.account

        try:
            self.update_metadata_sync(
                permission=permission,
                dataset=updated_dataset,
                account=account,
                action=action,
            )
        except ConflictingGlueDatabases:
            if action is DatasetAccountPermissionAction.add:
                updated_dataset = self._update_read_access(action.inverse, updated_dataset, permission, s3_resource)
            raise
        except (UnsupportedAssumeMetadataRole, CannotAssumeMetadataRole):
            if enforce_metadata_sync:
                updated_dataset = self._update_read_access(action.inverse, updated_dataset, permission, s3_resource)
                raise
            LOG.warning(
                f"Glue DB for dataset {dataset.id} and region {s3_resource.region} not updated in target account "
                f"{account.friendly_name_and_id} because the glue push role could not be assumed."
            )
        except (GlueEncryptionFailed, ConflictingReadAccessModificationInProgress):
            updated_dataset = self._update_read_access(action.inverse, updated_dataset, permission, s3_resource)
            raise
        finally:
            if updated_dataset != dataset:
                self._sns_publisher.publish(
                    entity_type=EntityType.DATASET,
                    operation=Operation.UPDATE,
                    payload=updated_dataset,
                    message_consistency=MessageConsistency.CONFIRMED,
                )

    def _update_read_access(
        self,
        action: DatasetAccountPermissionAction,
        dataset: Dataset,
        permission: DatasetAccountPermission,
        s3_resource: GenericS3Resource,
    ) -> Dataset:
        with self._datasets_table.update_permissions_transaction(
            dataset_id=dataset.id,
            permission=permission,
            action=action,
        ) as updated_dataset:
            self._s3_resource_manager.update_bucket_read_access(
                s3_resource=s3_resource,
                dataset=updated_dataset,
                resource_account=self._config.account_store.query_resource_account(
                    account_ids=s3_resource.resource_account_id, environments=self._config.environment
                ),
            )
        return updated_dataset

    def _update_resource_link(
        self,
        glue_database: GlueDatabase,
        account: GenericAccount,
        action: DatasetAccountPermissionAction,
    ) -> None:
        if action == DatasetAccountPermissionAction.add:
            try:
                self._resource_link.create_resource_link(account.id, glue_database)
            except GlueDatabaseAlreadyExists as error:
                raise ConflictingGlueDatabases(glue_database.name) from error
        else:
            self._resource_link.delete_resource_link(account.id, glue_database)

    def _update_lake_formation_permissions(
        self,
        glue_database: GlueDatabase,
        account: GenericAccount,
        action: DatasetAccountPermissionAction,
    ) -> None:
        if action == DatasetAccountPermissionAction.add:
            self._lake_formation_service.grant_read_access(account.id, glue_database)
        else:
            self._lake_formation_service.revoke_read_access(account.id, glue_database)

    def update_metadata_sync(
        self,
        permission: DatasetAccountPermission,
        dataset: Dataset,
        account: GenericAccount,
        action: DatasetAccountPermissionAction,
    ) -> None:
        """
        Create/delete the resource link and grant/revoke the lakeformation permissions, if applicable.

        If a glue resource exists for this dataset, will create a resource link to the glue database in the
        target account when granting access. For a revoked permission, will delete the
        resource link if it exists.

        If the sync type is of type lake formation, in addition to the created/deleted resource links, permissions
        to the target database for the account will be granted/revoked.
        """
        try:
            glue_sync = self._resources_table.get_glue_sync(
                dataset_id=dataset.id, stage=permission.stage, region=permission.region
            )
            glue_database = glue_sync.glue_database
        except ResourceNotFound:
            return

        if glue_sync.sync_type is SyncType.lake_formation:
            self._update_lake_formation_permissions(glue_database=glue_database, account=account, action=action)

        self._update_resource_link(glue_database=glue_database, account=account, action=action)

    def create_missing_resource_links(
        self,
        dataset: Dataset,
        stage: Stage,
        region: Region,
    ) -> None:
        """
        Create the missing resource links for existing permissions to a newly created glue resource.

        Do nothing if the target account is no longer registered with the CDH or a conflicting database exists in the
        target account.
        """
        for permission in dataset.filter_permissions(stage=stage, region=region, sync_type=SyncType.resource_link):
            try:
                account = self._accounts_table.get(permission.account_id)
            except AccountNotFound:
                continue

            with suppress(ConflictingGlueDatabases):
                self.update_metadata_sync(
                    permission=permission, dataset=dataset, account=account, action=DatasetAccountPermissionAction.add
                )

    def delete_metadata_syncs_for_glue_sync(self, glue_sync: GenericGlueSyncResource, dataset: Dataset) -> None:
        """
        Delete all resource links for existing permissions to a given glue resource.

        Do nothing if the target account is no longer registered with the CDH.
        """
        for permission in dataset.filter_permissions(stage=glue_sync.stage, region=glue_sync.region):
            try:
                account = self._accounts_table.get(permission.account_id)
            except AccountNotFound:
                continue

            if permission.sync_type is not SyncType.resource_link:
                LOG.error(
                    f"Permission sync-type {permission.sync_type.value} for dataset {dataset.id} in account "
                    f"{account.id}, stage {glue_sync.stage.value} and region {glue_sync.region.value} is unsupported. "
                    f"Cannot remove the database."
                )
                continue

            LOG.info(
                f"Deleting {permission.sync_type.value} for dataset {dataset.id} in account {account.id}, "
                f"stage {glue_sync.stage.value} and region {glue_sync.region.value}."
            )

            self._resource_link.delete_resource_link(
                target_account_id=account.id, source_database=glue_sync.glue_database
            )

    def remove_permissions_across_datasets(self, account: GenericAccount) -> None:
        """Remove all dataset access permissions for a given account."""
        for dataset in self._datasets_table.list():
            for permission in dataset.permissions:
                if account.id == permission.account_id:
                    s3_resource = self._resources_table.get_s3(
                        dataset_id=dataset.id, stage=permission.stage, region=permission.region
                    )
                    with suppress(ConflictingGlueDatabases):
                        self.add_or_remove_permission(
                            validated_permission=ValidatedDatasetAccessPermission(
                                dataset=dataset,
                                account=account,
                                s3_resource=s3_resource,
                                permission=permission,
                            ),
                            action=DatasetAccountPermissionAction.remove,
                            enforce_metadata_sync=False,
                        )


class ConflictingGlueDatabases(Exception):
    """Exception class for creating already existing glue databases."""

    def __init__(self, database_name: str):
        self.database_name = database_name
        super().__init__(f"A glue database with the name {database_name} already exists.")
