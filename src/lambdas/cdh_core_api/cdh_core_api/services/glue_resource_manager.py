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
from typing import Generic
from typing import Type

from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericNewGlueSyncBody
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.lake_formation_service import LakeFormationService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.metadata_role_assumer import CannotAssumeMetadataRole
from cdh_core_api.services.resource_link import GlueEncryptionFailed
from cdh_core_api.services.resource_link import ResourceLink

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.utils import FailedToDeleteResourcesStillAssociating
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.entities.lock import Lock
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import UnprocessableEntityError
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.constants import CREATED_BY_CORE_API_TAG
from cdh_core.primitives.constants import GOVERNED_BY_LAKE_FORMATION_TAG


class GlueResourceManager(Generic[GenericGlueSyncResource, GenericNewGlueSyncBody]):
    """Handles glue resources."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        aws: AwsClientFactory,
        config: Config,
        lake_formation_service: LakeFormationService,
        lock_service: LockService,
        resource_link: ResourceLink,
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        glue_sync_resource_type: Type[GenericGlueSyncResource],
        data_explorer_sync: DataExplorerSync,
    ):
        self._aws = aws
        self._config = config
        self._lake_formation_service = lake_formation_service
        self._lock_service = lock_service
        self._resource_link = resource_link
        self._resources_table = resources_table
        self._glue_sync_resource_type = glue_sync_resource_type
        self._data_explorer_sync = data_explorer_sync

    def create_glue_sync(  # pylint: disable=too-many-arguments
        self,
        dataset: Dataset,
        body: GenericNewGlueSyncBody,
        resource_account: ResourceAccount,
        owner_account_id: AccountId,
        user: str,
    ) -> GenericGlueSyncResource:
        """Create and return a glue resource for the given parameters, if it does not exist and is not locked."""
        sync_type = body.syncType or SyncType.resource_link

        database_name = self._create_glue_database_name(dataset_id=dataset.id, stage=body.stage)

        self._check_existing_glue_syncs(dataset_id=dataset.id, stage=body.stage, region=body.region)

        lock = self._lock_service.acquire_lock(
            item_id=dataset.id,
            scope=LockingScope.glue_sync_resource,
            region=body.region,
            stage=body.stage,
            data={"owner": owner_account_id},
        )
        dt_now = datetime.now()

        try:
            self._check_for_conflicting_database(
                database_name=database_name,
                region=body.region,
                owner_account_id=owner_account_id,
            )
        except ConflictError:
            self._lock_service.release_lock(lock)
            raise
        except CannotAssumeMetadataRole as err:
            self._lock_service.release_lock(lock)
            raise UnprocessableEntityError(
                f"Could not create glue database in the provider account {owner_account_id}, because assuming the "
                f"metadata role failed. This is most likely due to manual changes to the role "
                f"{dataset.hub.partition.value}:iam::{owner_account_id}:role/cdh/cdh-glue-push-user-role"
            ) from err

        glue_db = GlueDatabase(name=database_name, account_id=resource_account.id, region=body.region)
        glue_db_arn = glue_db.arn
        glue_resource = self._glue_sync_resource_type(
            dataset_id=dataset.id,
            hub=dataset.hub,
            database_name=database_name,
            resource_account_id=resource_account.id,
            arn=glue_db_arn,
            creator_user_id=user,
            creation_date=dt_now,
            region=body.region,
            stage=body.stage,
            update_date=dt_now,
            owner_account_id=owner_account_id,
            sync_type=sync_type,
        )

        self._setup_glue_databases_and_permissions(glue_resource=glue_resource, lock=lock)

        self._resources_table.create(glue_resource)
        if sync_type == SyncType.lake_formation:
            self._data_explorer_sync.update_lake_formation_access_for_data_explorer(dataset=dataset, glue_db=glue_db)
        self._lock_service.release_lock(lock)
        return glue_resource

    def _create_glue_database_name(self, dataset_id: str, stage: Stage) -> DatabaseName:
        return DatabaseName(f"{self._config.prefix}{dataset_id}_{stage.value}")

    def _check_existing_glue_syncs(self, dataset_id: DatasetId, stage: Stage, region: Region) -> None:
        if self._resources_table.exists(ResourceType.glue_sync, dataset_id, stage, region):
            raise GlueSyncAlreadyExists(dataset_id, stage, region)

    def _check_for_conflicting_database(
        self,
        database_name: DatabaseName,
        region: Region,
        owner_account_id: AccountId,
    ) -> None:
        if self._resource_link.glue_db_exists(
            GlueDatabase(name=database_name, account_id=owner_account_id, region=region)
        ):
            raise ConflictError(
                f"Cannot create resource-link for database {database_name}, as it would overwrite an existing glue "
                f"database with the same name."
            )

    def _setup_glue_databases_and_permissions(self, glue_resource: GenericGlueSyncResource, lock: Lock) -> None:
        self._create_database_and_resource_link(glue_resource=glue_resource, lock=lock)
        if glue_resource.owner_account_id != glue_resource.resource_account_id:
            if glue_resource.sync_type is SyncType.resource_link:
                self._create_ram_permissions(glue_resource=glue_resource)
            elif glue_resource.sync_type is SyncType.lake_formation:
                self._setup_lake_formation_permissions(glue_resource=glue_resource)

    def _create_database_and_resource_link(self, glue_resource: GenericGlueSyncResource, lock: Lock) -> None:
        glue_client = self._aws.glue_client(
            account_id=glue_resource.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=glue_resource.region,
        )
        glue_client.create_database(
            database_name=glue_resource.database_name,
            remove_default_permissions=glue_resource.sync_type is SyncType.lake_formation,
        )

        if glue_resource.owner_account_id != glue_resource.resource_account_id:
            try:
                self._resource_link.create_resource_link(
                    target_account_id=glue_resource.owner_account_id,
                    source_database=glue_resource.glue_database,
                )
            except GlueEncryptionFailed as error:
                glue_client.delete_database_if_present(database_name=glue_resource.database_name)
                self._lock_service.release_lock(lock)
                raise UnprocessableEntityError(
                    f"Encrypting glue database {glue_resource.database_name} in account "
                    f"{glue_resource.owner_account_id} failed. Please make sure the Master Key defined in your Glue "
                    f"Catalog settings is configured correctly and try again."
                ) from error

    def _create_ram_permissions(self, glue_resource: GenericGlueSyncResource) -> None:
        ram_client = self._aws.ram_client(
            account_id=glue_resource.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=glue_resource.region,
        )
        ram_client.create_glue_resource_share_with_write_permissions(
            database=glue_resource.glue_database,
            target_account_id=glue_resource.owner_account_id,
            tags=CREATED_BY_CORE_API_TAG,
        )

    def _setup_lake_formation_permissions(self, glue_resource: GenericGlueSyncResource) -> None:
        s3_resource = self._resources_table.get_s3(
            dataset_id=glue_resource.dataset_id, stage=glue_resource.stage, region=glue_resource.region
        )
        self._tag_lf_governed_bucket(s3_resource)
        self._lake_formation_service.setup_lake_formation_governance(
            glue_resource=glue_resource, s3_resource=s3_resource
        )
        self._lake_formation_service.setup_provider_access(glue_resource=glue_resource, s3_resource=s3_resource)

    def _tag_lf_governed_bucket(self, s3_resource: GenericS3Resource) -> None:
        s3_client = self._aws.s3_client(
            account_id=s3_resource.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=s3_resource.region,
        )
        s3_client.add_bucket_tags(name=s3_resource.arn.identifier, tags=GOVERNED_BY_LAKE_FORMATION_TAG)

    def _untag_lf_governed_bucket(self, s3_resource: GenericS3Resource) -> None:
        s3_client = self._aws.s3_client(
            account_id=s3_resource.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=s3_resource.region,
        )
        s3_client.remove_bucket_tags(name=s3_resource.arn.identifier, tags=GOVERNED_BY_LAKE_FORMATION_TAG)

    def delete_glue_sync(self, glue_resource: GenericGlueSyncResource) -> None:
        """Delete a glue resource and the associated resource link, ram or lake formation permissions and database."""
        lock = self._lock_service.acquire_lock(
            item_id=glue_resource.dataset_id,
            scope=LockingScope.glue_sync_resource,
            region=glue_resource.region,
            stage=glue_resource.stage,
            data=glue_resource.to_payload().to_plain_dict(),
        )

        self._teardown_glue_databases_and_permissions(glue_resource=glue_resource, lock=lock)

        self._resources_table.delete(
            resource_type=ResourceType.glue_sync,
            dataset_id=glue_resource.dataset_id,
            stage=glue_resource.stage,
            region=glue_resource.region,
        )

        self._lock_service.release_lock(lock)

    def _teardown_glue_databases_and_permissions(self, glue_resource: GenericGlueSyncResource, lock: Lock) -> None:
        try:
            if glue_resource.sync_type is SyncType.resource_link:
                self._delete_ram_permissions(glue_resource=glue_resource)
            elif glue_resource.sync_type is SyncType.lake_formation:
                self._teardown_lake_formation_permissions(glue_resource=glue_resource)
        except FailedToDeleteResourcesStillAssociating as error:
            self._lock_service.release_lock(lock)
            raise UnprocessableEntityError(
                f"Could not remove the write permissions for resource link {glue_resource.database_name}, because they"
                " have not finished associating. Please try again later."
            ) from error
        self._remove_database_and_resource_link(glue_resource=glue_resource)

    def _remove_database_and_resource_link(self, glue_resource: GenericGlueSyncResource) -> None:
        self._resource_link.delete_resource_link(
            target_account_id=glue_resource.owner_account_id,
            source_database=glue_resource.glue_database,
        )
        self._remove_resource_account_database(glue_resource)

    def _remove_resource_account_database(self, glue_resource: GenericGlueSyncResource) -> None:
        glue_client = self._aws.glue_client(
            account_id=glue_resource.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=glue_resource.region,
        )
        glue_client.delete_database_if_present(glue_resource.database_name)

    def _delete_ram_permissions(self, glue_resource: GenericGlueSyncResource) -> None:
        ram_client = self._aws.ram_client(
            account_id=glue_resource.resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=glue_resource.region,
        )
        ram_client.revoke_glue_share_if_necessary(glue_resource.glue_database)

    def _teardown_lake_formation_permissions(self, glue_resource: GenericGlueSyncResource) -> None:
        s3_resource = self._resources_table.get_s3(
            dataset_id=glue_resource.dataset_id, stage=glue_resource.stage, region=glue_resource.region
        )
        self._lake_formation_service.teardown_provider_access(glue_resource=glue_resource, s3_resource=s3_resource)
        self._untag_lf_governed_bucket(s3_resource)
        self._lake_formation_service.teardown_lake_formation_governance(s3_resource)


class GlueSyncAlreadyExists(Exception):
    """Signals that a glue resource already exists for the database, stage and region combination."""

    def __init__(self, dataset_id: str, stage: Stage, region: Region):
        super().__init__(
            f"Dataset {dataset_id} already contains a Glue Sync resource "
            f"for stage {stage.value} and region {region.value}"
        )
