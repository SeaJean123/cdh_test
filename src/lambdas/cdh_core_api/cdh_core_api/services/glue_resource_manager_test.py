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
from dataclasses import replace
from datetime import datetime
from typing import Optional
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.resources import NewGlueSyncBody
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.glue_resource_manager import GlueResourceManager
from cdh_core_api.services.glue_resource_manager import GlueSyncAlreadyExists
from cdh_core_api.services.lake_formation_service import LakeFormationService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.lock_service import ResourceIsLocked
from cdh_core_api.services.metadata_role_assumer import CannotAssumeMetadataRole
from cdh_core_api.services.resource_link import GlueEncryptionFailed
from cdh_core_api.services.resource_link import ResourceLink
from freezegun import freeze_time

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.glue_client import GlueClient
from cdh_core.aws_clients.ram_client import FailedToDeleteResourceShareResourcesStillAssociating
from cdh_core.aws_clients.ram_client import RamClient
from cdh_core.aws_clients.s3_client import S3Client
from cdh_core.aws_clients.utils import FailedToDeleteResourcesStillAssociating
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.entities.lock_test import build_lock
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import UnprocessableEntityError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core.primitives.constants import CREATED_BY_CORE_API_TAG
from cdh_core.primitives.constants import GOVERNED_BY_LAKE_FORMATION_TAG
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class GlueSyncTestCase:
    @pytest.fixture(autouse=True)
    def service_setup(self) -> None:
        self.stage = build_stage()
        self.hub = build_hub()
        self.resource_account = build_resource_account(stage=self.stage, hub=self.hub)
        self.region = build_region(self.hub.partition)
        self.dataset = build_dataset(hub=self.hub)
        self.owner_account_id = build_account_id()
        self.user = Builder.build_random_string()
        self.resource = build_glue_sync_resource(
            dataset=self.dataset,
            stage=self.resource_account.stage,
            region=self.region,
            resource_account_id=self.resource_account.id,
        )
        self.s3_resource = build_s3_resource()
        self.config = build_config()
        self.resources_table = Mock(ResourcesTable)
        self.resources_table.exists.return_value = False
        self.resources_table.list_glue_sync.return_value = []
        self.resources_table.get_s3.return_value = self.s3_resource
        self.aws = Mock(AwsClientFactory)
        self.glue_client = Mock(GlueClient)
        self.aws.glue_client.return_value = self.glue_client
        self.ram_client = Mock(RamClient)
        self.aws.ram_client.return_value = self.ram_client
        self.s3_client = Mock(S3Client)
        self.aws.s3_client.return_value = self.s3_client
        self.lake_formation_service = Mock(LakeFormationService)
        self.lock_service = Mock(LockService)
        self.lock = Mock()
        self.lock_service.acquire_lock.return_value = self.lock
        self.resource_link = Mock(ResourceLink)
        self.resource_link.glue_db_exists.return_value = False
        self.data_explorer_sync = Mock(DataExplorerSync)
        self.glue_resource_manager: GlueResourceManager[GlueSyncResource, NewGlueSyncBody] = GlueResourceManager(
            aws=self.aws,
            resources_table=self.resources_table,
            config=self.config,
            lake_formation_service=self.lake_formation_service,
            lock_service=self.lock_service,
            resource_link=self.resource_link,
            glue_sync_resource_type=GlueSyncResource,
            data_explorer_sync=self.data_explorer_sync,
        )


@pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
class TestCreateGlueSync(GlueSyncTestCase):
    @pytest.fixture(autouse=True)
    def extended_setup(self, service_setup: None, sync_type: SyncType) -> None:  # pylint: disable=unused-argument
        self.sync_type = sync_type
        self.body = NewGlueSyncBody(
            datasetId=self.dataset.id, stage=self.stage, region=self.region, syncType=self.sync_type
        )
        self.database_name = DatabaseName(f"{self.config.prefix}{self.dataset.id}_{self.stage.value}")
        self.arn = build_arn(
            service="glue",
            region=self.region,
            account_id=self.resource_account.id,
            resource=f"database/{self.database_name}",
        )
        self.created = datetime.now()
        self.expected_resource = GlueSyncResource(
            dataset_id=self.dataset.id,
            database_name=self.database_name,
            hub=self.hub,
            resource_account_id=self.resource_account.id,
            arn=self.arn,
            creator_user_id=self.user,
            creation_date=self.created,
            region=self.region,
            stage=self.stage,
            update_date=self.created,
            owner_account_id=self.owner_account_id,
            sync_type=self.sync_type,
        )

    def assert_create_successful(self, resource: GlueSyncResource) -> None:
        self.assert_resource_created(resource)
        self.assert_resource_link_created()

    def assert_resource_created(
        self, resource: GlueSyncResource, expected_resource: Optional[GlueSyncResource] = None
    ) -> None:
        if not expected_resource:
            expected_resource = self.expected_resource

        assert resource == expected_resource
        self.resources_table.exists.assert_called_once_with(
            ResourceType.glue_sync, self.dataset.id, self.stage, self.region
        )
        self.lock_service.acquire_lock.assert_called_once_with(
            item_id=self.dataset.id,
            scope=LockingScope.glue_sync_resource,
            region=self.region,
            stage=self.stage,
            data={"owner": expected_resource.owner_account_id},
        )
        self.resource_link.glue_db_exists.assert_called_once_with(
            GlueDatabase(name=self.database_name, account_id=expected_resource.owner_account_id, region=self.region)
        )
        self.glue_client.create_database.assert_called_once_with(
            database_name=self.database_name,
            remove_default_permissions=expected_resource.sync_type is SyncType.lake_formation,
        )
        self.resources_table.create.assert_called_once_with(resource)
        self.lock_service.release_lock.assert_called_once_with(self.lock)
        if self.sync_type == SyncType.lake_formation:
            self.data_explorer_sync.update_lake_formation_access_for_data_explorer.assert_called_once_with(
                dataset=self.dataset,
                glue_db=GlueDatabase(name=self.database_name, account_id=self.resource_account.id, region=self.region),
            )

    def assert_resource_link_created(self) -> None:
        self.resource_link.create_resource_link.assert_called_once_with(
            target_account_id=self.owner_account_id,
            source_database=self.expected_resource.glue_database,
        )
        if self.sync_type is SyncType.resource_link:
            self.ram_client.create_glue_resource_share_with_write_permissions.assert_called_once_with(
                database=self.expected_resource.glue_database,
                target_account_id=self.owner_account_id,
                tags=CREATED_BY_CORE_API_TAG,
            )
        else:
            self.s3_client.add_bucket_tags.assert_called_once_with(
                name=self.s3_resource.arn.identifier, tags=GOVERNED_BY_LAKE_FORMATION_TAG
            )
            self.lake_formation_service.setup_lake_formation_governance.assert_called_once_with(
                glue_resource=self.expected_resource, s3_resource=self.s3_resource
            )
            self.lake_formation_service.setup_provider_access.assert_called_once_with(
                glue_resource=self.expected_resource, s3_resource=self.s3_resource
            )

    def assert_not_created(self) -> None:
        self.assert_resource_not_created()
        self.assert_resource_link_not_created()

    def assert_resource_not_created(self) -> None:
        self.glue_client.create_database.assert_not_called()
        self.resources_table.create.assert_not_called()

    def assert_resource_link_not_created(self) -> None:
        self.resource_link.create_resource_link.assert_not_called()
        self.ram_client.create_glue_resource_share_with_write_permissions.assert_not_called()
        self.s3_client.add_bucket_tags.assert_not_called()
        self.lake_formation_service.setup_lake_formation_governance.assert_not_called()
        self.lake_formation_service.setup_provider_access.assert_not_called()

    def test_create_successful(self) -> None:
        with freeze_time(self.created):
            resource = self.glue_resource_manager.create_glue_sync(
                dataset=self.dataset,
                body=self.body,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=self.user,
            )

        self.assert_create_successful(resource)

    def test_dont_create_glue_sync_that_already_exists(self) -> None:
        self.resources_table.exists.return_value = True

        with assert_raises(GlueSyncAlreadyExists(self.dataset.id, self.stage, self.region)):
            self.glue_resource_manager.create_glue_sync(
                dataset=self.dataset,
                body=self.body,
                resource_account=self.resource_account,
                user=self.user,
                owner_account_id=self.owner_account_id,
            )

        self.resources_table.exists.assert_called_once_with(
            ResourceType.glue_sync, self.dataset.id, self.stage, self.region
        )
        self.lock_service.acquire_lock.assert_not_called()
        self.assert_not_created()

    def test_create_glue_sync_but_lock_present(self) -> None:
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(build_lock(), build_lock())

        with pytest.raises(ResourceIsLocked):
            self.glue_resource_manager.create_glue_sync(
                dataset=self.dataset,
                body=self.body,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=Builder.build_random_string(),
            )

        self.resource_link.glue_db_exists.assert_not_called()
        self.assert_not_created()

    def test_conflict_error_when_existing_db(self) -> None:
        self.resource_link.glue_db_exists.return_value = True
        with pytest.raises(ConflictError):
            self.glue_resource_manager.create_glue_sync(
                dataset=self.dataset,
                body=self.body,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=self.user,
            )

        self.lock_service.release_lock.assert_called_once_with(self.lock)
        self.assert_not_created()

    def test_cannot_assume_metadata_role(self) -> None:
        self.resource_link.glue_db_exists.side_effect = CannotAssumeMetadataRole(build_role_arn())

        with pytest.raises(UnprocessableEntityError):
            self.glue_resource_manager.create_glue_sync(
                dataset=self.dataset,
                body=self.body,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=self.user,
            )

        self.lock_service.release_lock.assert_called_once_with(self.lock)
        self.assert_not_created()

    def test_provider_and_resource_accounts_are_the_same(self) -> None:
        expected_resource = replace(self.expected_resource, owner_account_id=self.resource_account.id)
        with freeze_time(self.created):
            resource = self.glue_resource_manager.create_glue_sync(
                dataset=self.dataset,
                body=self.body,
                resource_account=self.resource_account,
                owner_account_id=self.resource_account.id,
                user=self.user,
            )

        self.assert_resource_created(resource, expected_resource)
        self.assert_resource_link_not_created()

    def test_unprocessable_entity_when_glue_database_cannot_be_encrypted(self) -> None:
        self.resource_link.create_resource_link.side_effect = GlueEncryptionFailed(
            build_account_id(), build_region(), Builder.build_random_string()
        )
        with pytest.raises(UnprocessableEntityError):
            self.glue_resource_manager.create_glue_sync(
                dataset=self.dataset,
                body=self.body,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=self.user,
            )

        self.lock_service.release_lock.assert_called_once_with(self.lock)
        self.ram_client.create_glue_resource_share_with_write_permissions.assert_not_called()
        self.glue_client.delete_database_if_present.assert_called_once_with(database_name=self.database_name)


class TestDeleteGlueSync(GlueSyncTestCase):
    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_delete_successful(self, sync_type: SyncType) -> None:
        self.resource = replace(self.resource, sync_type=sync_type)

        self.glue_resource_manager.delete_glue_sync(glue_resource=self.resource)

        if sync_type is SyncType.resource_link:
            self.ram_client.revoke_glue_share_if_necessary.assert_called_once_with(self.resource.glue_database)
            self.s3_client.remove_bucket_tags.assert_not_called()
            self.lake_formation_service.teardown_lake_formation_governance.assert_not_called()
            self.lake_formation_service.teardown_provider_access.assert_not_called()
        else:
            self.ram_client.revoke_glue_share_if_necessary.assert_not_called()
            self.s3_client.remove_bucket_tags.assert_called_once_with(
                name=self.s3_resource.arn.identifier, tags=GOVERNED_BY_LAKE_FORMATION_TAG
            )
            self.lake_formation_service.teardown_lake_formation_governance.assert_called_once_with(self.s3_resource)
            self.lake_formation_service.teardown_provider_access.assert_called_once_with(
                glue_resource=self.resource, s3_resource=self.s3_resource
            )
        self.resource_link.delete_resource_link.assert_called_once_with(
            target_account_id=self.resource.owner_account_id,
            source_database=self.resource.glue_database,
        )
        self.glue_client.delete_database_if_present.assert_called_once_with(self.resource.database_name)
        self.resources_table.delete.assert_called_once_with(
            resource_type=ResourceType.glue_sync,
            dataset_id=self.dataset.id,
            stage=self.resource.stage,
            region=self.resource.region,
        )

    def test_lock_cannot_be_acquired(self) -> None:
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(build_lock(), build_lock())
        with pytest.raises(ResourceIsLocked):
            self.glue_resource_manager.delete_glue_sync(self.resource)
        self.lock_service.release_lock.assert_not_called()

    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_resource_share_still_associating(self, sync_type: SyncType) -> None:
        self.resource = replace(self.resource, sync_type=sync_type)
        if sync_type is SyncType.resource_link:
            self.ram_client.revoke_glue_share_if_necessary.side_effect = (
                FailedToDeleteResourceShareResourcesStillAssociating(Builder.build_random_string())
            )
        else:
            self.lake_formation_service.teardown_provider_access.side_effect = FailedToDeleteResourcesStillAssociating()
        with pytest.raises(UnprocessableEntityError):
            self.glue_resource_manager.delete_glue_sync(self.resource)

    def test_catalog_delete_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.resources_table.delete.side_effect = exception
        with assert_raises(exception):
            self.glue_resource_manager.delete_glue_sync(self.resource)
        self._assert_resource_was_locked()

    def test_database_delete_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.glue_client.delete_database_if_present.side_effect = exception
        with assert_raises(exception):
            self.glue_resource_manager.delete_glue_sync(self.resource)
        self._assert_resource_was_locked()

    def _assert_resource_was_locked(self) -> None:
        self.lock_service.acquire_lock.assert_called_once_with(
            item_id=self.dataset.id,
            scope=LockingScope.glue_sync_resource,
            region=self.resource.region,
            stage=self.resource.stage,
            data=self.resource.to_payload().to_plain_dict(),
        )
        self.lock_service.release_lock.assert_not_called()
