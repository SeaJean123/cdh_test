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
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.dataset_permissions_manager import ConflictingGlueDatabases
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.dataset_permissions_validator import ValidatedDatasetAccessPermission
from cdh_core_api.services.lake_formation_service import ConflictingReadAccessModificationInProgress
from cdh_core_api.services.lake_formation_service import LakeFormationService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.metadata_role_assumer import AssumableAccountSpec
from cdh_core_api.services.metadata_role_assumer import CannotAssumeMetadataRole
from cdh_core_api.services.metadata_role_assumer import UnsupportedAssumeMetadataRole
from cdh_core_api.services.resource_link import GlueEncryptionFailed
from cdh_core_api.services.resource_link import ResourceLink
from cdh_core_api.services.s3_resource_manager import S3ResourceManager
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher

from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermissionAction
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_sync_type
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import UnprocessableEntityError
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class BaseDatasetPermissionsManagerTest:
    def setup_method(self) -> None:
        self.datasets_table = Mock(DatasetsTable)
        self.s3_resource_account = build_resource_account()
        account_store = Mock(AccountStore)
        account_store.query_resource_account.return_value = self.s3_resource_account
        self.config = build_config(account_store=account_store)
        self.s3_resource_manager = Mock(S3ResourceManager)
        self.lake_formation_service = Mock(LakeFormationService)
        self.sns_publisher = Mock(SnsPublisher)
        self.lock_service = Mock(LockService)
        self.lock = Mock()
        self.lock_service.acquire_lock.return_value = self.lock
        self.resource_link = Mock(ResourceLink)
        self.accounts_table = Mock(AccountsTable)
        self.resources_table = Mock(ResourcesTable)
        self.dataset_permissions_manager: DatasetPermissionsManager[
            Account, S3Resource, GlueSyncResource
        ] = DatasetPermissionsManager(
            config=self.config,
            datasets_table=self.datasets_table,
            lock_service=self.lock_service,
            s3_resource_manager=self.s3_resource_manager,
            lake_formation_service=self.lake_formation_service,
            sns_publisher=self.sns_publisher,
            accounts_table=self.accounts_table,
            resource_link=self.resource_link,
            resources_table=self.resources_table,
        )
        self.account = build_account()
        self.dataset = build_dataset()
        self.stage = build_stage()
        self.region = build_region()
        self.s3_resource = build_s3_resource(dataset=self.dataset, stage=self.stage, region=self.region)
        self.permission = build_dataset_account_permission(
            account_id=self.account.id,
            region=self.region,
            stage=self.stage,
            sync_type=build_sync_type(),
        )
        self.validated_permission = ValidatedDatasetAccessPermission(
            permission=self.permission,
            dataset=self.dataset,
            account=self.account,
            s3_resource=self.s3_resource,
        )
        self.updated_dataset = build_dataset()
        update_dataset_transaction_mock = MagicMock()
        update_dataset_transaction_mock.__enter__.side_effect = [self.updated_dataset, self.dataset]
        self.datasets_table.update_permissions_transaction.return_value = update_dataset_transaction_mock


class TestAddOrRemoveDatasetPermissionHandleErrors(BaseDatasetPermissionsManagerTest):
    @patch.object(DatasetPermissionsManager, "add_or_remove_permission")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_add_or_remove_permission_success(
        self,
        mocked_add_or_remove_permission: Mock,
        action: DatasetAccountPermissionAction,
    ) -> None:
        self.dataset_permissions_manager.add_or_remove_permission_handle_errors(
            self.validated_permission,
            action,
        )
        mocked_add_or_remove_permission.assert_called_once_with(self.validated_permission, action)

    @patch.object(DatasetPermissionsManager, "add_or_remove_permission")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_add_or_remove_permission_conflicting_database(
        self, mocked_add_or_remove_permission: Mock, action: DatasetAccountPermissionAction
    ) -> None:
        mocked_add_or_remove_permission.side_effect = ConflictingGlueDatabases("")
        with pytest.raises(ConflictError):
            self.dataset_permissions_manager.add_or_remove_permission_handle_errors(self.validated_permission, action)

    @patch.object(DatasetPermissionsManager, "add_or_remove_permission")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_add_or_remove_permission_glue_encryption_failed(
        self, mocked_add_or_remove_permission: Mock, action: DatasetAccountPermissionAction
    ) -> None:
        mocked_add_or_remove_permission.side_effect = GlueEncryptionFailed(Mock(), Mock(), Mock())
        with pytest.raises(UnprocessableEntityError):
            self.dataset_permissions_manager.add_or_remove_permission_handle_errors(self.validated_permission, action)

    @patch.object(DatasetPermissionsManager, "add_or_remove_permission")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_add_or_remove_permission_conflicting_read_access_modification(
        self, mocked_add_or_remove_permission: Mock, action: DatasetAccountPermissionAction
    ) -> None:
        mocked_add_or_remove_permission.side_effect = ConflictingReadAccessModificationInProgress(Mock())
        with pytest.raises(UnprocessableEntityError):
            self.dataset_permissions_manager.add_or_remove_permission_handle_errors(self.validated_permission, action)


class TestAddOrRemoveDatasetPermission(BaseDatasetPermissionsManagerTest):
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_add_or_remove_permission_successful(self, action: DatasetAccountPermissionAction) -> None:
        self.dataset_permissions_manager.add_or_remove_permission(
            self.validated_permission,
            action=action,
        )

        self.lock_service.acquire_lock.assert_called_once_with(
            item_id=self.validated_permission.dataset.id,
            scope=LockingScope.s3_resource,
            region=self.validated_permission.s3_resource.region,
            stage=self.validated_permission.s3_resource.stage,
            data={"datasetId": self.validated_permission.dataset.id},
        )
        self.s3_resource_manager.update_bucket_read_access.assert_called_once_with(
            s3_resource=self.s3_resource,
            dataset=self.updated_dataset,
            resource_account=self.s3_resource_account,
        )
        self.datasets_table.update_permissions_transaction.assert_called_once_with(
            dataset_id=self.dataset.id,
            permission=self.permission,
            action=action,
        )
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=self.updated_dataset,
            message_consistency=MessageConsistency.CONFIRMED,
        )
        self.lock_service.release_lock.assert_called_once_with(self.lock)

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_publish_if_changed(
        self, mocked_update_metadata_sync: Mock, action: DatasetAccountPermissionAction
    ) -> None:
        error = Exception("my error")
        mocked_update_metadata_sync.side_effect = error

        with pytest.raises(Exception) as exc_info:
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action=action,
            )
        assert exc_info.value == error

        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=self.updated_dataset,
            message_consistency=MessageConsistency.CONFIRMED,
        )

    @patch.object(DatasetPermissionsManager, "_update_read_access")
    @patch.object(DatasetPermissionsManager, "_handle_metadata_update")
    @pytest.mark.parametrize("error_updating_read_access", [True, False])
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_s3_resource_unlocked(
        self,
        mock_update_read_access: Mock,
        mock_handle_metadata_update: Mock,
        error_updating_read_access: bool,
        action: DatasetAccountPermissionAction,
    ) -> None:
        error = Exception(Builder.build_random_string())
        if error_updating_read_access:
            mock_update_read_access.side_effect = error
        else:
            mock_handle_metadata_update.side_effect = error

        with assert_raises(error):
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action=action,
            )

        self.lock_service.release_lock.assert_called_once_with(self.lock)


class TestUpdateMetadataSync(BaseDatasetPermissionsManagerTest):
    def setup_method(self) -> None:
        super().setup_method()
        self.glue_resource = build_glue_sync_resource()
        self.resources_table.get_glue_sync.return_value = self.glue_resource

    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_update_metadata_sync_on_add(self, sync_type: SyncType) -> None:
        glue_sync = build_glue_sync_resource(dataset=self.dataset, sync_type=sync_type)
        self.resources_table.get_glue_sync.return_value = glue_sync

        self.dataset_permissions_manager.update_metadata_sync(
            permission=self.permission,
            dataset=self.dataset,
            account=self.account,
            action=DatasetAccountPermissionAction.add,
        )

        self.resource_link.create_resource_link.assert_called_once_with(
            self.account.id,
            glue_sync.glue_database,
        )
        if sync_type is SyncType.lake_formation:
            self.lake_formation_service.grant_read_access.assert_called_once_with(
                self.account.id, glue_sync.glue_database
            )
        else:
            self.lake_formation_service.grant_read_access.assert_not_called()

    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_update_metadata_sync_on_remove(self, sync_type: SyncType) -> None:
        glue_sync = build_glue_sync_resource(dataset=self.dataset, sync_type=sync_type)
        self.resources_table.get_glue_sync.return_value = glue_sync

        self.dataset_permissions_manager.update_metadata_sync(
            permission=self.permission,
            dataset=self.dataset,
            account=self.account,
            action=DatasetAccountPermissionAction.remove,
        )

        self.resource_link.delete_resource_link.assert_called_once_with(
            self.account.id,
            glue_sync.glue_database,
        )
        if sync_type is SyncType.lake_formation:
            self.lake_formation_service.revoke_read_access.assert_called_once_with(
                self.account.id, glue_sync.glue_database
            )
        else:
            self.lake_formation_service.grant_read_access.assert_not_called()

    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    def test_update_metadata_sync_resource_not_found(self, action: DatasetAccountPermissionAction) -> None:
        self.resources_table.get_glue_sync.side_effect = ResourceNotFound(self.dataset.id, "")

        self.dataset_permissions_manager.update_metadata_sync(
            permission=self.permission,
            dataset=self.dataset,
            account=self.account,
            action=action,
        )

        self.resource_link.create_resource_link.assert_not_called()
        self.resource_link.delete_resource_link.assert_not_called()


class TestMetadataUpdateRollbacks(BaseDatasetPermissionsManagerTest):
    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    @pytest.mark.parametrize("enforce", [True, False])
    def test_add_or_remove_calls_metadata_update(
        self, patched_update_metadata_sync: Mock, action: DatasetAccountPermissionAction, enforce: bool
    ) -> None:
        self.dataset_permissions_manager.add_or_remove_permission(
            self.validated_permission,
            action,
            enforce_metadata_sync=enforce,
        )

        patched_update_metadata_sync.assert_called_once_with(
            permission=self.validated_permission.permission,
            dataset=self.updated_dataset,
            account=self.validated_permission.account,
            action=action,
        )

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    def test_rollback_on_conflicting_glue_databases(self, patched_update_metadata_sync: Mock) -> None:
        patched_update_metadata_sync.side_effect = ConflictingGlueDatabases("")
        action = DatasetAccountPermissionAction.add

        with pytest.raises(ConflictingGlueDatabases):
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action,
            )

        self._assert_rollback_performed(action)

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    def test_no_rollback_on_remove_conflict(self, patched_update_metadata_sync: Mock) -> None:
        patched_update_metadata_sync.side_effect = ConflictingGlueDatabases("")
        action = DatasetAccountPermissionAction.remove

        with pytest.raises(ConflictingGlueDatabases):
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action,
            )

        self._assert_no_rollback_performed(action)

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    @pytest.mark.parametrize(
        "metadata_error",
        [
            UnsupportedAssumeMetadataRole(AssumableAccountSpec.from_account(build_account())),
            CannotAssumeMetadataRole(build_role_arn()),
        ],
    )
    def test_ignore_metadata_error_when_not_enforcing(
        self, patched_update_metadata_sync: Mock, action: DatasetAccountPermissionAction, metadata_error: Exception
    ) -> None:
        patched_update_metadata_sync.side_effect = metadata_error

        self.dataset_permissions_manager.add_or_remove_permission(
            self.validated_permission, action, enforce_metadata_sync=False
        )

        self._assert_no_rollback_performed(action)

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    @pytest.mark.parametrize(
        "metadata_error",
        [
            UnsupportedAssumeMetadataRole(AssumableAccountSpec.from_account(build_account())),
            CannotAssumeMetadataRole(build_role_arn()),
        ],
    )
    def test_rollback_on_metadata_error_when_enforcing(
        self, patched_update_metadata_sync: Mock, action: DatasetAccountPermissionAction, metadata_error: Exception
    ) -> None:
        patched_update_metadata_sync.side_effect = metadata_error

        with pytest.raises(type(metadata_error)):
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action,
                enforce_metadata_sync=True,
            )

        self._assert_rollback_performed(action)

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    @pytest.mark.parametrize("enforce", [True, False])
    def test_rollback_on_failed_glue_encryption(
        self, patched_update_metadata_sync: Mock, action: DatasetAccountPermissionAction, enforce: bool
    ) -> None:
        patched_update_metadata_sync.side_effect = GlueEncryptionFailed(self.account.id, self.region, "")

        with pytest.raises(GlueEncryptionFailed):
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action,
                enforce_metadata_sync=enforce,
            )

        self._assert_rollback_performed(action)

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    @pytest.mark.parametrize("enforce", [True, False])
    def test_rollback_on_conflicting_read_access_modification(
        self, patched_update_metadata_sync: Mock, action: DatasetAccountPermissionAction, enforce: bool
    ) -> None:
        patched_update_metadata_sync.side_effect = ConflictingReadAccessModificationInProgress("")

        with pytest.raises(ConflictingReadAccessModificationInProgress):
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action,
                enforce_metadata_sync=enforce,
            )

        self._assert_rollback_performed(action)

    @patch.object(DatasetPermissionsManager, "update_metadata_sync")
    @pytest.mark.parametrize("action", DatasetAccountPermissionAction)
    @pytest.mark.parametrize("enforce", [True, False])
    def test_no_rollback_on_other_errors(
        self, patched_update_metadata_sync: Mock, action: DatasetAccountPermissionAction, enforce: bool
    ) -> None:
        error = Exception("my error")
        patched_update_metadata_sync.side_effect = error

        with pytest.raises(Exception) as exc_info:
            self.dataset_permissions_manager.add_or_remove_permission(
                self.validated_permission,
                action,
                enforce_metadata_sync=enforce,
            )
        assert exc_info.value == error

        self._assert_no_rollback_performed(action)

    def _assert_rollback_performed(self, action: DatasetAccountPermissionAction) -> None:
        self.datasets_table.update_permissions_transaction.assert_has_calls(
            [
                call(dataset_id=self.dataset.id, permission=self.permission, action=action),
                call(
                    dataset_id=self.updated_dataset.id,
                    permission=self.permission,
                    action=action.inverse,
                ),
            ],
            any_order=True,
        )
        self.s3_resource_manager.update_bucket_read_access.assert_has_calls(
            [
                call(
                    s3_resource=self.s3_resource,
                    dataset=self.updated_dataset,
                    resource_account=self.s3_resource_account,
                ),
                call(s3_resource=self.s3_resource, dataset=self.dataset, resource_account=self.s3_resource_account),
            ]
        )
        self.sns_publisher.publish.assert_not_called()

    def _assert_no_rollback_performed(self, action: DatasetAccountPermissionAction) -> None:
        self.datasets_table.update_permissions_transaction.assert_called_once_with(
            dataset_id=self.dataset.id, permission=self.permission, action=action
        )
        self.s3_resource_manager.update_bucket_read_access.assert_called_once_with(
            s3_resource=self.s3_resource,
            dataset=self.updated_dataset,
            resource_account=self.s3_resource_account,
        )
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=self.updated_dataset,
            message_consistency=MessageConsistency.CONFIRMED,
        )


@patch.object(Dataset, "filter_permissions")
@patch.object(DatasetPermissionsManager, "update_metadata_sync")
class TestCreateMissingResourceLinksInternal(BaseDatasetPermissionsManagerTest):
    def setup_method(self) -> None:
        super().setup_method()
        self.permissions = [build_dataset_account_permission() for _ in range(2)]

    def test_create_successful(
        self,
        mocked_update_metadata_sync: Mock,
        mocked_filter_permissions: Mock,
    ) -> None:
        mocked_filter_permissions.return_value = frozenset(self.permissions)
        self.accounts_table.get.return_value = self.account

        self.dataset_permissions_manager.create_missing_resource_links(self.dataset, self.stage, self.region)

        mocked_update_metadata_sync.assert_has_calls(
            [
                call(
                    permission=permission,
                    dataset=self.dataset,
                    account=self.account,
                    action=DatasetAccountPermissionAction.add,
                )
                for permission in self.permissions
            ],
            any_order=True,
        )

    def test_create_does_not_fail_when_account_not_found(
        self,
        mocked_update_metadata_sync: Mock,
        mocked_filter_permissions: Mock,
    ) -> None:
        mocked_filter_permissions.return_value = frozenset(self.permissions)
        self.accounts_table.get.side_effect = AccountNotFound("")

        self.dataset_permissions_manager.create_missing_resource_links(self.dataset, self.stage, self.region)

        mocked_filter_permissions.assert_called_once_with(
            stage=self.stage, region=self.region, sync_type=SyncType.resource_link
        )
        self.accounts_table.get.assert_has_calls(
            [call(permission.account_id) for permission in self.permissions], any_order=True
        )
        mocked_update_metadata_sync.assert_not_called()

    def test_create_does_not_fail_for_conflicting_glue_databases_exceptions(
        self,
        mocked_update_metadata_sync: Mock,
        mocked_filter_permissions: Mock,
    ) -> None:
        mocked_filter_permissions.return_value = frozenset(self.permissions)
        self.accounts_table.get.return_value = self.account
        mocked_update_metadata_sync.side_effect = ConflictingGlueDatabases("")

        self.dataset_permissions_manager.create_missing_resource_links(self.dataset, self.stage, self.region)

        mocked_update_metadata_sync.assert_has_calls(
            [
                call(
                    permission=permission,
                    dataset=self.dataset,
                    account=self.account,
                    action=DatasetAccountPermissionAction.add,
                )
                for permission in self.permissions
            ],
            any_order=True,
        )


@patch.object(Dataset, "filter_permissions")
class TestDeleteMetadataSyncsForGlueSyncInternal(BaseDatasetPermissionsManagerTest):
    def setup_method(self) -> None:
        super().setup_method()
        self.accounts = [build_account() for _ in range(3)]
        self.permissions = [
            build_dataset_account_permission(account_id=account.id, sync_type=SyncType.resource_link)
            for account in self.accounts
        ]
        self.glue_sync = build_glue_sync_resource()

    def test_delete_successful(self, mocked_filter_permissions: Mock) -> None:
        mocked_filter_permissions.return_value = self.permissions
        self.accounts_table.get.side_effect = self.accounts

        self.dataset_permissions_manager.delete_metadata_syncs_for_glue_sync(self.glue_sync, self.dataset)

        self.resource_link.delete_resource_link.assert_has_calls(
            [
                call(target_account_id=account.id, source_database=self.glue_sync.glue_database)
                for account in self.accounts
            ],
            any_order=True,
        )

    def test_delete_does_not_fail_when_account_not_found(self, mocked_filter_permissions: Mock) -> None:
        mocked_filter_permissions.return_value = self.permissions
        self.accounts_table.get.side_effect = AccountNotFound("")

        self.dataset_permissions_manager.delete_metadata_syncs_for_glue_sync(self.glue_sync, self.dataset)

        mocked_filter_permissions.assert_called_once_with(stage=self.glue_sync.stage, region=self.glue_sync.region)
        self.accounts_table.get.assert_has_calls(
            [call(permission.account_id) for permission in self.permissions], any_order=True
        )
        self.resource_link.delete_resource_link.assert_not_called()

    @pytest.mark.parametrize("unsupported_sync_type", [SyncType.glue_sync, SyncType.lake_formation])
    def test_delete_logs_error_but_does_not_fail_on_unsupported_sync_type(
        self, mocked_filter_permissions: Mock, unsupported_sync_type: SyncType
    ) -> None:
        mocked_filter_permissions.return_value = [
            build_dataset_account_permission(account_id=account.id, sync_type=unsupported_sync_type)
            for account in self.accounts
        ]
        self.accounts_table.get.side_effect = self.accounts

        with patch("cdh_core_api.services.dataset_permissions_manager.LOG") as log_mock:
            self.dataset_permissions_manager.delete_metadata_syncs_for_glue_sync(self.glue_sync, self.dataset)

            mocked_filter_permissions.assert_called_once_with(stage=self.glue_sync.stage, region=self.glue_sync.region)
            self.accounts_table.get.assert_has_calls(
                [call(permission.account_id) for permission in self.permissions], any_order=True
            )
            assert log_mock.error.call_count == len(self.accounts)
            self.resource_link.delete_resource_link.assert_not_called()


class TestRemovePermissionAcrossDatasets(BaseDatasetPermissionsManagerTest):
    @pytest.mark.parametrize("sync_type", SyncType)
    def test_remove_permissions_across_datasets(self, sync_type: SyncType) -> None:
        first_permission = build_dataset_account_permission(account_id=self.account.id, sync_type=sync_type)
        first_dataset_with_access = build_dataset(permissions=frozenset({first_permission}))
        first_s3_resource = build_s3_resource(
            dataset=first_dataset_with_access, stage=first_permission.stage, region=first_permission.region
        )
        second_permission = build_dataset_account_permission(account_id=self.account.id, sync_type=sync_type)
        second_dataset_with_access = build_dataset(permissions=frozenset({second_permission}))
        second_s3_resource = build_s3_resource(
            dataset=second_dataset_with_access, stage=second_permission.stage, region=second_permission.region
        )
        dataset_without_access = build_dataset(permissions=frozenset())
        self.datasets_table.list.return_value = [
            first_dataset_with_access,
            second_dataset_with_access,
            dataset_without_access,
        ]
        self.resources_table.get_s3.side_effect = lambda dataset_id, stage, region: {
            (first_dataset_with_access.id, first_permission.stage, first_permission.region): first_s3_resource,
            (second_dataset_with_access.id, second_permission.stage, second_permission.region): second_s3_resource,
        }[(dataset_id, stage, region)]
        self.dataset_permissions_manager.add_or_remove_permission = Mock()  # type: ignore

        self.dataset_permissions_manager.remove_permissions_across_datasets(self.account)

        self.dataset_permissions_manager.add_or_remove_permission.assert_has_calls(
            [
                call(
                    validated_permission=ValidatedDatasetAccessPermission(
                        dataset=first_dataset_with_access,
                        account=self.account,
                        s3_resource=first_s3_resource,
                        permission=first_permission,
                    ),
                    action=DatasetAccountPermissionAction.remove,
                    enforce_metadata_sync=False,
                ),
                call(
                    validated_permission=ValidatedDatasetAccessPermission(
                        dataset=second_dataset_with_access,
                        account=self.account,
                        s3_resource=second_s3_resource,
                        permission=second_permission,
                    ),
                    action=DatasetAccountPermissionAction.remove,
                    enforce_metadata_sync=False,
                ),
            ],
            any_order=True,
        )
