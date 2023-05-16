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
from datetime import datetime
from typing import Generator
from typing import List
from typing import Set
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import ResourceAlreadyExists
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.kms_service import KmsService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.lock_service import ResourceIsLocked
from cdh_core_api.services.s3_bucket_manager import S3BucketManager
from cdh_core_api.services.s3_bucket_manager import S3ResourceSpecification
from cdh_core_api.services.s3_resource_manager import S3ResourceManager
from cdh_core_api.services.sns_topic_manager import SnsTopicManager
from freezegun import freeze_time

from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.aws_clients.s3_client import BucketNotEmpty
from cdh_core.aws_clients.sns_client import SnsTopic
from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.account_store_test import build_account_store
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_kms_key_arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.lock_test import build_lock
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class CreateBucketTestCase:
    @pytest.fixture(autouse=True)
    def service_setup(
        self, datasets_table: DatasetsTable, resources_table: ResourcesTable
    ) -> None:  # pylint: disable=unused-argument
        self.hub = build_hub()
        self.dataset = build_dataset(hub=self.hub)
        self.region = build_region()
        self.stage = build_stage()
        environment = build_environment()

        self.resource_account = build_resource_account(hub=self.hub, stage=self.stage, environment=environment)
        self.owner_account_id = build_account_id()
        self.config = build_config(account_store=build_account_store([self.resource_account]), environment=environment)
        self.resources_table = resources_table
        self.datasets_table = datasets_table
        self.datasets_table.create(self.dataset)
        self.kms_service = Mock(KmsService)

        self.now = datetime.now()
        self.expected_bucket_arn = build_arn("s3")
        self.expected_kms_arn = build_kms_key_arn()
        self.expected_kms_key = KmsKey.parse_from_arn(self.expected_kms_arn)
        self.kms_service.get_shared_key.return_value = self.expected_kms_key
        self.kms_key_readers = {build_account_id() for _ in range(5)}
        self.kms_key_writers = {build_account_id() for _ in range(5)}
        self.expected_topic_arn = build_arn("sns")
        self.s3_bucket_manager = Mock(S3BucketManager)
        self.s3_bucket_manager.create_bucket.return_value = self.expected_bucket_arn
        self.sns_topic_manager = Mock(SnsTopicManager)
        self.sns_topic_manager.create_topic.return_value = SnsTopic(
            name="unused", arn=self.expected_topic_arn, region=self.region
        )
        self.lock_service = Mock(LockService)
        self.data_explorer_sync = MagicMock(DataExplorerSync)
        self.expected_spec = S3ResourceSpecification(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            resource_account_id=self.resource_account.id,
            owner_id=self.owner_account_id,
        )


class TestCreateBucket(CreateBucketTestCase):
    @pytest.fixture(autouse=True)
    def extended_setup(self, service_setup: None) -> Generator[None, None, None]:  # pylint: disable=unused-argument
        self.s3_resource_manager = S3ResourceManager(
            resources_table=self.resources_table,
            datasets_table=self.datasets_table,
            config=self.config,
            s3_bucket_manager=self.s3_bucket_manager,
            sns_topic_manager=self.sns_topic_manager,
            lock_service=self.lock_service,
            kms_service=self.kms_service,
            s3_resource_type=S3Resource,
            data_explorer_sync=self.data_explorer_sync,
        )
        # patch this auxiliary method, as it is tested separately
        with patch.object(
            self.s3_resource_manager,
            "_get_reader_and_writer_account_ids",
            Mock(return_value=(self.kms_key_readers, self.kms_key_writers)),
        ):
            yield

    def test_resource_already_exists(self) -> None:
        self.resources_table.create(
            build_s3_resource(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
            ),
        )
        with pytest.raises(ConflictError):
            self.s3_resource_manager.create_bucket(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                resource_account=self.resource_account,
                user=Builder.build_random_string(),
                owner_account_id=self.owner_account_id,
            )
        self.lock_service.acquire_lock.assert_not_called()
        self.kms_service.regenerate_key_policy.assert_not_called()

    def test_create_successfully(self) -> None:
        lock = build_lock(
            item_id=self.dataset.id,
            scope=LockingScope.s3_resource,
            stage=self.stage,
            region=self.region,
        )
        user = Builder.build_random_string()

        self.lock_service.acquire_lock.return_value = lock
        with freeze_time(self.now):
            resource = self.s3_resource_manager.create_bucket(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=user,
            )

        assert self.resources_table.list() == [resource]
        assert resource.kms_key_arn == self.expected_kms_arn
        self.s3_bucket_manager.create_bucket.assert_called_once_with(
            spec=self.expected_spec, kms_key=self.expected_kms_key
        )
        resource_account = self.config.account_store.query_resource_account(
            hubs=self.dataset.hub, stages=self.stage, environments=self.config.environment, only_default=True
        )
        self.kms_service.regenerate_key_policy.assert_called_once_with(
            kms_key=self.expected_kms_key,
            resource_account=resource_account,
            account_ids_with_read_access=self.kms_key_readers,
            account_ids_with_write_access={*self.kms_key_writers, self.owner_account_id},
        )
        self.sns_topic_manager.create_topic.assert_called_once_with(
            self.expected_spec, topic_name=resource.name, kms_key_arn=self.expected_kms_arn
        )
        self.lock_service.release_lock.assert_called_once_with(lock)
        # pylint: disable=no-member, protected-access
        self.s3_resource_manager._get_reader_and_writer_account_ids.assert_called_once_with(  # type: ignore
            kms_key=self.expected_kms_key, resource_account_id=self.resource_account.id
        )
        self.data_explorer_sync.update_bucket_access_for_data_explorer.assert_called_once_with(self.dataset, resource)

    def test_no_database_entry_if_bucket_creation_fails(self) -> None:
        self.s3_bucket_manager.create_bucket.side_effect = ValueError
        with pytest.raises(ValueError):
            self.s3_resource_manager.create_bucket(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=Builder.build_random_string(),
            )

        assert self.resources_table.list() == []

    def test_create_s3_bucket_but_lock_present(self) -> None:
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(build_lock(), build_lock())
        with pytest.raises(ResourceIsLocked):
            self.s3_resource_manager.create_bucket(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=Builder.build_random_string(),
            )
        self.kms_service.regenerate_key_policy.assert_not_called()

    def test_create_s3_bucket_fail_lock_present(self) -> None:
        self.s3_bucket_manager.create_bucket.side_effect = ResourceAlreadyExists(
            dataset_id=self.dataset.id, range_key="range_key"
        )
        with suppress(ResourceAlreadyExists):
            self.s3_resource_manager.create_bucket(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=Builder.build_random_string(),
            )

        self.lock_service.acquire_lock.assert_called_once_with(
            item_id=self.dataset.id,
            region=self.region,
            scope=LockingScope.s3_resource,
            stage=self.stage,
            data={"datasetId": self.dataset.id},
        )

        resource_account = self.config.account_store.query_resource_account(
            hubs=self.dataset.hub, stages=self.stage, environments=self.config.environment, only_default=True
        )
        self.kms_service.regenerate_key_policy.assert_called_once_with(
            kms_key=self.expected_kms_key,
            resource_account=resource_account,
            account_ids_with_read_access=self.kms_key_readers,
            account_ids_with_write_access={*self.kms_key_writers, self.owner_account_id},
        )
        self.lock_service.release_lock.assert_not_called()

    def test_create_fails_on_kms_lock(self) -> None:
        lock = build_lock(
            item_id=self.dataset.id,
            scope=LockingScope.s3_resource,
            stage=self.stage,
            region=self.region,
        )

        self.lock_service.acquire_lock.return_value = lock

        self.kms_service.regenerate_key_policy.side_effect = ResourceIsLocked(
            new_lock=build_lock(scope=LockingScope.kms_key),
            old_lock=build_lock(scope=LockingScope.kms_key),
        )
        with pytest.raises(ResourceIsLocked):
            self.s3_resource_manager.create_bucket(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                resource_account=self.resource_account,
                owner_account_id=self.owner_account_id,
                user=Builder.build_random_string(),
            )

        assert self.resources_table.list() == []
        self.s3_bucket_manager.create_bucket.assert_not_called()
        resource_account = self.config.account_store.query_resource_account(
            hubs=self.dataset.hub, stages=self.stage, environments=self.config.environment, only_default=True
        )
        self.kms_service.regenerate_key_policy.assert_called_once_with(
            kms_key=self.expected_kms_key,
            resource_account=resource_account,
            account_ids_with_read_access=self.kms_key_readers,
            account_ids_with_write_access={*self.kms_key_writers, self.owner_account_id},
        )
        self.sns_topic_manager.create_topic.assert_not_called()
        self.lock_service.release_lock.assert_called_once_with(lock)


class TestDeleteBucket:
    @pytest.fixture(autouse=True)
    def setup_method(self) -> Generator[None, None, None]:
        self.dataset = build_dataset()
        self.s3_resource = build_s3_resource(dataset=self.dataset)
        self.resources_table = Mock(ResourcesTable)
        self.datasets_table = Mock(DatasetsTable)
        self.kms_service = Mock(KmsService)

        self.resource_account = build_resource_account(
            account_id=self.s3_resource.resource_account_id, hub=self.s3_resource.hub, stage=self.s3_resource.stage
        )
        self.config = build_config(
            account_store=build_account_store([self.resource_account]), environment=self.resource_account.environment
        )

        self.s3_bucket_manager = Mock(S3BucketManager)
        self.sns_topic_manager = Mock(SnsTopicManager)
        self.lock_service = Mock(LockService)
        self.s3_resource_manager = S3ResourceManager(
            resources_table=self.resources_table,
            datasets_table=self.datasets_table,
            config=self.config,
            s3_bucket_manager=self.s3_bucket_manager,
            sns_topic_manager=self.sns_topic_manager,
            lock_service=self.lock_service,
            kms_service=self.kms_service,
            s3_resource_type=S3Resource,
            data_explorer_sync=Mock(),
        )
        self.lock = build_lock(
            item_id=self.dataset.id,
            scope=LockingScope.s3_resource,
            stage=self.s3_resource.stage,
            region=self.s3_resource.region,
        )

        self.kms_key_readers = {build_account_id() for _ in range(5)}
        self.kms_key_writers = {build_account_id() for _ in range(5)}
        with patch.object(
            self.s3_resource_manager,
            "_get_reader_and_writer_account_ids",
            Mock(return_value=(self.kms_key_readers, self.kms_key_writers)),
        ):
            yield

    def test_delete_successful(self) -> None:
        self.lock_service.acquire_lock.return_value = self.lock

        self.s3_resource_manager.delete_bucket(s3_resource=self.s3_resource)

        self.resources_table.delete.assert_called_once_with(
            resource_type=ResourceType.s3,
            dataset_id=self.dataset.id,
            stage=self.s3_resource.stage,
            region=self.s3_resource.region,
        )
        self.sns_topic_manager.delete_topic.assert_called_once_with(topic_arn=self.s3_resource.sns_topic_arn)
        self.s3_bucket_manager.delete_bucket.assert_called_once_with(
            account_id=self.s3_resource.resource_account_id,
            region=self.s3_resource.region,
            bucket_name=self.s3_resource.name,
        )
        self.lock_service.release_lock.assert_called_once_with(self.lock)
        self.kms_service.regenerate_key_policy.assert_called_once_with(
            kms_key=KmsKey.parse_from_arn(self.s3_resource.kms_key_arn),
            resource_account=self.resource_account,
            account_ids_with_read_access=self.kms_key_readers,
            account_ids_with_write_access=self.kms_key_writers,
        )

    def test_lock_cannot_be_acquired(self) -> None:
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(self.lock, build_lock())
        with pytest.raises(ResourceIsLocked):
            self.s3_resource_manager.delete_bucket(s3_resource=self.s3_resource)
        self.lock_service.release_lock.assert_not_called()

    def test_catalog_delete_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.resources_table.delete.side_effect = exception
        with assert_raises(exception):
            self.s3_resource_manager.delete_bucket(s3_resource=self.s3_resource)
        self._assert_resource_was_locked()

    def test_bucket_delete_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.s3_bucket_manager.delete_bucket.side_effect = exception
        with assert_raises(exception):
            self.s3_resource_manager.delete_bucket(s3_resource=self.s3_resource)
        self._assert_resource_was_locked()

    def test_bucket_delete_fails_because_bucket_is_not_empty(self) -> None:
        self.lock_service.acquire_lock.return_value = self.lock
        self.s3_bucket_manager.delete_bucket.side_effect = BucketNotEmpty(self.s3_resource.name)
        with pytest.raises(ForbiddenError):
            self.s3_resource_manager.delete_bucket(s3_resource=self.s3_resource)
        self.lock_service.release_lock.assert_called_once_with(self.lock)

    def test_topic_delete_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.sns_topic_manager.delete_topic.side_effect = exception
        with assert_raises(exception):
            self.s3_resource_manager.delete_bucket(s3_resource=self.s3_resource)
        self._assert_resource_was_locked()

    def _assert_resource_was_locked(self) -> None:
        self.lock_service.acquire_lock.assert_called_once_with(
            item_id=self.s3_resource.dataset_id,
            scope=LockingScope.s3_resource,
            region=self.s3_resource.region,
            stage=self.s3_resource.stage,
            data=self.s3_resource.to_payload().to_plain_dict(),
        )
        self.lock_service.release_lock.assert_not_called()


class TestUpdateReadAccessTransaction:
    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_update(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        stage, other_stage = Builder.choose_without_repetition(Stage, 2)
        region, other_region = Builder.choose_without_repetition(Region, 2)
        new_account_ids = [build_account_id(), build_account_id()]
        dataset = build_dataset(
            permissions=frozenset(
                {
                    build_dataset_account_permission(account_id=new_account_ids[0], region=region, stage=stage),
                    build_dataset_account_permission(account_id=new_account_ids[1], region=region, stage=stage),
                    build_dataset_account_permission(account_id=build_account_id(), region=other_region, stage=stage),
                    build_dataset_account_permission(account_id=build_account_id(), region=region, stage=other_stage),
                }
            )
        )
        s3_resource = build_s3_resource(dataset=dataset, stage=stage, region=region)
        kms_key_readers = {build_account_id() for _ in range(5)}
        kms_key_writers = {build_account_id() for _ in range(5)}

        resources_table = Mock(ResourcesTable)
        datasets_table = Mock(DatasetsTable)
        s3_bucket_manager = Mock(S3BucketManager)
        s3_bucket_manager.update_bucket_policy_read_access_statement_transaction.return_value = MagicMock()
        sns_topic_manager = Mock(SnsTopicManager)
        sns_topic_manager.update_policy_transaction.return_value = MagicMock()
        lock_service = Mock(LockService)
        kms_service = Mock(KmsService)
        resource_account = Mock()
        s3_resource_manager = S3ResourceManager(
            resources_table=resources_table,
            datasets_table=datasets_table,
            config=Mock(),
            s3_bucket_manager=s3_bucket_manager,
            sns_topic_manager=sns_topic_manager,
            lock_service=lock_service,
            kms_service=kms_service,
            s3_resource_type=S3Resource,
            data_explorer_sync=Mock(),
        )

        with patch.object(
            s3_resource_manager,
            "_get_reader_and_writer_account_ids",
            Mock(return_value=(kms_key_readers, kms_key_writers)),
        ):
            s3_resource_manager.update_bucket_read_access(
                s3_resource=s3_resource,
                dataset=dataset,
                resource_account=resource_account,
            )

            s3_bucket_manager.update_bucket_policy_read_access_statement_transaction.assert_called_once_with(
                s3_resource=s3_resource, account_ids_with_read_access=frozenset(new_account_ids)
            )
            sns_topic_manager.update_policy_transaction.assert_called_once_with(
                topic=SnsTopic(
                    s3_resource.sns_topic_arn.identifier,
                    s3_resource.sns_topic_arn,
                    s3_resource.region,
                ),
                owner_account_id=s3_resource.owner_account_id,
                account_ids_with_read_access=sorted(new_account_ids),
            )
            kms_service.regenerate_key_policy.assert_called_once_with(
                kms_key=KmsKey.parse_from_arn(s3_resource.kms_key_arn),
                resource_account=resource_account,
                account_ids_with_read_access=kms_key_readers,
                account_ids_with_write_access=kms_key_writers,
            )


class AccountIdsWithKmsAccessTestCase:
    @pytest.fixture(autouse=True)
    def service_setup(self) -> None:
        self.buckets: List[S3Resource] = []
        self.datasets: List[Dataset] = []
        self.resources_table = Mock(ResourcesTable)
        self.datasets_table = Mock(DatasetsTable)
        self.resources_table.list_s3.return_value = self.buckets
        self.datasets_table.list.return_value = self.datasets
        self.provider_account_id = build_account_id()
        self.provider_key = KmsKey.parse_from_arn(build_kms_key_arn())
        self.kms_service = Mock(KmsService)


# pylint: disable=protected-access
class TestAccountIdsWithKmsAccess(AccountIdsWithKmsAccessTestCase):
    @pytest.fixture(autouse=True)
    def extended_setup(self, service_setup: None) -> None:  # pylint: disable=unused-argument
        s3_resource_manager = S3ResourceManager(
            resources_table=self.resources_table,
            datasets_table=self.datasets_table,
            config=Mock(),
            s3_bucket_manager=Mock(),
            sns_topic_manager=Mock(),
            lock_service=Mock(),
            kms_service=self.kms_service,
            s3_resource_type=S3Resource,
            data_explorer_sync=Mock(),
        )
        self.s3_resource_manager = s3_resource_manager

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_all_matching_buckets_are_added(
        self, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:
        expected_accounts_with_read_access: Set[AccountId] = set()
        expected_accounts_with_write_access: Set[AccountId] = set()
        for _ in range(5):
            permission = build_dataset_account_permission()
            dataset = build_dataset(permissions=frozenset({permission}))
            bucket = build_s3_resource(
                dataset=dataset, stage=permission.stage, region=permission.region, kms_key_arn=self.provider_key.arn
            )
            expected_accounts_with_read_access.add(permission.account_id)
            expected_accounts_with_write_access.update({bucket.owner_account_id, bucket.resource_account_id})
            self.datasets.append(dataset)
            self.buckets.append(bucket)

        (
            account_ids_with_read_access,
            account_ids_with_write_access,
        ) = self.s3_resource_manager._get_reader_and_writer_account_ids(
            kms_key=self.provider_key, resource_account_id=self.provider_account_id
        )

        assert account_ids_with_read_access == expected_accounts_with_read_access
        assert account_ids_with_write_access == set(expected_accounts_with_write_access)
        self.resources_table.list_s3.assert_called_once_with(
            resource_account=self.provider_account_id, region=self.provider_key.region
        )

    def test_permissions_for_other_stages_are_ignored(self) -> None:
        stage, other_stage = Builder.choose_without_repetition(Stage, 2)
        permission = build_dataset_account_permission(stage=other_stage)
        dataset = build_dataset(permissions=frozenset({permission}))
        self.datasets.append(dataset)
        bucket = build_s3_resource(
            dataset=dataset, stage=stage, region=permission.region, kms_key_arn=self.provider_key.arn
        )
        self.buckets.append(bucket)

        (
            account_ids_with_read_access,
            account_ids_with_write_access,
        ) = self.s3_resource_manager._get_reader_and_writer_account_ids(
            kms_key=self.provider_key, resource_account_id=self.provider_account_id
        )

        assert account_ids_with_read_access == set()
        assert account_ids_with_write_access == {bucket.owner_account_id, bucket.resource_account_id}

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_permissions_for_other_regions_are_ignored(
        self, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:
        region, other_region = Builder.choose_without_repetition(Region, 2)
        permission = build_dataset_account_permission(region=other_region)
        dataset = build_dataset(permissions=frozenset({permission}))
        self.datasets.append(dataset)
        bucket = build_s3_resource(
            dataset=dataset, stage=permission.stage, region=region, kms_key_arn=self.provider_key.arn
        )
        self.buckets.append(bucket)

        (
            account_ids_with_read_access,
            account_ids_with_write_access,
        ) = self.s3_resource_manager._get_reader_and_writer_account_ids(
            kms_key=self.provider_key, resource_account_id=self.provider_account_id
        )

        assert account_ids_with_read_access == set()
        assert account_ids_with_write_access == {bucket.owner_account_id, bucket.resource_account_id}
