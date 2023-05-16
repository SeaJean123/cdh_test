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
import random
from dataclasses import replace
from datetime import datetime
from typing import cast
from typing import List
from typing import Set
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.datasets_test import build_update_dataset_body
from cdh_core_api.catalog.datasets_table import DatasetAlreadyExists
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.dataset_manager import DatasetManager
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.lock_service import ResourceIsLocked
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.validation.datasets import DeletableSourceIdentifier
from cdh_core_api.validation.datasets import DeletableSupportGroup

from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.dataset import DatasetLineage
from cdh_core.entities.dataset import DatasetTags
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.dataset_test import build_dataset_tags
from cdh_core.entities.dataset_test import build_external_link
from cdh_core.entities.glue_database_test import build_glue_database
from cdh_core.entities.lock import Lock
from cdh_core.entities.lock_test import build_lock
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetPurpose
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.dataset_properties import ExternalLinkType
from cdh_core.enums.dataset_properties import IngestFrequency
from cdh_core.enums.dataset_properties import RetentionPeriod
from cdh_core.enums.dataset_properties import SupportLevel
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.dataset_properties_test import build_dataset_purpose
from cdh_core.enums.dataset_properties_test import build_ingest_frequency
from cdh_core.enums.dataset_properties_test import build_retention_period
from cdh_core.enums.dataset_properties_test import build_support_level
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import NotFoundError
from cdh_core_dev_tools.testing.builder import Builder


class DatasetManagerTestCase:
    @pytest.fixture(autouse=True)
    def service_setup(self, time_travel: None) -> None:  # pylint: disable=unused-argument
        self.config = build_config()
        self.datasets_table = Mock(DatasetsTable)
        self.resources_table = Mock(ResourcesTable)
        self.lock_service = Mock(LockService)
        self.data_explorer_sync = MagicMock(DataExplorerSync)
        self.lock = Mock()
        self.lock_service.acquire_lock.return_value = self.lock
        self.dataset_manager = DatasetManager(
            datasets_table=self.datasets_table,
            resources_table=self.resources_table,
            lock_service=self.lock_service,
            data_explorer_sync=self.data_explorer_sync,
        )


class TestCreateDataset(DatasetManagerTestCase):
    @pytest.fixture(autouse=True)
    def extend_setup(self, service_setup: None) -> None:  # pylint: disable=unused-argument
        self.dataset = build_dataset()

    def test_create_dataset_successful(self) -> None:
        self.dataset_manager.create_dataset(self.dataset)

        self.datasets_table.create.assert_called_once_with(self.dataset)
        self.lock_service.release_lock.assert_called_once_with(self.lock)

    def test_create_dataset_exists_already_fails(self) -> None:
        self.datasets_table.create.side_effect = DatasetAlreadyExists("datasetId")

        with pytest.raises(ConflictError):
            self.dataset_manager.create_dataset(self.dataset)

        self.lock_service.release_lock.assert_called_once_with(self.lock)

    def test_create_locked(self) -> None:
        exception = ResourceIsLocked(build_lock(), build_lock())
        self.lock_service.acquire_lock.side_effect = exception

        with pytest.raises(type(exception)):
            self.dataset_manager.create_dataset(self.dataset)

        self.lock_service.acquire_lock.assert_called_once_with(item_id=self.dataset.id, scope=LockingScope.dataset)
        self.datasets_table.create.assert_not_called()
        self.lock_service.release_lock.assert_not_called()


class TestDeleteDataset(DatasetManagerTestCase):
    @pytest.fixture(autouse=True)
    def extend_setup(self, service_setup: None) -> None:  # pylint: disable=unused-argument
        self.dataset = build_dataset()
        self.sns_publisher = Mock(SnsPublisher)

    def test_delete_dataset(self) -> None:
        self.datasets_table.list.return_value = []

        self.dataset_manager.delete_dataset(dataset=self.dataset, sns_publisher=self.sns_publisher)

        self.datasets_table.delete.assert_called_once_with(self.dataset.id)
        self.lock_service.release_lock.assert_called_once_with(self.lock)

    def test_delete_dataset_update_lineage(self) -> None:
        random_dataset_ids = {build_dataset_id() for _ in range(3)}
        all_datasets = [
            build_dataset(
                lineage=DatasetLineage({self.dataset.id, *random_dataset_ids}),
            ),
            build_dataset(
                lineage=DatasetLineage({self.dataset.id}),
            ),
        ]
        self.datasets_table.list.return_value = all_datasets
        updated_datasets = [build_dataset() for _ in all_datasets]
        self.datasets_table.update.side_effect = updated_datasets

        self.dataset_manager.delete_dataset(
            dataset=self.dataset,
            sns_publisher=self.sns_publisher,
        )

        expected_update_calls = [
            call(
                dataset=all_datasets[0],
                lineage=DatasetLineage(random_dataset_ids),
                update_date=datetime.now(),
            ),
            call(dataset=all_datasets[1], lineage=DatasetLineage(set()), update_date=datetime.now()),
        ]

        expected_sns_publish_calls = [
            call(
                entity_type=EntityType.DATASET,
                operation=Operation.UPDATE,
                payload=updated_datasets[0],
                message_consistency=MessageConsistency.CONFIRMED,
            ),
            call(
                entity_type=EntityType.DATASET,
                operation=Operation.UPDATE,
                payload=updated_datasets[1],
                message_consistency=MessageConsistency.CONFIRMED,
            ),
        ]

        self.datasets_table.delete.assert_called_once_with(self.dataset.id)
        self.datasets_table.update.assert_has_calls(expected_update_calls)
        self.sns_publisher.publish.assert_has_calls(expected_sns_publish_calls)
        self.lock_service.release_lock.assert_called_once_with(self.lock)

    def test_delete_locked(self) -> None:
        exception = ResourceIsLocked(build_lock(), build_lock())
        self.lock_service.acquire_lock.side_effect = exception

        with pytest.raises(type(exception)):
            self.dataset_manager.delete_dataset(self.dataset, sns_publisher=self.sns_publisher)

        self.lock_service.acquire_lock.assert_called_once_with(item_id=self.dataset.id, scope=LockingScope.dataset)
        self.datasets_table.update.assert_not_called()
        self.lock_service.release_lock.assert_not_called()
        self.sns_publisher.publish.assert_not_called()


class TestUpdateDataset(DatasetManagerTestCase):
    @pytest.fixture(autouse=True)
    def extend_setup(self, service_setup: None) -> None:  # pylint: disable=unused-argument
        self.external_links = [
            ExternalLink(url=Builder.build_random_string(), name=Builder.build_random_string(), type=link_type)
            for link_type in ExternalLinkType
        ]
        self.dataset = build_dataset(external_links=self.external_links)
        self.body = build_update_dataset_body()

    def _check_update_dataset_successful(self) -> None:
        dataset = self.dataset_manager.update_dataset(old_dataset=self.dataset, body=self.body)
        assert dataset == self.updated_dataset
        self.datasets_table.update.assert_called_once_with(
            dataset=self.dataset,
            update_date=datetime.now(),
            confidentiality=self.body.confidentiality,
            contains_pii=self.body.containsPii,
            description=self.body.description,
            documentation=self.body.documentation,
            external_links=[link.to_external_link() for link in self.body.externalLinks]
            if self.body.externalLinks is not None
            else None,
            friendly_name=self.body.friendlyName,
            hub_visibility=self.body.hubVisibility,
            ingest_frequency=self.body.ingestFrequency,
            labels=None if self.body.labels is None else set(self.body.labels),
            lineage=None if self.body.upstreamLineage is None else DatasetLineage(self.body.upstreamLineage),
            preview_available=self.body.previewAvailable,
            purpose=None if self.body.purpose is None else set(self.body.purpose),
            quality_score=self.body.qualityScore,
            retention_period=self.body.retentionPeriod,
            support_group=self.body.supportGroup,
            support_level=self.body.supportLevel,
            source_identifier=self.body.sourceIdentifier,
            status=self.body.status,
            tags=self.body.tags,
        )
        assert self.lock_service.release_lock.call_count == self.lock_service.acquire_lock.call_count
        self.lock_service.release_lock.assert_called_with(self.lock)

    def test_update_no_field(self) -> None:
        self.updated_dataset = self.dataset
        self.datasets_table.update.return_value = self.dataset

        self._check_update_dataset_successful()

    def test_update_all_simple_dataset_fields(self) -> None:
        updates = {
            "confidentiality": build_confidentiality(),
            "contains_pii": not self.dataset.contains_pii,
            "description": Builder.build_random_string(),
            "documentation": Builder.build_random_string(length=1000),
            "external_links": [build_external_link() for _ in range(3)],
            "friendly_name": Builder.build_random_string(),
            "hub_visibility": {build_hub() for _ in range(3)},
            "ingest_frequency": build_ingest_frequency(),
            "labels": {Builder.build_random_string()},
            "purpose": {build_dataset_purpose()},
            "retention_period": build_retention_period(),
            "source_identifier": Builder.build_random_string(),
            "status": random.choice([status for status in DatasetStatus if status is not DatasetStatus.RELEASED]),
            "support_group": "foo:bar",
            "support_level": build_support_level(),
            "tags": build_dataset_tags(),
            "quality_score": random.randint(0, 100),
        }
        self.updated_dataset = replace(self.dataset, **updates, update_date=datetime.now())
        self.body = build_update_dataset_body(
            confidentiality=cast(Confidentiality, updates["confidentiality"]),
            contains_pii=cast(bool, updates["contains_pii"]),
            description=cast(str, updates["description"]),
            documentation=cast(str, updates["documentation"]),
            external_links=cast(List[ExternalLink], updates["external_links"]),
            friendly_name=cast(str, updates["friendly_name"]),
            hub_visibility=cast(Set[Hub], updates["hub_visibility"]),
            ingest_frequency=cast(IngestFrequency, updates["ingest_frequency"]),
            labels=cast(Set[str], updates["labels"]),
            purpose=cast(Set[DatasetPurpose], updates["purpose"]),
            retention_period=cast(RetentionPeriod, updates["retention_period"]),
            source_identifier=cast(DeletableSourceIdentifier, updates["source_identifier"]),
            status=cast(DatasetStatus, updates["status"]),
            support_group=cast(DeletableSupportGroup, updates["support_group"]),
            support_level=cast(SupportLevel, updates["support_level"]),
            tags=cast(DatasetTags, updates["tags"]),
            quality_score=cast(int, updates["quality_score"]),
        )
        self.datasets_table.update.return_value = self.updated_dataset

        self._check_update_dataset_successful()
        self.data_explorer_sync.update_bucket_access_for_data_explorer.assert_not_called()
        self.data_explorer_sync.update_lake_formation_access_for_data_explorer.assert_not_called()

    @pytest.mark.parametrize("attribute", ["documentation", "source_identifier", "support_group"])
    def test_reset_optional_string_attribute(self, attribute: str) -> None:
        self.body = build_update_dataset_body(**{attribute: ""})  # type: ignore
        self.updated_dataset = replace(self.dataset, **{attribute: None})
        self.datasets_table.update.return_value = self.updated_dataset

        self._check_update_dataset_successful()

    def test_update_locked_dataset_fails(self) -> None:
        exception = ResourceIsLocked(build_lock(), build_lock())
        self.lock_service.acquire_lock.side_effect = exception

        with pytest.raises(type(exception)):
            self.dataset_manager.update_dataset(old_dataset=self.dataset, body=self.body)

        self.lock_service.acquire_lock.assert_called_once_with(item_id=self.dataset.id, scope=LockingScope.dataset)
        self.datasets_table.update.assert_not_called()
        self.lock_service.release_lock.assert_not_called()

    def test_update_preview_available(self) -> None:
        n_resources = 3
        new_preview_available = not self.dataset.preview_available
        s3_locks = [Mock(Lock) for _ in range(len(Stage) * len(self.dataset.hub.regions))]
        locks = [self.lock] + s3_locks
        self.lock_service.acquire_lock.side_effect = locks
        self.updated_dataset = replace(self.dataset, preview_available=new_preview_available)
        self.body = build_update_dataset_body(preview_available=new_preview_available)
        self.datasets_table.update.return_value = self.updated_dataset
        buckets = [
            build_s3_resource(dataset=self.dataset, resource_account_id=self.get_random_resource_account().id)
            for _ in range(n_resources)
        ]
        glue_databases = [
            build_glue_database(account_id=self.get_random_resource_account().id) for _ in range(n_resources)
        ]
        glue_syncs = [
            build_glue_sync_resource(
                dataset=self.dataset,
                sync_type=SyncType.lake_formation,
                database_name=database.name,
                resource_account_id=database.account_id,
                region=database.region,
            )
            for database in glue_databases
        ]
        self.resources_table.list_s3.return_value = buckets
        self.resources_table.list_glue_sync.return_value = glue_syncs

        self._check_update_dataset_successful()

        self.lock_service.acquire_lock.assert_has_calls(
            calls=[
                call(
                    item_id=self.dataset.id,
                    scope=LockingScope.s3_resource,
                    region=region,
                    stage=stage,
                    data={"datasetId": self.dataset.id},
                )
                for stage in Stage
                for region in self.dataset.hub.regions
            ]
        )
        self.data_explorer_sync.update_bucket_access_for_data_explorer.assert_has_calls(
            calls=[call(dataset=self.updated_dataset, bucket=bucket) for bucket in buckets], any_order=True
        )
        self.data_explorer_sync.update_lake_formation_access_for_data_explorer.assert_has_calls(
            [call(dataset=self.updated_dataset, glue_db=database) for database in glue_databases], any_order=True
        )
        self.lock_service.release_lock.assert_has_calls(calls=[call(lock) for lock in s3_locks])

    def get_random_resource_account(self) -> ResourceAccount:
        return Builder.get_random_element(self.config.account_store.query_resource_accounts(self.config.environment))

    @pytest.mark.parametrize("change_pii", [True, False])
    def test_all_locks_released_if_dataset_not_found(self, change_pii: bool) -> None:
        if change_pii:
            new_preview_available = not self.dataset.preview_available
            self.body = build_update_dataset_body(preview_available=new_preview_available)
        self.datasets_table.update.side_effect = DatasetNotFound(self.dataset.id)

        with pytest.raises(NotFoundError):
            self.dataset_manager.update_dataset(old_dataset=self.dataset, body=self.body)

        assert self.lock_service.release_lock.call_count == self.lock_service.acquire_lock.call_count
