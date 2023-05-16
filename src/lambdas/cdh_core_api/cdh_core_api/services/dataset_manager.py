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
from typing import List

from cdh_core_api.bodies.datasets import get_external_links
from cdh_core_api.bodies.datasets import UpdateDatasetBody
from cdh_core_api.catalog.datasets_table import DatasetAlreadyExists
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import DatasetLineage
from cdh_core.entities.lock import Lock
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import NotFoundError


class DatasetManager:
    """Handles datasets."""

    def __init__(
        self,
        datasets_table: DatasetsTable,
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        lock_service: LockService,
        data_explorer_sync: DataExplorerSync,
    ):
        self.datasets_table = datasets_table
        self.resources_table = resources_table
        self.lock_service = lock_service
        self.data_explorer_sync = data_explorer_sync

    def create_dataset(self, dataset: Dataset) -> None:
        """Create the given dataset.

        Lock the dataset, add the given dataset to the dynamo, then release the lock.
        """
        lock = self.lock_service.acquire_lock(item_id=dataset.id, scope=LockingScope.dataset)
        try:
            self.datasets_table.create(dataset)
        except DatasetAlreadyExists as error:
            raise ConflictError(error) from error
        finally:
            self.lock_service.release_lock(lock)

    def delete_dataset(self, dataset: Dataset, sns_publisher: SnsPublisher) -> None:
        """Delete the given dataset.

        Lock the dataset, remove it from the dynamo, the release the lock. Afterwards, remove it from the lineage of
        all other datasets.
        """
        lock = self.lock_service.acquire_lock(item_id=dataset.id, scope=LockingScope.dataset)
        try:
            self.datasets_table.delete(dataset.id)
        except DatasetNotFound as error:
            raise NotFoundError(error) from error
        finally:
            self.lock_service.release_lock(lock)

        self._remove_dataset_id_from_lineage(
            dataset_id=dataset.id,
            sns_publisher=sns_publisher,
        )

    def _remove_dataset_id_from_lineage(self, dataset_id: DatasetId, sns_publisher: SnsPublisher) -> None:
        for dataset in self.datasets_table.list():
            updated_lineage = {
                other_dataset_id for other_dataset_id in dataset.lineage.upstream if dataset_id != other_dataset_id
            }

            if len(updated_lineage) != len(dataset.lineage.upstream):
                updated_dataset = self.datasets_table.update(
                    dataset=dataset, lineage=DatasetLineage(upstream=updated_lineage), update_date=datetime.now()
                )
                sns_publisher.publish(
                    entity_type=EntityType.DATASET,
                    operation=Operation.UPDATE,
                    payload=updated_dataset,
                    message_consistency=MessageConsistency.CONFIRMED,
                )

    def update_dataset(self, old_dataset: Dataset, body: UpdateDatasetBody) -> Dataset:
        """Update the given dataset with the changes in the given body.

        Lock the dataset, update the dynamo, then release the lock.
        """
        lock = self.lock_service.acquire_lock(item_id=old_dataset.id, scope=LockingScope.dataset)
        if preview_available_change := body.previewAvailable not in {None, old_dataset.preview_available}:
            s3_locks = self._lock_all_potential_s3_resources(old_dataset)

        try:
            updated_dataset = self.datasets_table.update(
                dataset=old_dataset,
                update_date=datetime.now(),
                confidentiality=body.confidentiality,
                contains_pii=body.containsPii,
                description=body.description,
                documentation=body.documentation,
                external_links=get_external_links(body),
                friendly_name=body.friendlyName,
                hub_visibility=body.hubVisibility,
                ingest_frequency=body.ingestFrequency,
                labels=None if body.labels is None else set(body.labels),
                lineage=None if body.upstreamLineage is None else DatasetLineage(upstream=body.upstreamLineage),
                preview_available=body.previewAvailable,
                purpose=None if body.purpose is None else set(body.purpose),
                retention_period=body.retentionPeriod,
                source_identifier=body.sourceIdentifier,
                status=body.status,
                support_group=body.supportGroup,
                support_level=body.supportLevel,
                tags=body.tags,
                quality_score=body.qualityScore,
            )
        except DatasetNotFound as error:
            self.lock_service.release_lock(lock)
            if preview_available_change:
                self._release_all_s3_locks(s3_locks)
            raise NotFoundError(error) from error

        if preview_available_change:
            for bucket in self.resources_table.list_s3(dataset_id=updated_dataset.id):
                self.data_explorer_sync.update_bucket_access_for_data_explorer(dataset=updated_dataset, bucket=bucket)

            self._release_all_s3_locks(s3_locks)

            for glue_sync in self.resources_table.list_glue_sync(dataset_id=updated_dataset.id):
                if glue_sync.sync_type == SyncType.lake_formation:
                    self.data_explorer_sync.update_lake_formation_access_for_data_explorer(
                        dataset=updated_dataset, glue_db=glue_sync.glue_database
                    )

        self.lock_service.release_lock(lock)

        return updated_dataset

    def _lock_all_potential_s3_resources(self, dataset: Dataset) -> List[Lock]:
        s3_locks = []
        for stage in Stage:
            for region in dataset.hub.regions:
                s3_locks.append(
                    self.lock_service.acquire_lock(
                        item_id=dataset.id,
                        scope=LockingScope.s3_resource,
                        region=region,
                        stage=stage,
                        data={"datasetId": dataset.id},
                    )
                )

        return s3_locks

    def _release_all_s3_locks(self, s3_locks: List[Lock]) -> None:
        for s3_lock in s3_locks:
            self.lock_service.release_lock(s3_lock)
