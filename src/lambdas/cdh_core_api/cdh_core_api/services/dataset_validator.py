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
from typing import Collection
from typing import List
from typing import Optional
from typing import Set

from cdh_core_api.bodies.datasets import get_external_links
from cdh_core_api.bodies.datasets import NewDatasetBody
from cdh_core_api.bodies.datasets import UpdateDatasetBody
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.utils import fetch_dataset
from cdh_core_api.services.utils import get_user
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from marshmallow import ValidationError

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import DatasetLineage
from cdh_core.entities.request import RequesterIdentity
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.hubs import Hub
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.iterables import unwrap_singleton


class DatasetValidator:
    """Validator for datasets."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        authorization_api: AuthorizationApi,
        authorizer: Authorizer[GenericAccount],
        config: Config,
        requester_identity: RequesterIdentity,
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
    ):
        self.authorization_api = authorization_api
        self.authorizer = authorizer
        self.config = config
        self.requester_identity = requester_identity
        self.resources_table = resources_table
        self.visible_data_loader = visible_data_loader

    def validate_new_dataset_body(self, body: NewDatasetBody, hub: Hub) -> Dataset:
        """Check whether a new account body is valid and return the corresponding dataset object.

        Check that the requester is allowed to create the dataset, and that all datasets given in the lineage
        exist and are visible to them.
        """
        owner_account_id = body.ownerAccountId or self.requester_identity.account_id
        preview_available = (
            body.previewAvailable
            if body.previewAvailable is not None
            else not (body.containsPii or body.confidentiality is Confidentiality.secret)
        )
        self.authorizer.check_requester_may_create_dataset(
            hub=hub, business_object=body.businessObject, layer=body.layer, owner_account_id=owner_account_id
        )

        self._validate_new_lineage(upstream_lineage=body.upstreamLineage)

        dt_now = datetime.now()
        return Dataset(
            id=Dataset.build_id(body.businessObject, body.name, body.layer, hub),
            business_object=body.businessObject,
            hub=hub,
            confidentiality=body.confidentiality,
            contains_pii=body.containsPii,
            creator_user_id=get_user(
                requester_identity=self.requester_identity, config=self.config, authorization_api=self.authorization_api
            ),
            creation_date=dt_now,
            description=body.description,
            documentation=body.documentation,
            external_links=get_external_links(body) or [],
            friendly_name=body.friendlyName,
            hub_visibility=body.hubVisibility or set(),
            ingest_frequency=body.ingestFrequency,
            labels=set() if body.labels is None else set(body.labels),
            layer=body.layer,
            lineage=DatasetLineage(upstream=body.upstreamLineage or set()),
            name=body.name,
            owner_account_id=owner_account_id,
            permissions=frozenset(),
            preview_available=preview_available,
            purpose=set() if body.purpose is None else set(body.purpose),
            quality_score=None,
            retention_period=body.retentionPeriod,
            source_identifier=body.sourceIdentifier,
            status=DatasetStatus.initial_value(),
            support_group=body.supportGroup,
            support_level=body.supportLevel,
            tags=body.tags,
            update_date=dt_now,
        )

    def _validate_new_lineage(self, upstream_lineage: Optional[Set[DatasetId]]) -> None:
        visible_datasets = self._get_relevant_datasets(upstream_lineage or set())
        self._validate_lineage(upstream_lineage, visible_datasets)

    def _get_relevant_datasets(self, relevant_dataset_ids: Set[DatasetId]) -> List[Dataset]:
        return self.visible_data_loader.get_datasets_cross_hub(list(relevant_dataset_ids))

    def _validate_lineage(self, lineage: Optional[Set[DatasetId]], existing_datasets: Collection[Dataset]) -> None:
        if lineage is None:
            return
        for dataset_id in lineage:
            self._assert_dataset_exists(dataset_id, existing_datasets)

    @staticmethod
    def _assert_dataset_exists(dataset_id: str, existing_datasets: Collection[Dataset]) -> None:
        if dataset_id in {dataset.id for dataset in existing_datasets}:
            return
        raise ValidationError(f"Dataset {dataset_id} does not exist")

    def validate_deletion(self, dataset_id: DatasetId, hub: Hub) -> Dataset:
        """Check if the dataset with the given id in the given hub can be deleted.

        Check that the requester is authorized to delete the dataset, and that there are no resources attached to it.
        """
        dataset = fetch_dataset(hub=hub, dataset_id=dataset_id, visible_data_loader=self.visible_data_loader)
        self.authorizer.check_requester_may_delete_dataset(dataset=dataset)
        if resources := self.resources_table.list(dataset_id=dataset.id):
            raise ForbiddenError(
                f"Deletion of dataset {dataset.id} is not possible as there is at least one resource. "
                "They can be deleted via a DELETE request at "
                + str(
                    [
                        f"/{res.hub}/resources/{res.type.value}/{res.dataset_id}/{res.stage.value}/{res.region.value}"
                        for res in resources
                    ]
                )
                + "."
            )
        return dataset

    def validate_update_dataset_body(self, dataset_id: DatasetId, body: UpdateDatasetBody, hub: Hub) -> Dataset:
        """Check whether the given update body is valid for the given dataset in the given hub.

        Check the dataset is visible to the requester, and that they are allowed to update it. Additionally, if the
        lineage is updated, check that all datasets in the new lineage are visible to the requester, and if the status
        is changed to RELEASED, check that they may release the dataset.
        """
        old_dataset = self._validate_lineage_and_get_old_dataset(
            upstream_lineage=body.upstreamLineage, dataset_id=dataset_id
        )
        if old_dataset.hub is not hub:
            raise NotFoundError(
                f"Dataset with ID {dataset_id} belongs to hub {old_dataset.hub.value!r}, not {hub.value!r}"
            )
        self.authorizer.check_requester_may_update_dataset(dataset=old_dataset)
        if body.status != old_dataset.status and body.status is DatasetStatus.RELEASED:
            self.authorizer.check_requester_may_release_dataset()

        return old_dataset

    def _validate_lineage_and_get_old_dataset(
        self, upstream_lineage: Optional[Set[DatasetId]], dataset_id: DatasetId
    ) -> Dataset:
        relevant_dataset_ids = upstream_lineage or set()
        relevant_datasets = self._get_relevant_datasets(relevant_dataset_ids.union({dataset_id}))
        self._validate_lineage(upstream_lineage, existing_datasets=relevant_datasets)
        try:
            return unwrap_singleton(dataset for dataset in relevant_datasets if dataset.id == dataset_id)
        except ValueError as error:
            raise NotFoundError(f"Dataset with ID {dataset_id} not found") from error
