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
from __future__ import annotations

from contextlib import contextmanager
from contextlib import suppress
from dataclasses import replace
from datetime import datetime
from logging import getLogger
from typing import Any
from typing import cast
from typing import Dict
from typing import FrozenSet
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set

from cdh_core_api.catalog.base import BaseTable
from cdh_core_api.catalog.base import conditional_check_failed
from cdh_core_api.catalog.base import create_model
from cdh_core_api.catalog.base import DateTimeAttribute
from cdh_core_api.catalog.base import DynamoItemIterator
from cdh_core_api.catalog.base import LastEvaluatedKey
from cdh_core_api.catalog.base import LazyEnumAttribute
from pynamodb.attributes import BooleanAttribute
from pynamodb.attributes import ListAttribute
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.attributes import UnicodeSetAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.exceptions import PutError
from pynamodb.exceptions import UpdateError
from pynamodb.expressions.condition import Comparison
from pynamodb.expressions.update import Action
from pynamodb.models import Model
from pynamodb_attributes import IntegerAttribute
from pynamodb_attributes import UnicodeEnumAttribute

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetAccountPermissionAction
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import DatasetLineage
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset import SourceIdentifier
from cdh_core.entities.dataset import SupportGroup
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetPurpose
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.dataset_properties import ExternalLinkType
from cdh_core.enums.dataset_properties import IngestFrequency
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties import RetentionPeriod
from cdh_core.enums.dataset_properties import SupportLevel
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.optionals import apply_if_not_none
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)


class _DatasetAccountPermissionAttribute(MapAttribute[str, Any]):
    account_id = UnicodeAttribute()
    region = LazyEnumAttribute[Region](lambda: Region)
    stage = UnicodeEnumAttribute(Stage)
    sync_type = UnicodeEnumAttribute(SyncType)

    @classmethod
    def from_dataset_account_permissions(
        cls, dataset_account_permissions: FrozenSet[DatasetAccountPermission]
    ) -> List[_DatasetAccountPermissionAttribute]:
        return [
            _DatasetAccountPermissionAttribute(
                account_id=p.account_id, region=p.region, stage=p.stage, sync_type=p.sync_type
            )  # type: ignore[no-untyped-call]
            for p in sorted(dataset_account_permissions, key=str)
        ]


class _ExternalLinkAttribute(MapAttribute[str, Any]):
    url = UnicodeAttribute()
    name = UnicodeAttribute(null=True)
    type = LazyEnumAttribute[ExternalLinkType](lambda: ExternalLinkType)

    @classmethod
    def from_external_links(cls, external_links: List[ExternalLink]) -> List[_ExternalLinkAttribute]:
        return [
            _ExternalLinkAttribute(url=link.url, name=link.name, type=link.type)  # type: ignore[no-untyped-call]
            for link in external_links
        ]


class _DatasetLineageAttribute(MapAttribute[str, Any]):
    upstream = UnicodeSetAttribute()

    @classmethod
    def from_lineage(cls, lineage: DatasetLineage) -> _DatasetLineageAttribute:
        return _DatasetLineageAttribute(upstream=lineage.upstream)  # type: ignore[no-untyped-call]

    @property
    def lineage(self) -> DatasetLineage:
        return DatasetLineage(upstream=cast(Set[DatasetId], self.upstream or set()))


class _DatasetModel(Model):
    id = UnicodeAttribute(hash_key=True)
    business_object = LazyEnumAttribute[BusinessObject](lambda: BusinessObject)
    hub = LazyEnumAttribute[Hub](lambda: Hub)
    confidentiality = UnicodeEnumAttribute(Confidentiality)
    contains_pii = BooleanAttribute()
    creator_user_id = UnicodeAttribute(null=True)
    creation_date = DateTimeAttribute()
    description = UnicodeAttribute()
    documentation = UnicodeAttribute(null=True)
    external_links: ListAttribute[_ExternalLinkAttribute] = ListAttribute(of=_ExternalLinkAttribute)
    friendly_name = UnicodeAttribute()
    hub_visibility = UnicodeSetAttribute()
    ingest_frequency = UnicodeEnumAttribute(IngestFrequency, null=True)
    labels = UnicodeSetAttribute()
    layer = UnicodeEnumAttribute(Layer)
    lineage = _DatasetLineageAttribute()  # type: ignore[no-untyped-call]
    name = UnicodeAttribute()
    permissions: ListAttribute[_DatasetAccountPermissionAttribute] = ListAttribute(
        of=_DatasetAccountPermissionAttribute
    )
    preview_available = BooleanAttribute()
    quality_score = IntegerAttribute(null=True)
    retention_period = UnicodeEnumAttribute(RetentionPeriod, null=True)
    source_identifier = UnicodeAttribute(null=True)
    support_level = UnicodeEnumAttribute(SupportLevel, null=True)
    tags: MapAttribute[str, Any] = MapAttribute()  # type: ignore[no-untyped-call]
    update_date = DateTimeAttribute()
    owner_account_id = UnicodeAttribute()
    support_group = UnicodeAttribute(null=True)
    status = LazyEnumAttribute[DatasetStatus](lambda: DatasetStatus)
    purpose = UnicodeSetAttribute()

    def dataset(self) -> Dataset:
        """Create a dataset from the model."""
        purpose: Set[DatasetPurpose] = set()
        if self.purpose is not None:
            purpose = {DatasetPurpose(purpose) for purpose in self.purpose}
        hub_visibility: Set[Hub] = set()
        if self.hub_visibility is not None:
            hub_visibility = {Hub(hub) for hub in self.hub_visibility}
        return Dataset(
            id=DatasetId(self.id),
            business_object=self.business_object,
            hub=Hub(self.hub),
            confidentiality=self.confidentiality,
            contains_pii=self.contains_pii,
            creator_user_id=self.creator_user_id,
            creation_date=self.creation_date,
            description=self.description,
            documentation=self.documentation,
            external_links=[ExternalLink(url=link.url, name=link.name, type=link.type) for link in self.external_links],
            friendly_name=self.friendly_name,
            hub_visibility=hub_visibility,
            ingest_frequency=self.ingest_frequency if self.ingest_frequency else IngestFrequency.undefined,
            labels=self.labels or set(),
            layer=self.layer,
            lineage=self.lineage.lineage,
            name=self.name,
            permissions=frozenset(
                {
                    DatasetAccountPermission(
                        account_id=AccountId(p.account_id), stage=p.stage, region=p.region, sync_type=p.sync_type
                    )
                    for p in self.permissions
                }
            ),
            preview_available=self.preview_available,
            retention_period=self.retention_period if self.retention_period else RetentionPeriod.undefined,
            source_identifier=SourceIdentifier(self.source_identifier),
            support_level=self.support_level if self.support_level else SupportLevel.undefined,
            tags=self.tags.as_dict(),  # type: ignore[no-untyped-call]
            update_date=self.update_date,
            owner_account_id=AccountId(self.owner_account_id),
            support_group=SupportGroup(self.support_group),
            status=self.status,
            purpose=purpose,
            quality_score=self.quality_score,
        )

    @classmethod
    def from_dataset(cls, dataset: Dataset) -> "_DatasetModel":
        """Create a model based on a dataset object."""
        return cls(
            id=dataset.id,
            business_object=dataset.business_object,
            hub=dataset.hub,
            confidentiality=dataset.confidentiality,
            contains_pii=dataset.contains_pii,
            creator_user_id=dataset.creator_user_id,
            creation_date=dataset.creation_date,
            description=dataset.description,
            documentation=dataset.documentation,
            external_links=_ExternalLinkAttribute.from_external_links(dataset.external_links),
            friendly_name=dataset.friendly_name,
            hub_visibility={hub.value for hub in dataset.hub_visibility},
            ingest_frequency=dataset.ingest_frequency,
            labels=dataset.labels,
            layer=dataset.layer,
            lineage=_DatasetLineageAttribute.from_lineage(dataset.lineage),
            name=dataset.name,
            permissions=_DatasetAccountPermissionAttribute.from_dataset_account_permissions(dataset.permissions),
            preview_available=dataset.preview_available,
            retention_period=dataset.retention_period,
            source_identifier=dataset.source_identifier,
            support_level=dataset.support_level,
            tags=dataset.tags,
            update_date=dataset.update_date,
            owner_account_id=dataset.owner_account_id,
            support_group=dataset.support_group,
            status=dataset.status,
            purpose={purpose.value for purpose in dataset.purpose},
            quality_score=dataset.quality_score,
        )


# pylint: disable=no-member
class DatasetsTable(BaseTable):
    """Represents the DynamoDB table for datasets."""

    def __init__(self, prefix: str = ""):
        self._model = create_model(table=f"{prefix}cdh-datasets", model=_DatasetModel, module=__name__)

    def get(self, dataset_id: str) -> Dataset:
        """Return a single dataset."""
        return self._get(dataset_id).dataset()

    def _get(self, dataset_id: str) -> _DatasetModel:
        try:
            return self._model.get(dataset_id, consistent_read=True)
        except DoesNotExist as error:
            raise DatasetNotFound(dataset_id) from error

    def list(
        self, hub: Optional[Hub] = None, owner: Optional[AccountId] = None, consistent_read: bool = True
    ) -> List[Dataset]:
        """Return a list of all matching datasets."""
        return list(self.get_datasets_iterator(hub=hub, owner=owner, consistent_read=consistent_read))

    def get_datasets_iterator(
        self,
        hub: Optional[Hub] = None,
        owner: Optional[AccountId] = None,
        consistent_read: bool = True,
        last_evaluated_key: Optional[LastEvaluatedKey] = None,
    ) -> DynamoItemIterator[Dataset]:
        """Get an iterator over all matching datasets."""
        filter_expression: Optional[Comparison] = None
        if hub:
            filter_expression &= _DatasetModel.hub == hub
        if owner:
            filter_expression &= _DatasetModel.owner_account_id == owner
        result_iterator = self._model.scan(
            consistent_read=consistent_read,
            filter_condition=filter_expression,
            last_evaluated_key=last_evaluated_key,
        )
        return DynamoItemIterator(
            items=(model.dataset() for model in result_iterator),
            get_last_evaluated_key=lambda: apply_if_not_none(LastEvaluatedKey)(result_iterator.last_evaluated_key),
        )

    def create(self, dataset: Dataset) -> None:
        """Create a dataset."""
        try:
            self._model.from_dataset(dataset).save(_DatasetModel.id.does_not_exist())
        except PutError as error:
            if conditional_check_failed(error):
                raise DatasetAlreadyExists(dataset.id) from error
            raise error

    def delete(self, dataset_id: str) -> None:
        """Delete a dataset."""
        try:
            self._model.get(dataset_id, consistent_read=True).delete()
        except DoesNotExist as error:
            raise DatasetNotFound(dataset_id) from error

    def exists(self, dataset_id: str) -> bool:
        """Return True if the dataset exists."""
        try:
            self.get(dataset_id)
        except DatasetNotFound:
            return False
        return True

    def update(  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
        self,
        dataset: Dataset,
        update_date: datetime,
        business_object: Optional[BusinessObject] = None,
        confidentiality: Optional[Confidentiality] = None,
        contains_pii: Optional[bool] = None,
        description: Optional[str] = None,
        documentation: Optional[str] = None,
        external_links: Optional[List[ExternalLink]] = None,
        friendly_name: Optional[str] = None,
        hub_visibility: Optional[Set[Hub]] = None,
        ingest_frequency: Optional[IngestFrequency] = None,
        labels: Optional[Set[str]] = None,
        lineage: Optional[DatasetLineage] = None,
        preview_available: Optional[bool] = None,
        purpose: Optional[Set[DatasetPurpose]] = None,
        retention_period: Optional[RetentionPeriod] = None,
        source_identifier: Optional[SourceIdentifier] = None,
        status: Optional[DatasetStatus] = None,
        support_group: Optional[SupportGroup] = None,
        support_level: Optional[SupportLevel] = None,
        tags: Optional[Dict[str, str]] = None,
        quality_score: Optional[int] = None,
    ) -> Dataset:
        """Update dataset properties."""
        updated_dataset = dataset
        actions: List[Action] = []
        if business_object:
            actions.append(_DatasetModel.business_object.set(business_object))
            updated_dataset = replace(updated_dataset, business_object=business_object)
        if confidentiality:
            actions.append(_DatasetModel.confidentiality.set(confidentiality))
            updated_dataset = replace(updated_dataset, confidentiality=confidentiality)
        if contains_pii is not None:
            actions.append(_DatasetModel.contains_pii.set(contains_pii))
            updated_dataset = replace(updated_dataset, contains_pii=contains_pii)
        if description is not None:
            actions.append(_DatasetModel.description.set(description))
            updated_dataset = replace(updated_dataset, description=description)
        if documentation is not None:
            if documentation == "":
                actions.append(_DatasetModel.documentation.remove())
            else:
                actions.append(_DatasetModel.documentation.set(documentation))
            updated_dataset = replace(updated_dataset, documentation=documentation or None)
        if external_links is not None:
            actions.append(_DatasetModel.external_links.set(_ExternalLinkAttribute.from_external_links(external_links)))
            updated_dataset = replace(updated_dataset, external_links=external_links)
        if friendly_name is not None:
            actions.append(_DatasetModel.friendly_name.set(friendly_name))
            updated_dataset = replace(updated_dataset, friendly_name=friendly_name)
        if hub_visibility is not None:
            if len(hub_visibility) > 0:
                actions.append(_DatasetModel.hub_visibility.set({hub.value for hub in hub_visibility}))
            else:
                actions.append(_DatasetModel.hub_visibility.remove())
            updated_dataset = replace(updated_dataset, hub_visibility=hub_visibility)
        if ingest_frequency:
            actions.append(_DatasetModel.ingest_frequency.set(ingest_frequency))
            updated_dataset = replace(updated_dataset, ingest_frequency=ingest_frequency)
        if labels is not None:
            if len(labels) > 0:
                actions.append(_DatasetModel.labels.set(labels))
            else:
                actions.append(_DatasetModel.labels.remove())
            updated_dataset = replace(updated_dataset, labels=labels)
        if lineage is not None:
            actions.append(_DatasetModel.lineage.set(_DatasetLineageAttribute.from_lineage(lineage)))
            updated_dataset = replace(updated_dataset, lineage=lineage)
        if preview_available is not None:
            actions.append(_DatasetModel.preview_available.set(preview_available))
            updated_dataset = replace(updated_dataset, preview_available=preview_available)
        if purpose is not None:
            if len(purpose) > 0:
                actions.append(_DatasetModel.purpose.set({purpose.value for purpose in purpose}))
            else:
                actions.append(_DatasetModel.purpose.remove())
            updated_dataset = replace(updated_dataset, purpose=purpose)
        if retention_period:
            actions.append(_DatasetModel.retention_period.set(retention_period))
            updated_dataset = replace(updated_dataset, retention_period=retention_period)
        if source_identifier is not None:
            if source_identifier == "":
                actions.append(_DatasetModel.source_identifier.remove())
            else:
                actions.append(_DatasetModel.source_identifier.set(source_identifier))
            updated_dataset = replace(updated_dataset, source_identifier=source_identifier or None)
        if status:
            actions.append(_DatasetModel.status.set(status))
            updated_dataset = replace(updated_dataset, status=status)
        if support_group is not None:
            if support_group == "":
                actions.append(_DatasetModel.support_group.remove())
            else:
                actions.append(_DatasetModel.support_group.set(support_group))
            updated_dataset = replace(updated_dataset, support_group=support_group or None)
        if support_level:
            actions.append(_DatasetModel.support_level.set(support_level))
            updated_dataset = replace(updated_dataset, support_level=support_level)
        if tags is not None:
            actions.append(_DatasetModel.tags.set(tags))
            updated_dataset = replace(updated_dataset, tags=tags)
        if quality_score is not None:
            actions.append(_DatasetModel.quality_score.set(quality_score))
            updated_dataset = replace(updated_dataset, quality_score=quality_score)

        if actions:
            actions.append(_DatasetModel.update_date.set(update_date))
            updated_dataset = replace(updated_dataset, update_date=update_date)

            try:
                self._model.from_dataset(dataset).update(
                    actions=actions,
                    condition=(_DatasetModel.id.exists()),
                )
            except UpdateError as error:
                if conditional_check_failed(error):
                    raise DatasetNotFound(dataset.id) from error
                raise

        return updated_dataset

    @contextmanager
    def update_permissions_transaction(
        self,
        dataset_id: str,
        permission: DatasetAccountPermission,
        action: DatasetAccountPermissionAction,
    ) -> Iterator[Dataset]:
        """Update the dataset permissions in a transactional manner."""
        dataset = self._update_permissions(dataset_id=dataset_id, permission=permission, action=action)
        try:
            yield dataset
        except:  # noqa: E722 (bare-except)
            with suppress(Exception):
                self.rollback_permissions_action(
                    dataset_id=dataset_id, permission=permission, action_to_rollback=action
                )
            raise

    def rollback_permissions_action(
        self, dataset_id: str, permission: DatasetAccountPermission, action_to_rollback: DatasetAccountPermissionAction
    ) -> None:
        """Revert the dataset permission action."""
        try:
            self._update_permissions(dataset_id=dataset_id, permission=permission, action=action_to_rollback.inverse)
        except:  # noqa: E722 (bare-except)
            LOG.exception(f"Could not roll back permissions for dataset {dataset_id} ")
            raise

    def _update_permissions(
        self,
        dataset_id: str,
        permission: DatasetAccountPermission,
        action: DatasetAccountPermissionAction,
    ) -> Dataset:
        dataset_model = self._get(dataset_id)
        new_permissions = DatasetsTable._resolve_new_permissions(
            current_permissions=dataset_model.dataset().permissions, permission=permission, action=action
        )
        return self._set_permissions(dataset_id, new_permissions)

    @staticmethod
    def _resolve_new_permissions(
        current_permissions: FrozenSet[DatasetAccountPermission],
        permission: DatasetAccountPermission,
        action: DatasetAccountPermissionAction,
    ) -> FrozenSet[DatasetAccountPermission]:
        if action is DatasetAccountPermissionAction.remove:
            new_permissions = current_permissions - {permission}
        elif action is DatasetAccountPermissionAction.add:
            new_permissions = current_permissions | {permission}
        else:
            raise ValueError(f"DatasetAccountPermissionAction {action.value!r} cannot be applied to datasets")
        return frozenset(new_permissions)

    def _set_permissions(self, dataset_id: str, new_permissions: FrozenSet[DatasetAccountPermission]) -> Dataset:
        dataset_model = self._get(dataset_id)
        try:
            dataset_model.update(
                actions=[
                    _DatasetModel.permissions.set(
                        _DatasetAccountPermissionAttribute.from_dataset_account_permissions(new_permissions)
                    )
                ],
                condition=_DatasetModel.permissions == dataset_model.permissions,
            )
        except UpdateError as error:
            if conditional_check_failed(error):
                raise DatasetUpdateInconsistent(dataset_id) from error
            raise error
        return dataset_model.dataset()

    def batch_get(self, dataset_ids: List[DatasetId]) -> List[Dataset]:
        """Request a list of datasets via batch request."""
        return [model.dataset() for model in self._model.batch_get(items=dataset_ids)]


class DatasetNotFound(Exception):
    """Signals that the requested dataset cannot be found."""

    def __init__(self, dataset_id: str):
        super().__init__(f"Dataset {dataset_id} was not found")


class DatasetAlreadyExists(Exception):
    """Signals that a dataset is already present."""

    def __init__(self, dataset_id: str):
        super().__init__(f"Dataset {dataset_id} already exists")


class DatasetUpdateInconsistent(Exception):
    """Signals that the database is in an inconsistent state."""

    def __init__(self, dataset_id: str):
        super().__init__(f"Inconsistent state during update of dataset {dataset_id}")
