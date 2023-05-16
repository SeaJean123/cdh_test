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
# pylint: disable=invalid-name
from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Set
from typing import Union

from cdh_core_api.api.validation import field
from cdh_core_api.validation.base import owner_account_id_field
from cdh_core_api.validation.datasets import dataset_labels_field
from cdh_core_api.validation.datasets import DeletableSourceIdentifier
from cdh_core_api.validation.datasets import DeletableSupportGroup
from cdh_core_api.validation.datasets import validate_dataset_description
from cdh_core_api.validation.datasets import validate_dataset_documentation
from cdh_core_api.validation.datasets import validate_dataset_friendly_name
from cdh_core_api.validation.datasets import validate_dataset_lineage
from cdh_core_api.validation.datasets import validate_dataset_name
from marshmallow import fields
from marshmallow.validate import Range

from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import DatasetTags
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset import SourceIdentifier
from cdh_core.entities.dataset import SupportGroup
from cdh_core.entities.dataset_participants import DatasetParticipantId
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
from cdh_core.primitives.account_id import AccountId


@dataclass(frozen=True)
class DatasetParticipantBodyPart:
    """Represents the attributes needed to build a dataset participant."""

    idp: str
    id: DatasetParticipantId


@dataclass(frozen=True)
class ExternalLinkBody:
    """Represents the attributes needed to build an external link object."""

    type: ExternalLinkType
    url: str = field(
        metadata={
            "marshmallow_field": fields.Url(
                schemes={"http", "https"}, error_messages={"invalid": "{input} is not a valid URL."}
            )
        }
    )
    name: Optional[str] = field(default=None)

    def to_external_link(self) -> ExternalLink:
        """Convert the body to a corresponding ExternalLink instance."""
        return ExternalLink(
            url=self.url,
            name=self.name,
            type=self.type,
        )


@dataclass(frozen=True)
class NewDatasetBody:
    """Represents the attributes needed to build a new dataset."""

    name: str = field(validator=validate_dataset_name)
    businessObject: BusinessObject = field()
    containsPii: bool = field()
    description: str = field(validator=validate_dataset_description)
    friendlyName: str = field(validator=validate_dataset_friendly_name)
    layer: Layer = field()
    tags: DatasetTags = field()
    engineers: List[DatasetParticipantBodyPart] = field()
    confidentiality: Confidentiality = field()
    documentation: Optional[str] = field(default=None, validator=validate_dataset_documentation)
    ingestFrequency: IngestFrequency = field(default=IngestFrequency.undefined)
    labels: Optional[List[str]] = dataset_labels_field()
    ownerAccountId: Optional[AccountId] = owner_account_id_field()
    previewAvailable: Optional[bool] = field(default=None)
    purpose: Optional[List[DatasetPurpose]] = field(default=None)
    retentionPeriod: RetentionPeriod = field(default=RetentionPeriod.undefined)
    sourceIdentifier: Optional[SourceIdentifier] = field(default=None)
    stewards: Optional[List[DatasetParticipantBodyPart]] = field(default=None)
    externalLinks: Optional[List[ExternalLinkBody]] = field(default=None)
    hubVisibility: Optional[Set[Hub]] = field(default=None)
    upstreamLineage: Optional[Set[DatasetId]] = field(default=None, validator=validate_dataset_lineage)
    supportGroup: Optional[SupportGroup] = field(default=None)
    supportLevel: SupportLevel = field(default=SupportLevel.undefined)


@dataclass(frozen=True)
class UpdateDatasetBody:
    """Represents the attributes needed to update a dataset."""

    confidentiality: Optional[Confidentiality] = field(default=None)
    containsPii: Optional[bool] = field(default=None)
    description: Optional[str] = field(validator=validate_dataset_description, default=None)
    documentation: Optional[str] = field(validator=validate_dataset_documentation, default=None)
    friendlyName: Optional[str] = field(validator=validate_dataset_friendly_name, default=None)
    ingestFrequency: Optional[IngestFrequency] = field(default=None)
    labels: Optional[List[str]] = dataset_labels_field()
    previewAvailable: Optional[bool] = field(default=None)
    purpose: Optional[List[DatasetPurpose]] = field(default=None)
    engineers: Optional[List[DatasetParticipantBodyPart]] = field(default=None)
    stewards: Optional[List[DatasetParticipantBodyPart]] = field(default=None)
    retentionPeriod: Optional[RetentionPeriod] = field(default=None)
    sourceIdentifier: Optional[DeletableSourceIdentifier] = field(default=None)
    status: Optional[DatasetStatus] = field(default=None)
    supportLevel: Optional[SupportLevel] = field(default=None)
    tags: Optional[DatasetTags] = field(default=None)
    externalLinks: Optional[List[ExternalLinkBody]] = field(default=None)
    hubVisibility: Optional[Set[Hub]] = field(default=None)
    upstreamLineage: Optional[Set[DatasetId]] = field(default=None, validator=validate_dataset_lineage)
    supportGroup: Optional[DeletableSupportGroup] = field(default=None)
    qualityScore: Optional[int] = field(default=None, validator=Range(min=0, max=100))


@dataclass(frozen=True)
class DatasetAccountPermissionBody:
    """Request body for dataset access revocation requests."""

    accountId: AccountId
    region: Region
    stage: Stage


def get_external_links(body: Union[NewDatasetBody, UpdateDatasetBody]) -> Optional[List[ExternalLink]]:
    """Extract the ExternalLink object from the dataset bodies."""
    return [link.to_external_link() for link in body.externalLinks] if body.externalLinks is not None else None


@dataclass(frozen=True)
class DatasetAccountPermissionPostBody(DatasetAccountPermissionBody):
    """Request body for dataset access requests."""

    syncType: Optional[SyncType] = None
