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

import random
import string
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from enum import Enum
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import NewType
from typing import Optional
from typing import Set

from dataclasses_json import config
from marshmallow import fields

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.dates import date_input
from cdh_core.dates import date_output
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipants
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

RANDOM_STRING_CHARS = string.ascii_lowercase + string.digits

DatasetId = NewType("DatasetId", str)
SourceIdentifier = NewType("SourceIdentifier", str)
SupportGroup = NewType("SupportGroup", str)
DatasetTags = NewType("DatasetTags", Dict[str, str])


@dataclass(frozen=True)
class DatasetAccountPermission(DataClassJsonCDHMixin):
    """Defines account permission on a region/stage basis."""

    account_id: AccountId
    stage: Stage
    region: Region
    sync_type: SyncType

    def __str__(self) -> str:
        """Represent DatasetAccountPermission by a tuple of its fields."""
        return f"({self.account_id}, {self.region.value}, {self.stage.value}, {self.sync_type.value})"


@dataclass(frozen=True)
class ResponseDatasetPermissions(DataClassJsonCDHMixin):
    """This class is used to transfer multiple dataset permissions between the server and client."""

    permissions: FrozenSet[DatasetAccountPermission]


# pylint: disable=invalid-name
class DatasetAccountPermissionAction(Enum):
    """Defines actions that can be performed with a DatasetAccountPermission."""

    add = "add"
    remove = "remove"

    @property
    def inverse(self) -> "DatasetAccountPermissionAction":
        """Return the inverse operation of a DatasetAccountPermissionAction."""
        return {
            DatasetAccountPermissionAction.add: DatasetAccountPermissionAction.remove,
            DatasetAccountPermissionAction.remove: DatasetAccountPermissionAction.add,
        }[self]


@dataclass(frozen=True)
class ExternalLink:
    """A reference to an external resource."""

    name: Optional[str]
    type: ExternalLinkType
    url: str


@dataclass(frozen=True)
class DatasetLineage:
    """A reference to related datasets."""

    upstream: Set[DatasetId]


@dataclass(frozen=True)
class Dataset(DataClassJsonCDHMixin):
    """Datasets are the core building blocks of data residing in Cloud Data Hub.

    Their owners can create resources in them to provide data. Access is controlled on the level of datasets.
    """

    id: DatasetId  # pylint: disable=invalid-name
    business_object: BusinessObject
    confidentiality: Confidentiality
    contains_pii: bool
    creator_user_id: Optional[str]
    creation_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=fields.DateTime(format="iso"))
    )
    description: str
    documentation: Optional[str]
    external_links: List[ExternalLink]
    friendly_name: str
    hub: Hub
    hub_visibility: Set[Hub]
    ingest_frequency: IngestFrequency
    labels: Set[str]
    layer: Layer
    lineage: DatasetLineage
    name: str
    owner_account_id: AccountId
    permissions: FrozenSet[DatasetAccountPermission]
    preview_available: bool
    purpose: Set[DatasetPurpose]
    quality_score: Optional[int]
    retention_period: RetentionPeriod
    source_identifier: Optional[SourceIdentifier]
    status: DatasetStatus
    support_group: Optional[SupportGroup]
    support_level: SupportLevel
    tags: DatasetTags
    update_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=fields.DateTime(format="iso"))
    )

    @staticmethod
    def build_id(business_object: BusinessObject, name: str, layer: Layer, hub: Hub) -> DatasetId:
        """Return the id of a dataset.

        The id is built by concatenating the names of the business object, dataset, layer and in some cases of the hub.
        """
        if hub == Hub.default():
            return DatasetId(f"{business_object.value}_{name}_{layer.value}")
        return DatasetId(f"{hub.value}_{business_object.value}_{name}_{layer.value}")

    def filter_permissions(
        self,
        account_id: Optional[AccountId] = None,
        stage: Optional[Stage] = None,
        region: Optional[Region] = None,
        sync_type: Optional[SyncType] = None,
    ) -> FrozenSet[DatasetAccountPermission]:
        """Return all dataset permissions filtered by account_id, stage, region, and/or sync_type.

        The result is retrieved from the permissions field of the dataset.
        """
        return frozenset(
            {
                permission
                for permission in self.permissions
                if account_id in {None, permission.account_id}
                and stage in {None, permission.stage}
                and region in {None, permission.region}
                and sync_type in {None, permission.sync_type}
            }
        )

    def build_cdh_bucket_name(self, prefix: str = "") -> str:
        """
        Create a name for the S3 bucket in the given dataset.

        S3 bucket names use one global namespace. To avoid collisions, we include a random part.
        To be precise, we use the following naming scheme:

            <prefix>cdh-[<hub>-]<business_object>-<dataset_name>-<layer>-<random 4 chars>

        The length of each part is restricted so that we stay below the limit of 63 characters for an S3 bucket:
            - prefix (e.g. cdhx007): 7
            - hub (e.g. mars): 7
            - business_object (e.g. hr): 12
            - dataset_name (chosen by the user): 20
            - layer (e.g. sem): 3
        Together with 'cdh', up to 5 hyphens, 4 random characters and 2 characters reserved for future use,
        this gives exactly 63 characters.
        """
        random_suffix = "".join(random.choices(RANDOM_STRING_CHARS, k=4))
        return "-".join([prefix + "cdh", self.id.replace("_", "-"), random_suffix])

    def get_account_ids_with_read_access(self, stage: Stage, region: Region) -> FrozenSet[AccountId]:
        """Return all accounts that have access to the dataset in a certain stage and region.

        The result is retrieved from the permissions field of the dataset.
        """
        return frozenset({permission.account_id for permission in self.filter_permissions(stage=stage, region=region)})


@dataclass(frozen=True)
class ResponseDataset(Dataset):
    """
    This class is used to transfer a dataset between the server and client.

    The fields 'engineers' and 'stewards' are not part of the internal data model and therefore added in this class.
    """

    engineers: List[DatasetParticipant]
    stewards: List[DatasetParticipant]

    @classmethod
    def from_dataset(
        cls,
        dataset: Dataset,
        dataset_participants: DatasetParticipants,
    ) -> ResponseDataset:
        """Create a ResponseDataset based on a Dataset."""
        return cls(
            **dataset.__dict__,
            engineers=dataset_participants.engineers,
            stewards=dataset_participants.stewards,
        )


@dataclass(frozen=True)
class ResponseDatasets(DataClassJsonCDHMixin):
    """This class is used to transfer multiple datasets between the server and client."""

    datasets: List[ResponseDataset]
