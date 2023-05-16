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
from dataclasses import dataclass
from typing import Optional

from cdh_core_api.api.validation import field
from cdh_core_api.validation.base import owner_account_id_field

from cdh_core.entities.dataset import DatasetId
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId


@dataclass(frozen=True)
class NewS3BucketBody:
    """Represents the attributes needed to create a new s3 resource."""

    datasetId: DatasetId  # pylint: disable=invalid-name
    stage: Stage = field()
    region: Region = field()
    ownerAccountId: Optional[AccountId] = owner_account_id_field()  # pylint: disable=invalid-name


@dataclass(frozen=True)
class NewGlueSyncBody(NewS3BucketBody):
    """Represents the attributes needed to create a new glue-sync resource."""

    syncType: Optional[SyncType] = None  # pylint: disable=invalid-name
