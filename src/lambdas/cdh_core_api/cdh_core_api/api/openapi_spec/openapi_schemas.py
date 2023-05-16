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
from cdh_core_api.api.openapi_spec.openapi import OpenApiEnum
from cdh_core_api.api.openapi_spec.openapi import OpenApiEnumAsString

from cdh_core.entities.accounts import AccountRoleType
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
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

ACCOUNT_TYPE_SCHEMA = OpenApiEnum.from_enum_type(AccountType)
AFFILIATION_SCHEMA = OpenApiEnum.from_enum_type(Affiliation)
BUSINESS_OBJECT_SCHEMA = OpenApiEnum.from_enum_type(BusinessObject)
CONFIDENTIALITY_SCHEMA = OpenApiEnum.from_enum_type(Confidentiality)
DATASET_PURPOSE_SCHEMA = OpenApiEnum.from_enum_type(DatasetPurpose)
DATASET_STATUS_SCHEMA = OpenApiEnum.from_enum_type(DatasetStatus)
DATASET_EXTERNAL_LINK_TYPE_SCHEMA = OpenApiEnum.from_enum_type(ExternalLinkType)
HUB_SCHEMA = OpenApiEnumAsString.from_enum_type(Hub)
INGEST_FREQUENCY_SCHEMA = OpenApiEnum.from_enum_type(IngestFrequency)
LAYER_SCHEMA = OpenApiEnum.from_enum_type(Layer)
REGION_SCHEMA = OpenApiEnum.from_enum_type(Region)
RETENTION_PERIOD_SCHEMA = OpenApiEnum.from_enum_type(RetentionPeriod)
ACCOUNT_ROLE_TYPE_SCHEMA = OpenApiEnum.from_enum_type(AccountRoleType)
STAGE_SCHEMA = OpenApiEnum.from_enum_type(Stage)
SUPPORT_LEVEL_SCHEMA = OpenApiEnum.from_enum_type(SupportLevel)
SYNC_TYPE_SCHEMA = OpenApiEnum.from_enum_type(SyncType)
