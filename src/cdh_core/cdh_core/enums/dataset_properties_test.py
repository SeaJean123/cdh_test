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
from enum import Enum
from typing import Type

import pytest

from cdh_core.config.config_file import ConfigFile
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
from cdh_core_dev_tools.testing.builder import Builder


def build_layer() -> Layer:
    return random.choice(list(Layer))


def build_business_object() -> BusinessObject:
    return random.choice(list(BusinessObject))


def build_ingest_frequency() -> IngestFrequency:
    return random.choice(list(IngestFrequency))


def build_retention_period() -> RetentionPeriod:
    return random.choice(list(RetentionPeriod))


def build_support_level() -> SupportLevel:
    return random.choice(list(SupportLevel))


def build_sync_type() -> SyncType:
    return random.choice(list(SyncType))


def build_dataset_status() -> DatasetStatus:
    return random.choice(list(DatasetStatus))


def build_dataset_purpose() -> DatasetPurpose:
    return random.choice(list(DatasetPurpose))


def build_external_link_type() -> ExternalLinkType:
    return random.choice(list(ExternalLinkType))


def build_confidentiality() -> Confidentiality:
    return random.choice(list(Confidentiality))


def build_confidentiality_not_secret() -> Confidentiality:
    return Builder.get_random_element(list(Confidentiality), exclude={Confidentiality.secret})


@pytest.mark.usefixtures("mock_config_file")
def test_business_object_friendly_names(mock_config_file: ConfigFile) -> None:
    for entry in mock_config_file.business_object.instances.values():
        assert BusinessObject(entry.value).friendly_name == entry.friendly_name


@pytest.mark.usefixtures("mock_config_file")
def test_dataset_purpose_friendly_names(mock_config_file: ConfigFile) -> None:
    for entry in mock_config_file.dataset_purpose.instances.values():
        assert DatasetPurpose(entry.value).friendly_name == entry.friendly_name


@pytest.mark.usefixtures("mock_config_file")
def test_external_link_types_friendly_names(mock_config_file: ConfigFile) -> None:
    for entry in mock_config_file.dataset_external_link_type.instances.values():
        assert ExternalLinkType(entry.value).friendly_name == entry.friendly_name


@pytest.mark.parametrize("enum", [Layer, IngestFrequency, RetentionPeriod, SupportLevel, Confidentiality])
def test_enum_has_friendly_names_defined_for_all_members(enum: Type[Enum]) -> None:
    for member in enum:
        assert hasattr(member, "friendly_name")
        assert member.friendly_name
