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
# pylint: disable=unused-argument
from random import choice
from typing import Optional

import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region


def build_region(partition: Optional[Partition] = None) -> Region:
    if partition is None:
        return choice(list(Region))
    return choice([region for region in Region if region.partition is partition])


def build_partition(region: Optional[Region] = None) -> Partition:
    return region.partition if region else choice(list(Partition))


@pytest.mark.usefixtures("mock_config_file")
class TestRegion:
    def test_every_region_located_in_its_partition(self, mock_config_file: ConfigFile) -> None:
        for region in {entry.value for entry in mock_config_file.region.instances.values()}:
            partition = Region(region).partition
            assert region in mock_config_file.partition.instances[partition.name].regions

    def test_preferred_region_is_default_region(self, mock_config_file: ConfigFile) -> None:
        for partition in mock_config_file.partition.instances.values():
            assert Region.preferred(Partition(partition.value)) == Region(partition.default_region)


@pytest.mark.usefixtures("mock_config_file")
class TestPartition:
    def test_default(self, mock_config_file: ConfigFile) -> None:
        assert Partition.default() == Partition("aws")
