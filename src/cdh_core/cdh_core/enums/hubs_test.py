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
from typing import Optional

import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub


def build_hub(partition: Optional[Partition] = None) -> Hub:
    if partition is None:
        return random.choice(list(Hub))
    return random.choice([hub for hub in Hub if hub.partition is partition])


def build_non_default_hub() -> Hub:
    return random.choice([hub for hub in Hub if hub is not Hub.default()])


# pylint: disable=unused-argument
@pytest.mark.parametrize(
    "mock_config_file",
    [
        replace(
            CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
            hub=ConfigFile.Hub(
                instances={
                    "GLOBAL": ConfigFile.Hub.Entry(
                        value="global", environments={"dev", "prod"}, regions={"eu-central-1", "us-east-1"}
                    ),
                    "MARS": ConfigFile.Hub.Entry(value="mars", environments={"dev"}, regions={"eu-central-1"}),
                    "DE": ConfigFile.Hub.Entry(value="de", environments={"prod"}, regions={"eu-central-1"}),
                    "CN": ConfigFile.Hub.Entry(value="cn", environments={"dev", "prod"}, regions={"cn-north-1"}),
                    "VENUS": ConfigFile.Hub.Entry(value="venus", environments={"dev"}, regions={"cn-north-1"}),
                    "CHINA": ConfigFile.Hub.Entry(value="china", environments={"prod"}, regions={"cn-north-1"}),
                },
            ),
        )
    ],
    indirect=True,
)
class TestHubs:
    def test_default(self, mock_config_file: ConfigFile) -> None:
        assert Hub.default() == Hub("global")
        assert Hub.default(Partition("aws")) == Hub("global")
        assert Hub.default(Partition("aws-cn")) == Hub("cn")

    def test_get_hubs(self, mock_config_file: ConfigFile) -> None:
        assert Hub.get_hubs(environment=Environment("dev")) == frozenset(
            {
                Hub("mars"),
                Hub("global"),
                Hub("venus"),
                Hub("cn"),
            }
        )
        assert Hub.get_hubs(environment=Environment("dev"), partition=Partition("aws")) == frozenset(
            {Hub("mars"), Hub("global")}
        )
        assert Hub.get_hubs(environment=Environment("dev"), partition=Partition("aws-cn")) == frozenset(
            {Hub("venus"), Hub("cn")}
        )
        assert Hub.get_hubs(environment=Environment("prod")) == frozenset(
            {
                Hub("global"),
                Hub("de"),
                Hub("cn"),
                Hub("china"),
            }
        )
        assert Hub.get_hubs(environment=Environment("prod"), partition=Partition("aws")) == frozenset(
            {Hub("global"), Hub("de")}
        )
        assert Hub.get_hubs(environment=Environment("prod"), partition=Partition("aws-cn")) == frozenset(
            {Hub("cn"), Hub("china")}
        )

    def test_friendly_name(self, mock_config_file: ConfigFile) -> None:
        assert Hub("global").friendly_name == "Global Cloud Data Hub"
        assert Hub("de").friendly_name == "DE Cloud Data Hub"
        assert Hub("china").friendly_name == "CN China Cloud Data Hub"

    def test_regions(self, mock_config_file: ConfigFile) -> None:
        assert Hub("global").regions == {
            Region("eu-central-1"),
            Region("us-east-1"),
        }
        assert Hub("de").regions == {Region("eu-central-1")}
        assert Hub("china").regions == {Region("cn-north-1")}

    def test_partitions(self, mock_config_file: ConfigFile) -> None:
        assert Hub("global").partition is Partition("aws")
        assert Hub("china").partition is Partition("aws-cn")
