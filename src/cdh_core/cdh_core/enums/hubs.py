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

from enum import Enum
from functools import lru_cache
from typing import Collection
from typing import Optional
from typing import Set
from typing import TYPE_CHECKING

from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment


class HubMixin(Enum):
    """Provides the logic for the Enum Hub."""

    @classmethod
    def default(cls, partition: Optional[Partition] = None) -> Hub:
        """Return default Hub object."""
        partition = partition or Partition.default()
        return Hub(ConfigFileLoader.get_config().partition.instances[partition.name].default_hub)

    @staticmethod
    @lru_cache()
    def get_hubs(environment: Environment, partition: Optional[Partition] = None) -> Collection[Hub]:
        """Return a collection of hubs for a combination of environments and partition, based on the configuration."""
        allowed_partitions = [partition] if partition else Partition
        return frozenset(
            {
                Hub(instance.value)
                for instance in ConfigFileLoader.get_config().hub.instances.values()
                if environment.value in instance.environments
                and Region(next(iter(instance.regions))).partition in allowed_partitions  # type: ignore
            }
        )

    @property
    def friendly_name(self) -> str:
        """Return the human readable name of a Hub."""
        hub_value = ConfigFileLoader.get_config().hub.instances[self.name].value
        hub_name = hub_value.upper() if len(hub_value) < 4 else hub_value.capitalize()
        return f"{self.partition.short_name.upper()} {hub_name} Cloud Data Hub".strip()

    @property
    def regions(self) -> Set[Region]:
        """Return the regions a Hub is within."""
        return {Region(region) for region in ConfigFileLoader.get_config().hub.instances[self.name].regions}

    @property
    def partition(self) -> Partition:
        """Return the partition a Hub is in."""
        return next(iter(self.regions)).partition


if TYPE_CHECKING:
    Hub = HubMixin
else:
    Hub = Enum(
        "Hub",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().hub.instances.items()},
        type=HubMixin,
        module=__name__,
    )
Hub.__doc__ = (
    "High-level organizational category. For different hubs, data resides in separate AWS accounts. Each Hub "
    "can possess its own specific configuration, e.g. which AWS regions are supported."
)
