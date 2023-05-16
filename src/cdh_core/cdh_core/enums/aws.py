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

import re
from enum import Enum
from typing import TYPE_CHECKING

from cdh_core.config.config_file_loader import ConfigFileLoader


class RegionMixin(Enum):
    """Provides the logic for the Enum Region."""

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for a region."""
        return ConfigFileLoader.get_config().region.instances[self.name].friendly_name

    @property
    def partition(self) -> Partition:
        """Return the partition of this region."""
        for partition, entry in ConfigFileLoader.get_config().partition.instances.items():
            if self.value in entry.regions:
                return Partition[partition]  # type: ignore
        raise ValueError("This should never happen. Should have been checked by the ConfigFileLoader.")

    @staticmethod
    def preferred(partition: Partition) -> Region:
        """Return the preferred / primary region of a partition."""
        return Region(ConfigFileLoader.get_config().partition.instances[partition.name].default_region)


if TYPE_CHECKING:
    Region = RegionMixin
else:
    Region = Enum(
        "Region",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().region.instances.items()},
        type=RegionMixin,
        module=__name__,
    )
Region.__doc__ = "AWS region of the resource"


class PartitionMixin(Enum):
    """Provides the logic for the Enum Partition."""

    @classmethod
    def default(cls) -> Partition:
        """Return default Partition object."""
        return Partition(ConfigFileLoader.get_config().partition.default_value)

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for a partition."""
        return ConfigFileLoader.get_config().partition.instances[self.name].friendly_name

    @property
    def short_name(self) -> str:
        """Strip `aws-` from the beginning of the partition value."""
        return str(re.sub("aws-?", "", self.value))


if TYPE_CHECKING:
    Partition = PartitionMixin
else:
    Partition = Enum(
        "Partition",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().partition.instances.items()},
        type=PartitionMixin,
        module=__name__,
    )
Partition.__doc__ = "AWS partition of the resource"
