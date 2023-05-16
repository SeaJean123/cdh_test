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
from enum import Enum
from typing import Set
from typing import TYPE_CHECKING

from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.enums.aws import Partition
from cdh_core.enums.resource_properties import Stage


class EnvironmentMixin(Enum):
    """Provides the logic for the Enum Environment."""

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for an environment."""
        return ConfigFileLoader.get_config().environment.instances[self.name].friendly_name

    @property
    def stages_with_extended_metrics(self) -> Set[Stage]:
        """Return the stages of an environment for which extended metrics are tracked."""
        return {
            Stage(stage)
            for stage in ConfigFileLoader.get_config().environment.instances[self.name].stages_with_extended_metrics
        }

    @property
    def is_test_environment(self) -> bool:
        """Return True if the environment is used for functional tests."""
        return ConfigFileLoader.get_config().environment.instances[self.name].is_test_environment

    def get_domain(self, partition: Partition) -> str:
        """Return the domain under which the CDH API is available in this environment."""
        return ConfigFileLoader.get_config().environment.instances[self.name].domain[partition.value]


if TYPE_CHECKING:
    Environment = EnvironmentMixin
else:
    Environment = Enum(
        "Environment",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().environment.instances.items()},
        type=EnvironmentMixin,
        module=__name__,
    )
Environment.__doc__ = (
    "Different environments can be used for testing and developing platform capabilities "
    "completely separated from productive workloads."
)
