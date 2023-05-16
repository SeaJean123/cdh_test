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


# pylint: disable=invalid-name
class Stage(Enum):
    """
    Deployment stage to be used.

    This pertains to the data hosted in CDH, not the platform itself.
    """

    dev = "dev"
    int = "int"
    prod = "prod"

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        if self is Stage.dev:
            return "Development"
        if self is Stage.int:
            return "Integration"
        if self is Stage.prod:
            return "Production"
        raise ValueError(f"This enum value is not supported: {self}")


class ResourceType(Enum):
    """Type of the resource."""

    s3 = "s3"
    glue_sync = "glue-sync"

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        if self is ResourceType.s3:
            return "S3"
        if self is ResourceType.glue_sync:
            return "Glue Sync"
        raise ValueError(f"This enum value is not supported: {self}")
