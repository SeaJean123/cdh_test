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
from typing import Dict

from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region


@dataclass(frozen=True)
class Credential:
    """Dataclass for AWS credentials."""

    access_key_id: str
    secret_access_key: str
    partition: Partition

    @property
    def boto_format(self) -> Dict[str, str]:
        """Return AWS credentials in Boto format."""
        return {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "region_name": self.region.value,
        }

    @property
    def region(self) -> Region:
        """Return default region for partition attribute."""
        return Region.preferred(partition=self.partition)

    @property
    def tf_env_format(self) -> Dict[str, str]:
        """Return AWS credentials in Terraform environment format."""
        partition_short = self.partition.short_name
        partition_short = f"{partition_short}_" if partition_short else partition_short
        return {
            f"TF_VAR_{partition_short}tf_access_key": self.access_key_id,
            f"TF_VAR_{partition_short}tf_secret_key": self.secret_access_key,
        }
