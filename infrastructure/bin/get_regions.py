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
import json
import sys

from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region


def main(partition: Partition) -> None:
    """Output all regions configured for the given partition."""
    print(  # noqa: T201
        json.dumps({region.value: region.friendly_name for region in Region if region.partition is partition})
    )


if __name__ == "__main__":
    tf_input = json.loads(sys.stdin.readline())
    main(
        partition=Partition(tf_input["partition"]),
    )
