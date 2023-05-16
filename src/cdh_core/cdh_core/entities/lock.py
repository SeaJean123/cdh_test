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
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Optional

from cdh_core.enums.aws import Region
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import Stage


@dataclass(frozen=True)
class Lock:
    """Dataclass containing lock information."""

    lock_id: str
    scope: LockingScope
    timestamp: datetime
    data: Dict[str, Any]
    request_id: str

    @staticmethod
    def build_id(
        item_id: str,
        scope: LockingScope,
        region: Optional[Region] = None,
        stage: Optional[Stage] = None,
    ) -> str:
        """Return the id of a lock.

        The id is build by concatenating the item ID, the scope, the stage and the region.
        If the region or the stage is not supplied, dummy strings are used in their place.
        """
        region_str = region.value if region else "no_region"
        stage_str = stage.value if stage else "no_stage"
        return f"{item_id}_{scope.value}_{stage_str}_{region_str}"
