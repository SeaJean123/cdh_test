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
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Optional

from cdh_core.entities.lock import Lock
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.locking_test import build_locking_scope
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core_dev_tools.testing.builder import Builder


def build_lock(
    item_id: Optional[str] = None,
    stage: Optional[Stage] = None,
    region: Optional[Region] = None,
    scope: Optional[LockingScope] = None,
    timestamp: Optional[datetime] = None,
    data: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> Lock:
    scope = scope or build_locking_scope()
    stage = stage or build_stage()
    region = region or build_region()
    item_id = item_id or Builder.build_random_string()

    return Lock(
        lock_id=Lock.build_id(item_id=item_id, scope=scope, region=region, stage=stage),
        scope=scope,
        timestamp=timestamp or datetime.now(),
        data=data or {},
        request_id=request_id or Builder.build_request_id(),
    )


class TestLock:
    def test_build_id(self) -> None:
        item_id = Builder.build_random_string()
        scope = build_locking_scope()
        region = build_region()
        stage = build_stage()

        assert (
            Lock.build_id(item_id=item_id, scope=scope, region=region, stage=stage)
            == f"{item_id}_{scope.value}_{stage.value}_{region.value}"
        )

    def test_build_id_no_region(self) -> None:
        item_id = Builder.build_random_string()
        scope = build_locking_scope()
        stage = build_stage()

        assert (
            Lock.build_id(item_id=item_id, scope=scope, stage=stage)
            == f"{item_id}_{scope.value}_{stage.value}_no_region"
        )

    def test_build_id_no_stage(self) -> None:
        item_id = Builder.build_random_string()
        scope = build_locking_scope()
        region = build_region()

        assert (
            Lock.build_id(item_id=item_id, scope=scope, region=region)
            == f"{item_id}_{scope.value}_no_stage_{region.value}"
        )
