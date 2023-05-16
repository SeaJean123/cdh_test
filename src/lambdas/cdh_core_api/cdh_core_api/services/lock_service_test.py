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
# pylint: disable=protected-access
from datetime import datetime

import pytest
from cdh_core_api.catalog.locks_table import LockNotFound
from cdh_core_api.catalog.locks_table import LocksTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.lock_service import ResourceIsLocked
from freezegun import freeze_time
from mypy_boto3_dynamodb.service_resource import Table

from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.locking import LockingScope
from cdh_core_dev_tools.testing.builder import Builder

NOW = datetime.now()


@freeze_time(NOW)
class TestLockService:
    @pytest.fixture(autouse=True)
    def service_setup(self, resource_name_prefix: str, mock_locks_dynamo_table: Table) -> None:
        self.mock_locks_dynamo_table = mock_locks_dynamo_table
        self.locks_table = LocksTable(resource_name_prefix)
        self.lock_service = LockService(config=build_config(prefix=resource_name_prefix))
        self.lock_service.set_request_id(request_id=Builder.build_request_id())

    def test_reset_counter(self) -> None:
        dataset = build_dataset()
        resource = build_s3_resource(dataset=dataset)
        self.lock_service.acquire_lock(
            item_id=dataset.id,
            scope=LockingScope.s3_resource,
            region=resource.region,
            stage=resource.stage,
            data={},
        )
        assert self.lock_service.lock_count == 1
        self.lock_service.set_request_id(request_id=Builder.build_request_id())
        assert self.lock_service.lock_count == 0

    def test_acquire_lock(self) -> None:
        dataset = build_dataset()
        resource = build_s3_resource(dataset=dataset)
        data = {"attribute": "value"}
        assert self.lock_service.lock_count == 0
        lock = self.lock_service.acquire_lock(
            item_id=dataset.id,
            scope=LockingScope.s3_resource,
            region=resource.region,
            stage=resource.stage,
            data=data,
        )
        assert self.locks_table.get(lock.lock_id) == lock
        assert self.lock_service.lock_count == 1

    def test_release_lock(self) -> None:
        dataset = build_dataset()
        resource = build_s3_resource(dataset=dataset)
        assert self.lock_service.lock_count == 0
        lock = self.lock_service._create_lock(
            item_id=dataset.id, scope=LockingScope.s3_resource, region=resource.region, stage=resource.stage
        )
        self.locks_table.create(lock)
        self.lock_service.release_lock(lock)

        assert not self.locks_table.exists(lock.lock_id)
        assert self.lock_service.lock_count == -1
        with pytest.raises(LockNotFound):
            self.locks_table.get(lock.lock_id)

    def test_lock_exists(self) -> None:
        dataset = build_dataset()
        resource = build_s3_resource(dataset=dataset)
        lock = self.lock_service.acquire_lock(
            item_id=dataset.id,
            scope=LockingScope.s3_resource,
            region=resource.region,
            stage=resource.stage,
            data={},
        )
        assert self.locks_table.exists(lock.lock_id)

    def test_acquire_twice_should_throw_exception(self) -> None:
        dataset = build_dataset()
        resource = build_s3_resource(dataset=dataset)
        self.lock_service.acquire_lock(
            item_id=dataset.id,
            scope=LockingScope.s3_resource,
            region=resource.region,
            stage=resource.stage,
            data={},
        )
        with pytest.raises(ResourceIsLocked):
            self.lock_service.acquire_lock(
                item_id=dataset.id,
                scope=LockingScope.s3_resource,
                region=resource.region,
                stage=resource.stage,
                data={},
            )

    def test_acquire_lock_missing_optional_arguments(self) -> None:
        dataset = build_dataset()
        data = {"attribute": "value"}
        assert self.lock_service.lock_count == 0
        lock = self.lock_service.acquire_lock(
            item_id=dataset.id,
            scope=LockingScope.s3_resource,
            data=data,
        )
        assert self.locks_table.get(lock.lock_id) == lock
        assert self.lock_service.lock_count == 1
