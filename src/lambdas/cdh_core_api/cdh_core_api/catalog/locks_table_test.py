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

import pytest
from cdh_core_api.catalog.locks_table import LockAlreadyExists
from cdh_core_api.catalog.locks_table import LocksTable
from mypy_boto3_dynamodb.service_resource import Table

from cdh_core.entities.lock import Lock
from cdh_core.enums.locking import LockingScope
from cdh_core_dev_tools.testing.builder import Builder


def test_get_existing_lock(mock_locks_dynamo_table: Table, resource_name_prefix: str) -> None:
    expected_lock = Lock(
        lock_id=Builder.build_random_string(),
        data={},
        timestamp=datetime.now(),
        scope=LockingScope.s3_resource,
        request_id=Builder.build_request_id(),
    )
    mock_locks_dynamo_table.put_item(Item=build_dynamo_json(expected_lock))

    assert LocksTable(resource_name_prefix).get(expected_lock.lock_id) == expected_lock


def test_create_new_lock(mock_locks_dynamo_table: Table, resource_name_prefix: str) -> None:
    lock_id = Builder.build_random_string()
    expected_lock: Lock = Lock(
        lock_id=lock_id,
        data={},
        timestamp=datetime.now(),
        scope=LockingScope.s3_resource,
        request_id=Builder.build_request_id(),
    )
    LocksTable(resource_name_prefix).create(expected_lock)
    dynamo_response = mock_locks_dynamo_table.get_item(Key={"lock_id": lock_id})["Item"]

    assert dynamo_response == build_dynamo_json(expected_lock)


@pytest.mark.usefixtures("mock_locks_dynamo_table")
def test_create_existing_lock(resource_name_prefix: str) -> None:
    lock_id = Builder.build_random_string()
    expected_lock: Lock = Lock(
        lock_id=lock_id,
        data={},
        timestamp=datetime.now(),
        scope=LockingScope.s3_resource,
        request_id=Builder.build_request_id(),
    )
    LocksTable(resource_name_prefix).create(expected_lock)

    assert LocksTable(resource_name_prefix).exists(expected_lock.lock_id)

    with pytest.raises(LockAlreadyExists):
        LocksTable(resource_name_prefix).create(expected_lock)


def build_dynamo_json(lock: Lock) -> Dict[str, Any]:
    return {
        "lock_id": lock.lock_id,
        "scope": lock.scope.value,
        "timestamp": lock.timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "data": lock.data,
        "request_id": lock.request_id,
    }
