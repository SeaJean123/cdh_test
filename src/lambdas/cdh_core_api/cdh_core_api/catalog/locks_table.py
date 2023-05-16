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
from typing import Any
from typing import List

from cdh_core_api.catalog.base import BaseTable
from cdh_core_api.catalog.base import conditional_check_failed
from cdh_core_api.catalog.base import create_model
from cdh_core_api.catalog.base import DateTimeAttribute
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.exceptions import PutError
from pynamodb.models import Model
from pynamodb_attributes import UnicodeEnumAttribute

from cdh_core.entities.lock import Lock
from cdh_core.enums.locking import LockingScope


class _LockModel(Model):
    lock_id = UnicodeAttribute(hash_key=True)
    scope = UnicodeEnumAttribute(LockingScope)
    timestamp = DateTimeAttribute()
    data: MapAttribute[str, Any] = MapAttribute[str, Any]()  # type: ignore[no-untyped-call]
    request_id = UnicodeAttribute()

    def lock(self) -> Lock:
        """Create a lock from the model."""
        return Lock(
            lock_id=self.lock_id,
            scope=self.scope,
            timestamp=self.timestamp,
            data=self.data.as_dict(),  # type: ignore[no-untyped-call]
            request_id=self.request_id,
        )

    @classmethod
    def from_lock(cls, lock: Lock) -> "_LockModel":
        """Create a model based on a lock object."""
        return cls(
            lock_id=lock.lock_id, scope=lock.scope, timestamp=lock.timestamp, data=lock.data, request_id=lock.request_id
        )


# pylint: disable=no-member
class LocksTable(BaseTable):
    """Represents the DynamoDB table for locks."""

    def __init__(self, prefix: str = ""):
        self._model = create_model(table=f"{prefix}cdh-locks", model=_LockModel, module=__name__)

    def get(self, lock_id: str) -> Lock:
        """Return a single lock."""
        try:
            return self._model.get(lock_id, consistent_read=True).lock()
        except DoesNotExist as error:
            raise LockNotFound(lock_id) from error

    def list(self) -> List[Lock]:
        """Return a list of all locks."""
        return [model.lock() for model in self._model.scan(consistent_read=True)]

    def create(self, lock: Lock) -> None:
        """Create a lock."""
        try:
            self._model.from_lock(lock).save(_LockModel.lock_id.does_not_exist())
        except PutError as error:
            if conditional_check_failed(error):
                raise LockAlreadyExists(lock.lock_id) from error
            raise error

    def delete(self, lock: Lock) -> None:
        """Delete a lock."""
        try:
            self._model.from_lock(lock).delete()
        except DoesNotExist as error:
            raise LockNotFound(lock.lock_id) from error

    def exists(self, lock_id: str) -> bool:
        """Return True if the lock exists."""
        try:
            self.get(lock_id)
        except LockNotFound:
            return False
        return True


class LockNotFound(Exception):
    """Signals that the requested lock cannot be found."""

    def __init__(self, lock_id: str):
        super().__init__(f"Lock {lock_id} was not found")


class LockAlreadyExists(Exception):
    """Signals that a lock is already present."""

    def __init__(self, lock_id: str):
        super().__init__(f"Lock {lock_id} already exists")
