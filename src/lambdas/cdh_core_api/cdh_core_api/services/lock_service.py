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
from contextlib import suppress
from datetime import datetime
from logging import getLogger
from typing import Any
from typing import Dict
from typing import Optional

from cdh_core_api.catalog.locks_table import LockAlreadyExists
from cdh_core_api.catalog.locks_table import LockNotFound
from cdh_core_api.catalog.locks_table import LocksTable
from cdh_core_api.config import Config

from cdh_core.entities.lock import Lock
from cdh_core.enums.aws import Region
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import LockError

LOG = getLogger(__name__)


class LockService:
    """Manages the locking of items."""

    def __init__(self, config: Config):
        self._locks_table = LocksTable(prefix=config.prefix)
        self._request_id: str
        self._lock_counter: int

    def set_request_id(self, request_id: str) -> None:
        """Set a request id and reset the lock_counter to 0."""
        self._request_id = request_id
        self._lock_counter = 0

    def _create_lock(  # pylint: disable=too-many-arguments
        self,
        item_id: str,
        scope: LockingScope,
        region: Optional[Region],
        stage: Optional[Stage],
        data: Optional[Dict[Any, Any]] = None,
    ) -> Lock:
        return Lock(
            lock_id=Lock.build_id(
                item_id=item_id,
                scope=scope,
                stage=stage,
                region=region,
            ),
            scope=scope,
            timestamp=datetime.now(),
            data=data or {},
            request_id=self._request_id,
        )

    def acquire_lock(  # pylint: disable=too-many-arguments
        self,
        item_id: str,
        scope: LockingScope,
        region: Optional[Region] = None,
        stage: Optional[Stage] = None,
        data: Optional[Dict[Any, Any]] = None,
    ) -> Lock:
        """Obtain a lock for the specified resource to prevent concurrent changes.

        Raises ResourceIsLocked if the resource is locked by another process.
        """
        lock = self._create_lock(item_id=item_id, scope=scope, region=region, stage=stage, data=data)
        try:
            self._locks_table.create(lock)
            self._lock_counter += 1
            return lock
        except LockAlreadyExists as error:
            LOG.warning(f"Possible race condition detected. Lock= {str(lock)}")
            old_lock = None
            with suppress(LockNotFound):  # Between lock.create() and lock.get() the existing lock was deleted
                old_lock = self._locks_table.get(lock.lock_id)
            raise ResourceIsLocked(new_lock=lock, old_lock=old_lock) from error

    def release_lock(self, lock: Lock) -> None:
        """Remove the given lock."""
        self._locks_table.delete(lock=lock)
        self._lock_counter -= 1

    @property
    def lock_count(self) -> int:
        """Return the lock counter."""
        return self._lock_counter


class ResourceIsLocked(LockError):
    """Signals the lock to be created for the resource already exists."""

    def __init__(self, new_lock: Lock, old_lock: Optional[Lock]):
        if old_lock:
            super().__init__(
                f"Resource {new_lock.lock_id} is currently locked. Please reach out "
                + "to the CDH team if this problem persists."
            )
        else:
            super().__init__(
                f"Resource {new_lock.lock_id} was locked during the request. Please try again and reach out "
                + "to the CDH team if this problem persists."
            )

    def to_dict(self, request_id: Optional[str] = None) -> Dict[str, str]:
        """Return the error as standardized dict, which can be converted to JSON."""
        error_dict = super().to_dict(request_id)
        error_dict["Retryable"] = "True"
        return error_dict
