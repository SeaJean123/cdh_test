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
from logging import getLogger
from typing import Generic

from cdh_core_api.catalog.accounts_table import AccountAlreadyExists
from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.generic_types import GenericUpdateAccountBody
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.lock_service import LockService

from cdh_core.enums.locking import LockingScope
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)


class AccountManager(Generic[GenericAccount, GenericUpdateAccountBody]):
    """Handles accounts for a generic account class."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        accounts_table: GenericAccountsTable[GenericAccount],
        dataset_permissions_manager: DatasetPermissionsManager[
            GenericAccount, GenericS3Resource, GenericGlueSyncResource
        ],
        lock_service: LockService,
    ):
        self._accounts_table = accounts_table
        self._lock_service = lock_service
        self._dataset_permissions_manager = dataset_permissions_manager

    def create(self, account: GenericAccount) -> GenericAccount:
        """Create an account.

        Lock the account, create it and then release the lock.

        :param account the account to be created
        :returns the newly created account
        """
        lock = self._lock_service.acquire_lock(account.id, scope=LockingScope.account)
        try:
            self._accounts_table.create(account)
        except AccountAlreadyExists as error:
            raise ConflictError(error) from error
        finally:
            self._lock_service.release_lock(lock)
        return account

    def update(self, account_id: AccountId, body: GenericUpdateAccountBody) -> GenericAccount:
        """Update an account.

        Lock the account, update it and then release the lock.

        :param update body for the account
        :returns the updated account
        """
        lock = self._lock_service.acquire_lock(item_id=account_id, scope=LockingScope.account)
        updated_account = self._accounts_table.update(
            account_id=account_id,
            update_date=datetime.now(),
            admin_roles=body.adminRoles,
            affiliation=body.affiliation,
            business_objects=body.businessObjects,
            friendly_name=body.friendlyName,
            group=body.group,
            layers=body.layers,
            responsibles=body.responsibles,
            roles=[role.to_account_role() for role in body.roles] if body.roles is not None else None,
            stages=body.stages,
            type=body.type,
            visible_in_hubs=body.visibleInHubs,
        )
        self._lock_service.release_lock(lock)
        return updated_account

    def _perform_deletion_cleanup(self, account: GenericAccount) -> None:
        self._dataset_permissions_manager.remove_permissions_across_datasets(account)

    def delete(self, account: GenericAccount) -> None:
        """Delete an account.

        Lock the account, delete it and then release the lock.

        :param account the account to be deleted
        :raises NotFoundError if the account was not found
        """
        lock = self._lock_service.acquire_lock(item_id=account.id, scope=LockingScope.account)

        self._perform_deletion_cleanup(account)

        try:
            self._accounts_table.delete(account.id)
        except AccountNotFound as err:
            raise NotFoundError(err) from err
        finally:
            self._lock_service.release_lock(lock)
