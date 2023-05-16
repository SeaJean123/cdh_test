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
from dataclasses import replace
from datetime import datetime
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.services.account_manager import AccountManager
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.lock_service import ResourceIsLocked

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.lock_test import build_lock
from cdh_core.enums.locking import LockingScope
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import NotFoundError
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class TestAccountManager:
    @pytest.fixture(autouse=True)
    def service_setup(
        self, accounts_table: AccountsTable, time_travel: None  # pylint: disable=unused-argument
    ) -> None:
        self.accounts_table = accounts_table
        self.lock_service = Mock(LockService)
        self.lock = Mock()
        self.lock_service.acquire_lock.return_value = self.lock

        self.account = build_account()
        self.update_body = UpdateAccountBody()

        self.dataset_permissions_manager = Mock(DatasetPermissionsManager)

        self.account_manager: AccountManager[Account, UpdateAccountBody] = AccountManager(
            accounts_table=self.accounts_table,
            lock_service=self.lock_service,
            dataset_permissions_manager=self.dataset_permissions_manager,
        )

    def test_create_account_successful(self) -> None:
        lock = build_lock(
            item_id=self.account.id,
            scope=LockingScope.account,
        )
        self.lock_service.acquire_lock.return_value = lock

        created_account = self.account_manager.create(account=self.account)

        self.lock_service.acquire_lock.assert_called_once_with(self.account.id, scope=LockingScope.account)
        self.lock_service.release_lock.assert_called_once_with(lock)
        assert created_account == self.account
        assert self.accounts_table.get(self.account.id) == self.account

    def test_create_account_already_exists_fails(self) -> None:
        self.accounts_table.create(self.account)

        with pytest.raises(ConflictError):
            self.account_manager.create(account=self.account)

        self.lock_service.acquire_lock.assert_called_once_with(self.account.id, scope=LockingScope.account)
        self.lock_service.release_lock.assert_called_once_with(self.lock)

    def test_create_account_locked_fails(self) -> None:
        exception = ResourceIsLocked(build_lock(), build_lock())
        self.lock_service.acquire_lock.side_effect = exception

        with assert_raises(exception):
            self.account_manager.create(account=self.account)

        self.lock_service.acquire_lock.assert_called_once_with(self.account.id, scope=LockingScope.account)
        self.lock_service.release_lock.assert_not_called()

    def test_update_successful(self) -> None:
        self.accounts_table.create(self.account)
        self.update_body = replace(self.update_body, friendlyName=Builder.build_random_string())
        expected_account = replace(
            self.account, friendly_name=self.update_body.friendlyName, update_date=datetime.now()
        )
        lock = build_lock(
            item_id=self.account.id,
            scope=LockingScope.account,
        )
        self.lock_service.acquire_lock.return_value = lock

        updated_account = self.account_manager.update(account_id=self.account.id, body=self.update_body)

        self.lock_service.acquire_lock.assert_called_once_with(item_id=self.account.id, scope=LockingScope.account)
        self.lock_service.release_lock.assert_called_once_with(lock)
        assert updated_account == expected_account
        assert self.accounts_table.get(self.account.id) == expected_account

    def test_update_account_locked_fails(self) -> None:
        self.accounts_table.create(self.account)
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(build_lock(), build_lock())
        with pytest.raises(ResourceIsLocked):
            self.account_manager.update(account_id=self.account.id, body=self.update_body)

        self.lock_service.acquire_lock.assert_called_once_with(item_id=self.account.id, scope=LockingScope.account)
        self.lock_service.release_lock.assert_not_called()

    def test_delete_successful(self) -> None:
        self.accounts_table.create(self.account)

        self.account_manager.delete(self.account)

        self.lock_service.acquire_lock.assert_called_once_with(item_id=self.account.id, scope=LockingScope.account)
        self.lock_service.release_lock.assert_called_once_with(self.lock)
        self.dataset_permissions_manager.remove_permissions_across_datasets.assert_called_once_with(self.account)

    def test_delete_locked_fails(self) -> None:
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(Mock(), None)
        with pytest.raises(ResourceIsLocked):
            self.account_manager.delete(self.account)
        self.dataset_permissions_manager.remove_permissions_across_datasets.assert_not_called()

    def test_delete_non_existent_account_fails(self) -> None:
        with pytest.raises(NotFoundError):
            self.account_manager.delete(self.account)
        self.lock_service.acquire_lock.assert_called_once_with(item_id=self.account.id, scope=LockingScope.account)
        self.dataset_permissions_manager.remove_permissions_across_datasets.assert_called_once_with(self.account)
        self.lock_service.release_lock.assert_called_once_with(self.lock)
