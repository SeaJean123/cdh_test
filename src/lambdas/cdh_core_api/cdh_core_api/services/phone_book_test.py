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
import abc
from typing import Callable
from typing import Collection

import pytest
from cdh_core_api.config_test import build_config
from cdh_core_api.services.phone_book import PhoneBook

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.account_store_test import build_account_store
from cdh_core.entities.accounts_test import build_base_account
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_sts_assumed_role_arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.environment import Environment
from cdh_core.enums.environment_test import build_environment
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class _TestPhoneBook:
    @pytest.fixture(autouse=True)
    def service_setup(self) -> None:
        self.perform_setup(build_environment())

    def perform_setup(self, environment: Environment) -> None:
        self.environment = environment
        self.account_store = self.build_account_store()
        self.config = build_config(account_store=self.account_store, environment=self.environment)
        self.phone_book = PhoneBook(self.config)

    @staticmethod
    def build_account_store() -> AccountStore:
        multipurpose_account_id = build_account_id()
        super_account = [
            build_base_account(account_id=multipurpose_account_id, purpose=purpose, environment=env)
            for env in Environment
            for purpose in AccountPurpose
        ]
        accounts = [
            build_base_account(purpose=purpose, environment=environment)
            for _ in range(3)
            for environment in Environment
            for purpose in AccountPurpose
        ]
        return build_account_store(super_account + accounts)

    @abc.abstractmethod
    def get_method(self) -> Callable[[Arn], bool]:
        ...

    @abc.abstractmethod
    def get_privileged_account_ids(self) -> Collection[AccountId]:
        ...

    @abc.abstractmethod
    def get_privileged_role_names(self) -> Collection[str]:
        ...

    def get_unprivileged_role_names(self) -> Collection[str]:
        return [Builder.build_random_string()]

    def get_random_account_id(self, purpose: AccountPurpose) -> AccountId:
        return Builder.get_random_element(
            self.account_store.query_accounts(environments=self.environment, account_purposes=purpose)
        ).id

    def test_root_user_unknown_account(self) -> None:
        requester = Arn(f"arn:{build_partition().value}:iam::{build_account_id()}:root")
        assert not self.get_method()(requester)

    def test_root_user_known_account(self) -> None:
        for account in self.account_store.get_all_accounts():
            requester = Arn(f"arn:{account.partition.value}:iam::{account.id}:root")
            assert not self.get_method()(requester)

    def test_iam_role_unknown_account(self) -> None:
        requester = build_arn(service="iam")
        assert not self.get_method()(requester)

    def test_iam_role_known_account(self) -> None:
        for account in self.account_store.get_all_accounts():
            requester = build_arn(service="iam", account_id=account.id)
            assert not self.get_method()(requester)

    def test_assumed_role_unknown_account(self) -> None:
        for role_name in self.get_privileged_role_names():
            requester = build_sts_assumed_role_arn(role_name=role_name)
            assert not self.get_method()(requester)

    def test_unprivileged_account(self) -> None:
        privileged_account_ids = self.get_privileged_account_ids()
        for account in self.account_store.get_all_accounts():
            if account.id not in privileged_account_ids:
                for role_name in self.get_privileged_role_names():
                    requester = build_sts_assumed_role_arn(account.id, role_name)
                    assert not self.get_method()(requester)

    def test_unprivileged_role(self) -> None:
        for account_id in self.get_privileged_account_ids():
            for role_name in self.get_unprivileged_role_names():
                requester = build_sts_assumed_role_arn(account_id, role_name)
                assert not self.get_method()(requester)

    def test_privileged(self) -> None:
        for account_id in self.get_privileged_account_ids():
            for role_name in self.get_privileged_role_names():
                requester = build_sts_assumed_role_arn(account_id, role_name)
                assert self.get_method()(requester)


@pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
class _TestPhoneBookMockedConfig(_TestPhoneBook):
    @pytest.fixture(autouse=True)
    def service_setup(  # type: ignore[override] # pylint: disable=unused-argument, arguments-differ
        self, mock_config_file: ConfigFile
    ) -> None:
        self.perform_setup(build_environment())

    @abc.abstractmethod
    def get_method(self) -> Callable[[Arn], bool]:
        ...

    @abc.abstractmethod
    def get_privileged_account_ids(self) -> Collection[AccountId]:
        ...

    @abc.abstractmethod
    def get_privileged_role_names(self) -> Collection[str]:
        ...


class TestPrivilegedCoreApiRole(_TestPhoneBook):
    def get_method(self) -> Callable[[Arn], bool]:
        return self.phone_book.is_privileged_core_api_role

    def get_privileged_account_ids(self) -> Collection[AccountId]:
        return {self.config.lambda_account_id}

    def get_privileged_role_names(self) -> Collection[str]:
        return {"CDHX-DevOps"}


class TestPrivilegedPortalRole(_TestPhoneBook):
    def get_method(self) -> Callable[[Arn], bool]:
        return self.phone_book.is_privileged_portal_role

    def get_privileged_role_names(self) -> Collection[str]:
        name = "data-portal-api-privileged"
        return {name, Builder.build_random_string() + name, "e2e-test-runner"}

    def get_unprivileged_role_names(self) -> Collection[str]:
        addition = Builder.build_random_string()
        return {
            "some-other-role",
            "data-portal-api-privileged" + addition,
            "e2e-test-runner" + addition,
            addition + "e2e-test-runner",
        }

    def get_privileged_account_ids(self) -> Collection[AccountId]:
        return {
            account.id
            for account in self.account_store.query_accounts(
                environments=self.environment, account_purposes=AccountPurpose("portal")
            )
        }


class TestCoreApiAdminRole(_TestPhoneBook):
    def get_method(self) -> Callable[[Arn], bool]:
        return self.phone_book.is_core_api_admin_role

    def get_privileged_account_ids(self) -> Collection[AccountId]:
        return {self.config.lambda_account_id}

    def get_privileged_role_names(self) -> Collection[str]:
        return {"CDHX-DevOps"}


class TestAuthorizationRole(_TestPhoneBook):
    def get_method(self) -> Callable[[Arn], bool]:
        return self.phone_book.is_authorization_role

    def get_privileged_account_ids(self) -> Collection[AccountId]:
        return {
            account.id
            for account in self.account_store.query_accounts(
                environments=self.environment, account_purposes=AccountPurpose("iam")
            )
        }

    def get_privileged_role_names(self) -> Collection[str]:
        return {
            "authorization-task",
            "some-authorization-task",
            "authorization-task-role",
            "some-authorization-task-role",
            "someauthorization-taskrole",
        }


class TestFunctionalTestsMainNonTestEnvironment(_TestPhoneBookMockedConfig):
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile) -> None:  # type: ignore[override]
        self.perform_setup(next(env for env in list(Environment) if not env.is_test_environment))

    def get_method(self) -> Callable[[Arn], bool]:
        return self.phone_book.is_functional_tests_user_role

    def get_privileged_role_names(self) -> Collection[str]:
        return {}

    def get_privileged_account_ids(self) -> Collection[AccountId]:
        return {}


class TestFunctionalTestsMainTestEnvironment(_TestPhoneBookMockedConfig):
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile) -> None:  # type: ignore[override]
        self.perform_setup(next(env for env in list(Environment) if env.is_test_environment))

    def get_method(self) -> Callable[[Arn], bool]:
        return self.phone_book.is_functional_tests_user_role

    def get_privileged_role_names(self) -> Collection[str]:
        return {self.config.functional_tests_user_role_name}

    def get_privileged_account_ids(self) -> Collection[AccountId]:
        return {
            account.id
            for account in self.config.account_store.query_accounts(
                environments=frozenset(Environment), account_purposes=AccountPurpose("test")
            )
        }
