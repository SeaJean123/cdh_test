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
# pylint: disable=unused-argument
from dataclasses import replace
from typing import Collection
from typing import Optional
from typing import Union

import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.account_store import QueryAccountAmbiguous
from cdh_core.entities.account_store import QueryAccountNotFound
from cdh_core.entities.accounts import BaseAccount
from cdh_core.entities.accounts import HubAccount
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.accounts import SecurityAccount
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId
from cdh_core_dev_tools.testing.builder import Builder


def build_account_store(
    accounts: Optional[Collection[Union[BaseAccount, HubAccount, ResourceAccount, SecurityAccount]]] = None
) -> AccountStore:
    if accounts:
        return AccountStore(accounts=accounts)
    return AccountStore([build_resource_account(stage_priority=i) for i in range(10)])


@pytest.mark.parametrize(
    "mock_config_file",
    [
        replace(
            CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
            account=ConfigFile.Account(
                instances_per_purpose={
                    "test": ConfigFile.Account.PurposeEntry(
                        account_instances={
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("000000000000"), partition="aws-cn", environment="prod", hub="cn"
                            ),
                        },
                    ),
                    "api": ConfigFile.Account.PurposeEntry(
                        account_instances={
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("111111111111"),
                                partition="aws",
                                environment="prod",
                            ),
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("222222222222"),
                                partition="aws-cn",
                                environment="prod",
                            ),
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("333333333333"),
                                partition="aws",
                                environment="dev",
                            ),
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("444444444444"),
                                partition="aws-cn",
                                environment="dev",
                            ),
                        },
                    ),
                    "resources": ConfigFile.Account.PurposeEntry(
                        account_instances={
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("555555555555"),
                                partition="aws",
                                environment="prod",
                                hub="global",
                                stage=Stage.int.value,
                                stage_priority=0,
                            ),
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("666666666666"),
                                partition="aws",
                                environment="prod",
                                hub="global",
                                stage=Stage.int.value,
                                stage_priority=1,
                            ),
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("777777777777"),
                                partition="aws-cn",
                                environment="prod",
                                hub="cn",
                                stage=Stage.dev.value,
                                stage_priority=0,
                            ),
                        },
                    ),
                    "security": ConfigFile.Account.PurposeEntry(
                        account_instances={
                            Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                id=AccountId("888888888888"), partition="aws", environment="prod", hub="global"
                            ),
                        },
                    ),
                },
            ),
        )
    ],
    indirect=True,
)
class TestAccountStore:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        AccountStore.clear_all_caches()
        self.test_accounts = [
            HubAccount(
                purpose=AccountPurpose("test"),
                partition=Partition("aws-cn"),
                id=AccountId("000000000000"),
                environment=Environment("prod"),
                hub=Hub("cn"),
            )
        ]
        self.api_accounts = [
            BaseAccount(
                id=AccountId("111111111111"),
                partition=Partition("aws"),
                environment=Environment("prod"),
                purpose=AccountPurpose("api"),
            ),
            BaseAccount(
                id=AccountId("222222222222"),
                partition=Partition("aws-cn"),
                environment=Environment("prod"),
                purpose=AccountPurpose("api"),
            ),
            BaseAccount(
                id=AccountId("333333333333"),
                partition=Partition("aws"),
                environment=Environment("dev"),
                purpose=AccountPurpose("api"),
            ),
            BaseAccount(
                id=AccountId("444444444444"),
                partition=Partition("aws-cn"),
                environment=Environment("dev"),
                purpose=AccountPurpose("api"),
            ),
        ]
        self.resource_accounts = [
            ResourceAccount(
                id=AccountId("555555555555"),
                hub=Hub("global"),
                stage=Stage.int,
                partition=Partition("aws"),
                purpose=AccountPurpose("resources"),
                environment=Environment("prod"),
                stage_priority=0,
            ),
            ResourceAccount(
                id=AccountId("666666666666"),
                hub=Hub("global"),
                stage=Stage.int,
                partition=Partition("aws"),
                purpose=AccountPurpose("resources"),
                environment=Environment("prod"),
                stage_priority=1,
            ),
            ResourceAccount(
                id=AccountId("777777777777"),
                hub=Hub("cn"),
                stage=Stage.dev,
                partition=Partition("aws-cn"),
                purpose=AccountPurpose("resources"),
                environment=Environment("prod"),
                stage_priority=0,
            ),
        ]
        self.security_accounts = [
            SecurityAccount(
                purpose=AccountPurpose("security"),
                partition=Partition("aws"),
                id=AccountId("888888888888"),
                environment=Environment("prod"),
                hub=Hub("global"),
            )
        ]
        self.accounts = [*self.test_accounts, *self.api_accounts, *self.resource_accounts, *self.security_accounts]

    def test_get_all_accounts(self, mock_config_file: ConfigFile) -> None:
        assert AccountStore().get_all_accounts() == frozenset(self.accounts)

    def test_query_accounts(self, mock_config_file: ConfigFile) -> None:
        assert AccountStore().query_accounts(account_ids=self.accounts[0].id, environments=frozenset(Environment)) == {
            self.accounts[0]
        }
        assert AccountStore().query_accounts(
            hubs=self.resource_accounts[0].hub, environments=frozenset(Environment)
        ) == {self.resource_accounts[0], self.resource_accounts[1], self.security_accounts[0]}
        assert AccountStore().query_accounts(
            stages=self.resource_accounts[0].stage, environments=frozenset(Environment)
        ) == {self.resource_accounts[0], self.resource_accounts[1]}
        assert AccountStore().query_accounts(
            partitions=self.api_accounts[1].partition, environments=frozenset(Environment)
        ) == {self.test_accounts[0], self.api_accounts[1], self.api_accounts[3], self.resource_accounts[2]}

    def test_query_resource_accounts(self, mock_config_file: ConfigFile) -> None:
        for account in AccountStore().query_resource_accounts(environments=frozenset(Environment)):
            assert account.purpose == AccountPurpose("resources")

    def test_query_resource_account_defaults(self, mock_config_file: ConfigFile) -> None:
        assert AccountStore().query_resource_accounts(
            environments=frozenset(Environment), only_default=True
        ) == frozenset({self.resource_accounts[1], self.resource_accounts[2]})

    def test_query_account_one_match(self, mock_config_file: ConfigFile) -> None:
        assert (
            AccountStore().query_account(account_ids=self.accounts[0].id, environments=self.accounts[0].environment)
            == self.accounts[0]
        )

    def test_query_account_more_than_one_match(self, mock_config_file: ConfigFile) -> None:
        with pytest.raises(QueryAccountAmbiguous) as err:
            AccountStore().query_account(
                account_purposes=AccountPurpose("resources"), environments=frozenset(Environment)
            )
        assert err.value.accounts == frozenset(self.resource_accounts)

    def test_query_account_no_match(self, mock_config_file: ConfigFile) -> None:
        with pytest.raises(QueryAccountNotFound):
            AccountStore().query_account(
                account_purposes=AccountPurpose("security"),
                environments=frozenset(Environment),
                partitions=Partition("aws-cn"),
            )

    def test_query_resource_account(self, mock_config_file: ConfigFile) -> None:
        with pytest.raises(QueryAccountNotFound):
            AccountStore().query_resource_account(environments=frozenset(Environment), stages=Stage.prod)
        with pytest.raises(QueryAccountAmbiguous):
            AccountStore().query_resource_account(environments=frozenset(Environment))
        assert self.resource_accounts[2] == AccountStore().query_resource_account(
            environments=Environment("prod"), stages=Stage.dev
        )

    def test_get_security_account_for_hub(self, mock_config_file: ConfigFile) -> None:
        assert AccountStore().get_security_account_for_hub(hub=Hub("global")) == self.security_accounts[0]
        assert AccountStore().get_security_account_for_hub(hub=Hub("cn")) == self.security_accounts[0]
