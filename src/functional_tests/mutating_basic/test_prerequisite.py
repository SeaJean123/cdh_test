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
# pylint: disable=redefined-outer-name
from http import HTTPStatus

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.clients.http_client import HttpStatusCodeNotInExpectedCodes
from cdh_core.entities.accounts import HubAccount
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.primitives.account_id import AccountId
from functional_tests.mutating_basic.conftest import MutatingBasicTestConfig
from functional_tests.mutating_basic.conftest import MutatingBasicTestSetup
from functional_tests.utils import get_stages


def test_register_necessary_accounts(
    mutating_basic_test_config: MutatingBasicTestConfig, mutating_basic_test_setup: MutatingBasicTestSetup
) -> None:
    for account in mutating_basic_test_config.test_accounts:
        _register_test_account(mutating_basic_test_config, mutating_basic_test_setup, account)


def test_deregister_account(
    mutating_basic_test_config: MutatingBasicTestConfig, mutating_basic_test_setup: MutatingBasicTestSetup
) -> None:
    account = mutating_basic_test_setup.test_consumer_account
    _register_test_account(mutating_basic_test_config, mutating_basic_test_setup, account, AccountType.usecase)
    assert _account_exists(account_id=account.id, core_api_client=mutating_basic_test_setup.core_api_client)

    mutating_basic_test_setup.core_api_client.deregister_account(account.id)
    if _account_exists(account_id=account.id, core_api_client=mutating_basic_test_setup.core_api_client):
        raise Exception(f"Account {account.id!r} should be unregistered!")

    _register_test_account(mutating_basic_test_config, mutating_basic_test_setup, account, AccountType.usecase)


def _account_exists(account_id: AccountId, core_api_client: CoreApiClient) -> bool:
    try:
        core_api_client.get_account(account_id)
        return True
    except HttpStatusCodeNotInExpectedCodes as error:
        if error.status_code is HTTPStatus.NOT_FOUND:
            return False
        raise


# This function should not be used when tests are run in parallel with mutating integration tests,
# since they both make changes to our test account for mutating tests
def _register_test_account(
    mutating_basic_test_config: MutatingBasicTestConfig,
    mutating_basic_test_setup: MutatingBasicTestSetup,
    account: HubAccount,
    account_type: AccountType = AccountType.provider,
) -> None:
    mutating_basic_test_setup.core_api_client.register_account(
        account_id=account.id,
        admin_roles=mutating_basic_test_config.admin_roles,
        affiliation=Affiliation("cdh"),
        business_objects=list(BusinessObject),
        friendly_name=account.gen_friendly_name,
        hub=account.hub,
        layers=list(Layer),
        responsibles=[],
        roles=mutating_basic_test_config.roles,
        stages=get_stages(account.hub, mutating_basic_test_config.environment),
        type=account_type,
        visible_in_hubs=[],
        fail_if_exists=False,
    )
