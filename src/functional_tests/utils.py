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
from random import randint
from typing import cast
from typing import List

import boto3

from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import HubAccount
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId

MAIN_TEST_ACCOUNT_ID = AccountId("978627400017")


def get_current_test_account(partition: Partition, environment: Environment) -> HubAccount:
    """Return the ID of the currently used test account."""
    current_account_id = AccountId(boto3.client("sts").get_caller_identity()["Account"])

    return _get_test_account(partition, environment, current_account_id)


def get_main_test_account(partition: Partition, environment: Environment) -> HubAccount:
    """Return the ID of the main test account."""
    if environment is Environment("dev"):
        # On DEV, there are two test accounts, but one of them is special as it owns
        # resources used in known_data tests and thus also can not be deleted in
        # integration tests
        return _get_test_account(partition, environment, MAIN_TEST_ACCOUNT_ID)

    return get_current_test_account(partition, environment)


def _get_test_account(partition: Partition, environment: Environment, account_id: AccountId) -> HubAccount:
    return cast(
        HubAccount,
        AccountStore().query_account(
            account_ids=account_id,
            account_purposes=AccountPurpose("test"),
            partitions=partition,
            environments=environment,
        ),
    )


def get_stages(hub: Hub, environment: Environment) -> List[Stage]:
    """Return a list of stages of the resource accounts for the given hub and environment."""
    return list(
        {account.stage for account in AccountStore().query_resource_accounts(hubs=hub, environments=environment)}
    )


def add_random_suffix(text: str) -> str:
    """Suffix a string with a random integer."""
    return f"{text}{randint(1000, 9999)}"
