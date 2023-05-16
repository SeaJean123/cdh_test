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
import itertools
import json
import random
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from typing import Any
from typing import Collection
from typing import Dict
from typing import Sequence

import pytest
from asserts import assert_count_equal
from cdh_core_api.catalog.accounts_table import AccountAlreadyExists
from cdh_core_api.catalog.accounts_table import AccountModel
from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.base_test import get_nullable_attributes
from mypy_boto3_dynamodb.service_resource import Table

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.accounts_test import build_account_role
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class AccountsTableTest:
    @pytest.fixture(autouse=True)
    def dynamo_setup(self, resource_name_prefix: str, mock_accounts_dynamo_table: Table) -> None:
        self.mock_accounts_dynamo_table = mock_accounts_dynamo_table
        self.resources_table = AccountsTable(resource_name_prefix)
        self.account_id = build_account_id()
        self.account = build_account(account_id=self.account_id)
        self.dt_now = datetime.now()
        self.accounts_table = AccountsTable(resource_name_prefix)


class TestAccount(AccountsTableTest):
    def test_create_account(self) -> None:
        account = build_account(
            account_id=self.account_id,
            business_objects=list(Builder.choose_without_repetition(list(BusinessObject), 2)),
            # We use integers for cost-related numbers because dynamo uses a Decimal object internally
            # and slightly modifies the float during conversion by adding high-digit noise...
            cost_history={Builder.build_random_string(): random.randint(0, 100)},
            data={
                Builder.build_random_string(): random.randint(0, 100),
                Builder.build_random_string(): [Builder.build_random_string()],
            },
            estimated_cost=random.randint(0, 100),
            forecasted_cost=random.randint(0, 100),
            friendly_name=Builder.build_random_string(),
            group=Builder.build_random_string(),
            layers=list(Builder.choose_without_repetition(list(Layer), 3)),
            provider_role_arns={build_account_id(): build_role_arn(), build_account_id(): build_role_arn()},
            responsibles=[Builder.build_random_string(), Builder.build_random_string()],
            request_id=Builder.build_random_string(),
            roles=[build_account_role() for _ in range(2)],
            stages=list(Builder.choose_without_repetition(list(Stage), 3)),
            visible_in_hubs=[build_hub(), build_hub()],
            update_date=Builder.build_random_datetime(),
        )

        self.accounts_table.create(account)

        dynamo_raw = self.mock_accounts_dynamo_table.scan()
        dynamo_item = dynamo_raw["Items"][0]
        assert dynamo_item == build_dynamo_json(account)

    def test_create_account_minimal(self) -> None:
        account = build_account(self.account_id)
        self.accounts_table.create(account)

        dynamo_raw = self.mock_accounts_dynamo_table.scan()
        dynamo_item = dynamo_raw["Items"][0]
        assert dynamo_item == build_dynamo_json(account)

    def test_get_account(self) -> None:
        self.mock_accounts_dynamo_table.put_item(Item=build_dynamo_json(self.account))
        account_from_dynamo = self.accounts_table.get(self.account_id)
        assert account_from_dynamo == self.account

    def test_get_all_nullable_fields_none(self) -> None:
        account = build_account()
        dynamo_item = build_dynamo_json(account)
        for attribute in get_nullable_attributes(AccountModel):
            dynamo_item.pop(attribute, None)
        self.mock_accounts_dynamo_table.put_item(Item=dynamo_item)

        self.accounts_table.get(account.id)  # no exception is raised

    def test_create_and_get_account(self) -> None:
        self.accounts_table.create(self.account)
        account_from_db = self.accounts_table.get(self.account_id)
        assert account_from_db == self.account

    def test_create_existing_account_fails(self) -> None:
        account = build_account(self.account_id)
        self.accounts_table.create(account)
        with pytest.raises(AccountAlreadyExists):
            self.accounts_table.create(account)

    def test_get_nonexistent_account_fails(self) -> None:
        with pytest.raises(AccountNotFound):
            self.accounts_table.get(self.account_id)

    def test_get_all_accounts(self) -> None:
        account_id1 = build_account_id()
        account_id2 = build_account_id()
        self.accounts_table.create(build_account(account_id1))
        self.accounts_table.create(build_account(account_id2))
        account_list = self.accounts_table.get_all_accounts()
        assert len(account_list) == 2
        assert {account.id for account in account_list} == {account_id1, account_id2}

    def test_update_missing_account(self) -> None:
        new_hub = Builder.get_random_element(Hub, exclude=[self.account.hub])

        with pytest.raises(AccountNotFound):
            self.accounts_table.update(account_id=self.account_id, update_date=self.dt_now, hub=new_hub)

    def test_update_no_changes(self) -> None:
        self.accounts_table.create(self.account)

        updated_account = self.accounts_table.update(account_id=self.account_id, update_date=datetime.now())

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == self.account

    def test_update_admin_roles(self) -> None:
        self.accounts_table.create(self.account)
        new_admin_roles = ["some", "roles"]

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, admin_roles=new_admin_roles)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, admin_roles=new_admin_roles, update_date=self.dt_now)

    def test_update_stages(self) -> None:
        self.accounts_table.create(self.account)
        new_stages = list(Builder.choose_without_repetition(list(Stage), 3))

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, stages=new_stages)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, stages=new_stages, update_date=self.dt_now)

    def test_update_type(self) -> None:
        self.accounts_table.create(self.account)
        new_type = Builder.get_random_element(list(AccountType), [self.account.type])
        updated_account = self.accounts_table.update(self.account_id, self.dt_now, type=new_type)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, type=new_type, update_date=self.dt_now)

    def test_update_group(self) -> None:
        self.accounts_table.create(self.account)
        group = Builder.build_random_string()

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, group=group)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, group=group, update_date=self.dt_now)

    def test_update_group_ignores_empty_string(self) -> None:
        self.accounts_table.create(self.account)

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, group="")

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == self.account

    def test_update_business_objects(self) -> None:
        self.accounts_table.create(self.account)
        new_bo = list(Builder.choose_without_repetition(list(BusinessObject), 2))

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, business_objects=new_bo)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, business_objects=new_bo, update_date=self.dt_now)

    def test_update_affiliation(self) -> None:
        self.accounts_table.create(self.account)
        new_affiliation = Builder.get_random_element(list(Affiliation), [self.account.affiliation])

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, affiliation=new_affiliation)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, affiliation=new_affiliation, update_date=self.dt_now)

    def test_update_layers(self) -> None:
        self.accounts_table.create(self.account)
        new_layers = list(Builder.choose_without_repetition(list(Layer), 3))

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, layers=new_layers)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, layers=new_layers, update_date=self.dt_now)

    def test_update_roles(self) -> None:
        self.accounts_table.create(self.account)
        new_roles = [build_account_role() for _ in range(3)]

        updated_account = self.accounts_table.update(self.account_id, self.dt_now, roles=new_roles)

        assert updated_account == self.accounts_table.get(self.account_id)
        assert updated_account == replace(self.account, roles=new_roles, update_date=self.dt_now)

    def test_delete_account(self) -> None:
        self.mock_accounts_dynamo_table.put_item(Item=build_dynamo_json(self.account))
        assert len(self.mock_accounts_dynamo_table.scan()["Items"]) == 1
        self.accounts_table.delete(self.account.id)
        assert len(self.mock_accounts_dynamo_table.scan()["Items"]) == 0

    def test_delete_nonexisting_account(self) -> None:
        with pytest.raises(AccountNotFound):
            self.accounts_table.delete(build_account_id())


@pytest.mark.usefixtures("mock_accounts_dynamo_table")
class TestGetAccountIterator(AccountsTableTest):
    def test_iterate_empty_dynamo(
        self,
    ) -> None:
        iterator = self.accounts_table.get_accounts_iterator()
        with pytest.raises(StopIteration):
            next(iterator)

    def test_iterate_all(self) -> None:
        expected_accounts = [build_account() for _ in range(5)]
        self._fill_dynamo(accounts=expected_accounts)

        iterator = self.accounts_table.get_accounts_iterator()

        assert_count_equal(list(iterator), expected_accounts)
        assert iterator.last_evaluated_key is None

    def test_last_evaluated_mid_iteration_not_none(self) -> None:
        expected_accounts = [build_account() for _ in range(5)]
        self._fill_dynamo(accounts=expected_accounts)

        iterator = self.accounts_table.get_accounts_iterator()

        iteration_over = False
        for _ in iterator:
            assert not iteration_over
            iteration_over = iterator.last_evaluated_key is None

    def test_iterate_with_last_evaluated_key_resumes(self) -> None:
        expected_accounts = [build_account() for _ in range(10)]
        self._fill_dynamo(accounts=expected_accounts)
        cutoff = random.randint(1, len(expected_accounts) - 1)
        first_iterator = self.accounts_table.get_accounts_iterator()
        for _ in range(cutoff):
            next(first_iterator)

        second_iterator = self.accounts_table.get_accounts_iterator(
            last_evaluated_key=first_iterator.last_evaluated_key
        )

        for first_item, second_item in itertools.zip_longest(first_iterator, second_iterator):
            assert first_item == second_item
            assert first_iterator.last_evaluated_key == second_iterator.last_evaluated_key

    def _fill_dynamo(self, accounts: Collection[Account]) -> None:
        shuffled_accounts: Sequence[Account] = random.sample(list(accounts), len(accounts))
        with self.mock_accounts_dynamo_table.batch_writer() as batch:
            for account in shuffled_accounts:
                batch.put_item(build_dynamo_json(account))


def build_dynamo_json(account: Account) -> Dict[str, Any]:
    dynamo_dict = {
        "account_id": account.id,
        "admin_roles": account.admin_roles,
        "affiliation": account.affiliation.value,
        "business_objects": [bo.value for bo in account.business_objects],
        "data": json.dumps(account.data),
        "friendly_name": account.friendly_name,
        "hub": account.hub.value,
        "layers": [layer.value for layer in account.layers],
        "provider_role_arns": {account_id: str(arn) for account_id, arn in account.provider_role_arns.items()},
        "stages": [stage.value for stage in account.stages],
        "type": account.type.value,
        "visible_in_hubs": [hub.value for hub in account.visible_in_hubs] or [],
        "responsibles": account.responsibles,
        "roles": [
            {"name": role.name, "path": role.path, "type": role.type.value, "friendly_name": role.friendly_name}
            for role in account.roles
        ],
    }
    if account.creation_date:
        dynamo_dict["creation_date"] = account.creation_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    if account.update_date:
        dynamo_dict["update_date"] = account.update_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    if account.group:
        dynamo_dict["group"] = account.group
    if account.cost_history is not None:
        dynamo_dict["cost_history"] = {str(key): Decimal(value) for key, value in account.cost_history.items()}
    if account.estimated_cost is not None:
        dynamo_dict["estimated_cost"] = Decimal(account.estimated_cost)
    if account.forecasted_cost is not None:
        dynamo_dict["forecasted_cost"] = Decimal(account.forecasted_cost)
    if account.request_id:
        dynamo_dict["request_id"] = account.request_id
    return dynamo_dict
