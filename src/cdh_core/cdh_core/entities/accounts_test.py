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
import random
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.entities.accounts import BaseAccount
from cdh_core.entities.accounts import HubAccount
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.accounts import ResponseAccount
from cdh_core.entities.accounts import ResponseAccountWithoutCosts
from cdh_core.entities.accounts import SecurityAccount
from cdh_core.entities.arn import Arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.accounts_test import build_account_purpose
from cdh_core.enums.accounts_test import build_account_type
from cdh_core.enums.accounts_test import build_affiliation
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties_test import build_layer
from cdh_core.enums.environment import Environment
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder

# pylint: disable=unused-argument


def build_base_account(
    account_id: Optional[AccountId] = None,
    environment: Optional[Environment] = None,
    partition: Optional[Partition] = None,
    purpose: Optional[AccountPurpose] = None,
) -> BaseAccount:
    return BaseAccount(
        id=account_id or build_account_id(),
        environment=environment or build_environment(),
        partition=partition or build_partition(),
        purpose=purpose or random.choice([p for p in list(AccountPurpose) if not p.hub_specific]),
    )


def build_hub_account(
    hub: Optional[Hub] = None,
    account_id: Optional[AccountId] = None,
    environment: Optional[Environment] = None,
    partition: Optional[Partition] = None,
    purpose: Optional[AccountPurpose] = None,
) -> HubAccount:
    return HubAccount(
        hub=hub or build_hub(),
        id=account_id or build_account_id(),
        environment=environment or build_environment(),
        partition=partition or build_partition(),
        purpose=purpose or random.choice([p for p in list(AccountPurpose) if not p.hub_specific]),
    )


def build_resource_account(
    hub: Optional[Hub] = None,
    stage: Optional[Stage] = None,
    account_id: Optional[AccountId] = None,
    environment: Optional[Environment] = None,
    stage_priority: int = 0,
    partition: Optional[Partition] = None,
) -> ResourceAccount:
    return ResourceAccount(
        stage=stage or build_stage(),
        id=account_id or build_account_id(),
        hub=hub or build_hub(),
        environment=environment or build_environment(),
        stage_priority=stage_priority,
        partition=partition or build_partition(),
        purpose=AccountPurpose("resources"),
    )


def build_security_account(
    hub: Optional[Hub] = None,
    account_id: Optional[AccountId] = None,
    environment: Optional[Environment] = None,
    partition: Optional[Partition] = None,
) -> SecurityAccount:
    return SecurityAccount(
        id=account_id or build_account_id(),
        hub=hub or build_hub(),
        environment=environment or build_environment(),
        partition=partition or build_partition(),
        purpose=AccountPurpose("security"),
    )


def build_account_role(account_type: Optional[AccountRoleType] = None) -> AccountRole:
    return AccountRole(
        name=Builder.build_random_string(),
        path=f"/{Builder.build_random_string()}/",
        type=account_type or random.choice(list(AccountRoleType)),
        friendly_name=Builder.build_random_string(),
    )


@pytest.mark.parametrize(
    "mock_config_file",
    [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS],
    indirect=True,
)
class TestAccounts:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        self.account_id = build_account_id()
        self.partition = Partition("aws-cn")
        self.purpose = AccountPurpose("api")
        self.hub = Hub("cn")
        self.environment = Environment("prod")
        self.stage = Stage("int")
        self.base_account = BaseAccount(
            id=self.account_id,
            environment=self.environment,
            partition=self.partition,
            purpose=self.purpose,
        )
        self.hub_account = HubAccount(
            id=self.account_id,
            environment=self.environment,
            partition=self.partition,
            purpose=self.purpose,
            hub=self.hub,
        )
        self.resource_account = ResourceAccount(
            id=self.account_id,
            environment=self.environment,
            partition=self.partition,
            purpose=self.purpose,
            hub=self.hub,
            stage=self.stage,
        )

    def test_gen_friendly_name(self, mock_config_file: ConfigFile) -> None:
        assert self.base_account.gen_friendly_name == f"CDH Api ({self.account_id})"
        assert self.hub_account.gen_friendly_name == f"CDH Api CN ({self.account_id})"
        assert self.resource_account.gen_friendly_name == f"CDH Api CN INT ({self.account_id})"

    def test_alias(self, mock_config_file: ConfigFile) -> None:
        assert self.base_account.alias == f"prod-cn-api-{self.account_id}"
        assert self.hub_account.alias == f"prod-cn-api-cn-{self.account_id}"
        assert self.resource_account.alias == f"prod-cn-api-cn-int-{self.account_id}"

    def test_alias_correct_for_all_combinations(self, mock_config_file: ConfigFile) -> None:
        for partition in Partition:
            for environment in Environment:
                for purpose in AccountPurpose:
                    alias = BaseAccount(
                        id=AccountId("123"),
                        environment=environment,
                        purpose=purpose,
                        partition=partition,
                    ).alias
                    assert "--" not in alias

    def test_resource_account_get_assumable_role_name(self, mock_config_file: ConfigFile) -> None:
        account_id = build_account_id()
        partition = build_partition()
        account = ResourceAccount(
            id=account_id,
            environment=build_environment(),
            partition=partition,
            purpose=build_account_purpose(),
            hub=build_hub(),
            stage=build_stage(),
        )
        prefix = Builder.build_random_string()
        assert account.get_assumable_role_arn_for_core_api(prefix) == Arn(
            f"arn:{partition.value}:iam::{account_id}:role/{prefix}cdh-core-api-assumable-resources"
        )

    def test_security_account_assumable_role_name(self, mock_config_file: ConfigFile) -> None:
        account_id = build_account_id()
        partition = build_partition()
        environment = build_environment()
        account = SecurityAccount(
            id=account_id,
            environment=environment,
            partition=partition,
            purpose=build_account_purpose(),
            hub=build_hub(),
        )
        assert account.get_assumable_role_arn_for_core_api(environment=environment) == Arn(
            f"arn:{partition.value}:iam::{account_id}:role/cdh-core-api-assumable-security-{environment.value}"
        )


def build_account(
    account_id: Optional[AccountId] = None,
    admin_roles: Optional[List[str]] = None,
    business_objects: Optional[List[BusinessObject]] = None,
    data: Optional[Dict[str, Any]] = None,
    account_type: Optional[AccountType] = None,
    group: Optional[str] = None,
    stages: Optional[List[Stage]] = None,
    provider_role_arns: Optional[Dict[AccountId, Arn]] = None,
    affiliation: Optional[Affiliation] = None,
    hub: Optional[Hub] = None,
    responsibles: Optional[List[str]] = None,
    request_id: Optional[str] = None,
    roles: Optional[List[AccountRole]] = None,
    friendly_name: Optional[str] = None,
    layers: Optional[List[Layer]] = None,
    cost_history: Optional[Dict[str, float]] = None,
    estimated_cost: Optional[float] = None,
    forecasted_cost: Optional[float] = None,
    visible_in_hubs: Optional[List[Hub]] = None,
    creation_date: Optional[datetime] = None,
    update_date: Optional[datetime] = None,
) -> Account:
    account_id = account_id or build_account_id()
    creation_date = creation_date or datetime.now()
    return Account(
        id=account_id,
        admin_roles=admin_roles if admin_roles is not None else [Builder.build_random_string() for _ in range(3)],
        business_objects=business_objects or [],
        cost_history=cost_history or {},
        data=data or {},
        estimated_cost=estimated_cost,
        forecasted_cost=forecasted_cost,
        friendly_name=friendly_name or account_id,
        group=group,
        hub=hub or build_hub(),
        layers=layers or [build_layer()],
        provider_role_arns=provider_role_arns or {},
        responsibles=responsibles or [],
        request_id=request_id,
        roles=roles or [],
        stages=stages or [],
        type=account_type or build_account_type(),
        affiliation=affiliation or build_affiliation(),
        visible_in_hubs=visible_in_hubs or [],
        creation_date=creation_date,
        update_date=update_date or creation_date,
    )


def build_response_account(
    account_id: Optional[AccountId] = None,
    admin_roles: Optional[List[str]] = None,
    business_objects: Optional[List[BusinessObject]] = None,
    data: Optional[Dict[str, Any]] = None,
    account_type: Optional[AccountType] = None,
    group: Optional[str] = None,
    stages: Optional[List[Stage]] = None,
    provider_role_arns: Optional[Dict[AccountId, Arn]] = None,
    affiliation: Optional[Affiliation] = None,
    hub: Optional[Hub] = None,
    responsibles: Optional[List[str]] = None,
    request_id: Optional[str] = None,
    roles: Optional[List[AccountRole]] = None,
    friendly_name: Optional[str] = None,
    layers: Optional[List[Layer]] = None,
    cost_history: Optional[Dict[str, float]] = None,
    estimated_cost: Optional[float] = None,
    forecasted_cost: Optional[float] = None,
    visible_in_hubs: Optional[List[Hub]] = None,
    creation_date: Optional[datetime] = None,
    update_date: Optional[datetime] = None,
) -> ResponseAccount:
    return build_account(
        account_id=account_id,
        admin_roles=admin_roles,
        business_objects=business_objects,
        data=data,
        account_type=account_type,
        group=group,
        stages=stages,
        provider_role_arns=provider_role_arns,
        affiliation=affiliation,
        hub=hub,
        responsibles=responsibles,
        request_id=request_id,
        roles=roles,
        friendly_name=friendly_name,
        layers=layers,
        cost_history=cost_history,
        estimated_cost=estimated_cost,
        forecasted_cost=forecasted_cost,
        visible_in_hubs=visible_in_hubs,
        creation_date=creation_date,
        update_date=update_date,
    ).to_response_account(ResponseAccount)


def build_response_account_without_costs(
    account_id: Optional[AccountId] = None,
    admin_roles: Optional[List[str]] = None,
    business_objects: Optional[List[BusinessObject]] = None,
    data: Optional[Dict[str, Any]] = None,
    account_type: Optional[AccountType] = None,
    group: Optional[str] = None,
    stages: Optional[List[Stage]] = None,
    provider_role_arns: Optional[Dict[AccountId, Arn]] = None,
    affiliation: Optional[Affiliation] = None,
    hub: Optional[Hub] = None,
    responsibles: Optional[List[str]] = None,
    request_id: Optional[str] = None,
    roles: Optional[List[AccountRole]] = None,
    friendly_name: Optional[str] = None,
    layers: Optional[List[Layer]] = None,
    cost_history: Optional[Dict[str, float]] = None,
    estimated_cost: Optional[float] = None,
    forecasted_cost: Optional[float] = None,
    visible_in_hubs: Optional[List[Hub]] = None,
    creation_date: Optional[datetime] = None,
    update_date: Optional[datetime] = None,
) -> ResponseAccountWithoutCosts:
    return build_account(
        account_id=account_id,
        admin_roles=admin_roles,
        business_objects=business_objects,
        data=data,
        account_type=account_type,
        group=group,
        stages=stages,
        provider_role_arns=provider_role_arns,
        affiliation=affiliation,
        hub=hub,
        responsibles=responsibles,
        request_id=request_id,
        roles=roles,
        friendly_name=friendly_name,
        layers=layers,
        cost_history=cost_history,
        estimated_cost=estimated_cost,
        forecasted_cost=forecasted_cost,
        visible_in_hubs=visible_in_hubs,
        creation_date=creation_date,
        update_date=update_date,
    ).to_response_account(ResponseAccountWithoutCosts)


class TestResponseAccount:
    account = build_account(
        cost_history={"2000-01": 1515454.46544879, "2000-02": 652544.32114},
        estimated_cost=567.89,
        forecasted_cost=123.345,
    )

    def test_with_costs(self) -> None:
        account_json = self.account.to_response_account(ResponseAccount).to_plain_dict()
        assert account_json["costHistory"] == {"2000-01": 1515454.47, "2000-02": 652544.32}
        assert account_json["estimatedCost"] == 567.89
        assert account_json["forecastedCost"] == 123.34

    def test_with_costs_not_set(self) -> None:
        account = replace(self.account, cost_history={}, estimated_cost=None, forecasted_cost=None)
        account_json = account.to_response_account(ResponseAccount).to_plain_dict()
        assert account_json["costHistory"] == {}
        assert account_json["estimatedCost"] is None
        assert account_json["forecastedCost"] is None

    def test_without_costs(self) -> None:
        account_json = self.account.to_response_account(ResponseAccountWithoutCosts).to_plain_dict()
        assert "costHistory" not in account_json
        assert "estimatedCost" not in account_json
        assert "forecastedCost" not in account_json

    def test_from_account_raises(self) -> None:
        @dataclass(frozen=True)
        class VerySpecificResponseAccount(ResponseAccountWithoutCosts):
            very_specific_attribute: str

        with pytest.raises(TypeError):
            self.account.to_response_account(VerySpecificResponseAccount)

    def test_inheritance(self) -> None:
        @dataclass(frozen=True)
        class NewAccount(Account):
            new_attribute: str

        @dataclass(frozen=True)
        class NewResponseAccount(ResponseAccount):
            new_attribute: str

        new_account = NewAccount(**asdict(self.account), new_attribute=Builder.build_random_string())

        response_account = new_account.to_response_account(NewResponseAccount)
        assert isinstance(response_account, NewResponseAccount)
        assert response_account.new_attribute == new_account.new_attribute
