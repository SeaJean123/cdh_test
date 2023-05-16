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
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from datetime import datetime
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

from dataclasses_json import config
from marshmallow import fields as mm_fields

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.dates import date_input
from cdh_core.dates import date_output
from cdh_core.entities.arn import Arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.aws import Partition
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.optionals import apply_if_not_none
from cdh_core.primitives.account_id import AccountId

AnyResponseAccountWithoutCosts = TypeVar("AnyResponseAccountWithoutCosts", bound="ResponseAccountWithoutCosts")


@dataclass(frozen=True)
class BaseAccount:
    """Most basic type of account."""

    id: AccountId  # pylint: disable=invalid-name
    environment: Environment
    partition: Partition
    purpose: AccountPurpose

    @property
    def gen_friendly_name(self) -> str:
        """Return a human friendly name."""
        return f"{self._gen_friendly_name_prefix} ({self.id})"

    @property
    def _gen_friendly_name_prefix(self) -> str:
        return f"CDH {self.purpose.value.capitalize()}"

    @property
    def alias(self) -> str:
        """Return a machine friendly name which we use for cdh-core-internal Terraform deployments."""
        return f"{self._alias_prefix}-{self.id}"

    @property
    def _alias_prefix(self) -> str:
        partition = f"{self.partition.short_name}-" if self.partition.short_name else ""
        return f"{self.environment.value}-{partition}{self.purpose.value}"


@dataclass(frozen=True)
class HubAccount(BaseAccount):
    """Accounts which are bound to a specific hub."""

    hub: Hub

    @property
    def _gen_friendly_name_prefix(self) -> str:
        return f"{super()._gen_friendly_name_prefix} {self.hub.value.upper()}"

    @property
    def _alias_prefix(self) -> str:
        return f"{super()._alias_prefix}-{self.hub.value}"


@dataclass(frozen=True)
class ResourceAccount(HubAccount):
    """
    Accounts containing the CDH-managed resources.

    Each resource account is specific to a hub and a stage. If there are multiple accounts for a hub/stage combination,
    the resource account with the highest priority (highest number) will be used for creating new resources.
    """

    stage: Stage
    stage_priority: int = 0

    def get_assumable_role_arn_for_core_api(self, prefix: str) -> Arn:
        """Return the AWS role to assume this resource account."""
        role_name = f"{prefix}cdh-core-api-assumable-resources"
        return Arn.get_role_arn(partition=self.partition, account_id=self.id, path="/", name=role_name)

    @property
    def _gen_friendly_name_prefix(self) -> str:
        return f"{super()._gen_friendly_name_prefix} {self.stage.value.upper()}"

    @property
    def _alias_prefix(self) -> str:
        return f"{super()._alias_prefix}-{self.stage.value}"


@dataclass(frozen=True)
class SecurityAccount(HubAccount):
    """Central account containing KMS keys.

    The API needs to be able to manage these keys via an assumable IAM role.
    """

    def get_assumable_role_arn_for_core_api(self, environment: Environment) -> Arn:
        """Return the AWS role the API can assume in this security account."""
        role_name = f"cdh-core-api-assumable-security-{environment.value}"
        return Arn.get_role_arn(partition=self.partition, account_id=self.id, path="/", name=role_name)


class AccountRoleType(Enum):
    """Type of role in an AWS account."""

    READ = "READ"
    WRITE = "WRITE"

    @property
    def friendly_name(self) -> str:
        """Return the friendly name of the role type."""
        return self.value.title()  # pylint: disable=no-member


@dataclass(frozen=True)
class AccountRole(DataClassJsonCDHMixin):
    """Basic attributes for a role in an AWS account."""

    name: str
    path: str
    type: AccountRoleType
    friendly_name: str


@dataclass(frozen=True)
class AccountAttributesWithoutCosts:
    """Basic account attributes excluding costs."""

    id: AccountId  # pylint: disable=invalid-name
    admin_roles: List[str]
    affiliation: Affiliation
    business_objects: List[BusinessObject]
    creation_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )
    data: Dict[str, Any]
    friendly_name: str
    group: Optional[str]
    hub: Hub
    layers: List[Layer]
    responsibles: List[str]
    request_id: Optional[str]
    roles: List[AccountRole]
    stages: List[Stage]
    type: AccountType
    update_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )
    visible_in_hubs: List[Hub]


ResponseAccountClass = TypeVar("ResponseAccountClass", bound=DataClassJsonCDHMixin)


@dataclass(frozen=True)
class Account(AccountAttributesWithoutCosts):
    """An AWS account that can be used in the context of Cloud Data Hub.

    For such an account to interact with CDH, e.g. get access to or create datasets and resources,
    it needs to be registered via the CDH API first.
    """

    cost_history: Optional[Dict[str, float]]
    estimated_cost: Optional[float]
    forecasted_cost: Optional[float]
    provider_role_arns: Dict[AccountId, Arn]

    @property
    def friendly_name_and_id(self) -> str:
        """Output accounts in the preferred format, e.g. in error messages."""
        return f"{self.friendly_name} ({self.id})"

    def to_response_account(self, response_account_class: Type[ResponseAccountClass]) -> ResponseAccountClass:
        """Convert the account to a specific 'ResponseAccountClass' used for transfer to a client."""
        account_dict = {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name in {g.name for g in fields(response_account_class)}  # type: ignore[arg-type]
        }
        try:
            return response_account_class(**account_dict)
        except TypeError as error:
            raise TypeError(
                f"Cannot generate response account type {response_account_class.__name__} from account type "
                f"{type(self).__name__}"
            ) from error


@dataclass(frozen=True)
class ResponseAccountWithoutCosts(AccountAttributesWithoutCosts, DataClassJsonCDHMixin):
    """This class is used to transfer an account between the server and client, without the cost information."""


def round_cost_info(amount: float) -> float:
    """Round cost info to the relevant decimal places."""
    return round(amount, 2)


@dataclass(frozen=True)
class ResponseAccount(ResponseAccountWithoutCosts):
    """This class is used to transfer an account between the server and client."""

    cost_history: Optional[Dict[str, float]] = field(
        metadata=config(
            encoder=lambda cost_history: {key: round_cost_info(amount) for key, amount in cost_history.items()}
            if cost_history
            else {}
        )
    )
    estimated_cost: Optional[float] = field(metadata=config(encoder=apply_if_not_none(round_cost_info)))
    forecasted_cost: Optional[float] = field(metadata=config(encoder=apply_if_not_none(round_cost_info)))


@dataclass(frozen=True)
class ResponseAccounts(DataClassJsonCDHMixin):
    """This class is used to transfer multiple accounts between the server and client."""

    accounts: List[ResponseAccountWithoutCosts]


@dataclass(frozen=True)
class Credentials:
    """This class represents security credentials that can be used to access an AWS account."""

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
