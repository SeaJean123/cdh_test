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
# pylint: disable=invalid-name
from dataclasses import dataclass
from datetime import datetime
from typing import List
from typing import Optional

from cdh_core_api.api.validation import field
from cdh_core_api.validation.accounts import admin_roles_field
from cdh_core_api.validation.accounts import role_name_field
from cdh_core_api.validation.accounts import role_path_field
from cdh_core_api.validation.accounts import validate_account_friendly_name
from cdh_core_api.validation.accounts import validate_account_roles
from cdh_core_api.validation.base import responsibles_field

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId


@dataclass(frozen=True)
class AccountRoleBody:
    """Represents the attributes needed to build a account role."""

    friendlyName: str
    name: str = role_name_field()
    type: AccountRoleType = field()
    path: str = role_path_field()

    def to_account_role(self) -> AccountRole:
        """Convert the body to a corresponding AccountRole instance."""
        return AccountRole(name=self.name, path=self.path, type=self.type, friendly_name=self.friendlyName)


@dataclass(frozen=True)
class NewAccountBody:
    """Represents the attributes needed to register an account with the CDH."""

    id: AccountId
    affiliation: Affiliation
    businessObjects: List[BusinessObject]
    group: Optional[str]
    hub: Hub
    layers: List[Layer]
    stages: List[Stage]
    type: AccountType
    visibleInHubs: List[Hub]
    requestId: Optional[str]
    friendlyName: str = field(validator=validate_account_friendly_name)
    responsibles: List[str] = responsibles_field(can_be_none=False)
    adminRoles: Optional[List[str]] = admin_roles_field(can_be_none=True)
    roles: Optional[List[AccountRoleBody]] = field(default=None, validator=validate_account_roles)

    def to_account(self) -> Account:
        """Convert a NewAccountBody object to an Account object."""
        dt_now = datetime.now()

        roles = [role.to_account_role() for role in self.roles] if self.roles else self._get_default_roles()

        return Account(
            id=self.id,
            admin_roles=self.adminRoles or [],
            business_objects=self.businessObjects,
            cost_history=None,
            estimated_cost=None,
            forecasted_cost=None,
            data={},
            friendly_name=self.friendlyName,
            group=self.group,
            hub=self.hub,
            layers=self.layers,
            provider_role_arns={},
            responsibles=self.responsibles,
            request_id=self.requestId,
            roles=roles,
            stages=self.stages,
            type=self.type,
            affiliation=self.affiliation,
            visible_in_hubs=self.visibleInHubs,
            creation_date=dt_now,
            update_date=dt_now,
        )

    @staticmethod
    def _get_default_roles() -> List[AccountRole]:
        write_role_name = "CDHDevOps"
        read_role_name = "CDHReadOnly"

        return [
            AccountRole(
                name=write_role_name, path="/", type=AccountRoleType.WRITE, friendly_name=f"{write_role_name} (write)"
            ),
            AccountRole(
                name=read_role_name, path="/", type=AccountRoleType.READ, friendly_name=f"{read_role_name} (read)"
            ),
        ]


@dataclass(frozen=True)
class UpdateAccountBody:
    """Represents the attributes needed to update an account registered with the CDH."""

    affiliation: Optional[Affiliation] = field(default=None)
    businessObjects: Optional[List[BusinessObject]] = field(default=None)
    group: Optional[str] = field(default=None)
    layers: Optional[List[Layer]] = field(default=None)
    stages: Optional[List[Stage]] = field(default=None)
    type: Optional[AccountType] = field(default=None)
    visibleInHubs: Optional[List[Hub]] = field(default=None)
    adminRoles: Optional[List[str]] = admin_roles_field(can_be_none=True)
    friendlyName: Optional[str] = field(validator=validate_account_friendly_name, default=None)
    responsibles: Optional[List[str]] = responsibles_field(can_be_none=True)
    roles: Optional[List[AccountRoleBody]] = field(default=None, validator=validate_account_roles)
