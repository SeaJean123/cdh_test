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
from typing import Optional

from cdh_core_api.bodies.accounts import AccountRoleBody
from cdh_core_api.bodies.accounts import NewAccountBody

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.accounts_test import build_account_role


def build_new_account_body(account: Optional[Account] = None) -> NewAccountBody:
    account = account or build_account()
    return NewAccountBody(
        id=account.id,
        adminRoles=account.admin_roles,
        affiliation=account.affiliation,
        businessObjects=account.business_objects,
        friendlyName=account.friendly_name,
        group=account.group,
        hub=account.hub,
        layers=account.layers,
        responsibles=account.responsibles,
        requestId=account.request_id,
        roles=[
            AccountRoleBody(name=role.name, path=role.path, type=role.type, friendlyName=role.friendly_name)
            for role in account.roles
        ]
        if account.roles
        else None,
        stages=account.stages,
        type=account.type,
        visibleInHubs=account.visible_in_hubs,
    )


class TestToAccount:
    def test_to_account_with_default_roles(self) -> None:
        body = build_new_account_body()

        account = body.to_account()

        assert [role.name for role in account.roles] == ["CDHDevOps", "CDHReadOnly"]

    def test_convert_to_account_roles_not_overwritten(self) -> None:
        roles = [build_account_role() for _ in range(3)]
        body = build_new_account_body(build_account(roles=roles))

        account = body.to_account()

        assert account.roles == roles
