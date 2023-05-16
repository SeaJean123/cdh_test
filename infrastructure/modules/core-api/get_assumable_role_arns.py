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
import json
import sys
from typing import Dict

from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.accounts import SecurityAccount
from cdh_core.entities.arn import Arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.environment import Environment


def main(environment: Environment, prefix: str) -> None:
    """Output a list of role ARNs that can be assumed by the Core API."""
    assumable_role_arns = _get_assumable_security_account_roles(environment) | _get_assumable_resource_account_roles(
        environment, prefix
    )
    print(json.dumps({account_id: str(role_arn) for account_id, role_arn in assumable_role_arns.items()}))  # noqa: T201


def _get_assumable_security_account_roles(environment: Environment) -> Dict[str, Arn]:
    assumable_role_arns: Dict[str, Arn] = {}
    for account in AccountStore().query_accounts(
        environments=frozenset(Environment),
        partitions=Partition.default(),
        account_purposes=AccountPurpose("security"),
    ):
        assert isinstance(account, SecurityAccount)
        assumable_role_arns[f"{account.id}_{account.purpose.value}"] = account.get_assumable_role_arn_for_core_api(
            environment=environment
        )
    return assumable_role_arns


def _get_assumable_resource_account_roles(environment: Environment, prefix: str) -> Dict[str, Arn]:
    assumable_role_arns: Dict[str, Arn] = {}
    for account in AccountStore().query_accounts(
        environments=environment,
        partitions=Partition.default(),
        account_purposes=AccountPurpose("resources"),
    ):
        assert isinstance(account, ResourceAccount)
        assumable_role_arns[f"{account.id}_{account.purpose.value}"] = account.get_assumable_role_arn_for_core_api(
            prefix
        )
    return assumable_role_arns


if __name__ == "__main__":
    tf_input = json.loads(sys.stdin.readline())
    main(
        environment=Environment(tf_input["environment"]),
        prefix=tf_input["prefix"],
    )
