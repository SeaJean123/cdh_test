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

from cdh_core.entities.account_store import AccountStore
from cdh_core.enums.environment import Environment
from cdh_core.primitives.account_id import AccountId


def main(environment: Environment, account_id: AccountId) -> None:
    """Output the hub for a given account and environment."""
    hub = AccountStore().query_resource_account(environments=environment, account_ids=account_id).hub

    print(json.dumps({"hub": hub.value}))  # noqa: T201


if __name__ == "__main__":
    tf_input = json.loads(sys.stdin.readline())
    main(
        environment=Environment(tf_input["environment"]),
        account_id=AccountId(tf_input["account_id"]),
    )
