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
import os
from typing import List

from cdh_core.entities.account_store import AccountStore
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.environment import Environment


def main(environment: Environment, purposes: List[AccountPurpose]) -> None:
    """Output tuples of purposes and account ids for the requested environment and purposes.

    Hint: An account can be assigned to multiple purposes.
    """
    accounts = AccountStore().query_accounts(environments=environment, account_purposes=frozenset(purposes))

    print(json.dumps([{"purpose": account.purpose.value, "account": account.id} for account in accounts]))  # noqa: T201


if __name__ == "__main__":
    main(
        environment=Environment(os.environ["ENVIRONMENT"]),
        purposes=[AccountPurpose(purpose) for purpose in json.loads(os.environ["PURPOSES"])],
    )
