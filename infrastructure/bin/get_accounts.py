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
from typing import Optional

from cdh_core.entities.account_store import AccountStore
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub


def main(
    environment: Optional[Environment],
    purpose: Optional[AccountPurpose],
    partition: Optional[Partition],
    default_hub_only: bool,
) -> None:
    """Output information about all accounts of a specific purpose."""
    accounts = AccountStore().query_accounts(
        environments=environment or frozenset(Environment),
        account_purposes=purpose,
        partitions=partition,
        hubs=Hub.default(partition) if default_hub_only else None,
    )

    print(  # noqa: T201
        json.dumps({account.id: f"arn:{account.partition.value}:iam::{account.id}:root" for account in accounts})
    )


def _parse_boolean_input(value: str) -> bool:
    return value.lower() == "true"


if __name__ == "__main__":
    tf_input = json.loads(sys.stdin.readline())
    main(
        environment=Environment(tf_input["environment"]) if "environment" in tf_input else None,
        purpose=AccountPurpose(tf_input["purpose"]) if "purpose" in tf_input else None,
        partition=Partition(tf_input["partition"]) if "partition" in tf_input else None,
        default_hub_only=_parse_boolean_input(tf_input.get("default_hub_only", "false")),
    )
