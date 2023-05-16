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
import time
from typing import Any
from typing import Dict
from typing import Optional

import boto3

from cdh_core.aws_clients.boto_retry_decorator import create_boto_retry_decorator
from cdh_core.entities.account_store import AccountStore
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment
from cdh_core.primitives.account_id import AccountId


def assume_role(
    account_id: AccountId,
    role: str,
    prefix: str,
    role_session_name: Optional[str] = None,
    credentials: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assume a role and retrieve the credentials."""
    partition = next(
        iter(AccountStore().query_accounts(environments=frozenset(Environment), account_ids=account_id))
    ).partition

    kwargs = credentials
    kwargs = kwargs.copy() if kwargs else {}
    kwargs["region_name"] = Region.preferred(partition).value
    role_arn = f"arn:{partition.value}:iam::{account_id}:role/{prefix}{role}"
    if not role_session_name:
        role_session_name = "oss-functional-tests"
    thread_safe_session = boto3.session.Session()
    client = thread_safe_session.client("sts", **kwargs)
    response = AssumeRoleRetry().assume_role(client, role_arn, role_session_name)

    return {
        "aws_access_key_id": response["Credentials"]["AccessKeyId"],
        "aws_secret_access_key": response["Credentials"]["SecretAccessKey"],
        "aws_session_token": response["Credentials"]["SessionToken"],
    }


class AssumeRoleRetry:
    """Retry Class for the assume role method."""

    retry = create_boto_retry_decorator("_sleep")

    def __init__(self) -> None:
        self._sleep = time.sleep

    @retry(num_attempts=6, wait_between_attempts=3, retryable_error_codes=["AccessDenied"])
    def assume_role(self, client: Any, role_arn: str, role_session_name: str) -> Any:
        """Class to retry the assume_role method on an sts client."""
        return client.assume_role(RoleArn=role_arn, RoleSessionName=role_session_name)
