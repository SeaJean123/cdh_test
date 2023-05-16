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
import string
from logging import getLogger

from botocore.exceptions import ClientError
from cdh_core_api.config import Config

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.entities.account_store import QueryAccountNotFound
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import ServiceUnavailableError
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)


class AccountIdVerifier:
    """Verifies that an account exists."""

    def __init__(self, config: Config, aws: AwsClientFactory):
        self._config = config
        self._aws = aws

    def verify(self, account_id: AccountId, partition: Partition) -> None:
        """Verify that an account in a partition exists."""
        try:
            self._verify_helper(account_id=account_id, partition=partition)
        except ClientError as error:
            if (
                error.response["Error"]["Code"] == "InvalidParameter"
                and "PrincipalNotFound" in error.response["Error"]["Message"]
            ):
                raise ForbiddenError(
                    f"Account id {account_id} does not exist in AWS partition {partition.value}"
                ) from error
        except Exception as error:  # pylint: disable=broad-except
            LOG.exception(f"Cannot verify existence of account id {account_id}")
            raise ServiceUnavailableError(f"Cannot verify existence of account id {account_id}") from error

    def _verify_helper(self, account_id: AccountId, partition: Partition) -> None:
        random_suffix = "".join(random.choices(string.ascii_lowercase, k=4))
        topic_name = f"{self._config.prefix}cdh-verify-account-{account_id}-{random_suffix}"
        resource_accounts = self._config.account_store.query_resource_accounts(
            environments=self._config.environment,
            partitions=partition,
        )
        try:
            resource_account = next(iter(resource_accounts))
        except StopIteration as err:
            raise QueryAccountNotFound() from err

        cdh_sns_client = self._aws.sns_client(
            resource_account.id, resource_account.purpose, Region.preferred(partition)
        )
        sns_arn = cdh_sns_client.create_sns(name=topic_name, tags={"verify-existence-of-account-id": "to-be-deleted"})
        try:
            sns_policy = cdh_sns_client.get_sns_policy(sns_arn)
            sns_policy.statements[0]["Principal"] = {"AWS": ["*", str(account_id)]}
            cdh_sns_client.set_sns_policy(sns_arn=sns_arn, sns_policy=sns_policy)
        finally:
            cdh_sns_client.delete_sns_topic(sns_arn=sns_arn)
