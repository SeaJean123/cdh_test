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
from functools import lru_cache
from logging import getLogger
from typing import FrozenSet
from typing import Set

from cdh_core_api.config import Config
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.validation.base import validate_region_in_hub

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.kms_client import AliasNotFound
from cdh_core.aws_clients.kms_client import KeyNotFound
from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.locking import LockingScope
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.constants import CREATED_BY_CORE_API_TAG

LOG = getLogger(__name__)


class KmsService:
    """Create KMS Policies and get shared key information."""

    def __init__(self, config: Config, aws: AwsClientFactory, lock_service: LockService):
        self._resource_name_prefix = config.prefix
        self._environment = config.environment
        self._resource_accounts = config.account_store.query_resource_accounts(environments=config.environment)
        self._aws = aws
        self._lock_service = lock_service
        self._account_store = config.account_store

    @classmethod
    def get_shared_key_alias(
        cls, resource_name_prefix: str, resource_account: ResourceAccount, environment: Environment
    ) -> str:
        """Get a shared key alias."""
        return f"alias/{resource_name_prefix}cdh-{environment.value}-{resource_account.hub.value}-{resource_account.id}"

    def get_existing_shared_key(self, resource_account: ResourceAccount, region: Region) -> KmsKey:
        """Get an existing shared key."""
        alias_name = self.get_shared_key_alias(self._resource_name_prefix, resource_account, self._environment)
        security_account = self._account_store.get_security_account_for_hub(hub=resource_account.hub)
        client = self._aws.kms_client(
            account_id=security_account.id,
            account_purpose=security_account.purpose,
            region=region,
        )

        return client.get_key_by_alias_name(alias_name)

    @lru_cache(maxsize=256)  # noqa: B019 # service instantiated only once per lambda runtime
    def get_shared_key(self, resource_account: ResourceAccount, region: Region) -> KmsKey:
        """Get a shared key, create if it does not already exist."""
        alias_name = self.get_shared_key_alias(self._resource_name_prefix, resource_account, self._environment)
        security_account = self._account_store.get_security_account_for_hub(hub=resource_account.hub)
        client = self._aws.kms_client(
            account_id=security_account.id,
            account_purpose=security_account.purpose,
            region=region,
        )
        try:
            # check if key already exists in region
            key = client.get_key_by_alias_name(alias_name)
            LOG.info(
                f"Key found for expected name {alias_name} in region {region.value} "
                f"using this one instead of creating a new one"
            )
        except (AliasNotFound, KeyNotFound):
            region = validate_region_in_hub(resource_account.hub, region)
            key_policy = self._create_key_policy(resource_account=resource_account)
            tags = {
                **CREATED_BY_CORE_API_TAG,
                "owner": resource_account.id,
                "environment": self._environment.value,
                "resourcePrefix": self._resource_name_prefix,
                "hub": resource_account.hub.value,
            }
            LOG.info("Creating new KMS key for resource account %s in %s", resource_account.id, region.value)
            lock = self._lock_service.acquire_lock(
                item_id=resource_account.id, scope=LockingScope.kms_key, region=region
            )
            key = client.create_key(
                key_policy,
                description=f"shared key for resources in {resource_account.id}",
                tags=tags,
                bypass_policy_lockout_safety_check=True,
            )
            client.create_alias(alias_name, key.id)
            self._lock_service.release_lock(lock)
            LOG.info(f"Created new KMS key for {resource_account.id} in {region.value}: {alias_name} -> {key.id}")
        return key

    def _create_key_policy(
        self,
        resource_account: ResourceAccount,
        account_ids_with_read_access: FrozenSet[AccountId] = frozenset(),
        account_ids_with_write_access: FrozenSet[AccountId] = frozenset(),
    ) -> PolicyDocument:
        """Create a key policy and grant permission according the accounts passed."""
        security_account = self._account_store.get_security_account_for_hub(hub=resource_account.hub)
        user_arns = [f"arn:{resource_account.partition.value}:iam::{resource_account.id}:root"]
        user_arns.extend(
            [
                f"arn:{resource_account.partition.value}:iam::{str(account)}:root"
                for account in account_ids_with_write_access
            ]
        )
        statements = [
            {
                "Sid": "AllowEverythingForOnlyCDHSecurityAccount",
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:{resource_account.partition.value}:iam::{security_account.id}:root"},
                "Action": "kms:*",
                "Resource": "*",
            },
            {
                "Sid": "AllowKeyUsage",
                "Effect": "Allow",
                "Principal": {"AWS": user_arns},
                "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
                "Resource": "*",
            },
            {
                "Sid": "AllowAttachmentPersistentResources",
                "Effect": "Allow",
                "Principal": {
                    "AWS": user_arns,
                },
                "Action": ["kms:CreateGrant", "kms:ListGrants", "kms:RevokeGrant"],
                "Resource": "*",
                "Condition": {"Bool": {"kms:GrantIsForAWSResource": "true"}},
            },
        ]
        if account_ids_with_read_access:
            reader_arns = [
                f"arn:{resource_account.partition.value}:iam::{account_id}:root"
                for account_id in account_ids_with_read_access
            ]
            reader_statement = {
                "Sid": "GrantKeyUsage",
                "Effect": "Allow",
                "Principal": {
                    "AWS": reader_arns,
                },
                "Action": ["kms:Decrypt", "kms:DescribeKey"],
                "Resource": "*",
            }
            statements.append(reader_statement)
        return PolicyDocument.create_key_policy(statements)

    def regenerate_key_policy(
        self,
        kms_key: KmsKey,
        resource_account: ResourceAccount,
        account_ids_with_read_access: Set[AccountId],
        account_ids_with_write_access: Set[AccountId],
    ) -> None:
        """Regenerate a key policy for a specific KMS key according to the account-IDs passed."""
        client = self._aws.kms_client(kms_key.arn.account_id, AccountPurpose("security"), kms_key.region)
        lock = self._lock_service.acquire_lock(item_id=kms_key.id, scope=LockingScope.kms_key, region=kms_key.region)
        policy = self._create_key_policy(
            resource_account=resource_account,
            account_ids_with_read_access=frozenset(account_ids_with_read_access),
            account_ids_with_write_access=frozenset(account_ids_with_write_access),
        )
        LOG.info("Updating KMS key policy for %s in %s", kms_key.id, kms_key.region.value)
        client.set_policy(kms_key.id, policy)
        self._lock_service.release_lock(lock)

    def disable_key_by_alias(self, hub: Hub, region: Region, key_alias: str) -> None:
        """Disable a KMS key via its alias."""
        security_account = self._account_store.get_security_account_for_hub(hub=hub)
        client = self._aws.kms_client(
            account_id=security_account.id,
            account_purpose=security_account.purpose,
            region=region,
        )
        LOG.info(f"Disabling KMS key {key_alias}")

        key = client.get_key_by_alias_name(key_alias)

        client.disable_key_and_tag_timestamp(key_id=key.id)
