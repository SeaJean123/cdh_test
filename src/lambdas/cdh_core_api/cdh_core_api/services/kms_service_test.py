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
from typing import Any
from typing import Optional
from unittest.mock import call
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest
from cdh_core_api.config_test import build_config
from cdh_core_api.services.kms_service import KmsService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.lock_service import ResourceIsLocked
from marshmallow import ValidationError

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.kms_client import AliasNotFound
from cdh_core.aws_clients.kms_client import KmsClient
from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import HubAccount
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.accounts_test import build_security_account
from cdh_core.entities.arn import Arn
from cdh_core.entities.lock_test import build_lock
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.environment import Environment
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.locking import LockingScope
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.utils import build_and_set_moto_account_id


class TestKmsService:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_kms: Any) -> None:  # pylint: disable=W0613
        self.environment = build_environment()
        self.hub = build_hub()
        self.lock_service = Mock(LockService)
        moto_security_account_id = build_and_set_moto_account_id()
        self.security_account = build_security_account(
            account_id=AccountId(moto_security_account_id), hub=Hub.default()
        )
        self.region = build_region()
        self.resource_account = build_resource_account(hub=self.hub)
        self.account_store = AccountStore(accounts=[self.security_account, self.resource_account])
        self.boto_kms_client = boto3.client("kms", region_name=self.region.value)
        self.kms_client = KmsClient(self.boto_kms_client)
        self.aws = Mock(AwsClientFactory)
        self.aws.kms_client.return_value = self.kms_client
        self.config = build_config(environment=self.environment, account_store=self.account_store, prefix="")
        self.kms_service = KmsService(self.config, self.aws, self.lock_service)

    def create_key_in_kms(
        self,
        hub_account: HubAccount,
        environment: Environment,
        prefix: str = "",
        alias: Optional[str] = None,
    ) -> KmsKey:
        policy_document = PolicyDocument.create_key_policy([])
        key = self.kms_client.create_key(policy_document)
        if isinstance(hub_account, ResourceAccount):
            self.kms_client.create_alias(
                self.kms_service.get_shared_key_alias(prefix, hub_account, environment), key.id
            )
        elif alias:
            self.kms_client.create_alias(alias, key.id)
        return key

    def assert_correct_key_and_alias_in_kms(
        self,
        key_arn: Arn,
        resource_account: ResourceAccount,
        environment: Environment,
        prefix: str = "",
    ) -> None:
        key_data = self.boto_kms_client.describe_key(KeyId=self.kms_client.get_key_id_from_arn(key_arn))["KeyMetadata"]
        assert key_data["Arn"] == str(key_arn)
        all_aliases = self.boto_kms_client.list_aliases()["Aliases"]
        # Filter out AWS' own aliases (e.g. alias/aws/s3)
        custom_aliases = [alias for alias in all_aliases if not alias["AliasName"].startswith("alias/aws/")]
        assert len(custom_aliases) == 1
        assert custom_aliases[0]["AliasName"] == self.kms_service.get_shared_key_alias(
            prefix, resource_account=resource_account, environment=environment
        )

    def test_get_alias_name(self) -> None:
        assert (
            self.kms_service.get_shared_key_alias("", self.resource_account, self.environment)
            == f"alias/cdh-{self.environment.value}-{self.resource_account.hub.value}-{self.resource_account.id}"
        )
        assert (
            self.kms_service.get_shared_key_alias("cdhx1234", self.resource_account, self.environment)
            == f"alias/cdhx1234cdh-{self.environment.value}-"
            f"{self.resource_account.hub.value}-{self.resource_account.id}"
        )

    def test_get_shared_key_return_existing_key(self) -> None:
        key_arn = self.create_key_in_kms(self.resource_account, environment=self.environment).arn

        assert self.kms_service.get_shared_key(self.resource_account, self.region).arn == key_arn
        self.lock_service.acquire_lock.assert_not_called()

    def test_get_shared_key_create_new_key(self) -> None:
        lock = build_lock(scope=LockingScope.kms_key)
        self.lock_service.acquire_lock.return_value = lock

        key = self.kms_service.get_shared_key(self.resource_account, self.region)

        self.assert_correct_key_and_alias_in_kms(key.arn, self.resource_account, environment=self.environment)
        self.lock_service.acquire_lock.assert_called_once_with(
            item_id=self.resource_account.id, scope=LockingScope.kms_key, region=self.region
        )
        self.lock_service.release_lock.assert_called_once_with(lock)

    def test_get_shared_key_create_new_key_fails_on_lock(self) -> None:
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(
            build_lock(scope=LockingScope.kms_key), build_lock(scope=LockingScope.kms_key)
        )

        with pytest.raises(ResourceIsLocked):
            self.kms_service.get_shared_key(self.resource_account, self.region)
        self.lock_service.release_lock.assert_not_called()

    def test_get_shared_key_create_new_key_fails_on_invalid_region(self) -> None:
        self.lock_service.acquire_lock.return_value = build_lock(scope=LockingScope.kms_key)
        with patch.object(Hub, "regions", set(Region) - {self.region}):
            with pytest.raises(ValidationError):
                self.kms_service.get_shared_key(self.resource_account, self.region)

    def test_get_existing_shared_key_exists(self) -> None:
        key_arn = self.create_key_in_kms(self.resource_account, environment=self.environment).arn

        assert self.kms_service.get_existing_shared_key(self.resource_account, self.region).arn == key_arn

    def test_get_existing_shared_key_does_not_exist(self) -> None:
        with pytest.raises(AliasNotFound):
            self.kms_service.get_existing_shared_key(self.resource_account, self.region)

    def get_key_policy(self, key: KmsKey) -> PolicyDocument:
        policy = json.loads(self.boto_kms_client.get_key_policy(KeyId=key.id, PolicyName="default")["Policy"])
        return PolicyDocument(version=policy["Version"], statements=policy["Statement"])

    def test_correct_key_policy(self) -> None:
        key = self.kms_service.get_shared_key(self.resource_account, self.region)
        policy = self.get_key_policy(key)
        expected_users = [f"arn:aws:iam::{self.resource_account.id}:root"]

        assert policy.get_principals_with_action("kms:Encrypt") == {"AWS": expected_users}
        assert policy.get_principals_with_action("kms:Decrypt") == {"AWS": expected_users}
        assert not policy.has_statement_with_sid("GrantKeyUsage")

    def test_regenerate_key_policy(self) -> None:
        lock = build_lock(scope=LockingScope.kms_key)
        self.lock_service.acquire_lock.return_value = lock
        key = self.kms_service.get_shared_key(self.resource_account, self.region)

        expected_writers = {build_account_id(), build_account_id()}
        expected_readers = {build_account_id(), build_account_id()}
        self.kms_service.regenerate_key_policy(
            kms_key=key,
            resource_account=self.resource_account,
            account_ids_with_read_access=expected_readers,
            account_ids_with_write_access=expected_writers,
        )

        policy = self.get_key_policy(key)
        expected_writers.add(self.resource_account.id)
        expected_users_with_writer = [f"arn:aws:iam::{account_id}:root" for account_id in expected_writers]
        expected_users_with_reader = [f"arn:aws:iam::{account_id}:root" for account_id in expected_readers]

        assert set(policy.get_principals_with_action("kms:Encrypt")["AWS"]) == set(expected_users_with_writer)
        assert set(policy.get_principals_with_action("kms:Decrypt")["AWS"]) == set(
            expected_users_with_reader + expected_users_with_writer
        )
        self.lock_service.acquire_lock.assert_has_calls(
            [
                call(item_id=self.resource_account.id, scope=LockingScope.kms_key, region=self.region),
                call(item_id=key.id, scope=LockingScope.kms_key, region=self.region),
            ]
        )
        self.lock_service.release_lock.assert_called_with(lock)

    def test_regenerate_key_policy_fails_on_lock(self) -> None:
        self.lock_service.acquire_lock.side_effect = ResourceIsLocked(
            build_lock(scope=LockingScope.kms_key), build_lock(scope=LockingScope.kms_key)
        )
        resource_account = build_resource_account()
        other_kms_service = KmsService(self.config, self.aws, Mock())
        key = other_kms_service.get_shared_key(self.resource_account, self.region)

        with pytest.raises(ResourceIsLocked):
            self.kms_service.regenerate_key_policy(
                kms_key=key,
                resource_account=resource_account,
                account_ids_with_read_access=set(),
                account_ids_with_write_access=set(),
            )
        self.lock_service.release_lock.assert_not_called()

    def test_disable_key_by_alias_and_tag_timestamp(self) -> None:
        account_id = build_account_id()
        alias = f"alias/cdh-{account_id}"
        key_id = self.create_key_in_kms(self.security_account, alias=alias, environment=self.environment).id
        self.kms_service.disable_key_by_alias(hub=self.hub, region=self.region, key_alias=alias)

        assert self.boto_kms_client.describe_key(KeyId=key_id)["KeyMetadata"]["KeyState"] == "Disabled"
        assert any(
            tag["TagKey"] == "DisableTimestamp" for tag in self.boto_kms_client.list_resource_tags(KeyId=key_id)["Tags"]
        )

    def test_disable_key_by_alias_and_tag_timestamp_fails_on_non_existing_key(self) -> None:
        with pytest.raises(AliasNotFound):
            self.kms_service.disable_key_by_alias(
                hub=self.hub, region=self.region, key_alias=f"alias/cdh-{build_account_id()}"
            )
