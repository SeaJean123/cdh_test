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
from datetime import date
from typing import Any
from typing import Dict
from typing import Optional
from unittest.mock import Mock
from uuid import uuid4

import boto3
import pytest
from freezegun import freeze_time

from cdh_core.aws_clients.kms_client import AliasAlreadyExists
from cdh_core.aws_clients.kms_client import AliasNotFound
from cdh_core.aws_clients.kms_client import InvalidKeyArn
from cdh_core.aws_clients.kms_client import KeyNotFound
from cdh_core.aws_clients.kms_client import KmsAlias
from cdh_core.aws_clients.kms_client import KmsClient
from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.utils import build_and_set_moto_account_id

MOTO_ACCOUNT_ID = AccountId(build_and_set_moto_account_id())


# pylint: disable=too-many-public-methods
class TestKmsClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_kms: Any) -> None:  # pylint: disable=unused-argument
        self.region_name = build_region().value
        self.boto_kms_client = boto3.client("kms", region_name=self.region_name)
        self.kms_client = KmsClient(self.boto_kms_client)
        self.key_policy = PolicyDocument.create_key_policy([])

    def create_key(self, description: str = "some-key", tags: Optional[Dict[str, str]] = None) -> KmsKey:
        response = self.boto_kms_client.create_key(
            Policy=self.key_policy.encode(),
            Description=description,
            KeyUsage="ENCRYPT_DECRYPT",
            Origin="AWS_KMS",
            Tags=[{"TagKey": key, "TagValue": value} for key, value in (tags or {}).items()],
        )
        return KmsClient.convert_aws_to_kms_key(response)

    def create_alias(self, name: str, key_id: str) -> KmsAlias:
        region = self.boto_kms_client.meta.region_name
        self.boto_kms_client.create_alias(AliasName=name, TargetKeyId=key_id)
        return KmsAlias(
            name=name,
            target_key_id=key_id,
            arn=Arn(f"arn:aws:kms:{region}:{MOTO_ACCOUNT_ID}:{name}"),
        )

    def test_parse_from_arn_valid_key_arn(self) -> None:
        key_id = Builder.build_random_string()
        arn = build_arn("iam", resource=key_id)

        key = KmsKey.parse_from_arn(arn)

        assert key == KmsKey(id=key_id, arn=arn)

    def test_parse_from_arn_invalid_key_arn(self) -> None:
        arn = build_arn("lambda")
        with pytest.raises(InvalidKeyArn):
            KmsKey.parse_from_arn(arn)

    def test_list_aliases(self) -> None:
        key1 = self.create_key()
        key2 = self.create_key()
        alias1 = self.create_alias(name="alias/alias1_for_key_1", key_id=key1.id)
        alias2 = self.create_alias(name="alias/alias2_for_key_1", key_id=key1.id)
        alias3 = self.create_alias(name="alias/alias3_for_key_2", key_id=key2.id)

        assert set(self.kms_client.list_aliases()) == {alias1, alias2, alias3}

    def test_list_aliases_with_unassociated_key(self) -> None:
        alias_name = "alias/my-alias"
        key_arn = build_arn("kms")
        target_key_id = str(uuid4())
        boto_kms_client = Mock()
        boto_kms_client.list_aliases.return_value = {
            "Aliases": [{"AliasName": alias_name, "AliasArn": str(key_arn), "TargetKeyId": target_key_id}],
            "Truncated": False,
        }
        kms_client = KmsClient(boto_kms_client)

        assert kms_client.list_aliases() == [KmsAlias(name=alias_name, target_key_id=target_key_id, arn=key_arn)]

    def test_list_aliases_does_not_include_aws_keys(self) -> None:
        boto_kms_client = Mock()
        boto_kms_client.list_aliases.return_value = {
            "Aliases": [{"AliasName": "alias/aws/s3", "AliasArn": str(build_arn("kms"))}],
            "Truncated": False,
        }
        assert KmsClient(boto_kms_client).list_aliases() == []

    def test_find_alias(self) -> None:
        key = self.create_key()
        self.create_alias(name="alias/my_alias1", key_id=key.id)
        alias2 = self.create_alias(name="alias/my_alias2", key_id=key.id)
        assert self.kms_client.find_alias("alias/my_alias2") == alias2

    def test_find_non_existing_alias(self) -> None:
        with assert_raises(AliasNotFound("alias/my_alias")):
            self.kms_client.find_alias("alias/my_alias")

    def test_get_key_by_id(self) -> None:
        key = self.create_key()
        assert self.kms_client.get_key_by_id(key.id) == key

    def test_get_key_by_id_non_existing(self) -> None:
        key_id = str(uuid4())
        with assert_raises(KeyNotFound(key_id)):
            self.kms_client.get_key_by_id(key_id)

    def test_get_key_by_alias_name(self) -> None:
        key = self.create_key()
        alias_name = f"alias/{Builder.build_random_string()}"
        self.create_alias(name=alias_name, key_id=key.id)

        assert self.kms_client.get_key_by_alias_name(alias_name) == key

    def test_get_key_by_alias_name_non_existing_alias(self) -> None:
        alias_name = f"alias/{Builder.build_random_string()}"

        with assert_raises(AliasNotFound(alias_name)):
            self.kms_client.get_key_by_alias_name(alias_name)

    def test_create_key(self) -> None:
        key = self.kms_client.create_key(
            policy=self.key_policy,
            description="house-key",
            tags={"color": "blue"},
        )
        assert key == self.kms_client.get_key_by_id(key.id)

    @freeze_time()
    @pytest.mark.parametrize("existing_tags", [False, True])
    def test_disable_key(self, existing_tags: bool) -> None:
        tags = {Builder.build_random_string(): Builder.build_random_string() for _ in range(3)} if existing_tags else {}
        key = self.kms_client.create_key(policy=self.key_policy, tags=tags)
        self.kms_client.disable_key_and_tag_timestamp(key_id=key.id)
        assert self.boto_kms_client.describe_key(KeyId=key.id)["KeyMetadata"]["KeyState"] == "Disabled"
        assert self.boto_kms_client.list_resource_tags(KeyId=key.id)["Tags"] == [
            {"TagKey": tag_key, "TagValue": tag_value} for tag_key, tag_value in tags.items()
        ] + [{"TagKey": "DisableTimestamp", "TagValue": str(date.today())}]

    @freeze_time()
    @pytest.mark.parametrize("other_tags", [False, True])
    def test_disable_key_already_tagged_with_disable_timestamp(self, other_tags: bool) -> None:
        tags = {Builder.build_random_string(): Builder.build_random_string() for _ in range(3)} if other_tags else {}
        key = self.kms_client.create_key(policy=self.key_policy, tags=tags | {"DisableTimestamp": "2020-01-01"})
        self.kms_client.disable_key_and_tag_timestamp(key_id=key.id)
        assert self.boto_kms_client.describe_key(KeyId=key.id)["KeyMetadata"]["KeyState"] == "Disabled"
        assert self.boto_kms_client.list_resource_tags(KeyId=key.id)["Tags"] == [
            {"TagKey": tag_key, "TagValue": tag_value} for tag_key, tag_value in tags.items()
        ] + [{"TagKey": "DisableTimestamp", "TagValue": str(date.today())}]

    def test_disable_non_existing_key(self) -> None:
        with pytest.raises(KeyNotFound):
            self.kms_client.disable_key_and_tag_timestamp(key_id=Builder.build_random_string())

    def test_create_alias(self) -> None:
        key = self.create_key()
        self.kms_client.create_alias("alias/my_alias", key.id)
        expected_alias = KmsAlias(
            "alias/my_alias", key.id, Arn(f"arn:aws:kms:{self.region_name}:{MOTO_ACCOUNT_ID}:alias/my_alias")
        )
        assert self.kms_client.list_aliases() == [expected_alias]

    def test_create_existing_alias(self) -> None:
        key = self.create_key()
        self.kms_client.create_alias("alias/my_alias", key.id)
        with assert_raises(AliasAlreadyExists("alias/my_alias")):
            self.kms_client.create_alias("alias/my_alias", key.id)

    def test_get_policy(self) -> None:
        key = self.kms_client.create_key(policy=self.key_policy)
        assert self.kms_client.get_policy(key.id) == self.key_policy

    def test_get_policy_of_non_existing_key(self) -> None:
        key_id = str(uuid4())
        with assert_raises(KeyNotFound(key_id)):
            self.kms_client.get_policy(key_id)

    def test_set_policy(self) -> None:
        key = self.kms_client.create_key(policy=self.key_policy)
        new_policy = PolicyDocument.create_key_policy(
            [{"Sid": "some-sid", "Effect": "Allow", "Action": "kms:Encrypt", "Resource": "*"}]
        )
        self.kms_client.set_policy(key.id, new_policy)
        assert self.kms_client.get_policy(key.id) == new_policy

    def test_set_policy_for_non_existing_key(self) -> None:
        key_id = str(uuid4())
        with assert_raises(KeyNotFound(key_id)):
            self.kms_client.set_policy(key_id, self.key_policy)
