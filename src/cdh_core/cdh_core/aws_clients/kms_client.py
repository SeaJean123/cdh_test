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
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.utils import repeat_while_truncated_nextmarker
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_clients import PolicyDocumentType

if TYPE_CHECKING:
    from mypy_boto3_kms import KMSClient
    from mypy_boto3_kms.type_defs import DescribeKeyResponseTypeDef
    from mypy_boto3_kms.type_defs import CreateKeyResponseTypeDef
else:
    KMSClient = object
    DescribeKeyResponseTypeDef = Dict[str, Any]
    CreateKeyResponseTypeDef = Dict[str, Any]


@dataclass(frozen=True)
class KmsAlias:
    """Represents an AWS KMS alias."""

    name: str
    target_key_id: Optional[str]
    arn: Arn

    @property
    def region(self) -> Region:
        """Return the region contained in the ARN of the AWS KMS alias."""
        return Region(self.arn.region)


@dataclass(frozen=True)
class KmsKey:
    """Represents an AWS KMS key."""

    id: str  # pylint: disable=invalid-name
    arn: Arn

    @property
    def region(self) -> Region:
        """Return the region contained in the ARN of the AWS KMS key."""
        return Region(self.arn.region)

    @classmethod
    def parse_from_arn(cls, arn: Arn) -> KmsKey:
        """Return the KmsKey built from a KMS key ARN."""
        try:
            key_id = arn.identifier.split("/")[1]
        except IndexError as error:
            raise InvalidKeyArn(arn) from error
        return KmsKey(id=key_id, arn=arn)


class KmsClient:
    """Abstracts the boto3 KMS client."""

    def __init__(self, boto_kms_client: KMSClient):
        self._client = boto_kms_client

    @staticmethod
    def _check_alias_name(name: str) -> None:
        if not name.startswith("alias/"):
            raise ValueError('Alias names must start with "alias/"')

    @staticmethod
    def get_key_id_from_arn(arn: Arn) -> str:
        """Return the KMS key id of a given ARN."""
        if not arn.identifier.startswith("key/"):
            raise InvalidKeyArn(arn)
        return arn.identifier[len("key/") :]

    def list_aliases(self) -> List[KmsAlias]:
        """Return a list of all aliases."""
        aliases = [
            alias
            for alias in repeat_while_truncated_nextmarker(self._client.list_aliases, "Aliases")  # type:ignore
            if not alias["AliasName"].startswith("alias/aws/")
        ]
        return [
            KmsAlias(name=alias["AliasName"], target_key_id=alias.get("TargetKeyId"), arn=Arn(alias["AliasArn"]))
            for alias in aliases
        ]

    def find_alias(self, name: str) -> KmsAlias:
        """Return the KmsAlias of a given name."""
        self._check_alias_name(name)
        for alias in self.list_aliases():
            if alias.name == name:
                return alias
        raise AliasNotFound(name)

    def get_key_by_id(self, key_id: str) -> KmsKey:
        """Return a KmsKey instance built from the AWS KMS key information of the given id."""
        try:
            return self.convert_aws_to_kms_key(self._client.describe_key(KeyId=key_id))
        except self._client.exceptions.NotFoundException as error:
            raise KeyNotFound(key_id) from error

    def get_key_by_alias_name(self, alias_name: str) -> KmsKey:
        """Return a KmsKey instance for a given alias name."""
        alias = self.find_alias(alias_name)
        if not alias.target_key_id:
            raise UnassociatedKeyAlias(alias.arn)
        return self.get_key_by_id(alias.target_key_id)

    @staticmethod
    def convert_aws_to_kms_key(key_aws: Union[DescribeKeyResponseTypeDef, CreateKeyResponseTypeDef]) -> KmsKey:
        """Return a KmsKey instance built from the AWS KMS key information of the given id."""
        return KmsKey(id=key_aws["KeyMetadata"]["KeyId"], arn=Arn(key_aws["KeyMetadata"]["Arn"]))

    def create_key(
        self,
        policy: PolicyDocument,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        bypass_policy_lockout_safety_check: bool = False,
    ) -> KmsKey:
        """Create and return a KmsKey with the given policy, description, and tags."""
        return self.convert_aws_to_kms_key(
            self._client.create_key(
                Policy=policy.encode(),
                Description=description,
                KeyUsage="ENCRYPT_DECRYPT",
                Origin="AWS_KMS",
                Tags=[{"TagKey": key, "TagValue": value} for key, value in (tags or {}).items()],
                BypassPolicyLockoutSafetyCheck=bypass_policy_lockout_safety_check,
            )
        )

    def disable_key_and_tag_timestamp(self, key_id: str) -> None:
        """Disable the KMS key with the given id and tag it with the current timestamp."""
        try:
            self._client.disable_key(KeyId=key_id)
            self._add_or_update_tag(key_id=key_id, tag_key="DisableTimestamp", tag_value=str(date.today()))
        except self._client.exceptions.NotFoundException as error:
            raise KeyNotFound(key_id) from error

    def _add_or_update_tag(self, key_id: str, tag_key: str, tag_value: str) -> None:
        # we explicitly include the current tags to satisfy tag-based IAM logic
        tags = self._client.list_resource_tags(KeyId=key_id)["Tags"]
        tags_to_keep = [tag for tag in tags if tag["TagKey"] != tag_key]
        self._client.tag_resource(
            KeyId=key_id,
            Tags=tags_to_keep + [{"TagKey": tag_key, "TagValue": tag_value}],
        )

    def create_alias(self, name: str, key_id: str) -> None:
        """Create an alias for the KMS key with the given id and name."""
        self._check_alias_name(name)
        try:
            self._client.create_alias(AliasName=name, TargetKeyId=key_id)
        except self._client.exceptions.AlreadyExistsException as error:
            raise AliasAlreadyExists(name) from error

    def get_policy(self, key_id: str) -> PolicyDocument:
        """Return the PolicyDocument of the KMS key with the given id."""
        try:
            response = self._client.get_key_policy(KeyId=key_id, PolicyName="default")
        except self._client.exceptions.NotFoundException as error:
            raise KeyNotFound(key_id) from error

        policy_document = json.loads(response["Policy"])
        return PolicyDocument(
            version=policy_document["Version"],
            statements=policy_document["Statement"],
            policy_document_type=PolicyDocumentType.KMS,
        )

    def set_policy(self, key_id: str, kms_policy: PolicyDocument) -> None:
        """Set the policy of the KMS key with the given id."""
        try:
            self._client.put_key_policy(
                KeyId=key_id, PolicyName="default", Policy=kms_policy.encode(), BypassPolicyLockoutSafetyCheck=False
            )
        except self._client.exceptions.NotFoundException as error:
            raise KeyNotFound(key_id) from error


class InvalidKeyArn(Exception):
    """Signals a given ARN is not a valid KMS key ARN."""

    def __init__(self, arn: Arn):
        super().__init__(f"The given arn {arn} is not a valid KMS key arn")


class KeyNotFound(Exception):
    """Signals a KMS key id cannot be found."""

    def __init__(self, key_id: str):
        super().__init__(f"Could not find KMS key {key_id!r}")


class AliasNotFound(Exception):
    """Signals a KMS key alias cannot be found."""

    def __init__(self, name: str):
        super().__init__(f"Could not find KMS alias {name!r}")


class AliasAlreadyExists(Exception):
    """Signals a KMS key alias with the same name already exists."""

    def __init__(self, name: str):
        super().__init__(f"KMS alias {name!r} already exists")


class UnassociatedKeyAlias(Exception):
    """Throw an error when the alias passed is not associated to a KMS key."""

    def __init__(self, alias_arn: Arn):
        super().__init__(f"KMS alias {alias_arn} is not associated to a KMS key.")
