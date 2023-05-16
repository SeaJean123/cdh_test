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
from abc import abstractmethod
from dataclasses import dataclass
from logging import getLogger
from typing import Generic
from typing import Type
from typing import TypeVar

from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericUpdateAccountBody

from cdh_core.aws_clients.factory import AssumeRoleSessionProvider
from cdh_core.aws_clients.factory import BotocoreSessionWrapper
from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import BaseAccount
from cdh_core.entities.arn import Arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.hubs import Hub
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class _AssumableAccountSpecBase:
    account_id: AccountId
    account_type: AccountType
    hub: Hub


class GenericAssumableAccountSpec(Generic[GenericAccount, GenericUpdateAccountBody], _AssumableAccountSpecBase):
    """Generic assumable account spec.

    Inherit from this class to create a spec class for a specific account class.
    """

    @classmethod
    @abstractmethod
    def from_account(cls: Type[T], account: GenericAccount) -> T:
        """Convert an account instance to an assumable account spec."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_account_and_update_body(cls: Type[T], account: GenericAccount, body: GenericUpdateAccountBody) -> T:
        """Convert an account instance with its updated attributes to an assumable account spec."""
        raise NotImplementedError

    def supports_metadata_role(self) -> bool:
        """Return True if the metadata role is supported for an assumable account spec."""
        return self.account_type is not AccountType.technical


@dataclass(frozen=True)
class AssumableAccountSpec(GenericAssumableAccountSpec[Account, UpdateAccountBody]):
    """Represents an account in which the metadata role should be assumed."""

    @classmethod
    def from_account(cls, account: Account) -> "AssumableAccountSpec":
        """Convert an account instance to an assumable account spec."""
        return cls(account_id=account.id, account_type=account.type, hub=account.hub)

    @classmethod
    def from_account_and_update_body(cls, account: Account, body: UpdateAccountBody) -> "AssumableAccountSpec":
        """Convert an account instance with its updated attributes to an assumable account spec."""
        return cls(account_id=account.id, account_type=body.type or account.type, hub=account.hub)


class MetadataRoleAssumer(Generic[GenericAccount, GenericUpdateAccountBody]):
    """Assumes the metadata role in a user account that was provisioned during bootstrapping."""

    def __init__(
        self,
        assume_role_session_provider: AssumeRoleSessionProvider,
        account_store: AccountStore,
        assumable_account_spec_cls: Type[GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody]],
        config: Config,
    ):
        self._assume_role_session_provider = assume_role_session_provider
        self._account_store = account_store
        self._assumable_account_spec_cls = assumable_account_spec_cls
        self._config = config

    @staticmethod
    def _get_metadata_role_arn(
        account_spec: GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody]
    ) -> Arn:
        metadata_role = ConfigFileLoader().get_config().account.assumable_aws_role.metadata
        return Arn.get_role_arn(
            partition=account_spec.hub.partition,
            account_id=account_spec.account_id,
            path=metadata_role.path,
            name=metadata_role.name,
        )

    def _check_account_information(
        self, account_spec: GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody]
    ) -> None:
        if not account_spec.supports_metadata_role():
            raise UnsupportedAssumeMetadataRole(account_spec)

    def _retrieve_session(self, api_account: BaseAccount) -> BotocoreSessionWrapper:
        return self._assume_role_session_provider.get_session_wrapped(
            account_id=api_account.id, account_purpose=api_account.purpose
        )

    def assume(
        self, account_spec: GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody]
    ) -> BotocoreSessionWrapper:
        """Assumes the metadata role in the specified account."""
        self._check_account_information(account_spec)

        api_account = self._account_store.query_account(
            environments=self._config.environment,
            account_purposes=AccountPurpose("api"),
            partitions=account_spec.hub.partition,
        )

        api_session = self._retrieve_session(api_account)

        metadata_role_arn = self._get_metadata_role_arn(account_spec)
        try:
            return api_session.assume_role(role_arn=metadata_role_arn, session_name="MetadataRoleAssumer")
        except Exception as err:
            raise CannotAssumeMetadataRole(role_arn=metadata_role_arn) from err

    def assume_account(self, account: GenericAccount) -> BotocoreSessionWrapper:
        """Assumes the metadata role in the given account."""
        account_spec = self._assumable_account_spec_cls.from_account(account)
        return self.assume(account_spec)


class UnsupportedAssumeMetadataRole(Exception):
    """Signals the metadata role is not supported in the requested account."""

    def __init__(self, account_spec: GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody]) -> None:
        super().__init__(f"Metadata role not supported for ({account_spec}).")


class CannotAssumeMetadataRole(Exception):
    """Signals the metadata role with the given arn could not be assumed."""

    def __init__(self, role_arn: Arn) -> None:
        super().__init__(f"Cannot assume role ({role_arn}).")
