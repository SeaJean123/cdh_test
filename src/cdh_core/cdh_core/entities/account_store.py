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
# pylint: disable=too-many-arguments
from functools import lru_cache
from typing import Any
from typing import Callable
from typing import cast
from typing import Collection
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.accounts import BaseAccount
from cdh_core.entities.accounts import HubAccount
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.accounts import SecurityAccount
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId


T = TypeVar("T")  # pylint: disable=invalid-name
_ResultType = Union[BaseAccount, HubAccount, ResourceAccount, SecurityAccount]


class AccountStore:
    """The AccountStore contains all accounts the cdh deploys or knows about.

    It enables querying for accounts based on various parameters such as the account id, purpose, or environment.
    """

    _accounts: FrozenSet[_ResultType]

    def __init__(self, accounts: Optional[Collection[_ResultType]] = None) -> None:
        AccountStore.clear_all_caches()
        self._accounts = frozenset(accounts) if accounts else AccountStore._build_default_accounts()

    def get_all_accounts(self) -> Collection[_ResultType]:
        """Return all accounts."""
        return self._accounts

    @staticmethod
    def clear_all_caches() -> None:
        """Clear all LRU caches."""
        AccountStore._query_accounts.cache_clear()  # type: ignore
        AccountStore._build_default_accounts.cache_clear()  # type: ignore
        AccountStore.query_resource_accounts.cache_clear()  # type: ignore

    def query_accounts(
        self,
        environments: Union[Environment, FrozenSet[Environment]],
        account_ids: Optional[Union[AccountId, FrozenSet[AccountId]]] = None,
        account_purposes: Optional[Union[AccountPurpose, FrozenSet[AccountPurpose]]] = None,
        partitions: Optional[Union[Partition, FrozenSet[Partition]]] = None,
        hubs: Optional[Union[Hub, FrozenSet[Hub]]] = None,
        stages: Optional[Union[Stage, FrozenSet[Stage]]] = None,
    ) -> Collection[_ResultType]:
        """Return collection of accounts that matches the given parameters.

        Filter by parameters based on each argument with 'and', and 'or' within the collection per argument.
        """
        return self._query_accounts(
            account_ids=account_ids,
            environments=environments,
            account_purposes=account_purposes,
            partitions=partitions,
            hubs=hubs,
            stages=stages,
        )

    def query_account(
        self,
        environments: Union[Environment, FrozenSet[Environment]],
        account_ids: Optional[Union[AccountId, FrozenSet[AccountId]]] = None,
        account_purposes: Optional[Union[AccountPurpose, FrozenSet[AccountPurpose]]] = None,
        partitions: Optional[Union[Partition, FrozenSet[Partition]]] = None,
        hubs: Optional[Union[Hub, FrozenSet[Hub]]] = None,
        stages: Optional[Union[Stage, FrozenSet[Stage]]] = None,
    ) -> _ResultType:
        """Return exactly one account that matches the given parameters.

        Filter by given parameters based on each argument with 'and', and 'or' within the collection per argument.
        If the result contains no account or more than one, an exception is raised.
        """
        return AccountStore._assure_only_one_account_matches(
            self._query_accounts(
                account_purposes=account_purposes,
                account_ids=account_ids,
                environments=environments,
                partitions=partitions,
                hubs=hubs,
                stages=stages,
            )
        )

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def query_resource_accounts(
        self,
        environments: Union[Environment, FrozenSet[Environment]],
        account_ids: Optional[Union[AccountId, FrozenSet[AccountId]]] = None,
        partitions: Optional[Union[Partition, FrozenSet[Partition]]] = None,
        hubs: Optional[Union[Hub, FrozenSet[Hub]]] = None,
        stages: Optional[Union[Stage, FrozenSet[Stage]]] = None,
        only_default: bool = False,
    ) -> Collection[ResourceAccount]:
        """Return collection of ResourceAccounts that matches the given parameters.

        Filter by parameters based on each argument with 'and', and 'or' within the collection per argument.
        If only_default is set to True, the resource account with the highest stage_priority per hub-stage-combination
        is returned. This only affects the result, if multiple resource accounts are defined for a
        hub-stage-combination.
        """
        resource_accounts = cast(
            Collection[ResourceAccount],
            self._query_accounts(
                account_purposes=AccountPurpose("resources"),
                account_ids=account_ids,
                environments=environments,
                partitions=partitions,
                hubs=hubs,
                stages=stages,
            ),
        )
        if only_default:
            default_resource_accounts: Dict[Tuple[Hub, Stage], ResourceAccount] = {}
            for account in resource_accounts:
                if old_acc := default_resource_accounts.get((account.hub, account.stage)):
                    default_resource_accounts[(account.hub, account.stage)] = (
                        old_acc if old_acc.stage_priority > account.stage_priority else account
                    )
                else:
                    default_resource_accounts[(account.hub, account.stage)] = account
            return frozenset(default_resource_accounts.values())
        return resource_accounts

    def query_resource_account(
        self,
        environments: Union[Environment, FrozenSet[Environment]],
        account_ids: Optional[Union[AccountId, FrozenSet[AccountId]]] = None,
        partitions: Optional[Union[Partition, FrozenSet[Partition]]] = None,
        hubs: Optional[Union[Hub, FrozenSet[Hub]]] = None,
        stages: Optional[Union[Stage, FrozenSet[Stage]]] = None,
        only_default: bool = False,
    ) -> ResourceAccount:
        """Return exactly one ResourceAccount that matches the given parameters.

        Filter by parameters based on each argument with 'and', and 'or' within the collection per argument.
        If only_default is set to True, the resource account with the highest stage_priority for a
        hub-stage-combination is returned.
        If the result contains no account or more than one, an exception is raised.
        """
        return AccountStore._assure_only_one_account_matches(
            self.query_resource_accounts(
                environments=environments,
                account_ids=account_ids,
                partitions=partitions,
                hubs=hubs,
                stages=stages,
                only_default=only_default,
            )
        )

    @staticmethod
    def _assure_only_one_account_matches(accounts: Collection[T]) -> T:
        count = len(accounts)
        if count == 1:
            return next(iter(accounts))
        if count == 0:
            raise QueryAccountNotFound()
        raise QueryAccountAmbiguous(accounts)

    @staticmethod
    def _convert_arguments(arg: Optional[Union[T, FrozenSet[T]]]) -> FrozenSet[T]:
        if arg is None:
            return frozenset()
        if isinstance(arg, FrozenSet):
            return arg
        return frozenset({arg})

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def _query_accounts(
        self,
        environments: Union[Environment, FrozenSet[Environment]],
        account_ids: Optional[Union[AccountId, FrozenSet[AccountId]]] = None,
        account_purposes: Optional[Union[AccountPurpose, FrozenSet[AccountPurpose]]] = None,
        partitions: Optional[Union[Partition, FrozenSet[Partition]]] = None,
        hubs: Optional[Union[Hub, FrozenSet[Hub]]] = None,
        stages: Optional[Union[Stage, FrozenSet[Stage]]] = None,
    ) -> Collection[_ResultType]:
        return frozenset(
            filter(
                self._get_account_filter(
                    account_ids=self._convert_arguments(account_ids),
                    environments=self._convert_arguments(environments),
                    account_purposes=self._convert_arguments(account_purposes),
                    partitions=self._convert_arguments(partitions),
                    hubs=self._convert_arguments(hubs),
                    stages=self._convert_arguments(stages),
                ),
                self._accounts,
            )
        )

    @staticmethod
    def _get_account_filter(
        account_ids: FrozenSet[AccountId],
        environments: FrozenSet[Environment],
        account_purposes: FrozenSet[AccountPurpose],
        partitions: FrozenSet[Partition],
        hubs: FrozenSet[Hub],
        stages: FrozenSet[Stage],
    ) -> Callable[[_ResultType], bool]:
        attribute_mapping: Dict[str, FrozenSet[Any]] = {
            "id": account_ids,
            "purpose": account_purposes,
            "environment": environments,
            "partition": partitions,
            "hub": hubs,
            "stage": stages,
        }

        def account_filter(account: _ResultType) -> bool:
            for attribute, to_test in attribute_mapping.items():
                if to_test:
                    if not hasattr(account, attribute):
                        return False
                    if getattr(account, attribute) not in to_test:
                        return False
            return True

        return account_filter

    def get_security_account_for_hub(self, hub: Hub) -> SecurityAccount:
        """Return the SecurityAccount for a given hub.

        If there is no security account explicitly specified in the AccountStore, the security account of the default
        hub is used.
        """
        try:
            return cast(
                SecurityAccount,
                self.query_account(
                    environments=frozenset(Environment), account_purposes=AccountPurpose("security"), hubs=hub
                ),
            )
        except QueryAccountNotFound:
            return cast(
                SecurityAccount,
                self.query_account(
                    environments=frozenset(Environment), account_purposes=AccountPurpose("security"), hubs=Hub.default()
                ),
            )

    @staticmethod
    @lru_cache()
    def _build_default_accounts() -> FrozenSet[_ResultType]:
        accounts: List[_ResultType] = []
        for purpose, purpose_entry in ConfigFileLoader.get_config().account.instances_per_purpose.items():
            for _, account_entry in purpose_entry.account_instances.items():
                fields = {
                    "id": AccountId(account_entry.id),
                    "environment": Environment(account_entry.environment),
                    "partition": Partition(account_entry.partition),
                    "purpose": AccountPurpose(purpose),
                }
                if hub := account_entry.hub:
                    fields["hub"] = Hub(hub)
                if stage := account_entry.stage:
                    fields["stage"] = Stage(stage)
                    fields["stage_priority"] = account_entry.stage_priority

                if purpose == AccountPurpose("security").value:
                    accounts.append(SecurityAccount(**fields))  # type: ignore
                elif purpose == AccountPurpose("resources").value:
                    accounts.append(ResourceAccount(**fields))  # type: ignore
                elif AccountPurpose(purpose).hub_specific:
                    accounts.append(HubAccount(**fields))  # type: ignore
                else:
                    accounts.append(BaseAccount(**fields))  # type: ignore

        return frozenset(accounts)


class AccountStoreException(Exception):
    """Base class for exceptions raised in the AccountStore."""


class QueryAccountNotFound(AccountStoreException):
    """Signals that no account was found that matches the input parameters."""

    def __init__(self) -> None:
        super().__init__("No account matching condition found.")


class QueryAccountAmbiguous(AccountStoreException):
    """Signals that more than one account was found that matches the input parameters."""

    def __init__(self, accounts: Collection[Any]) -> None:
        # hide account information to be potentially visible by customers
        super().__init__("Not exactly one account matching condition found.")
        self.accounts = accounts
