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
from enum import Enum
from typing import TYPE_CHECKING

from cdh_core.config.config_file_loader import ConfigFileLoader


# pylint: disable=invalid-name


class AccountType(Enum):
    """Usage type of the account."""

    internal = "internal"  # accounts belonging to cdh team
    provider = "provider"
    technical = "technical"  # accounts that do not provide or consume data, e.g. tooling account
    usecase = "usecase"

    @property
    def friendly_name(self) -> str:
        """Return the friendly name of the account type."""
        return self.value.title()  # pylint: disable=no-member


class AccountPurposeMixin(Enum):
    """Provides the logic for the Enum AccountPurpose."""

    @property
    def deployed_by_cdh_core(self) -> bool:
        """Return True if a type of account is deployed via terraform by the cdh_core."""
        return self.name in {
            instance
            for instance, entry in ConfigFileLoader.get_config().account.purpose.instances.items()
            if entry.deployed_by_cdh_core
        }

    @property
    def hub_specific(self) -> bool:
        """Return True if a type of account is associated with a certain hub."""
        return self.name in {
            instance
            for instance, entry in ConfigFileLoader.get_config().account.purpose.instances.items()
            if entry.hub_specific
        }

    @property
    def can_be_owner(self) -> bool:
        """Return True if a type of account is allowed to own datasets and resources."""
        return self.name in {
            instance
            for instance, entry in ConfigFileLoader.get_config().account.purpose.instances.items()
            if entry.can_be_owner
        }


if TYPE_CHECKING:
    AccountPurpose = AccountPurposeMixin
else:
    AccountPurpose = Enum(
        "AccountPurpose",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().account.purpose.instances.items()},
        type=AccountPurposeMixin,
        module=__name__,
    )
AccountPurpose.__doc__ = "Defines the primary purpose of the account."


class AffiliationMixin(Enum):
    """Provides the logic for the Enum Affiliation."""

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for an affiliation."""
        return ConfigFileLoader.get_config().affiliation.instances[self.name].friendly_name

    @property
    def access_management(self) -> bool:
        """Return the access management for an affiliation."""
        return ConfigFileLoader.get_config().affiliation.instances[self.name].access_management


if TYPE_CHECKING:
    Affiliation = AffiliationMixin
else:
    Affiliation = Enum(
        "Affiliation",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().affiliation.instances.items()},
        type=AffiliationMixin,
        module=__name__,
    )
Affiliation.__doc__ = """The affiliation of an account is derived from the entity that provisioned it"""
