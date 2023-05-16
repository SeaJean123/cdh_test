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
from logging import getLogger
from typing import Generic

from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericUpdateAccountBody
from cdh_core_api.services.metadata_role_assumer import CannotAssumeMetadataRole
from cdh_core_api.services.metadata_role_assumer import GenericAssumableAccountSpec
from cdh_core_api.services.metadata_role_assumer import MetadataRoleAssumer
from cdh_core_api.services.metadata_role_assumer import UnsupportedAssumeMetadataRole

from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)


class AccountEnvironmentVerifier(Generic[GenericAccount, GenericUpdateAccountBody]):
    """Verifies the environment of an account."""

    def __init__(self, metadata_role_assumer: MetadataRoleAssumer[GenericAccount, GenericUpdateAccountBody]) -> None:
        self._metadata_role_assumer = metadata_role_assumer

    def verify(
        self,
        account_spec: GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody],
        strict: bool = False,
    ) -> None:
        """Verify the environment of an account by assuming its metadata role."""
        try:
            self._metadata_role_assumer.assume(account_spec)
        except CannotAssumeMetadataRole as err:
            raise AccountEnvironmentVerificationFailed(account_spec.account_id) from err
        except UnsupportedAssumeMetadataRole as err:
            if strict:
                raise AccountEnvironmentVerificationFailed(account_spec.account_id) from err


class AccountEnvironmentVerificationFailed(Exception):
    """Signals the environment verification of the given account failed."""

    def __init__(self, account_id: AccountId) -> None:
        super().__init__(f"Account Environment Verification failed for {account_id=}.")
