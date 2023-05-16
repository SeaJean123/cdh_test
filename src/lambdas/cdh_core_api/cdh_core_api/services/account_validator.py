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
from typing import Generic
from typing import Type

from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.generic_types import GenericUpdateAccountBody
from cdh_core_api.services.account_environment_verifier import AccountEnvironmentVerificationFailed
from cdh_core_api.services.account_environment_verifier import AccountEnvironmentVerifier
from cdh_core_api.services.account_id_verifier import AccountIdVerifier
from cdh_core_api.services.metadata_role_assumer import GenericAssumableAccountSpec
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id import AccountId


class AccountValidator(Generic[GenericAccount, GenericUpdateAccountBody]):
    """Validator for accounts."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        account_environment_verifier: AccountEnvironmentVerifier[GenericAccount, GenericUpdateAccountBody],
        account_id_verifier: AccountIdVerifier,
        accounts_table: GenericAccountsTable[GenericAccount],
        assumable_account_spec_cls: Type[GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody]],
        datasets_table: DatasetsTable,
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
    ):
        self._account_environment_verifier = account_environment_verifier
        self._account_id_verifier = account_id_verifier
        self._accounts_table = accounts_table
        self._assumable_account_spec_cls = assumable_account_spec_cls
        self._datasets_table = datasets_table
        self._resources_table = resources_table
        self._visible_data_loader = visible_data_loader

    def validate_new_account(self, account: GenericAccount) -> None:
        """Validate an account."""
        self._validate_new_account_static_part(account)
        self._validate_new_account_dynamic_part(account)

    def _validate_new_account_static_part(self, account: GenericAccount) -> None:
        pass

    def _validate_new_account_dynamic_part(self, account: GenericAccount) -> None:
        """Dynamically validate an account.

        Verifies that the account exists in the partition, it is not yet registered, and it has been bootstrapped
        correctly.

        :param account the account to be validated
        :raises ConflictError if the account is already registered or it has not been bootstrapped for the current
        environment
        """
        self._account_id_verifier.verify(account.id, account.hub.partition)
        if self._accounts_table.exists(account.id):
            raise ConflictError(f"Account {account.id} is already registered")
        try:
            self._account_environment_verifier.verify(
                account_spec=self._assumable_account_spec_cls.from_account(account)
            )
        except AccountEnvironmentVerificationFailed as err:
            raise ConflictError(f"Account {account.id} has not been bootstrapped for this env.") from err

    def validate_update_body(self, account: GenericAccount, body: GenericUpdateAccountBody) -> None:
        """Validate the update body for an existing account."""
        self._validate_update_body_static_part(account, body)
        self._validate_update_body_dynamic_part(account, body)

    def _validate_update_body_static_part(self, account: GenericAccount, body: GenericUpdateAccountBody) -> None:
        if body.stages:
            for stage in account.stages:
                if stage not in body.stages:
                    raise ForbiddenError(f"Cannot remove stage {stage}, this route only allows adding stages.")

    def _validate_update_body_dynamic_part(self, account: GenericAccount, body: GenericUpdateAccountBody) -> None:
        try:
            self._account_environment_verifier.verify(
                account_spec=self._assumable_account_spec_cls.from_account_and_update_body(account, body)
            )
        except AccountEnvironmentVerificationFailed as err:
            raise ConflictError(
                f"{account.id} has not been bootstrapped for this env and hence cannot be updated."
            ) from err

    def get_account(self, account_id: AccountId) -> GenericAccount:
        """Get an account if it is visible.

        :param account_id of the account to be returned
        :returns the requested account
        :raises NotFoundError if the account was not found or is not visible
        """
        try:
            return self._visible_data_loader.get_account(account_id)
        except AccountNotFound as err:
            raise NotFoundError(err) from err

    def check_account_can_be_deregistered(self, account: GenericAccount) -> None:
        """Check prerequisites for the deregistration of an account.

        :param account to be deregistered
        :raises ForbiddenError if the account still owns datasets or resources
        """
        owned_dataset_ids = [dataset.id for dataset in self._datasets_table.list(owner=account.id)]
        owned_resources_info = [
            (resource.type.value, resource.dataset_id, resource.stage.value, resource.region.value)
            for resource in self._resources_table.list(owner=account.id)
        ]

        message = ""
        if owned_dataset_ids:
            message += f"It owns datasets {owned_dataset_ids}. "
        if owned_resources_info:
            message += f"It owns resources {owned_resources_info}. "

        if message:
            raise ForbiddenError(f"Cannot deregister account {account.id!r} for the following reasons: {message}")
