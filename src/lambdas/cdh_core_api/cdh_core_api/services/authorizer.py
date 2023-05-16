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
from typing import Optional

from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.phone_book import PhoneBook

from cdh_core.entities.accounts import AccountRoleType
from cdh_core.entities.arn import Arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.resource import Resource
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.primitives.account_id import AccountId


class Authorizer(Generic[GenericAccount]):
    """Verifies the authorization of the requester."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        config: Config,
        requester_arn: Arn,
        auth_api: AuthorizationApi,
        phone_book: PhoneBook,
        accounts_table: GenericAccountsTable[GenericAccount],
    ):
        self._config = config
        self._requester_arn = requester_arn
        self._authorization_api = auth_api
        self._phone_book = phone_book
        self._accounts_table = accounts_table
        self._requester_account: Optional[GenericAccount] = None

    def _get_requester_account(self) -> GenericAccount:
        if self._requester_account is None:
            try:
                self._requester_account = self._accounts_table.get(self._requester_arn.account_id)
            except AccountNotFound as err:
                raise ForbiddenError("Requesting account is not registered with the CDH Core API") from err
        return self._requester_account

    def requester_is_account_admin(self, account_id: AccountId) -> bool:
        """Check whether the requester has an admin role of a given account."""
        requester_account = self._get_requester_account()
        admin_roles = requester_account.admin_roles + [
            role.name for role in requester_account.roles if role.type is AccountRoleType.WRITE
        ]
        return requester_account.id == account_id and self._requester_assumed_role_name in admin_roles

    @property
    def _requester_assumed_role_name(self) -> Optional[str]:
        try:
            return self._requester_arn.get_assumed_role_name()
        except ValueError:
            return None

    def _requester_has_api_admin_permissions(self) -> bool:
        return self._phone_book.is_core_api_admin_role(
            self._requester_arn
        ) or self._phone_book.is_functional_tests_user_role(self._requester_arn)

    def check_requester_may_manage_dataset_access(self, resource_owner_id: AccountId) -> None:
        """Check whether the requester is allowed to grant and revoke dataset access."""
        if self._requester_has_api_admin_permissions():
            return
        if self._phone_book.is_privileged_portal_role(self._requester_arn):
            return
        if not self.requester_is_account_admin(resource_owner_id):
            raise ForbiddenError(
                f"Only admin roles of the following accounts are allowed "
                f"to manage dataset permissions: owning account ({resource_owner_id}) "
                f"and CDH Core ({self._config.lambda_account_id})."
            )

    def _ownership_allowed_for_account_purposes(self, owner_account_id: AccountId) -> bool:
        matched_accounts = self._config.account_store.query_accounts(
            environments=frozenset(Environment),
            account_ids=owner_account_id,
        )
        if not matched_accounts:
            return True
        return any(account.purpose.can_be_owner for account in matched_accounts)

    def check_requester_may_create_dataset(
        self, hub: Hub, business_object: BusinessObject, layer: Layer, owner_account_id: AccountId
    ) -> None:
        """Check whether the requester is allowed to create a dataset with the given business object and layer."""
        if not self._ownership_allowed_for_account_purposes(owner_account_id):
            raise ForbiddenError(f"Account {owner_account_id} is not authorized to own datasets.")
        if self._config.using_authorization_api:
            allowed_to_create, error_message = self._authorization_api.is_dataset_creatable(
                layer=layer,
                hub=hub,
                business_object=business_object,
                owner_account_id=owner_account_id,
            )
            if not allowed_to_create:
                raise ForbiddenError(error_message)

    def check_requester_may_update_dataset(self, dataset: Dataset) -> None:
        """Check whether the requester is allowed to update the given dataset."""
        if self._requester_has_api_admin_permissions():
            return
        if self._phone_book.is_privileged_portal_role(self._requester_arn):
            return

        if self._config.using_authorization_api:
            allowed_to_update, error_message = self._authorization_api.is_dataset_updatable(dataset_id=dataset.id)
            if not allowed_to_update:
                raise ForbiddenError(error_message)

    def check_requester_may_release_dataset(self) -> None:
        """Check whether the requester is allowed to release any dataset."""
        if not self._phone_book.is_privileged_portal_role(self._requester_arn):
            raise ForbiddenError(f"Requester {self._requester_arn} is not authorized to release a dataset.")

    def check_requester_may_delete_dataset(self, dataset: Dataset) -> None:
        """Check whether the requester is allowed to delete the given dataset."""
        if self._config.using_authorization_api:
            allowed_to_delete, error_message = self._authorization_api.is_dataset_deletable(dataset_id=dataset.id)
            if not allowed_to_delete:
                raise ForbiddenError(error_message)

    def check_requester_may_manage_accounts(self) -> None:
        """Check whether the requester is allowed to manage any account."""
        if self._requester_has_api_admin_permissions():
            return
        if self._phone_book.is_privileged_portal_role(self._requester_arn):
            return
        raise ForbiddenError(f"Requester {self._requester_arn} is not authorized to manage accounts.")

    def check_requester_may_delete_accounts(self) -> None:
        """Check whether the requester is allowed to delete any account."""
        if self._requester_has_api_admin_permissions() or self._phone_book.is_privileged_portal_role(
            self._requester_arn
        ):
            return
        raise ForbiddenError(f"Requester {self._requester_arn} is not authorized to delete accounts.")

    def check_requester_may_delete_resource(self, resource: Resource) -> None:
        """Check whether the requester is allowed to delete the resource."""
        if self._config.using_authorization_api:
            allowed_to_delete, error_message = self._authorization_api.is_resource_deletable(resource)
            if not allowed_to_delete:
                raise ForbiddenError(error_message)

    def check_requester_may_create_resource(  # pylint: disable=too-many-arguments
        self,
        dataset: Dataset,
        stage: Stage,
        region: Region,
        resource_type: ResourceType,
        owner_account_id: AccountId,
    ) -> None:
        """Check whether the requester is allowed to create the resource."""
        if not self._ownership_allowed_for_account_purposes(owner_account_id):
            raise ForbiddenError(f"Account {owner_account_id} is not authorized to own resources.")
        if self._config.using_authorization_api:
            allowed_to_create, error_message = self._authorization_api.is_resource_creatable(
                dataset_id=dataset.id,
                stage=stage,
                resource_type=resource_type,
                region=region,
                owner_account_id=owner_account_id,
            )
            if not allowed_to_create:
                raise ForbiddenError(error_message)
