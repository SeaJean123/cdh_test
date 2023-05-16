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
from collections import defaultdict
from typing import Dict
from typing import Optional
from unittest.mock import Mock

import pytest
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.phone_book import PhoneBook

from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.accounts_test import build_base_account
from cdh_core.entities.accounts_test import build_hub_account
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.accounts_test import build_security_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_sts_assumed_role_arn
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.resource_test import build_resource
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.dataset_properties_test import build_layer
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties_test import build_resource_type
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


def build_mock_phone_book(return_values: Optional[Dict[str, bool]] = None) -> Mock:
    return_values = return_values or defaultdict(lambda: False)
    phone_book = Mock(PhoneBook)
    for method_name in dir(PhoneBook):
        if callable(getattr(PhoneBook, method_name)) and method_name.startswith("is_"):
            mock_method = Mock(return_value=return_values[method_name])
            setattr(phone_book, method_name, mock_method)
    return phone_book


class AuthorizerTestCase:
    def setup_method(self) -> None:
        self.config = build_config(use_authorization=True)
        self.accounts_table = Mock(GenericAccountsTable)
        self.accounts_table.get.return_value = build_account()
        self.authorization_api = Mock(AuthorizationApi)
        self.requester_arn = build_arn("sts")
        self.phone_book = build_mock_phone_book()
        self.authorizer = self.build_authorizer()

    def build_authorizer(self) -> Authorizer[Account]:
        return Authorizer(
            config=self.config,
            requester_arn=self.requester_arn,
            auth_api=self.authorization_api,
            phone_book=self.phone_book,
            accounts_table=self.accounts_table,
        )


class TestAuthorizerRequesterAdmin(AuthorizerTestCase):
    def test_is_admin_via_write_roles(self) -> None:
        admin_role = Builder.build_random_string()
        self.requester_arn = build_sts_assumed_role_arn(role_name=admin_role)
        account_id = self.requester_arn.account_id
        self.accounts_table.get.return_value = build_account(
            account_id=account_id,
            roles=[
                AccountRole(
                    name=admin_role,
                    path=Builder.build_random_string(),
                    type=AccountRoleType.WRITE,
                    friendly_name=Builder.build_random_string(),
                )
            ],
        )
        authorizer = self.build_authorizer()

        assert authorizer.requester_is_account_admin(account_id)

    def test_is_admin_via_admin_roles(self) -> None:
        admin_role = Builder.build_random_string()
        self.requester_arn = build_sts_assumed_role_arn(role_name=admin_role)
        account_id = self.requester_arn.account_id
        self.accounts_table.get.return_value = build_account(account_id=account_id, admin_roles=[admin_role])
        authorizer = self.build_authorizer()

        assert authorizer.requester_is_account_admin(account_id)

    def test_non_admin_role(self) -> None:
        self.requester_arn = build_sts_assumed_role_arn()
        account_id = self.requester_arn.account_id
        self.accounts_table.get.return_value = build_account(account_id=account_id)
        authorizer = self.build_authorizer()

        assert not authorizer.requester_is_account_admin(account_id)

    def test_other_account(self) -> None:
        authorizer = self.build_authorizer()

        assert not authorizer.requester_is_account_admin(build_account_id())


class TestAuthorizerDatasetPermissions(AuthorizerTestCase):
    def setup_method(self) -> None:
        super().setup_method()
        self.owner_id = build_account_id()
        self.dataset = build_dataset()
        self.admin_role = Builder.build_random_string()

    def test_unauthorized(self) -> None:
        phone_book_lookups = defaultdict(lambda: True)
        phone_book_lookups[PhoneBook.is_functional_tests_user_role.__name__] = False
        phone_book_lookups[PhoneBook.is_core_api_admin_role.__name__] = False
        phone_book_lookups[PhoneBook.is_privileged_portal_role.__name__] = False
        self.phone_book = build_mock_phone_book(phone_book_lookups)
        authorizer = self.build_authorizer()

        with pytest.raises(ForbiddenError):
            authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    def test_prefix_functional_test_role_may_manage_dataset_access(self) -> None:
        self.phone_book.is_functional_tests_user_role.return_value = True
        self.authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    def test_api_admin_may_manage_dataset_access(self) -> None:
        self.phone_book.is_core_api_admin_role.return_value = True
        self.authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    def test_portal_access_management_role_dev_may_manage_dataset_access(self) -> None:
        self.phone_book.is_privileged_portal_role.return_value = True
        self.authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    # Hint: resource_admin := resource_owner && in_admin_roles
    def test_resource_admin_may_manage_dataset_access(self) -> None:
        self.requester_arn = build_sts_assumed_role_arn(account_id=self.owner_id, role_name=self.admin_role)
        self.accounts_table.get.return_value = build_account(account_id=self.owner_id, admin_roles=[self.admin_role])
        authorizer = self.build_authorizer()

        authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    def test_requester_is_resource_owner_but_role_is_not_assumed_may_not_manage_dataset_access(self) -> None:
        self.requester_arn = build_sts_assumed_role_arn(account_id=self.owner_id, role_name=self.admin_role)
        authorizer = self.build_authorizer()

        with pytest.raises(ForbiddenError):
            authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    def test_requester_is_resource_owner_but_not_in_admin_roles_may_not_manage_dataset_access(self) -> None:
        self.requester_arn = build_sts_assumed_role_arn(
            account_id=self.owner_id, role_name=Builder.build_random_string()
        )
        authorizer = self.build_authorizer()

        with pytest.raises(ForbiddenError):
            authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    def test_requester_is_in_admin_roles_but_not_resource_owner_may_not_manage_dataset_access(self) -> None:
        self.requester_arn = build_sts_assumed_role_arn(account_id=build_account_id(), role_name=self.admin_role)
        self.accounts_table.get.return_value = build_account(
            account_id=self.requester_arn.account_id, admin_roles=[self.admin_role]
        )
        authorizer = self.build_authorizer()

        with pytest.raises(ForbiddenError):
            authorizer.check_requester_may_manage_dataset_access(self.owner_id)

    def test_dataset_admin_may_not_manage_dataset_access_if_not_resource_owner(self) -> None:
        self.requester_arn = build_sts_assumed_role_arn(
            account_id=self.dataset.owner_account_id, role_name=self.admin_role
        )
        self.accounts_table.get.return_value = build_account(
            account_id=self.dataset.owner_account_id,
            admin_roles=[self.admin_role],
        )
        authorizer = self.build_authorizer()

        with pytest.raises(ForbiddenError):
            authorizer.check_requester_may_manage_dataset_access(build_account_id())


class TestAuthorizerCreateDataset(AuthorizerTestCase):
    def test_forbidden_dataset_creation_throws_error(self) -> None:
        self.phone_book = build_mock_phone_book(return_values=defaultdict(lambda: True))
        error_message = Builder.build_random_string()
        self.authorization_api = Mock(AuthorizationApi)
        self.authorization_api.is_dataset_creatable.return_value = (False, error_message)
        authorizer = self.build_authorizer()

        with assert_raises(ForbiddenError(error_message)):
            authorizer.check_requester_may_create_dataset(
                hub=build_hub(),
                layer=build_layer(),
                business_object=build_business_object(),
                owner_account_id=build_account_id(),
            )

    def test_auth_api_allows(self) -> None:
        self.authorization_api = Mock(AuthorizationApi)
        self.authorization_api.is_dataset_creatable.return_value = (True, Builder.build_random_string())
        authorizer = self.build_authorizer()

        authorizer.check_requester_may_create_dataset(
            hub=build_hub(),
            layer=build_layer(),
            business_object=build_business_object(),
            owner_account_id=build_account_id(),
        )
        self.authorization_api.is_dataset_creatable.assert_called_once()

    def test_allowed_if_no_authorization(self) -> None:
        self.config = build_config(use_authorization=False)
        authorizer = self.build_authorizer()

        authorizer.check_requester_may_create_dataset(
            hub=build_hub(),
            layer=build_layer(),
            business_object=build_business_object(),
            owner_account_id=build_account_id(),
        )
        self.authorization_api.is_dataset_creatable.assert_not_called()

    @pytest.mark.parametrize("allowed_purpose", [AccountPurpose("portal"), AccountPurpose("test")])
    def test_allowed_for_account_owner_with_can_be_owner_purpose(self, allowed_purpose: AccountPurpose) -> None:
        account_id = build_account_id()
        if allowed_purpose is AccountPurpose("portal"):
            allowed_entry = build_base_account(purpose=allowed_purpose, account_id=account_id)
        else:
            allowed_entry = build_hub_account(purpose=allowed_purpose, account_id=account_id)
        not_allowed_entries = [
            build_security_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("api"), account_id=account_id),
            build_resource_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("iam"), account_id=account_id),
        ]

        account_store = AccountStore(accounts=[allowed_entry] + not_allowed_entries)
        self.config = build_config(use_authorization=False, account_store=account_store)
        authorizer = self.build_authorizer()

        authorizer.check_requester_may_create_dataset(
            hub=build_hub(),
            layer=build_layer(),
            business_object=build_business_object(),
            owner_account_id=account_id,
        )

    def test_forbidden_for_account_owner_without_can_be_owner_purpose(self) -> None:
        account_id = build_account_id()
        not_allowed_entries = [
            build_security_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("api"), account_id=account_id),
            build_resource_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("iam"), account_id=account_id),
        ]
        account_store = AccountStore(accounts=not_allowed_entries)
        self.config = build_config(use_authorization=False, account_store=account_store)
        authorizer = self.build_authorizer()

        with assert_raises(ForbiddenError(f"Account {account_id} is not authorized to own datasets.")):
            authorizer.check_requester_may_create_dataset(
                hub=build_hub(),
                layer=build_layer(),
                business_object=build_business_object(),
                owner_account_id=account_id,
            )


class TestAuthorizerCheckMayUpdateDataset(AuthorizerTestCase):
    def test_allow_if_no_authorization(self) -> None:
        config = build_config(use_authorization=False)
        authorizer: Authorizer[Account] = Authorizer(
            config=config,
            requester_arn=self.requester_arn,
            auth_api=self.authorization_api,
            phone_book=self.phone_book,
            accounts_table=self.accounts_table,
        )
        authorizer.check_requester_may_update_dataset(build_dataset())

    def test_allowed_with_authorization(self) -> None:
        self.authorization_api.is_dataset_updatable.return_value = True, Builder.build_random_string()

        self.authorizer.check_requester_may_update_dataset(build_dataset())

    def test_not_allowed_with_authorization(self) -> None:
        message = Builder.build_random_string()
        self.authorization_api.is_dataset_updatable.return_value = False, message

        with assert_raises(ForbiddenError(message)):
            self.authorizer.check_requester_may_update_dataset(build_dataset())

    def test_allowed_if_privileged_portal_role(self) -> None:
        self.phone_book.is_privileged_portal_role.return_value = True

        self.authorizer.check_requester_may_update_dataset(build_dataset())

        self.authorization_api.is_dataset_updatable.assert_not_called()

    def test_allowed_if_prefix_functional_test_role(self) -> None:
        self.phone_book.is_functional_tests_user_role.return_value = True

        self.authorizer.check_requester_may_update_dataset(build_dataset())

        self.authorization_api.is_dataset_updatable.assert_not_called()

    def test_allowed_if_api_admin(self) -> None:
        self.phone_book.is_core_api_admin_role.return_value = True

        self.authorizer.check_requester_may_update_dataset(build_dataset())

        self.authorization_api.is_dataset_updatable.assert_not_called()


class TestAuthorizerCheckMayReleaseDataset(AuthorizerTestCase):
    def test_not_allowed_unless_privileged_portal_role(self) -> None:
        with pytest.raises(ForbiddenError):
            self.authorizer.check_requester_may_release_dataset()

    def test_allowed_for_privileged_portal_role(self) -> None:
        self.phone_book.is_privileged_portal_role.return_value = True
        self.authorizer.check_requester_may_release_dataset()


class TestAuthorizerDeleteDataset(AuthorizerTestCase):
    def test_forbidden_dataset_deletion_throws_error(self) -> None:
        self.phone_book = build_mock_phone_book(defaultdict(lambda: True))
        error_message = Builder.build_random_string()
        self.authorization_api = Mock(AuthorizationApi)
        self.authorization_api.is_dataset_deletable.return_value = (False, error_message)
        authorizer = self.build_authorizer()

        with assert_raises(ForbiddenError(error_message)):
            authorizer.check_requester_may_delete_dataset(build_dataset())

    def test_auth_api_allows(self) -> None:
        dataset = build_dataset()
        self.authorization_api = Mock(AuthorizationApi)
        self.authorization_api.is_dataset_deletable.return_value = (True, Builder.build_random_string())
        authorizer = self.build_authorizer()

        authorizer.check_requester_may_delete_dataset(dataset)
        self.authorization_api.is_dataset_deletable.assert_called_once_with(dataset_id=dataset.id)

    def test_allowed_if_no_authorization(self) -> None:
        dataset = build_dataset()
        self.config = build_config(use_authorization=False)
        authorizer = self.build_authorizer()

        authorizer.check_requester_may_delete_dataset(dataset)

        self.authorization_api.is_dataset_deletable.assert_not_called()


class TestAuthorizerCheckMayManageAccounts(AuthorizerTestCase):
    def test_unauthorized(self) -> None:
        phone_book_lookups = defaultdict(lambda: True)
        phone_book_lookups[PhoneBook.is_core_api_admin_role.__name__] = False
        phone_book_lookups[PhoneBook.is_privileged_portal_role.__name__] = False
        phone_book_lookups[PhoneBook.is_functional_tests_user_role.__name__] = False
        self.phone_book = build_mock_phone_book(phone_book_lookups)
        self.authorizer = self.build_authorizer()

        with pytest.raises(ForbiddenError):
            self.authorizer.check_requester_may_manage_accounts()

    def test_api_admin_is_authorized(self) -> None:
        self.phone_book.is_core_api_admin_role.return_value = True
        self.authorizer.check_requester_may_manage_accounts()

    def test_portal_role_is_authorized(self) -> None:
        self.phone_book.is_privileged_portal_role.return_value = True
        self.authorizer.check_requester_may_manage_accounts()

    def test_functional_tests_user_role_is_authorized(self) -> None:
        self.phone_book.is_functional_tests_user_role.return_value = True
        self.authorizer.check_requester_may_manage_accounts()


class TestAuthorizerCheckMayDeleteAccounts(AuthorizerTestCase):
    def test_unauthorized(self) -> None:
        phone_book_lookups = defaultdict(lambda: True)
        phone_book_lookups[PhoneBook.is_core_api_admin_role.__name__] = False
        phone_book_lookups[PhoneBook.is_privileged_portal_role.__name__] = False
        phone_book_lookups[PhoneBook.is_functional_tests_user_role.__name__] = False
        self.phone_book = build_mock_phone_book(phone_book_lookups)
        self.authorizer = self.build_authorizer()

        with pytest.raises(ForbiddenError):
            self.authorizer.check_requester_may_delete_accounts()

    def test_api_admin_is_authorized(self) -> None:
        self.phone_book.is_core_api_admin_role.return_value = True
        self.authorizer.check_requester_may_delete_accounts()

    def test_portal_access_management_is_authorized(self) -> None:
        self.phone_book.is_privileged_portal_role.return_value = True
        self.authorizer.check_requester_may_delete_accounts()

    def test_functional_tests_user_role_is_authorized(self) -> None:
        self.phone_book.is_functional_tests_user_role.return_value = True
        self.authorizer.check_requester_may_delete_accounts()


class TestAuthorizerDeleteResource(AuthorizerTestCase):
    def test_allow_if_no_authorization(self) -> None:
        config = build_config(use_authorization=False)
        authorizer: Authorizer[Account] = Authorizer(
            config=config,
            requester_arn=self.requester_arn,
            auth_api=self.authorization_api,
            phone_book=self.phone_book,
            accounts_table=self.accounts_table,
        )
        authorizer.check_requester_may_delete_resource(build_resource())

    def test_authorized(self) -> None:
        self.authorization_api.is_resource_deletable.return_value = (True, None)

        self.authorizer.check_requester_may_delete_resource(build_resource())

    def test_forbidden_resource_deletion_unauthorized(self) -> None:
        error_message = Builder.build_random_string()
        self.authorization_api.is_resource_deletable.return_value = (False, error_message)

        with assert_raises(ForbiddenError(error_message)):
            self.authorizer.check_requester_may_delete_resource(build_resource())


class TestAuthorizerCreateResource(AuthorizerTestCase):
    def test_allow_if_no_authorization(self) -> None:
        config = build_config(use_authorization=False)
        authorizer: Authorizer[Account] = Authorizer(
            config=config,
            requester_arn=self.requester_arn,
            auth_api=self.authorization_api,
            phone_book=self.phone_book,
            accounts_table=self.accounts_table,
        )
        self._check_requester_may_create_resource(authorizer)

    def test_authorized(self) -> None:
        self.authorization_api.is_resource_creatable.return_value = (True, None)

        self._check_requester_may_create_resource()

    def test_forbidden_resource_creation_unauthorized(self) -> None:
        error_message = Builder.build_random_string()
        self.authorization_api.is_resource_creatable.return_value = (False, error_message)

        with assert_raises(ForbiddenError(error_message)):
            self._check_requester_may_create_resource()

    @pytest.mark.parametrize("allowed_purpose", [AccountPurpose("portal"), AccountPurpose("test")])
    def test_allowed_for_account_owner_with_can_be_owner_purpose(self, allowed_purpose: AccountPurpose) -> None:
        self.authorization_api.is_resource_creatable.return_value = (True, None)
        account_id = build_account_id()
        if allowed_purpose is AccountPurpose("portal"):
            allowed_entry = build_base_account(purpose=allowed_purpose, account_id=account_id)
        else:
            allowed_entry = build_hub_account(purpose=allowed_purpose, account_id=account_id)
        not_allowed_entries = [
            build_security_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("api"), account_id=account_id),
            build_resource_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("iam"), account_id=account_id),
        ]

        account_store = AccountStore(accounts=[allowed_entry] + not_allowed_entries)
        self.config = build_config(use_authorization=False, account_store=account_store)
        authorizer = self.build_authorizer()

        self._check_requester_may_create_resource(authorizer=authorizer, owner_account_id=account_id)

    def test_forbidden_for_account_owner_without_can_be_owner_purpose(self) -> None:
        self.authorization_api.is_resource_creatable.return_value = (True, None)
        account_id = build_account_id()
        not_allowed_entries = [
            build_security_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("api"), account_id=account_id),
            build_resource_account(account_id=account_id),
            build_base_account(purpose=AccountPurpose("iam"), account_id=account_id),
        ]
        account_store = AccountStore(accounts=not_allowed_entries)
        self.config = build_config(use_authorization=False, account_store=account_store)
        authorizer = self.build_authorizer()

        with assert_raises(ForbiddenError(f"Account {account_id} is not authorized to own resources.")):
            self._check_requester_may_create_resource(authorizer=authorizer, owner_account_id=account_id)

    def _check_requester_may_create_resource(
        self,
        authorizer: Optional[Authorizer[Account]] = None,
        resource_type: Optional[ResourceType] = None,
        owner_account_id: Optional[AccountId] = None,
    ) -> None:
        if not authorizer:
            authorizer = self.authorizer

        authorizer.check_requester_may_create_resource(
            dataset=build_dataset(),
            region=build_region(),
            stage=build_stage(),
            resource_type=resource_type or build_resource_type(),
            owner_account_id=owner_account_id or build_account_id(),
        )
