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
from datetime import datetime
from http import HTTPStatus
from random import randint
from typing import cast
from typing import Optional
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.accounts import NewAccountBody
from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.bodies.accounts_test import build_new_account_body
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.base_test import build_last_evaluated_key
from cdh_core_api.config_test import build_config
from cdh_core_api.endpoints.accounts import AccountPath
from cdh_core_api.endpoints.accounts import AccountQuerySchema
from cdh_core_api.endpoints.accounts import deregister_account
from cdh_core_api.endpoints.accounts import get_account
from cdh_core_api.endpoints.accounts import get_accounts
from cdh_core_api.endpoints.accounts import register_account
from cdh_core_api.endpoints.accounts import update_account
from cdh_core_api.services.account_manager import AccountManager
from cdh_core_api.services.account_validator import AccountValidator
from cdh_core_api.services.pagination_service import NextPageTokenContext
from cdh_core_api.services.pagination_service import PaginationService
from cdh_core_api.services.response_account_builder import ResponseAccountBuilder
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from freezegun import freeze_time

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import ResponseAccount
from cdh_core.entities.accounts import ResponseAccounts
from cdh_core.entities.accounts import ResponseAccountWithoutCosts
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.accounts_test import build_account_type
from cdh_core.enums.accounts_test import build_affiliation
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.dataset_properties_test import build_layer
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder

NOW = datetime.now()


def assert_account_unchanged(account: Account, accounts_table: AccountsTable) -> None:
    assert accounts_table.get(account.id) == account


class TestGetAccount:
    def setup_method(self) -> None:
        self.account = build_account()
        self.account_validator = Mock(AccountValidator)
        self.response_account_class = ResponseAccount
        self.response_account_builder: ResponseAccountBuilder[Account] = ResponseAccountBuilder()

    def _get_account(self) -> JsonResponse:
        return get_account(
            path=AccountPath(self.account.id),
            account_validator=self.account_validator,
            response_account_builder=self.response_account_builder,
        )

    def test_get_successful(self) -> None:
        self.account_validator.get_account.return_value = self.account
        response_account = self.account.to_response_account(self.response_account_class)
        assert self._get_account().body == response_account

    def test_get_unknown_account(self) -> None:
        self.account_validator.get_account.side_effect = NotFoundError()
        with pytest.raises(NotFoundError):
            self._get_account()


class TestGetAccounts:
    def setup_method(self) -> None:
        self.accounts = [build_account() for _ in range(10)]
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.pagination_service = Mock(PaginationService)
        self.pagination_service.decode_token.return_value = None

    @pytest.mark.parametrize("returns_last_evaluated_key", [True, False])
    def test_get_accounts(self, returns_last_evaluated_key: bool) -> None:
        last_evaluated_key = build_last_evaluated_key() if returns_last_evaluated_key else None
        self.visible_data_loader.get_accounts.return_value = [self.accounts, last_evaluated_key]

        returned_accounts = cast(
            ResponseAccounts,
            get_accounts(
                query=Mock(), config=Mock(), visible_data_loader=self.visible_data_loader, pagination_service=Mock()
            ).body,
        ).accounts

        assert returned_accounts == [
            account.to_response_account(ResponseAccountWithoutCosts) for account in self.accounts
        ]

    def test_return_next_page_token(self) -> None:
        last_evaluated_key = build_last_evaluated_key()
        self.visible_data_loader.get_accounts.return_value = ([], last_evaluated_key)
        encrypted_token = Builder.build_random_string()
        self.pagination_service.issue_token.return_value = encrypted_token

        response = get_accounts(
            query=AccountQuerySchema(),
            visible_data_loader=self.visible_data_loader,
            config=build_config(),
            pagination_service=self.pagination_service,
        )

        assert response.headers["nextPageToken"] == encrypted_token
        self.pagination_service.issue_token.assert_called_once_with(
            last_evaluated_key=last_evaluated_key,
            context=NextPageTokenContext.ACCOUNTS,
        )

    def test_with_next_page_token_in_query(self) -> None:
        self.visible_data_loader.get_accounts.return_value = ([], None)
        next_page_token = Builder.build_random_string()
        last_evaluated_key = build_last_evaluated_key()
        self.pagination_service.decode_token.return_value = last_evaluated_key
        page_size = randint(1, 10)

        get_accounts(
            query=AccountQuerySchema(nextPageToken=next_page_token),
            visible_data_loader=self.visible_data_loader,
            config=build_config(result_page_size=page_size),
            pagination_service=self.pagination_service,
        )

        assert self.visible_data_loader.get_accounts.call_args.kwargs["last_evaluated_key"] == last_evaluated_key
        assert self.visible_data_loader.get_accounts.call_args.kwargs["limit"] == page_size
        self.pagination_service.decode_token.assert_called_once_with(
            next_page_token=next_page_token, context=NextPageTokenContext.ACCOUNTS
        )


class TestRegisterAccount:
    def setup_method(self) -> None:
        self.account_manager = Mock(AccountManager)
        self.account_validator = Mock(AccountValidator)
        self.authorizer = Mock()
        self.sns_publisher = Mock(SnsPublisher)
        self.response_account_builder: ResponseAccountBuilder[Account] = ResponseAccountBuilder()

    def _register_account(
        self,
        body: Optional[NewAccountBody] = None,
        account: Optional[Account] = None,
    ) -> JsonResponse:
        return register_account(
            body=body or build_new_account_body(account),
            account_manager=self.account_manager,
            account_validator=self.account_validator,
            authorizer=self.authorizer,
            sns_publisher=self.sns_publisher,
            response_account_builder=self.response_account_builder,
        )

    def test_register_account_unauthorized_fails(self) -> None:
        self.authorizer.check_requester_may_manage_accounts.side_effect = ForbiddenError
        with pytest.raises(ForbiddenError):
            self._register_account()

        self.account_validator.validate_new_account.assert_not_called()

    @freeze_time(NOW)
    def test_register_account_successful(self) -> None:
        body = build_new_account_body()
        account = body.to_account()
        response_account = self.response_account_builder.get_response_account(account)

        response: JsonResponse = self._register_account(body=body)

        expected_account = account
        assert response.status_code == HTTPStatus.CREATED
        assert response.body == response_account

        self.authorizer.check_requester_may_manage_accounts.assert_called_once()
        self.account_validator.validate_new_account.assert_called_once_with(expected_account)
        self.account_manager.create.assert_called_once_with(expected_account)
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.ACCOUNT,
            operation=Operation.CREATE,
            payload=response_account,
        )

    def test_register_account_validation_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_validator.validate_new_account.side_effect = exception

        with assert_raises(exception):
            self._register_account()

        self.account_manager.create.assert_not_called()

    def test_register_account_create_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_manager.create.side_effect = exception

        with assert_raises(exception):
            self._register_account()

        self.sns_publisher.publish.assert_not_called()


class TestUpdateAccount:
    def setup_method(self) -> None:
        self.account = build_account()
        self.account_manager = Mock(AccountManager)
        self.account_validator = Mock(AccountValidator)
        self.account_validator.get_account.return_value = self.account
        self.authorizer = Mock()
        self.sns_publisher = Mock(SnsPublisher)
        self.response_account_builder: ResponseAccountBuilder[Account] = ResponseAccountBuilder()

    def _update_account(
        self,
        body: Optional[UpdateAccountBody] = None,
        path: Optional[AccountPath] = None,
    ) -> JsonResponse:
        return update_account(
            path=path or AccountPath(self.account.id),
            body=body or UpdateAccountBody(),
            account_manager=self.account_manager,
            account_validator=self.account_validator,
            authorizer=self.authorizer,
            sns_publisher=self.sns_publisher,
            response_account_builder=self.response_account_builder,
        )

    def test_update_account_unauthorized_fails(self) -> None:
        self.authorizer.check_requester_may_manage_accounts.side_effect = ForbiddenError
        with pytest.raises(ForbiddenError):
            self._update_account()

        self.account_validator.get_account.assert_not_called()

    def test_update_account_successful(self) -> None:
        self.account_validator.get_account.return_value = self.account
        updated_account = build_account()
        self.account_manager.update.return_value = updated_account
        response_account = updated_account.to_response_account(ResponseAccount)

        body = UpdateAccountBody(
            adminRoles=[Builder.build_random_string()],
            affiliation=build_affiliation(),
            businessObjects=[build_business_object()],
            friendlyName=Builder.build_random_string(),
            group=Builder.build_random_string(),
            layers=[build_layer()],
            responsibles=["someoneelse@example.com"],
            stages=[build_stage()],
            type=build_account_type(),
            visibleInHubs=[build_hub()],
        )

        response: JsonResponse = self._update_account(path=AccountPath(self.account.id), body=body)

        assert response.status_code == HTTPStatus.OK

        self.authorizer.check_requester_may_manage_accounts.assert_called_once()
        self.account_validator.get_account.assert_called_once_with(self.account.id)
        self.account_validator.validate_update_body.assert_called_once_with(self.account, body)
        self.account_manager.update.assert_called_once_with(self.account.id, body)
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.ACCOUNT,
            operation=Operation.UPDATE,
            payload=response_account,
        )

    def test_update_account_validator_get_account_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_validator.get_account.side_effect = exception

        with assert_raises(exception):
            self._update_account()

        self.account_validator.get_account.assert_called_once_with(self.account.id)
        self.account_validator.validate_update_body.assert_not_called()

    def test_update_account_validator_validate_body_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_validator.validate_update_body.side_effect = exception

        with assert_raises(exception):
            self._update_account()

        self.account_validator.validate_update_body.assert_called_once()
        self.account_manager.update.assert_not_called()

    def test_update_account_manager_update_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_manager.update.side_effect = exception

        with assert_raises(exception):
            self._update_account()

        self.account_manager.update.assert_called_once()
        self.sns_publisher.publish.assert_not_called()


class TestDeregisterAccount:
    def setup_method(self) -> None:
        self.account = build_account()
        self.account_manager = Mock(AccountManager)
        self.account_validator = Mock(AccountValidator)
        self.authorizer = Mock()
        self.sns_publisher = Mock(SnsPublisher)

    def _deregister_account(self, path: Optional[AccountPath] = None) -> JsonResponse:
        return deregister_account(
            path=path or AccountPath(self.account.id),
            account_manager=self.account_manager,
            account_validator=self.account_validator,
            authorizer=self.authorizer,
            sns_publisher=self.sns_publisher,
            response_account_builder=ResponseAccountBuilder(),
        )

    def test_deregister_account_unauthorized_fails(self) -> None:
        self.authorizer.check_requester_may_delete_accounts.side_effect = ForbiddenError
        with pytest.raises(ForbiddenError):
            self._deregister_account()

        self.account_validator.get_account.assert_not_called()

    def test_deregister_account_successful(self) -> None:
        self.account_validator.get_account.return_value = self.account
        response_account = self.account.to_response_account(ResponseAccount)

        response: JsonResponse = self._deregister_account(path=AccountPath(self.account.id))

        assert response.status_code == HTTPStatus.NO_CONTENT

        self.authorizer.check_requester_may_delete_accounts.assert_called_once()
        self.account_validator.get_account.assert_called_once_with(self.account.id)
        self.account_validator.check_account_can_be_deregistered.assert_called_once_with(self.account)
        self.account_manager.delete.assert_called_once_with(self.account)
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.ACCOUNT,
            operation=Operation.DELETE,
            payload=response_account,
        )

    def test_deregister_account_validator_get_account_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_validator.get_account.side_effect = exception

        with assert_raises(exception):
            self._deregister_account()

        self.account_validator.get_account.assert_called_once_with(self.account.id)
        self.account_validator.check_account_can_be_deregistered.assert_not_called()

    def test_deregister_account_validator_check_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_validator.check_account_can_be_deregistered.side_effect = exception

        with assert_raises(exception):
            self._deregister_account()

        self.account_validator.check_account_can_be_deregistered.assert_called_once()
        self.account_manager.delete.assert_not_called()

    def test_deregister_account_manager_delete_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_manager.delete.side_effect = exception

        with assert_raises(exception):
            self._deregister_account()

        self.account_manager.delete.assert_called_once()
