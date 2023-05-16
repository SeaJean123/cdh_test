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
from dataclasses import replace
from typing import Optional
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.services.account_environment_verifier import AccountEnvironmentVerificationFailed
from cdh_core_api.services.account_environment_verifier import AccountEnvironmentVerifier
from cdh_core_api.services.account_id_verifier import AccountIdVerifier
from cdh_core_api.services.account_validator import AccountValidator
from cdh_core_api.services.metadata_role_assumer import AssumableAccountSpec
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.resource_test import build_resource
from cdh_core.enums.accounts_test import build_account_type
from cdh_core.enums.accounts_test import build_affiliation
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.dataset_properties_test import build_layer
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class _BaseTestAccountValidator:
    @pytest.fixture(autouse=True)
    def service_setup(
        self,
        accounts_table: AccountsTable,
        resources_table: ResourcesTable,
        datasets_table: DatasetsTable,
    ) -> None:
        self.accounts_table = accounts_table
        self.resources_table = resources_table
        self.datasets_table = datasets_table
        self._build_account_validator()

    def setup_method(self) -> None:
        self.account = build_account()
        self.account_environment_verifier = Mock(AccountEnvironmentVerifier)
        self.account_id_verifier = Mock(AccountIdVerifier)
        self.visible_data_loader = Mock(VisibleDataLoader)

    def _build_account_validator(self) -> None:
        self.account_validator: AccountValidator[Account, UpdateAccountBody] = AccountValidator(
            account_environment_verifier=self.account_environment_verifier,
            account_id_verifier=self.account_id_verifier,
            accounts_table=self.accounts_table,
            assumable_account_spec_cls=AssumableAccountSpec,
            datasets_table=self.datasets_table,
            resources_table=self.resources_table,
            visible_data_loader=self.visible_data_loader,
        )

    def _check_account_environment_verifier_called(
        self,
        account: Account,
        body: Optional[UpdateAccountBody] = None,
    ) -> None:
        self.account_environment_verifier.verify.assert_called_once_with(
            account_spec=AssumableAccountSpec(
                account_id=account.id,
                hub=account.hub,
                account_type=(body.type if body else None) or account.type,
            )
        )


class TestAccountValidatorRegisterAccount(_BaseTestAccountValidator):
    def test_validate_new_account_successful(self) -> None:
        self.account_validator.validate_new_account(self.account)

        self.account_id_verifier.verify.assert_called_once_with(self.account.id, self.account.hub.partition)
        self._check_account_environment_verifier_called(self.account)

    def test_validate_new_account_account_id_verifier_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.account_id_verifier.verify.side_effect = exception

        with assert_raises(exception):
            self.account_validator.validate_new_account(self.account)

        self.account_id_verifier.verify.assert_called_once_with(self.account.id, self.account.hub.partition)
        self.account_environment_verifier.verify.assert_not_called()

    def test_validate_new_account_existing_account_fails(self) -> None:
        self.accounts_table.create(self.account)

        with pytest.raises(ConflictError):
            self.account_validator.validate_new_account(self.account)

        self.account_id_verifier.verify.assert_called_once_with(self.account.id, self.account.hub.partition)

    def test_validate_new_account_account_environment_verifier_fails(self) -> None:
        self.account_environment_verifier.verify.side_effect = AccountEnvironmentVerificationFailed(build_account_id())

        with pytest.raises(ConflictError):
            self.account_validator.validate_new_account(self.account)

        self._check_account_environment_verifier_called(self.account)


class TestAccountValidatorUpdateAccount(_BaseTestAccountValidator):
    def setup_method(self) -> None:
        super().setup_method()
        self.empty_body = UpdateAccountBody()

    def test_validate_update_body_no_field_successful(self) -> None:
        self.accounts_table.create(self.account)

        self.account_validator.validate_update_body(account=self.account, body=self.empty_body)

        self._check_account_environment_verifier_called(account=self.account)

    def test_validate_update_body_all_fields_successful(self) -> None:
        self.accounts_table.create(self.account)
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

        self.account_validator.validate_update_body(account=self.account, body=body)

        self._check_account_environment_verifier_called(account=self.account, body=body)

    def test_validate_update_body_removing_stage_fails(self) -> None:
        stage_1, stage_2 = Builder.choose_without_repetition(Stage, 2)
        account = replace(self.account, stages=[stage_1, stage_2])
        self.accounts_table.create(account)
        body = replace(self.empty_body, stages=[stage_1])

        with pytest.raises(ForbiddenError):
            self.account_validator.validate_update_body(account=account, body=body)

    def test_validate_update_body_add_stage_layer_business_object_successful(self) -> None:
        stage_1, stage_2 = Builder.choose_without_repetition(Stage, 2)
        layer_1, layer_2 = Builder.choose_without_repetition(Layer, 2)
        bo_1, bo_2 = Builder.choose_without_repetition(BusinessObject, 2)
        account = replace(
            self.account,
            stages=[stage_1],
            layers=[layer_1],
            business_objects=[bo_1],
        )
        self.accounts_table.create(account)
        body = replace(
            self.empty_body,
            stages=[stage_1, stage_2],
            layers=[layer_1, layer_2],
            businessObjects=[bo_1, bo_2],
        )

        self.account_validator.validate_update_body(account=account, body=body)

    def test_validate_update_body_environment_check_fails(self) -> None:
        self.account_environment_verifier.verify.side_effect = AccountEnvironmentVerificationFailed(build_account_id())
        with pytest.raises(ConflictError):
            self.account_validator.validate_update_body(account=self.account, body=self.empty_body)


class TestAccountValidatorGetAccount(_BaseTestAccountValidator):
    def test_get_account_successful(self) -> None:
        self.visible_data_loader.get_account.return_value = self.account
        assert self.account_validator.get_account(self.account.id) == self.account

    def test_get_account_that_does_not_exist_fails(self) -> None:
        self.visible_data_loader.get_account.side_effect = AccountNotFound(build_account_id())
        with pytest.raises(NotFoundError):
            self.account_validator.get_account(self.account.id)


class TestAccountValidatorDeleteAccount(_BaseTestAccountValidator):
    def setup_method(self) -> None:
        super().setup_method()
        self.resource = build_resource(owner_account_id=self.account.id)

    def test_deregister_check_successful(self) -> None:
        self.account_validator.check_account_can_be_deregistered(self.account)

    def test_deregister_check_account_owner_of_dataset_fails(self) -> None:
        self.accounts_table.create(self.account)

        self.datasets_table.create(build_dataset(owner_account_id=self.account.id))

        with pytest.raises(ForbiddenError):
            self.account_validator.check_account_can_be_deregistered(self.account)

    def test_deregister_check_account_owner_of_resource_fails(self) -> None:
        self.accounts_table.create(self.account)
        self.resources_table.create(self.resource)

        with pytest.raises(ForbiddenError):
            self.account_validator.check_account_can_be_deregistered(self.account)
