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
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.config_test import build_config
from cdh_core_api.services.metadata_role_assumer import AssumableAccountSpec
from cdh_core_api.services.metadata_role_assumer import CannotAssumeMetadataRole
from cdh_core_api.services.metadata_role_assumer import MetadataRoleAssumer
from cdh_core_api.services.metadata_role_assumer import UnsupportedAssumeMetadataRole

from cdh_core.aws_clients.factory import AssumeRoleSessionProvider
from cdh_core.aws_clients.factory import BotocoreSessionWrapper
from cdh_core.entities.account_store_test import build_account_store
from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.accounts_test import build_base_account
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.hubs_test import build_hub
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class TestMetadataRoleAssumer:
    def setup_method(self) -> None:
        self.account_id = build_account_id()
        self.hub = build_hub()
        self.account_type = Builder.get_random_element(list(AccountType), exclude={AccountType.technical})
        self.account_spec = AssumableAccountSpec(
            account_id=self.account_id,
            hub=self.hub,
            account_type=self.account_type,
        )
        self.api_account = build_base_account(purpose=AccountPurpose("api"), partition=self.hub.partition)
        self.account_store = build_account_store([self.api_account])
        self.config = build_config(
            account_store=self.account_store,
            environment=self.api_account.environment,
            lambda_account_id=self.api_account.id,
        )
        self.assume_role_session_provider = Mock(AssumeRoleSessionProvider)
        self.api_session = Mock(BotocoreSessionWrapper)
        self.assume_role_session_provider.get_session_wrapped.return_value = self.api_session
        self.metadata_role_assumer: MetadataRoleAssumer[Account, UpdateAccountBody] = MetadataRoleAssumer(
            self.assume_role_session_provider, self.account_store, AssumableAccountSpec, self.config
        )

    @patch.object(MetadataRoleAssumer, "_get_metadata_role_arn")
    def test_assume_successful(self, mocked_get_metadata_role_arn: Mock) -> None:
        metadata_role_arn = build_role_arn()
        mocked_get_metadata_role_arn.return_value = metadata_role_arn

        self.metadata_role_assumer.assume(account_spec=self.account_spec)

        self.assume_role_session_provider.get_session_wrapped.assert_called_once_with(
            account_id=self.api_account.id, account_purpose=self.api_account.purpose
        )
        mocked_get_metadata_role_arn.assert_called_once_with(self.account_spec)
        self.api_session.assume_role.assert_called_once_with(
            role_arn=metadata_role_arn, session_name="MetadataRoleAssumer"
        )

    def test_assume_not_supported_for_type_technical(self) -> None:
        account_type = AccountType.technical
        with pytest.raises(UnsupportedAssumeMetadataRole):
            self.metadata_role_assumer.assume(
                account_spec=AssumableAccountSpec(
                    account_id=self.account_id,
                    hub=self.hub,
                    account_type=account_type,
                )
            )

    def test_assume_role_fails(self) -> None:
        self.api_session.assume_role.side_effect = Exception()
        with pytest.raises(CannotAssumeMetadataRole):
            self.metadata_role_assumer.assume(account_spec=self.account_spec)

    @patch.object(MetadataRoleAssumer, "assume")
    def test_assume_account(self, mocked_assume: Mock) -> None:
        account = build_account()
        self.metadata_role_assumer.assume_account(account)
        mocked_assume.assert_called_once_with(AssumableAccountSpec.from_account(account))
