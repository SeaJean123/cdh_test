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
from contextlib import AbstractContextManager
from contextlib import nullcontext
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.services.account_environment_verifier import AccountEnvironmentVerificationFailed
from cdh_core_api.services.account_environment_verifier import AccountEnvironmentVerifier
from cdh_core_api.services.metadata_role_assumer import AssumableAccountSpec
from cdh_core_api.services.metadata_role_assumer import CannotAssumeMetadataRole
from cdh_core_api.services.metadata_role_assumer import UnsupportedAssumeMetadataRole

from cdh_core.entities.accounts import Account
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.enums.accounts_test import build_account_type
from cdh_core.enums.hubs_test import build_hub
from cdh_core.primitives.account_id_test import build_account_id

does_not_raise: AbstractContextManager = nullcontext()  # type: ignore


@pytest.mark.parametrize("strict", [True, False])
class TestAccountEnvironmentVerifier:
    def setup_method(self) -> None:
        self.metadata_role_assumer = Mock()
        self.account_environment_verifier: AccountEnvironmentVerifier[
            Account, UpdateAccountBody
        ] = AccountEnvironmentVerifier(self.metadata_role_assumer)
        self.account_spec = AssumableAccountSpec(
            account_id=build_account_id(),
            hub=build_hub(),
            account_type=build_account_type(),
        )

    def test_assume_successful(self, strict: bool) -> None:
        self.account_environment_verifier.verify(
            account_spec=self.account_spec,
            strict=strict,
        )

        self.metadata_role_assumer.assume.assert_called_once_with(self.account_spec)

    def test_role_unsupported(self, strict: bool) -> None:
        self.metadata_role_assumer.assume.side_effect = UnsupportedAssumeMetadataRole(self.account_spec)

        with pytest.raises(AccountEnvironmentVerificationFailed) if strict else does_not_raise:
            self.account_environment_verifier.verify(
                account_spec=self.account_spec,
                strict=strict,
            )

    def test_assume_fails(self, strict: bool) -> None:
        self.metadata_role_assumer.assume.side_effect = CannotAssumeMetadataRole(build_role_arn())
        with pytest.raises(AccountEnvironmentVerificationFailed):
            self.account_environment_verifier.verify(
                account_spec=self.account_spec,
                strict=strict,
            )
