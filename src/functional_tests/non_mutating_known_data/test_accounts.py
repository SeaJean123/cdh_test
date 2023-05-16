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
from functional_tests.conftest import NonMutatingTestSetup


class TestAccounts:
    """Test Class for all Account endpoints."""

    def test_get_all_accounts(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /accounts endpoint."""
        accounts = non_mutating_test_setup.core_api_client.get_accounts()
        assert len(accounts) > 0

    def test_get_account(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /accounts/{accountId} endpoint."""
        account_id = non_mutating_test_setup.test_provider_account.id
        account = non_mutating_test_setup.core_api_client.get_account(account_id)
        assert account.id == account_id
