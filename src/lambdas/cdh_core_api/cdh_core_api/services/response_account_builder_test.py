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
from cdh_core_api.services.response_account_builder import ResponseAccountBuilder

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import ResponseAccount
from cdh_core.entities.accounts_test import build_account


class TestResponseAccountBuilder:
    def setup_method(self) -> None:
        self.response_account_builder: ResponseAccountBuilder[Account] = ResponseAccountBuilder()

    def test_get_response_account(self) -> None:
        account = build_account()
        expected_response_account = account.to_response_account(ResponseAccount)
        assert self.response_account_builder.get_response_account(account) == expected_response_account
