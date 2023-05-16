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

from cdh_core_api.generic_types import GenericAccount

from cdh_core.entities.accounts import ResponseAccount


class ResponseAccountBuilder(Generic[GenericAccount]):
    """Builds the `ResponseAccount` and `ResponseAccounts` corresponding to given `Account`s."""

    @staticmethod
    def get_response_account(account: GenericAccount) -> ResponseAccount:
        """Convert an account to a response account that can be returned in an api endpoint."""
        return account.to_response_account(ResponseAccount)
