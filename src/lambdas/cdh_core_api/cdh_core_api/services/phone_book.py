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
import re
from contextlib import suppress
from typing import Collection
from typing import Optional

from cdh_core_api.config import Config

from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.account_store import QueryAccountAmbiguous
from cdh_core.entities.account_store import QueryAccountNotFound
from cdh_core.entities.arn import Arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.environment import Environment
from cdh_core.primitives.account_id import AccountId


class PhoneBook:
    """Keeps track of known requester ARNs or patterns of such to identify known requesters."""

    def __init__(self, config: Config):
        self._config = config

    def is_privileged_core_api_role(self, requester: Arn) -> bool:
        """Check whether the requester has assumed a privileged Core API role."""
        return self.is_core_api_admin_role(requester=requester)

    def is_core_api_admin_role(self, requester: Arn) -> bool:
        """Check whether the requester has assumed a Core API admin role."""
        admin_role_name = ConfigFileLoader().get_config().account.admin_role_name
        return self._is_role_in_privileged_account(
            requester=requester,
            expected_account_id=self._config.lambda_account_id,
            privileged_role_names=[admin_role_name],
        )

    def is_privileged_portal_role(self, requester: Arn) -> bool:
        """Check whether the requester has assumed a privileged portal role."""
        return self._is_role_in_privileged_account(
            requester=requester,
            expected_account_purpose=AccountPurpose("portal"),
            expected_environment=self._config.environment,
            privileged_role_name_regex=r"(data-portal-api-privileged|^e2e-test-runner)$",
        )

    def is_authorization_role(self, requester: Arn) -> bool:
        """Check whether the requester has assumed a privileged authorization role."""
        return self._is_role_in_privileged_account(
            requester=requester,
            expected_account_purpose=AccountPurpose("iam"),
            expected_environment=self._config.environment,
            privileged_role_name_regex="authorization-task",
        )

    def is_functional_tests_user_role(self, requester: Arn) -> bool:
        """Check whether the requester has assumed a functional tests user role."""
        return self._config.environment.is_test_environment and self._is_role_in_privileged_account(
            requester=requester,
            expected_account_purpose=AccountPurpose("test"),
            privileged_role_names=[self._config.functional_tests_user_role_name],
        )

    def _is_role_in_privileged_account(  # pylint: disable=too-many-arguments
        self,
        requester: Arn,
        expected_account_purpose: Optional[AccountPurpose] = None,
        expected_environment: Optional[Environment] = None,
        expected_account_id: Optional[AccountId] = None,
        privileged_role_names: Optional[Collection[str]] = None,
        privileged_role_name_regex: Optional[str] = None,
    ) -> bool:
        try:
            assumed_role_name = requester.get_assumed_role_name()
        except ValueError:
            return False

        return self._role_name_matches(
            assumed_role_name,
            expected_role_names=privileged_role_names,
            expected_role_name_regex=privileged_role_name_regex,
        ) and self._account_id_matches(
            account_id=requester.account_id,
            expected_account_id=expected_account_id,
            expected_account_purpose=expected_account_purpose,
            expected_environment=expected_environment,
        )

    @staticmethod
    def _role_name_matches(
        role_name: str, expected_role_names: Optional[Collection[str]], expected_role_name_regex: Optional[str]
    ) -> bool:
        if expected_role_names is not None and role_name not in expected_role_names:
            return False
        if expected_role_name_regex is not None:
            if re.search(expected_role_name_regex, role_name) is None:
                return False
        return True

    def _account_id_matches(
        self,
        account_id: AccountId,
        expected_account_id: Optional[AccountId],
        expected_account_purpose: Optional[AccountPurpose],
        expected_environment: Optional[Environment],
    ) -> bool:
        if expected_account_id is not None and account_id != expected_account_id:
            return False
        if expected_account_purpose:
            with suppress(QueryAccountAmbiguous):
                try:
                    self._config.account_store.query_account(
                        environments=expected_environment or frozenset(Environment),
                        account_ids=account_id,
                        account_purposes=expected_account_purpose,
                    )
                except QueryAccountNotFound:
                    return False
        return True
