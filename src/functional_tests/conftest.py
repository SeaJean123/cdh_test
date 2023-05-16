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
import os
from logging import getLogger
from urllib.parse import urlparse

import pytest
from aws_requests_auth.aws_auth import AWSRequestsAuth

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.clients.http_client import HttpClient
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment
from functional_tests.assume_role import assume_role
from functional_tests.utils import get_current_test_account
from functional_tests.utils import get_main_test_account

LOG = getLogger(__name__)

FUNCTIONAL_TESTS_ROLE = "cdh-core-functional-tests"
FUNCTIONAL_TESTS_VIEWER_ROLE = "cdh-core-functional-tests-viewer"


class NonMutatingTestSetup:
    """Test setup for non mutating functional tests."""

    __test__ = False

    def __init__(
        self,
    ) -> None:
        try:
            base_url = os.environ["BASE_URL"]
            self.environment = Environment(os.environ["ENVIRONMENT"])
        except KeyError as missing_key:
            raise OSError(f"Environment variable {missing_key} has to be set.") from missing_key
        self.partition = Partition(os.environ.get("AWS_PARTITION", "aws"))
        self.resource_name_prefix = os.environ.get("RESOURCE_NAME_PREFIX", "")
        self.test_provider_account = get_main_test_account(self.partition, self.environment)
        current_test_account = get_current_test_account(partition=self.partition, environment=self.environment)

        # Tests are run with the deployer role, which only has access to the functional test role in the same account.
        # If we are logged in with a different test account, we can go through the current account's functional test
        # role to get into the main test account
        if current_test_account != self.test_provider_account:
            current_test_account_credentials = assume_role(
                prefix=self.resource_name_prefix,
                account_id=current_test_account.id,
                role=FUNCTIONAL_TESTS_ROLE,
            )
        else:
            current_test_account_credentials = None

        provider_credentials = assume_role(
            prefix=self.resource_name_prefix,
            account_id=self.test_provider_account.id,
            role=FUNCTIONAL_TESTS_ROLE,
            credentials=current_test_account_credentials,
        )

        self.http_client = HttpClient(
            base_url=base_url,
            credentials=(
                AWSRequestsAuth(
                    aws_access_key=provider_credentials["aws_access_key_id"],
                    aws_secret_access_key=provider_credentials["aws_secret_access_key"],
                    aws_token=provider_credentials["aws_session_token"],
                    aws_host=urlparse(base_url).netloc,
                    aws_region=Region.preferred(self.partition).value,
                    aws_service="execute-api",
                )
            ),
        )
        self.core_api_client = CoreApiClient(self.http_client)

    def __repr__(self) -> str:
        """Return the class as a dict, when print() is called."""
        return str(self.__dict__)


@pytest.fixture(scope="module")
def non_mutating_test_setup() -> NonMutatingTestSetup:
    """Get the fixture for the non mutating test setup."""
    return NonMutatingTestSetup()
