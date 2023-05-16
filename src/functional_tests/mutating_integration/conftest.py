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
# pylint: disable=redefined-outer-name
import os
from logging import getLogger
from random import choice
from random import randint
from urllib.parse import urlparse

import pytest
from aws_requests_auth.aws_auth import AWSRequestsAuth

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.clients.http_client import HttpClient
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core_dev_tools.testing.builder import Builder
from functional_tests.assume_role import assume_role
from functional_tests.conftest import FUNCTIONAL_TESTS_ROLE
from functional_tests.utils import get_current_test_account
from functional_tests.utils import get_main_test_account

LOG = getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


class IntegrationTestConfig:
    """Test config for the integration tests."""

    def __init__(self) -> None:
        try:
            self.base_url = os.environ["BASE_URL"]
            self.environment = Environment(os.environ["ENVIRONMENT"])
        except KeyError as missing_key:
            raise OSError(f"Environment variable {missing_key} has to be set.") from missing_key

        assert self.environment is Environment("dev")
        self.partition = Partition(os.environ.get("AWS_PARTITION", "aws"))
        self.resource_name_prefix = os.environ.get("RESOURCE_NAME_PREFIX", "")
        self.test_role = FUNCTIONAL_TESTS_ROLE

        self.hubs = list(Hub.get_hubs(environment=self.environment, partition=self.partition))

    def __repr__(self) -> str:
        """Return the class as a dict, when print() is called."""
        return str(self.__dict__)


class IntegrationTestSetup:
    """Test setup for the integration tests."""

    def __init__(self, integration_test_config: IntegrationTestConfig) -> None:
        self.name_prefix = f"integration{randint(1, 999):03d}"
        self.hub = choice(integration_test_config.hubs)
        self.region = choice(list(self.hub.regions))
        self.stage = choice(
            [
                *AccountStore().query_resource_accounts(
                    hubs=self.hub, environments=integration_test_config.environment, only_default=True
                )
            ]
        ).stage

        self.test_provider_account = get_main_test_account(
            integration_test_config.partition, integration_test_config.environment
        )
        self.account_id_to_be_registered = Builder.get_random_element(
            to_choose_from=[
                account.id
                for account in AccountStore().query_accounts(
                    account_purposes=AccountPurpose("test"),
                    partitions=integration_test_config.partition,
                    environments=integration_test_config.environment,
                )
            ],
            exclude=[self.test_provider_account.id],
        )

        current_test_account = get_current_test_account(
            integration_test_config.partition, integration_test_config.environment
        )

        # Tests are run with the deployer role, which only has access to the functional test role in the same account.
        # If we are logged in with a different test account, we can go through the current account's functional test
        # role to get into the main test account
        if current_test_account != self.test_provider_account:
            current_test_account_credentials = assume_role(
                prefix=integration_test_config.resource_name_prefix,
                account_id=current_test_account.id,
                role=integration_test_config.test_role,
            )
        else:
            current_test_account_credentials = None

        self.provider_credentials = assume_role(
            prefix=integration_test_config.resource_name_prefix,
            account_id=self.test_provider_account.id,
            role=integration_test_config.test_role,
            credentials=current_test_account_credentials,
        )
        LOG.info(
            f"BASE_URL={integration_test_config.base_url} "
            f"RESOURCE_NAME_PREFIX={integration_test_config.resource_name_prefix} HUB={self.hub.value} "
            f"TEST_PROVIDER={self.test_provider_account.id}"
        )
        region = Region.preferred(self.hub.partition)
        self.http_client = HttpClient(
            base_url=integration_test_config.base_url,
            credentials=(
                AWSRequestsAuth(
                    aws_access_key=self.provider_credentials["aws_access_key_id"],
                    aws_secret_access_key=self.provider_credentials["aws_secret_access_key"],
                    aws_token=self.provider_credentials["aws_session_token"],
                    aws_host=urlparse(integration_test_config.base_url).netloc,
                    aws_region=region.value,
                    aws_service="execute-api",
                )
            ),
        )
        self.core_api_client = CoreApiClient(http_client=self.http_client)

        admin_roles = ["CDHDevOps", FUNCTIONAL_TESTS_ROLE]
        self.roles = [
            AccountRole(name=role, path="/", type=AccountRoleType.WRITE, friendly_name=role) for role in admin_roles
        ]
        self.core_api_client.register_account(
            account_id=self.test_provider_account.id,
            admin_roles=admin_roles,
            affiliation=Affiliation("cdh"),
            business_objects=list(BusinessObject),
            layers=list(Layer),
            friendly_name=f"CDH Test Provider/Consumer {self.hub.value.capitalize()}-Hub",
            stages=list(Stage),
            hub=self.hub,
            type=AccountType.provider,
            request_id="some-id",
            roles=self.roles,
            visible_in_hubs=[],
            fail_if_exists=False,
        )


@pytest.fixture(scope="module")
def integration_test_config() -> IntegrationTestConfig:
    """Get the fixture for the integration test config."""
    return IntegrationTestConfig()


@pytest.fixture(scope="module")
def integration_test_setup(integration_test_config: IntegrationTestConfig) -> IntegrationTestSetup:
    """Get the fixture for the integration test setup."""
    return IntegrationTestSetup(integration_test_config)
