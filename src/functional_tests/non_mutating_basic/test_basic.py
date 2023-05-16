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
from http import HTTPStatus
from typing import Any
from typing import Iterator

from cdh_core_api.config import Config

from cdh_core.enums.accounts import AccountType
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.hubs import Hub
from functional_tests.conftest import NonMutatingTestSetup


class TestAPIInfoEndpoint:
    """Test Class for the API Info Endpoint."""

    def test_api_info(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the /api-info endpoint."""
        response = non_mutating_test_setup.http_client.get("/api-info", expected_status_codes=[HTTPStatus.OK])
        assert len(response["servers"]) > 0


class TestConfigEndpoint:
    """Test Class for the Config Endpoint."""

    def test_get_config(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test getting the configuration."""
        response = non_mutating_test_setup.http_client.get("/config", expected_status_codes=[HTTPStatus.OK])

        enum_names = set(response["enums"].keys())
        hub_names = {hub["name"] for hub in response["hubs"]}
        expected_enum_names = {enum.__name__ for enum in Config.ENUMS_TO_EXPOSE}
        expected_hub_names = {hub.value for hub in Hub.get_hubs(environment=non_mutating_test_setup.environment)}

        assert enum_names == expected_enum_names
        assert hub_names == expected_hub_names

        for enum in [AccountType, BusinessObject, Layer]:
            values: Iterator[Any] = iter(enum)
            assert response["enums"][enum.__name__] == [
                {"value": value.value, "friendlyName": value.friendly_name} for value in values
            ]


class TestOptions:
    """Test Class for options calls to all bulk endpoints."""

    def test_options(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        hub = non_mutating_test_setup.test_provider_account.hub
        non_mutating_test_setup.http_client.options("/accounts", expected_status_codes=[HTTPStatus.OK])
        non_mutating_test_setup.http_client.options("/api-info", expected_status_codes=[HTTPStatus.OK])
        non_mutating_test_setup.http_client.options("/config", expected_status_codes=[HTTPStatus.OK])
        non_mutating_test_setup.http_client.options(
            f"/{hub.value}/businessObjects", expected_status_codes=[HTTPStatus.OK]
        )
        non_mutating_test_setup.http_client.options(f"/{hub.value}/datasets", expected_status_codes=[HTTPStatus.OK])
        non_mutating_test_setup.http_client.options(f"/{hub.value}/resources", expected_status_codes=[HTTPStatus.OK])
        non_mutating_test_setup.http_client.options(
            f"/{hub.value}/resources/glue-sync", expected_status_codes=[HTTPStatus.OK]
        )
        non_mutating_test_setup.http_client.options(f"/{hub.value}/resources/s3", expected_status_codes=[HTTPStatus.OK])
