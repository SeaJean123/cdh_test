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
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Set
from unittest.mock import Mock

import pytest
import yaml
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecGenerator
from cdh_core_api.app import openapi
from cdh_core_api.config import Config
from cdh_core_api.config_test import build_config
from cdh_core_api.endpoints.config import get_config
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs import Hub
from cdh_core_dev_tools.testing.builder import Builder

MAPPING = {enum.__name__: enum for enum in Config.ENUMS_TO_EXPOSE}


class ConfigTestCase:
    def setup_method(self) -> None:
        self.environment = build_environment()
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.config = build_config(environment=self.environment)
        self.visible_data_loader.get_hubs.return_value = self.config.hubs


class TestConfigEnums(ConfigTestCase):
    def test_all_openapi_enums_in_config(self) -> None:
        config_enums_response = self.get_config_enums_response()
        assert set(config_enums_response) >= self.get_open_api_enums_set()
        assert set(config_enums_response) >= self.get_open_api_enums_set_with_org_ids()

    def test_enum_mapping_complete(self) -> None:
        assert set(self.get_config_enums_response()) == set(MAPPING)

    @pytest.mark.parametrize("name, enum", MAPPING.items())
    def test_get_config_contains_enum(self, name: str, enum: Any) -> None:
        if name == Affiliation.__name__:
            assert self.get_config_enums_response()[name] == [
                {"value": entry.value, "friendlyName": entry.friendly_name, "accessManagement": entry.access_management}
                for entry in enum
            ]
        else:
            assert self.get_config_enums_response()[name] == [
                {"value": entry.value, "friendlyName": entry.friendly_name} for entry in enum
            ]

    def get_config_enums_response(self) -> Dict[str, Any]:
        response = get_config(self.config, self.visible_data_loader)
        assert isinstance(response.body, dict)
        return cast(Dict[str, Any], response.body["enums"])

    def get_open_api_enums_set(self) -> Set[str]:
        generator = OpenApiSpecGenerator.from_collector(openapi)
        return self._get_open_api_enums_set(generator)

    def get_open_api_enums_set_with_org_ids(self) -> Set[str]:
        generator = OpenApiSpecGenerator.from_collector(openapi, org_ids="some_id")
        return self._get_open_api_enums_set(generator)

    def _get_open_api_enums_set(self, generator: OpenApiSpecGenerator) -> Set[str]:
        specs = yaml.safe_load(generator.generate(Builder.build_random_url()))
        return {name for name, schema in specs["components"]["schemas"].items() if "enum" in schema}


class TestConfigHubs(ConfigTestCase):
    def test_all_hubs_returned(self) -> None:
        config_hubs_response = self.get_config_hubs_response()

        hub_names = [item["name"] for item in config_hubs_response]
        assert len(hub_names) == len(Hub.get_hubs(environment=self.environment))
        assert set(hub_names) == {hub.value for hub in Hub.get_hubs(environment=self.environment)}

    def test_hubs_are_correct(self) -> None:
        config_hubs_response = self.get_config_hubs_response()

        for hub in Hub.get_hubs(environment=self.environment):
            item = self._get_item_for_hub(config_hubs_response, hub.value)
            assert item["name"] == hub.value
            assert item["friendlyName"] == hub.friendly_name
            assert item["regions"] == sorted([region.value for region in hub.regions])
            assert item["partition"] == hub.partition.value

            expected_resource_accounts = self.config.account_store.query_resource_accounts(
                environments=self.environment, hubs=hub
            )
            assert len(item["resourceAccounts"]) == len(expected_resource_accounts)
            assert {account["id"]: account["stage"] for account in item["resourceAccounts"]} == {
                account.id: account.stage.value for account in expected_resource_accounts
            }

    @staticmethod
    def _get_item_for_hub(items: List[Dict[str, Any]], hub_name: str) -> Dict[str, Any]:
        item_for_hub = [item for item in items if item["name"] == hub_name]
        assert len(item_for_hub) == 1

        return item_for_hub[0]

    def get_config_hubs_response(self) -> List[Dict[str, Any]]:
        response = get_config(self.config, self.visible_data_loader)
        assert isinstance(response.body, dict)
        return cast(List[Dict[str, Any]], response.body["hubs"])
