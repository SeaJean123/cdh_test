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
import json
from typing import Any
from typing import Dict

from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecCollector
from cdh_core_api.config_test import build_config
from cdh_core_api.services.api_info_manager import ApiInfoManager

from cdh_core.enums.aws import Partition


class TestApiInfoManager:
    def setup_method(self) -> None:
        self.openapi = OpenApiSpecCollector()

    def test_get_without_prefix(self) -> None:
        config = build_config(prefix="")
        api_info_manager = ApiInfoManager(config, self.openapi)

        spec = api_info_manager.get()

        self._validate_spec(spec, f"https://{config.environment.get_domain(Partition.default())}", "cdh-core-api")

    def test_get_with_prefix(self) -> None:
        config = build_config()
        api_info_manager = ApiInfoManager(config, self.openapi)

        spec = api_info_manager.get()

        self._validate_spec(
            spec,
            f"https://{config.prefix}.{config.environment.get_domain(Partition.default())}",
            f"{config.prefix}cdh-core-api",
        )

    def _validate_spec(self, spec: Dict[str, Any], url: str, title: str) -> None:
        spec_json = json.dumps(spec)
        assert len(spec_json) > 0
        assert "x-amazon-apigateway" not in spec_json
        assert url == spec["servers"][0]["url"]
        assert title == spec["info"]["title"]

    def test_clean_spec(self) -> None:
        sample = {
            "x-test": "bad",
            "test": {"x-test": "bad", "test": "good"},
            "list": ["good", "x-test", {"x-test": "bad"}],
        }
        result = {"test": {"test": "good"}, "list": ["good", {}]}
        ApiInfoManager._clean_custom_extensions(sample)  # pylint: disable=protected-access
        assert result == sample
