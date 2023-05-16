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
from functools import lru_cache
from typing import Any
from typing import cast
from typing import Dict

import yaml
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecCollector
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecGenerator
from cdh_core_api.config import Config

from cdh_core.enums.aws import Partition


class ApiInfoManager:
    """Handles the Openapi Spec handling."""

    def __init__(self, config: Config, openapi: OpenApiSpecCollector):
        self._config = config
        self._openapi = openapi

    def _create_openapi_spec(self) -> str:
        generator = OpenApiSpecGenerator.from_collector(self._openapi)
        return generator.generate(self._config.environment.get_domain(Partition.default()))

    @lru_cache(maxsize=10)  # noqa: B019 # service instantiated only once per lambda runtime
    def get(self) -> Dict[str, Any]:
        """Return the openapi spec as dict."""
        spec = yaml.safe_load(self._create_openapi_spec())
        self._clean_custom_extensions(spec)
        self._set_metadata(spec)
        return cast(Dict[str, Any], spec)

    def _set_metadata(self, spec: Dict[str, Any]) -> None:
        api_name = "cdh-core-api"
        description = "CDH Core API"
        if self._config.prefix:
            api_name = self._config.prefix + api_name
            url = f"https://{self._config.prefix}.{self._config.environment.get_domain(Partition.default())}"
        else:
            url = f"https://{self._config.environment.get_domain(Partition.default())}"
        spec["info"]["title"] = api_name
        spec["info"]["description"] = description
        spec["servers"] = [{"url": url}]

    @classmethod
    def _clean_custom_extensions(cls, obj: Dict[str, Any]) -> None:
        prefix = "x-"
        if isinstance(obj, dict):
            for key in list(obj.keys()):
                if key.startswith(prefix):
                    del obj[key]
                else:
                    cls._clean_custom_extensions(obj[key])
        elif isinstance(obj, list):
            for i in reversed(range(len(obj))):
                if isinstance(obj[i], str) and obj[i].startswith(prefix):
                    del obj[i]
                else:
                    cls._clean_custom_extensions(obj[i])
