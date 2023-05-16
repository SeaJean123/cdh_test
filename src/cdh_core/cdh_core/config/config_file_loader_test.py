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
from pathlib import Path
from typing import Any

import pytest

from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core_dev_tools.testing.builder import Builder


# pylint: disable=protected-access
class TestConfigFileLoader:
    def test_sample_config(self, monkeypatch: Any) -> None:
        monkeypatch.setenv(
            ConfigFileLoader._ENVIRONMENT_KEY, str(Path(__file__).parents[4].absolute() / "cdh-core-config.yaml")
        )
        ConfigFileLoader.get_config.cache_clear()  # type: ignore
        assert len(ConfigFileLoader.get_config().partition.instances) == 1
        assert len(ConfigFileLoader.get_config().region.instances) == 1
        assert len(ConfigFileLoader.get_config().business_object.instances) == 2
        assert len(ConfigFileLoader.get_config().environment.instances) == 2
        assert len(ConfigFileLoader.get_config().hub.instances) == 2
        assert len(ConfigFileLoader.get_config().account.purpose.instances) == 6
        assert len(ConfigFileLoader.get_config().account.instances_per_purpose) == 5
        assert ConfigFileLoader.get_config().aws_service.s3.configured_limits.resource_account_bucket_limit == 100

    def test_get_config_missing_env_key(self, monkeypatch: Any) -> None:
        ConfigFileLoader.get_config.cache_clear()  # type: ignore
        monkeypatch.delenv(ConfigFileLoader._ENVIRONMENT_KEY, raising=False)
        with pytest.raises(RuntimeError):
            ConfigFileLoader.get_config()

    def test_get_config_false_env_key_value(self, monkeypatch: Any) -> None:
        ConfigFileLoader.get_config.cache_clear()  # type: ignore
        monkeypatch.setenv(ConfigFileLoader._ENVIRONMENT_KEY, Builder.build_random_string())
        with pytest.raises(RuntimeError):
            ConfigFileLoader.get_config()
