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
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import marshmallow_dataclass
import yaml

from cdh_core.config.config_file import ConfigFile


class ConfigFileLoader:
    """Loads the configuration for creating enums dynamically."""

    _ENVIRONMENT_KEY = "CDH_CORE_CONFIG_FILE_PATH"

    @staticmethod
    @lru_cache()
    def get_config() -> ConfigFile:
        """Return the Configuration either from cache or from file."""
        if ConfigFileLoader._ENVIRONMENT_KEY not in os.environ:
            raise RuntimeError(f"The environment key {ConfigFileLoader._ENVIRONMENT_KEY} is not set.")
        file_path = Path(os.environ[ConfigFileLoader._ENVIRONMENT_KEY])
        return ConfigFileLoader._read_config_file(file_path)

    @classmethod
    def _read_config_file(cls, file_path: Path) -> ConfigFile:
        """Return a config file from a custom file path."""
        if not file_path.is_file():
            raise RuntimeError(f"The config file {file_path} is not a file.")
        with file_path.open(encoding="utf-8") as file:
            config_file = marshmallow_dataclass.class_schema(ConfigFile)().load(yaml.load(file, Loader=yaml.FullLoader))
        if not isinstance(config_file, ConfigFile):
            raise RuntimeError(f"The type of the config file is not as expected: {type(config_file)}")
        return config_file
