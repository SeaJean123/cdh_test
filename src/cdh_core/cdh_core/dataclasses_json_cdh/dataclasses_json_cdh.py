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
from functools import lru_cache
from typing import Any
from typing import Dict

from dataclasses_json import config
from dataclasses_json import DataClassJsonMixin


@lru_cache(10000)
def _to_camel_case(name: str) -> str:
    parts = [part for part in name.split("_") if part]
    return "".join([part if i == 0 else part.capitalize() for i, part in enumerate(parts)])


CDH_DATACLASS_JSON_CONFIG = config(letter_case=_to_camel_case)["dataclasses_json"]


class DataClassJsonCDHMixin(DataClassJsonMixin):
    """Provides a preconfigured dataclasses_json Mixin."""

    dataclass_json_config = CDH_DATACLASS_JSON_CONFIG

    def to_plain_dict(self) -> Dict[str, Any]:
        """Create an untyped dict from the class."""
        data = json.loads(self.to_json())
        if isinstance(data, dict):
            return data
        raise TypeError(f"Result is not a dict it is: {type(data)}")
