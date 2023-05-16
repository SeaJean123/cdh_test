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
from typing import Dict

from botocore.config import Config

from cdh_core.iterables import unique
from cdh_core.log.log_safe import log_safe
from cdh_core.log.measure_time import MeasureTimeContextManager


@log_safe()
def handler(event: Dict[str, Any], context: Any) -> str:  # pylint: disable=unused-argument
    """Handle a lambda function."""
    with MeasureTimeContextManager("example"):
        return str(unique(["a", "b", "a"])[0])


def gen_config() -> Config:
    """Provide an example config."""
    return Config(region_name="asdf")
