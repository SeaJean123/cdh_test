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
import dataclasses
import json
import sys
from typing import List

from cdh_core.config.config_file_loader import ConfigFileLoader


def main(json_path_list: List[str]) -> None:
    """Provide the full config to terraform."""
    node = dataclasses.asdict(ConfigFileLoader().get_config())

    try:
        for key in json_path_list:
            node = node.get(key)  # type: ignore
    except KeyError as error:
        raise ValueError("The given path is invalid.") from error

    print(json.dumps({json_path_list[-1]: str(node)}))  # noqa: T201


if __name__ == "__main__":
    tf_input = json.loads(sys.stdin.readline())
    json_path = tf_input.get("json_path")
    if not json_path:
        raise ValueError("The given path is empty.")
    main(json_path_list=json_path.split("."))
