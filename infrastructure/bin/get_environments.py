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
import sys

from cdh_core.enums.environment import Environment


def main(test_environments_only: bool) -> None:
    """Output all environments as a mapping 'value' => 'friendlyName'."""
    print(  # noqa: T201
        json.dumps(
            {
                environment.value: environment.friendly_name
                for environment in Environment
                if not test_environments_only or environment.is_test_environment
            }
        )
    )


def _parse_boolean_input(value: str) -> bool:
    return value.lower() == "true"


if __name__ == "__main__":
    tf_input = json.loads(sys.stdin.readline())
    main(
        test_environments_only=_parse_boolean_input(tf_input["test_environments_only"])
        if "test_environments_only" in tf_input
        else False,
    )
