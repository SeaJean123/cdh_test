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

from cdh_core.config.config_file_loader import ConfigFileLoader


def main() -> None:
    """Output the admin role name."""
    admin_role_name = ConfigFileLoader.get_config().account.admin_role_name

    print(json.dumps({"admin_role_name": admin_role_name}))  # noqa: T201


if __name__ == "__main__":
    main()
