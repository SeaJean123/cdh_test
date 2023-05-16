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
from unittest.mock import Mock

from cdh_core_api.endpoints.api_info import get_api_info
from cdh_core_api.services.api_info_manager import ApiInfoManager


class TestApiInfo:
    def test_get_api_info(self) -> None:
        api_info_manager = Mock(ApiInfoManager)

        info = {"information": "powerful"}
        api_info_manager.get.return_value = info

        assert get_api_info(api_info_manager).body == info
