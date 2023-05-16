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

from cdh_core_api.config_test import build_config
from cdh_core_api.services.full_vision_check import FullVisionCheck
from cdh_core_api.services.phone_book import PhoneBook

from cdh_core.entities.arn_test import build_arn


class TestFullVisionCheck:
    def setup_method(self) -> None:
        self.config = build_config(use_authorization=True)
        self.requester = build_arn(service="sts")
        self.phone_book = Mock(PhoneBook)
        self.phone_book.is_privileged_core_api_role.return_value = False
        self.phone_book.is_privileged_portal_role.return_value = False
        self.phone_book.is_authorization_role.return_value = False
        self.full_vision_check = FullVisionCheck(self.config, self.phone_book)

    def test_full_vision_without_authorization_api(self) -> None:
        config = build_config(
            use_authorization=False,
        )
        full_vision_check = FullVisionCheck(config, self.phone_book)
        assert full_vision_check(self.requester)
        assert len(self.phone_book.method_calls) == 0

    def test_no_full_vision_with_authorization_api(self) -> None:
        assert not self.full_vision_check(self.requester)
        self.phone_book.is_privileged_core_api_role.assert_called_once()
        self.phone_book.is_privileged_portal_role.assert_called_once()

    def test_full_vision_for_privileged_core_api_role(self) -> None:
        self.phone_book.is_privileged_core_api_role.return_value = True
        assert self.full_vision_check(self.requester)

    def test_full_vision_for_privileged_portal_role(self) -> None:
        self.phone_book.is_privileged_portal_role.return_value = True
        assert self.full_vision_check(self.requester)

    def test_full_vision_for_authorization_role(self) -> None:
        self.phone_book.is_authorization_role.return_value = True
        assert self.full_vision_check(self.requester)
