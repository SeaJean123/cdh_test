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
from cdh_core_api.config import Config
from cdh_core_api.services.phone_book import PhoneBook

from cdh_core.entities.arn import Arn


class FullVisionCheck:
    """Checks whether a requester is allowed to see all entities managed by the core api."""

    def __init__(self, config: Config, phone_book: PhoneBook):
        self._config = config
        self._phone_book = phone_book

    def __call__(self, requester: Arn) -> bool:
        """Check whether the requester has full vision."""
        return (
            (not self._config.using_authorization_api)
            or self._phone_book.is_privileged_core_api_role(requester)
            or self._phone_book.is_privileged_portal_role(requester)
            or self._phone_book.is_authorization_role(requester)
        )
