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
from dataclasses import dataclass


@dataclass(frozen=True)
class AuthApi:
    """General info on an instance of the Authorization API and its affiliated endpoints."""

    auth_url: str
    cookie_name: str
    users_url: str

    @property
    def active(self) -> bool:
        """Indicate whether this instance of the Authorization API can be used."""
        return bool(self.auth_url) and bool(self.cookie_name)
