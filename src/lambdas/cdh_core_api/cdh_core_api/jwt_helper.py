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
from base64 import b64decode
from contextlib import suppress
from typing import Optional

from cdh_core_api.config import Config

from cdh_core.entities.request import Cookie
from cdh_core.entities.request import Request


def get_jwt_user_id(request: Request, config: Config) -> Optional[str]:
    """Extract the userid based on the JWT from the request."""
    jwt = get_jwt(request, config)
    return extract_jwt_user_id(jwt)


def extract_jwt_user_id(jwt: Optional[Cookie]) -> Optional[str]:
    """Decode a given JWT and extract the user ID, if defined."""
    if jwt:
        with suppress(Exception):
            _, jwt_payload, _ = jwt.value.split(".")
            # base64 does recognize incorrect padding, but cannot handle it
            jwt_info = json.loads(b64decode(jwt_payload + "==").decode("UTF-8"))
            return str(jwt_info["id"])
    return None


def get_jwt(request: Request, config: Config) -> Optional[Cookie]:
    """
    Fetch the JWT cookie from the HTTP header, if authorization API is used.

    Returns None otherwise and if cookie with expected name not present.
    """
    if not config.using_authorization_api:
        return None
    return request.get_cookie(config.authorization_api_params.cookie_name)
