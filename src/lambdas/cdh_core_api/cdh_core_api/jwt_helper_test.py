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
# pylint: disable=unused-argument
import json
from base64 import b64encode
from typing import Optional

import pytest
from cdh_core_api.config_test import build_config
from cdh_core_api.jwt_helper import get_jwt
from cdh_core_api.jwt_helper import get_jwt_user_id

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import SIMPLE_CONFIG_FILE
from cdh_core.entities.request import Cookie
from cdh_core.entities.request_test import build_request
from cdh_core.enums.resource_properties import Stage
from cdh_core_dev_tools.testing.builder import Builder

config = build_config()
JWT_COOKIE_NAME = config.authorization_api_params.cookie_name


def build_jwt_token(user_id: Optional[str] = None) -> str:
    """
    Create a new JWT token.

    base64 encoded information with removed padding (separated by a dot): '{"typ":"JWT","alg":"HS256"}'
    and '{"iat":1624945118,"exp":1624988318,"id":"someone@example.com","company":"foo","isLongLived":false}'
    """
    return ".".join(
        [
            b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode(),
            b64encode(
                json.dumps(
                    {"id": user_id or Builder.build_random_string(), "company": Builder.build_random_string()}
                ).encode()
            ).decode(),
            Builder.build_random_string(),
        ]
    )


@pytest.mark.parametrize("mock_config_file", [SIMPLE_CONFIG_FILE], indirect=True)
class TestGetJwtUserId:
    def test_get_non_existent_jwt_user_id(self, mock_config_file: ConfigFile) -> None:
        assert get_jwt_user_id(build_request(headers={}), config) is None

    def test_get_jwt_user_id_invalid_token_value(self, mock_config_file: ConfigFile) -> None:
        request = build_request(
            headers={
                "Cookie": f"{JWT_COOKIE_NAME}={Builder.build_random_string()}",
                "Origin": list(mock_config_file.stage_by_origin.instances)[0],
            },
        )
        assert get_jwt_user_id(request, config) is None

    def test_get_jwt_user_id_invalid_token_key(self, mock_config_file: ConfigFile) -> None:
        request = build_request(
            headers={
                "Cookie": f"{Builder.build_random_string()}={build_jwt_token()}",
                "Origin": list(mock_config_file.stage_by_origin.instances)[0],
            },
        )
        assert get_jwt_user_id(request, config) is None

    def test_get_jwt_user_id_successful(self, mock_config_file: ConfigFile) -> None:
        user_id = Builder.build_random_string()
        jwt_token = build_jwt_token(user_id)
        request = build_request(
            headers={"Cookie": f"{JWT_COOKIE_NAME}={jwt_token}"},
        )
        assert get_jwt_user_id(request, config) == user_id


@pytest.mark.parametrize("mock_config_file", [SIMPLE_CONFIG_FILE], indirect=True)
class TestGetJwt:
    def test_no_jwt_if_no_cookie_is_set(self, mock_config_file: ConfigFile) -> None:
        request = build_request(headers={"Cookie": f"{Builder.build_random_string()}={Builder.build_random_string()}"})
        assert get_jwt(request, config) is None

    def test_prefer_special_jwt_cookie_over_matching_portal_cookie(self, mock_config_file: ConfigFile) -> None:
        origin, stage = dict(mock_config_file.stage_by_origin.instances).popitem()
        request = build_request(
            headers={"Cookie": f"jwt_{stage}=wrong;{JWT_COOKIE_NAME}=xyz", "Origin": origin},
        )
        assert get_jwt(request, config) == Cookie(JWT_COOKIE_NAME, "xyz")

    def test_do_not_return_non_matching_portal_cookies(self, mock_config_file: ConfigFile) -> None:
        origin = list(mock_config_file.stage_by_origin.instances)[0]
        stage = Builder.get_random_element(
            [stage.value for stage in Stage], {mock_config_file.stage_by_origin.instances[origin]}
        )
        request = build_request(
            headers={"Cookie": f"jwt_{stage}=wrong", "Origin": origin},
        )
        assert get_jwt(request, config) is None
