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
import pytest
from cdh_core_api.validation.abstract import InvalidType
from cdh_core_api.validation.accounts import AWS_ROLE_NAME_REGEX
from cdh_core_api.validation.accounts import AWS_ROLE_PATH_REGEX
from cdh_core_api.validation.accounts import validate_account_roles
from marshmallow import ValidationError

from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType
from cdh_core_dev_tools.testing.builder import Builder


class TestRoleNameRegex:
    def test_invalid_format(self) -> None:
        for name in ["white space", "forward/slash", "{"]:
            assert not AWS_ROLE_NAME_REGEX.match(name)

    def test_valid(self) -> None:
        for name in ["some+name", "other@test.de"]:
            assert AWS_ROLE_NAME_REGEX.match(name)


class TestRolePathRegex:
    def test_invalid_format(self) -> None:
        for path in ["some/", "/other", "other/invalid/path"]:
            assert not AWS_ROLE_PATH_REGEX.match(path)

    def test_valid(self) -> None:
        for path in ["/", "/path/", "/longer/path/"]:
            assert AWS_ROLE_PATH_REGEX.match(path)


class TestValidateAccountRoles:
    def test_invalid_instance_type(self) -> None:
        with pytest.raises(InvalidType):
            validate_account_roles("invalid")

    def test_no_write_role_invalid(self) -> None:
        roles = [
            AccountRole(
                name=Builder.build_random_string(),
                path="/",
                type=AccountRoleType.READ,
                friendly_name=Builder.build_random_string(),
            )
            for _ in range(3)
        ]
        with pytest.raises(ValidationError):
            validate_account_roles(roles)

    def test_one_write_role_valid(self) -> None:
        roles = [
            AccountRole(
                name=Builder.build_random_string(),
                path="/",
                type=AccountRoleType.WRITE,
                friendly_name=Builder.build_random_string(),
            )
        ]
        validate_account_roles(roles)
