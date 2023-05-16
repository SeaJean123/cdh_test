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
import re
from typing import Any
from typing import List
from typing import Optional

from cdh_core_api.api.validation import field
from cdh_core_api.validation.abstract import InvalidType
from cdh_core_api.validation.abstract import StringValidator
from cdh_core_api.validation.base import list_field
from marshmallow import fields
from marshmallow import ValidationError
from marshmallow.validate import And
from marshmallow.validate import Length
from marshmallow.validate import Regexp

from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType

AWS_ROLE_NAME_REGEX = re.compile(r"^[\w+=,.@-]+$")
AWS_ROLE_PATH_REGEX = re.compile(r"^/([!-~]+/)*$")

# https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateRole.html
ROLE_NAME_FIELD = fields.Str(
    pattern=AWS_ROLE_NAME_REGEX.pattern,
    validate=And(Regexp(AWS_ROLE_NAME_REGEX), Length(min=1, max=64)),
    example="CDHDevOps",
)
ROLE_PATH_FIELD = fields.Str(
    pattern=AWS_ROLE_PATH_REGEX.pattern,
    validate=And(Regexp(AWS_ROLE_PATH_REGEX), Length(min=1, max=512)),
    example="/cdh/",
)


validate_account_friendly_name = StringValidator(min_length=3, max_length=150)


def admin_roles_field(can_be_none: bool = False, **kwargs: Any) -> Any:
    """Return a field that validates a list of admin roles."""
    return list_field(ROLE_NAME_FIELD, can_be_none=can_be_none, **kwargs)


def role_name_field(**kwargs: Any) -> Any:
    """Return a field that validates an AWS role name."""
    return field(metadata={"marshmallow_field": ROLE_NAME_FIELD}, **kwargs)


def role_path_field(**kwargs: Any) -> Any:
    """Return a field that validates an AWS role path."""
    return field(metadata={"marshmallow_field": ROLE_PATH_FIELD}, default="/", **kwargs)


def validate_account_roles(account_roles: object) -> Optional[List[AccountRole]]:
    """Return a list of account roles if it is valid."""
    if not isinstance(account_roles, list):
        raise InvalidType(type(account_roles), list)
    if not any(role.type is AccountRoleType.WRITE for role in account_roles):
        raise ValidationError(
            f"At least one account role with type {AccountRoleType.WRITE.value} needs to be provided."
        )
    return account_roles
