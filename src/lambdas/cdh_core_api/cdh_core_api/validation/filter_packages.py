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
import string
from typing import Any

from cdh_core_api.validation.abstract import StringValidator
from cdh_core_api.validation.base import list_field
from marshmallow import fields

validate_table_name = StringValidator(
    min_length=1,
    max_length=255,
    characters=string.ascii_letters + string.digits + "_",
    characters_description="ASCII letters, digits, and underscores",
)
validate_column_name = StringValidator(
    min_length=1,
    max_length=255,
    characters=string.ascii_letters + string.digits + "_.",
    characters_description="ASCII letters, digits, underscores, and .",
)
validate_filter_package_friendly_name = StringValidator(
    min_length=1,
    max_length=40,
    characters=string.ascii_letters + string.digits + "_- ",
    characters_description="ASCII letters, digits, spaces, -, and _",
)
validate_filter_package_description = StringValidator(min_length=5, max_length=1000, allow_newlines=True)


def column_names_field(can_be_none: bool = True, **kwargs: Any) -> Any:
    """Return a field that validates a list of aolumn names."""
    return list_field(inner_field=fields.String(validate=validate_column_name), can_be_none=can_be_none, **kwargs)
