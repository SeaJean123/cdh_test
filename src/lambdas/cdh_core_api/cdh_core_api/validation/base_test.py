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
from typing import Any
from typing import List
from typing import Optional

import pytest
from cdh_core_api.api.validation import SchemaValidator
from cdh_core_api.validation.base import EMAIL_REGEX
from cdh_core_api.validation.base import list_field
from marshmallow import fields
from marshmallow import ValidationError
from marshmallow.validate import Length


class TestEmailRegex:
    def test_invalid_format(self) -> None:
        for email in ["white space@cloud.de", "hansATweb.de", "@bcloud.de", "hans@", "hu@hu@web.de"]:
            assert not EMAIL_REGEX.match(email)

    def test_names(self) -> None:
        valid_emails = ["b.m.w@cloud.de", "äöü.a-z_Ä09@cloud.de"]
        for email in valid_emails:
            assert EMAIL_REGEX.match(email)
        assert not EMAIL_REGEX.match("bébé@cloud.de")


class TestListField:
    def setup_method(self) -> None:
        self.inner_field = fields.Str(validate=Length(min=1, max=5))
        self.valid_item = "valid"
        self.invalid_item = "invalid"

    def _validate(self, validator: SchemaValidator[Any], value: Optional[List[str]]) -> None:
        if value is None:
            validator({})
        else:
            validator({"items": value})

    def _check_valid_values(self, validator: SchemaValidator[Any], values: List[Optional[List[str]]]) -> None:
        for value in values:
            self._validate(validator, value)

    def _check_invalid_values(self, validator: SchemaValidator[Any], values: List[Optional[List[str]]]) -> None:
        for value in values:
            with pytest.raises(ValidationError):
                self._validate(validator, value)

    def test_allow_empty(self) -> None:
        @dataclass(frozen=True)
        class SomeBody:
            items: List[str] = list_field(inner_field=self.inner_field, allow_empty=True, can_be_none=False)

        validator = SchemaValidator(SomeBody)

        self._check_valid_values(validator=validator, values=[[], [self.valid_item]])
        self._check_invalid_values(validator=validator, values=[None, [self.invalid_item]])

    @pytest.mark.parametrize("allow_empty", [False, True])
    def test_can_be_none_true(self, allow_empty: bool) -> None:
        @dataclass(frozen=True)
        class SomeBody:
            items: List[str] = list_field(inner_field=self.inner_field, allow_empty=allow_empty, can_be_none=True)

        validator = SchemaValidator(SomeBody)

        self._check_valid_values(validator=validator, values=[None, [], [self.valid_item]])
        self._check_invalid_values(validator=validator, values=[[self.invalid_item]])

    def test_allow_empty_false(self) -> None:
        @dataclass(frozen=True)
        class SomeBody:
            items: List[str] = list_field(inner_field=self.inner_field, allow_empty=False)

        validator = SchemaValidator(SomeBody)

        self._check_valid_values(validator=validator, values=[[self.valid_item]])
        self._check_invalid_values(validator=validator, values=[None, [], [self.invalid_item]])
