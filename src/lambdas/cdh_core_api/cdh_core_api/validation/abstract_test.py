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
from enum import Enum
from typing import Callable
from typing import List

import pytest
from cdh_core_api.validation.abstract import create_enum_validator
from cdh_core_api.validation.abstract import StringValidator
from cdh_core_api.validation.abstract import validate_and_format_string_list
from marshmallow import ValidationError

from cdh_core_dev_tools.testing.assert_raises import assert_raises


class TestCreateEnumValidator:
    def test_enum_validator(self) -> None:
        class MyEnum(Enum):
            ELEMENT_A = "a"
            ELEMENT_B = "b"

        validator = create_enum_validator(MyEnum, "Invalid str", hide_sensitive_data=False)
        assert validator("a") == MyEnum.ELEMENT_A
        assert validator("b") == MyEnum.ELEMENT_B

        with assert_raises(ValidationError("Invalid str: c. Allowed values are: a, b")):
            validator("c")

    def test_allowed_values_parameter(self) -> None:
        class MyEnum(Enum):
            ONE = 1
            TWO = 2

        validator = create_enum_validator(MyEnum, "invalid", allowed_values=[MyEnum.ONE], hide_sensitive_data=False)
        assert validator(1) == MyEnum.ONE
        with assert_raises(ValidationError("invalid: 2. Allowed values are: 1")):
            validator(2)


class TestStringValidator:
    def test_min_length(self) -> None:
        validator = StringValidator(min_length=3)
        assert validator("123") == "123"
        assert validator("1234") == "1234"
        with assert_raises(ValidationError("Length must be at least 3 characters")):
            validator("12")

    def test_max_length(self) -> None:
        validator = StringValidator(max_length=3)
        assert validator("12") == "12"
        assert validator("123") == "123"
        with assert_raises(ValidationError("Length must be at most 3 characters")):
            validator("1234")

    def test_characters(self) -> None:
        validator = StringValidator(characters="abc")
        assert validator("") == ""
        assert validator("abbcabac") == "abbcabac"
        with assert_raises(ValidationError("Only the following characters are valid: abc")):
            validator("d")

    def test_characters_with_custom_description(self) -> None:
        validator = StringValidator(characters="abc", characters_description="a, b, and c")
        with assert_raises(ValidationError("Only the following characters are valid: a, b, and c")):
            validator("d")

    def test_to_regex_without_character_class(self) -> None:
        assert StringValidator(min_length=3, max_length=17).to_regex() == ".{3,17}"
        assert StringValidator(min_length=3).to_regex() == ".{3,}"
        assert StringValidator(max_length=17).to_regex() == ".{0,17}"

    def test_to_regex_with_character_class(self) -> None:
        assert StringValidator(characters="abc").to_regex() == "[abc]{0,}"

    def test_not_allowed_whitespaces(self) -> None:
        validator = StringValidator(min_length=1)
        with pytest.raises(ValidationError):
            validator("  ")

        with pytest.raises(ValidationError):
            validator(" a   ")

    def test_does_not_allow_newlines_by_default(self) -> None:
        validator = StringValidator()
        with pytest.raises(ValidationError):
            validator("a long\ntext")

    def test_allows_newlines_if_configured(self) -> None:
        validator = StringValidator(allow_newlines=True)
        assert validator("a long\ntext") == "a long\ntext"

    def test_combine_newlines_with_characters(self) -> None:
        validator = StringValidator(characters="abc", allow_newlines=True)
        assert validator("abc\nabc") == "abc\nabc"

        with pytest.raises(ValidationError):
            assert validator("abcd")

    def test_to_regex_with_newlines(self) -> None:
        assert StringValidator(allow_newlines=True).to_regex() == r"[\s\S]{0,}"
        # If the \\\n looks odd: https://bugs.python.org/issue42668
        assert StringValidator(characters="abc", allow_newlines=True).to_regex() == "[abc\\\n]{0,}"


class TestValidateStringList:
    def get_string_validator(self, accepted_values: List[str]) -> Callable[[str], str]:
        def string_validator(input_string: str) -> str:
            if input_string not in accepted_values:
                raise ValidationError("Bad input!")
            return input_string

        return string_validator

    def test_all_valid(self) -> None:
        validate_and_format_string_list("one, two, three", self.get_string_validator(["one", "two", "three"]))

    def test_one_rejection_suffices(self) -> None:
        with pytest.raises(ValidationError):
            validate_and_format_string_list("one, two, three", self.get_string_validator(["one", "three"]))

    def test_whitespace_normalized(self) -> None:
        assert validate_and_format_string_list("one,two ,  three ", lambda x: x) == "one, two, three"

    def test_custom_separator(self) -> None:
        assert validate_and_format_string_list("one::two::three", lambda x: x, separator="::") == "one, two, three"

    def test_modifying_validator(self) -> None:
        assert validate_and_format_string_list("one,two,three", lambda x: x.upper()) == "ONE, TWO, THREE"
