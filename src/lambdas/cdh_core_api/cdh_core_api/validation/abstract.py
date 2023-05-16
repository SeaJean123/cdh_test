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
from re import escape
from typing import Any
from typing import Callable
from typing import cast
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

from marshmallow import ValidationError
from marshmallow.fields import Field
from marshmallow.validate import Validator
from marshmallow_jsonschema.base import FIELD_VALIDATORS


AnyEnum = TypeVar("AnyEnum", bound=Enum)
T = TypeVar("T")  # pylint: disable=invalid-name


class InvalidType(ValidationError):
    """Signals that the type of an object is not the expected one."""

    def __init__(self, actual_type: Type[Any], expected_type: Type[Any]):
        super().__init__(f"Invalid type {actual_type.__name__}, must be {expected_type.__name__}")


def validate_type(data: object, expected_type: Type[T]) -> T:
    """Validate if the given object is of the expected type."""
    if not isinstance(data, expected_type):
        raise InvalidType(type(data), expected_type)
    return data


def create_enum_validator(
    enum: Type[AnyEnum],
    error_message: str,
    allowed_values: Optional[Collection[AnyEnum]] = None,
    hide_sensitive_data: bool = True,
) -> Callable[[Any], AnyEnum]:
    """Create a validator for an enum class."""
    values: Collection[AnyEnum] = cast(Collection[AnyEnum], allowed_values if allowed_values is not None else enum)

    def validator(enum_input: object) -> AnyEnum:
        try:
            output = enum(enum_input)
            if output in values:
                return output
            raise ValueError
        except ValueError as error:
            if hide_sensitive_data:
                raise ValidationError(f"{error_message}: {enum_input} is not allowed") from error
            allowed_strings = [str(item.value) for item in values]
            raise ValidationError(
                f'{error_message}: {enum_input}. Allowed values are: {", ".join(allowed_strings)}'
            ) from error

    return validator


class StringValidator(Validator):
    """Validates a string based on marshmallow."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        min_length: int = 0,
        max_length: Optional[int] = None,
        characters: Optional[str] = None,
        characters_description: Optional[str] = None,
        allow_newlines: bool = False,
    ):
        self.min_length = min_length
        self.max_length = max_length
        if characters and allow_newlines and "\n" not in characters:
            characters += "\n"
        self.characters = characters
        self.characters_description = characters_description
        self._characters_set = set(characters) if characters is not None else None
        self.allow_newlines = allow_newlines

    def __call__(self, input_string: object) -> str:
        """Validate the given string."""
        if not isinstance(input_string, str):
            raise InvalidType(type(input_string), str)

        if len(input_string) > len(input_string.strip()):
            raise ValidationError("Input should not have leading or trailing whitespaces")
        if len(input_string) < self.min_length:
            raise ValidationError(f"Length must be at least {self.min_length} characters")
        if self.max_length is not None and len(input_string) > self.max_length:
            raise ValidationError(f"Length must be at most {self.max_length} characters")

        if self._characters_set is not None and not all(c in self._characters_set for c in input_string):
            raise ValidationError(
                f"Only the following characters are valid: {self.characters_description or self.characters}"
            )
        if not self.allow_newlines and "\n" in input_string:
            raise ValidationError("Must not contain line-breaks")

        return input_string

    def to_regex(self) -> str:
        """
        Return the Validator as regex.

        OpenAPI specifies that regular expressions should follow the EcmaScript standard:
        https://www.ecma-international.org/ecma-262/5.1/#sec-15.10.1
        """
        if self.characters:
            allowed_characters = f"[{escape(self.characters)}]"
        else:
            # We could also use the re.DOTALL flag (s) here. But in JavaScript it only has been added
            # in ECMAScript 2018 and is not yet supported by older browsers.
            allowed_characters = r"[\s\S]" if self.allow_newlines else "."
        return f"{allowed_characters}{{{self.min_length},{self.max_length or ''}}}"

    # pylint: disable=unused-argument
    @staticmethod
    def _modify_json_schema(
        schema: Dict[str, Any],
        field: Field,
        validator: "StringValidator",
        parent_schema: Any,
    ) -> Dict[str, Any]:
        # This method is called by marshmallow_jsonschema to render additional information into the JSON schema.
        schema["pattern"] = validator.to_regex()
        if validator.min_length > 0:
            schema["minLength"] = validator.min_length
        if validator.max_length is not None:
            schema["maxLength"] = validator.max_length
        return schema


FIELD_VALIDATORS[StringValidator] = StringValidator._modify_json_schema  # pylint: disable=protected-access


def validate_and_format_string_list(
    string_list: str,
    item_validator: Callable[[str], str],
    separator: str = ",",
) -> str:
    """Return the given string if the elements are valid."""
    return _join([item_validator(item) for item in _split(string_list, separator)])


def _split(value: str, sep: str = ",") -> List[str]:
    return [part.strip() for part in value.split(sep)]


def _join(values: List[str]) -> str:
    return ", ".join(values)
