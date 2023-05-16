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
import dataclasses
from dataclasses import is_dataclass
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

import marshmallow_dataclass
from cdh_core_api.config import ValidationContext
from marshmallow import fields
from marshmallow import post_load
from marshmallow import Schema as MarshmallowSchema
from marshmallow import ValidationError
from marshmallow.fields import Field as MarshmallowField
from marshmallow.validate import Validator

AnyThing = TypeVar("AnyThing")
ValidatorFunction = Callable[[Any], Any]
ValidatorWithContextFunction = Callable[[ValidationContext, object], Any]


class BaseSchema(MarshmallowSchema):
    """
    Extends the MarshmallowSchema by the functionality to validate with a context.

    Custom type mappings (type -> Marshmallow field) can be registered here.
    marshmallow_dataclass will fall back to MarshmallowSchema.TYPE_MAPPING if it cannot find a type here.
    """

    TYPE_MAPPING: Dict[type, Type[MarshmallowField]] = {}

    @post_load
    def validate_with_context(self, data: Any, **_: Any) -> Any:
        """Return the data if it is valid based on the given context."""

        def validate_field(fields_obj: fields.Field, input_value: Any) -> None:
            validator = fields_obj.metadata.get("validator_with_context")
            if validator:
                result = validator(self.context, input_value)
                assert result is input_value  # We do not support validators that transform values.

        for attr_name, field_obj in self.dump_fields.items():
            value = data.get(attr_name)
            validate_field(field_obj, input_value=value)
            if isinstance(field_obj, fields.List):
                for entry in value or set():
                    validate_field(field_obj.inner, entry)

        return data


class SchemaValidator(Validator, Generic[AnyThing]):
    """Validates an object based on a type."""

    def __init__(self, schema: Type[AnyThing], context: Any = None):
        if not is_dataclass(schema):
            raise TypeError("SchemaValidator requires a dataclass")

        self.schema = schema
        self._validator = marshmallow_dataclass.class_schema(schema, base_schema=BaseSchema)()
        self._validator.context = context

    def __call__(self, input_object: object) -> AnyThing:
        """Return a concrete object if it is valid."""
        if not isinstance(input_object, dict):
            raise ValidationError("input must be a dictionary")
        return cast(AnyThing, self._validator.load(input_object))


class FunctionValidator(Validator):
    """Allows validating objects based on a given function."""

    def __init__(self, validator_function: ValidatorFunction):
        self.validator_function = validator_function

    def __call__(self, input_object: object) -> bool:
        """Return true if the given object is valid."""
        result = self.validator_function(input_object)
        assert result is input_object  # Marshmallow does not support validators that transform values.
        return True


def field(
    *,
    validator: Union[None, Validator, ValidatorFunction] = None,
    validator_with_context: Optional[ValidatorWithContextFunction] = None,
    **kwargs: Any,
) -> Any:
    """
    Use this method instead of dataclasses.field. It will properly validate metadata fields.

    Unfortunately, Mypy believes that this method creates fields with a default value. Thus the following example
    will raise an error ("Attributes without a default cannot follow attributes with one"):

        >>> @dataclass
        ... class Schema:
        ...     value1: str = field(...)
        ...     value2: int

    To solve this issue, you have to either change the order or add this function to all later fields:

        >>> @dataclass
        ... class Schema:
        ...     value1: str = field(...)
        ...     value2: int = field()
    """
    metadata: Dict[str, Any] = kwargs.pop("metadata", {})
    if validator is not None:
        if not isinstance(validator, Validator):
            validator = FunctionValidator(validator)
        metadata["validate"] = validator
    if validator_with_context:
        metadata["validator_with_context"] = validator_with_context
    return dataclasses.field(metadata=metadata, **kwargs)


def register_for_type(typ: type, force: bool = False) -> Callable[[Type[MarshmallowField]], Type[MarshmallowField]]:
    """
    Register a fixed field for the given type.

    Whenever this type appears in a schema, the given field will be used:
    After registering a field with
        >>> @register_for_type(Color)
        ... class MyCustomField(marshmallow.Field):
        ...     ...
    your custom field type will be implicitly used whenever 'Color' appears in a schema, so
        >>> @dataclass
        ... class MySchema:
        ...    color: Color
    is now equivalent to
        >>> @dataclass
        ... class MySchema:
        ...     color: Color = dataclasses.field(metadata={"marshmallow_field": MyCustomField()})
    """

    def decorator(field_cls: Type[MarshmallowField]) -> Type[MarshmallowField]:
        if force or typ not in BaseSchema.TYPE_MAPPING:
            BaseSchema.TYPE_MAPPING[typ] = field_cls
        else:
            raise FieldAlreadyRegisteredForType(typ)
        return field_cls

    return decorator


class FieldAlreadyRegisteredForType(Exception):
    """Signals that there already is a registered field for the given type."""

    def __init__(self, typ: type):
        super().__init__(f"A different field has already been registered for type {typ.__name__}")
