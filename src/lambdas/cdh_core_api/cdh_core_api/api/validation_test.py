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
from enum import Enum
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import NewType
from typing import Optional
from typing import Type
from unittest.mock import Mock
from unittest.mock import patch

import marshmallow_dataclass
import pytest
from cdh_core_api.api.validation import AnyThing
from cdh_core_api.api.validation import BaseSchema
from cdh_core_api.api.validation import field
from cdh_core_api.api.validation import FieldAlreadyRegisteredForType
from cdh_core_api.api.validation import register_for_type
from cdh_core_api.api.validation import SchemaValidator
from cdh_core_api.config import ValidationContext
from marshmallow import fields
from marshmallow import ValidationError
from marshmallow.validate import Length


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class TestSchemaValidator:
    def test_require_dataclass(self) -> None:
        class Schema:
            pass

        with pytest.raises(TypeError, match="SchemaValidator requires a dataclass"):
            SchemaValidator(Schema)

    def test_basic_types(self) -> None:
        @dataclass(frozen=True)
        class Schema:
            name: str
            age: int
            points: float
            wizard: bool

        validator = SchemaValidator(Schema)
        data = {"name": "Hans", "age": 42, "points": 3.141, "wizard": True}
        invalid_values = {"name": 123, "age": "young", "points": "enough", "wizard": "maybe"}

        assert validator(data) == Schema(**data)  # type: ignore

        for key, invalid_value in invalid_values.items():
            with pytest.raises(ValidationError):
                validator({**data, key: invalid_value})

    def test_int_is_accepted_as_float(self) -> None:
        @dataclass(frozen=True)
        class Schema:
            points: float

        validator = SchemaValidator(Schema)
        result = validator({"points": 3})
        assert isinstance(result, Schema)
        assert result == Schema(points=3)
        assert isinstance(result.points, float)

    def test_missing_key(self) -> None:
        @dataclass(frozen=True)
        class Schema:
            name: str
            age: int

        validator = SchemaValidator(Schema)
        with pytest.raises(ValidationError):
            validator({"name": "Hans"})

    def test_superfluous_key(self) -> None:
        @dataclass(frozen=True)
        class Schema:
            name: str
            age: int

        validator = SchemaValidator(Schema)
        with pytest.raises(ValidationError):
            validator({"name": "Hans", "age": 42, "job": "Clown"})

    def test_enums(self) -> None:
        class MyEnum(Enum):
            ATTRIBUTE_A = "a_value"
            ATTRIBUTE_B = "b_value"

        @dataclass(frozen=True)
        class Schema:
            value: MyEnum

        validator = SchemaValidator(Schema)
        schema = validator({"value": "a_value"})
        assert isinstance(schema, Schema)
        assert schema == Schema(value=MyEnum.ATTRIBUTE_A)
        with pytest.raises(ValidationError):
            validator({"value": "a"})

    def test_lists(self) -> None:
        @dataclass(frozen=True)
        class Schema:
            numbers: List[int]

        validator = SchemaValidator(Schema)
        empty_schema = validator({"numbers": []})
        assert isinstance(empty_schema, Schema)
        assert empty_schema == Schema(numbers=[])
        schema = validator({"numbers": [1, 4, 9]})
        assert isinstance(schema, Schema)
        assert schema == Schema(numbers=[1, 4, 9])
        with pytest.raises(ValidationError):
            validator({"numbers": 1})

    def test_list_of_enums(self) -> None:
        class MyEnum(Enum):
            ATTRIBUTE_A = "a"
            ATTRIBUTE_B = "b"

        @dataclass(frozen=True)
        class Schema:
            items: List[MyEnum]

        validator = SchemaValidator(Schema)
        schema = validator({"items": ["a", "b", "a"]})
        assert isinstance(schema, Schema)
        assert schema == Schema(items=[MyEnum.ATTRIBUTE_A, MyEnum.ATTRIBUTE_B, MyEnum.ATTRIBUTE_A])
        with pytest.raises(ValidationError):
            validator({"items": ["c"]})

    def test_dicts(self) -> None:
        @dataclass(frozen=True)
        class Schema:
            tags: Dict[str, str]

        validator = SchemaValidator(Schema)
        schema = validator({"tags": {"a": "b"}})
        assert isinstance(schema, Schema)
        assert schema == Schema(tags={"a": "b"})
        with pytest.raises(ValidationError):
            validator({"tags": {"a": 123}})

    def test_nested(self) -> None:
        @dataclass(frozen=True)
        class Inner:
            whatever: int

        @dataclass(frozen=True)
        class Middle:
            inner: Inner

        @dataclass(frozen=True)
        class Schema:
            middle: Dict[str, Middle]

        validator = SchemaValidator(Schema)
        schema = validator({"middle": {"some": {"inner": {"whatever": 42}}}})
        assert isinstance(schema, Schema)
        assert schema == Schema({"some": Middle(Inner(42))})
        with pytest.raises(ValidationError):
            validator({"middle": {"some": {"inner": {"whatever": "nan"}}}})


class TestField:
    def validate(self, schema: Type[AnyThing], value: Any, *, context: Any = None) -> Any:
        validator: SchemaValidator[AnyThing] = SchemaValidator(schema, context=context)
        return validator(value)

    def test_empty_field(self) -> None:
        @dataclass
        class Schema:
            name: str = field()

        assert self.validate(Schema, {"name": "Huhufant"}) == Schema(name="Huhufant")
        with pytest.raises(ValidationError):
            self.validate(Schema, {"name": 123})

    def test_pass_kwargs_of_field(self) -> None:
        @dataclass
        class Schema:
            name: str = field(default="abc")

        assert self.validate(Schema, {}) == Schema(name="abc")
        with pytest.raises(ValidationError):
            self.validate(Schema, {"name": 123})

    def test_with_marshmallow_validator(self) -> None:
        @dataclass
        class Schema:
            name: str = field(validator=Length(max=5))

        assert self.validate(Schema, {"name": "Hans"}) == Schema(name="Hans")
        with pytest.raises(ValidationError):
            self.validate(Schema, {"name": "Heinrich"})

    def test_with_validator_function(self) -> None:
        def validator(input_variable: Any) -> str:
            if not isinstance(input_variable, str):
                raise ValidationError("Invalid type")
            return input_variable

        @dataclass
        class Schema:
            name: str = field(validator=validator)

        assert self.validate(Schema, {"name": "Huhu"}) == Schema(name="Huhu")
        with pytest.raises(ValidationError):
            self.validate(Schema, {"name": 123})

    def test_validator_with_context(self) -> None:
        def validator(context: ValidationContext, input_variable: object) -> str:
            assert context is not None
            if not isinstance(input_variable, str):
                raise ValidationError("invalid type")
            return input_variable

        @dataclass
        class Schema:
            name: str = field(validator_with_context=validator)

        assert self.validate(Schema, {"name": "Huhu"}, context="Huhu") == Schema(name="Huhu")
        with pytest.raises(ValidationError):
            self.validate(Schema, {"name": 8}, context="Haha")

    def test_validator_is_not_called_for_none(self) -> None:
        validator = Mock()

        @dataclass
        class Schema:
            name: Optional[str] = field(validator=validator, default=None)

        assert self.validate(Schema, {"name": None}) == Schema(name=None)
        validator.assert_not_called()


@pytest.fixture()
def patch_type_mapping() -> Generator[None, None, None]:
    # @register_for_type modifies TYPE_MAPPING, so we patch it by an empty dict before each test.
    with patch("cdh_core_api.api.validation.BaseSchema.TYPE_MAPPING", new={}):
        yield


MyExampleType = NewType("MyExampleType", str)


@pytest.mark.usefixtures("patch_type_mapping")
class TestRegisterForType:
    def test_registered_type_appears_in_marshmallow_schema(self) -> None:
        @register_for_type(MyExampleType)
        class MyField(fields.Field):
            pass

        @dataclass()
        class MySchema:
            value: MyExampleType

        marshmallow_schema = marshmallow_dataclass.class_schema(MySchema, base_schema=BaseSchema)()
        assert isinstance(marshmallow_schema.fields["value"], MyField)

    def test_register_twice_without_force_fails(self) -> None:
        @register_for_type(MyExampleType)
        class MyField(fields.Field):  # pylint: disable=unused-variable
            pass

        with pytest.raises(FieldAlreadyRegisteredForType):

            @register_for_type(MyExampleType)
            class MyOtherField(fields.Field):  # pylint: disable=unused-variable
                pass

    def test_register_twice_with_force(self) -> None:
        @dataclass()
        class MySchema:
            value: MyExampleType

        @register_for_type(MyExampleType)
        class MyField(fields.Field):  # pylint: disable=unused-variable
            pass

        @register_for_type(MyExampleType, force=True)
        class MyOtherField(fields.Field):
            pass

        marshmallow_schema = marshmallow_dataclass.class_schema(MySchema, base_schema=BaseSchema)()
        assert isinstance(marshmallow_schema.fields["value"], MyOtherField)

    def test_with_validator(self) -> None:
        @register_for_type(MyExampleType)
        class MyField(fields.String):  # pylint: disable=unused-variable
            def __init__(self, **kwargs: Any):
                super().__init__(validate=Length(max=5), **kwargs)

        @dataclass()
        class MySchema:
            value: MyExampleType

        validator = SchemaValidator(MySchema)
        schema = validator({"value": "123"})
        assert isinstance(schema, MySchema)
        assert schema == MySchema(value=MyExampleType("123"))

        with pytest.raises(ValidationError):
            validator({"value": "toooo_long"})

    def test_with_validator_with_context(self) -> None:
        def validator_with_context(context: Any, input_variable: Any) -> Any:
            if input_variable != context:
                raise ValidationError("Input must equal context")
            return input_variable

        @register_for_type(MyExampleType)
        class MyField(fields.String):  # pylint: disable=unused-variable
            def __init__(self, **kwargs: Any):
                super().__init__(validator_with_context=validator_with_context, **kwargs)

        @dataclass()
        class MySchema:
            value: MyExampleType

        validator = SchemaValidator(MySchema, context="TheContext")
        schema = validator({"value": "TheContext"})
        assert isinstance(schema, MySchema)
        assert schema == MySchema(value=MyExampleType("TheContext"))

        with pytest.raises(ValidationError):
            validator({"value": "NoContext"})

    @pytest.mark.parametrize(
        "list_input,valid",
        [
            ([], True),
            (["TheContext"], True),
            (["TheContext", "TheContext"], True),
            (["NoContext"], False),
            (["TheContext", "NoContext"], False),
        ],
    )
    def test_list_with_validator_with_context(self, list_input: List[str], valid: bool) -> None:
        def validator_with_context(context: Any, input_to_validate: Any) -> Any:
            if input_to_validate != context:
                raise ValidationError("Input must equal context")
            return input_to_validate

        @register_for_type(MyExampleType)
        class MyField(fields.String):  # pylint: disable=unused-variable
            def __init__(self, **kwargs: Any):
                super().__init__(validator_with_context=validator_with_context, **kwargs)

        @dataclass()
        class MySchema:
            values: List[MyExampleType]

        validator = SchemaValidator(MySchema, context="TheContext")

        if valid:
            schema = validator({"values": list_input})
            assert isinstance(schema, MySchema)
            assert schema == MySchema(values=[MyExampleType(value) for value in list_input])
        else:
            with pytest.raises(ValidationError):
                validator({"values": list_input})
