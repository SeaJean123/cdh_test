from typing import Any

from marshmallow.fields import Field

class EnumField(Field):
    enum: Any
    _jsonschema_type_mapping: Any
