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
from dataclasses import Field
from dataclasses import fields
from dataclasses import is_dataclass
from operator import attrgetter
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple

from dataclasses_json.core import confs
from dataclasses_json.core import FieldOverride

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import CDH_DATACLASS_JSON_CONFIG

Serializer = Callable[[object], Any]


class SerializerFactory:
    """Create serializers for dataclasses, other custom types and enums."""

    @classmethod
    def create_serializer(cls) -> Serializer:
        """Create serializers for dataclasses, other custom types and enums."""
        dataclass_serializers: Dict[type, Serializer] = {}

        def serialize(obj: object) -> Any:
            if is_dataclass(obj):
                class_type = type(obj)
                # Mypy does not understand that types are hashable
                if class_type not in dataclass_serializers:
                    dataclass_serializers[class_type] = SerializerFactory._create_dataclass_serializer(class_type)
                return dataclass_serializers[class_type](obj)
            if isinstance(obj, (set, frozenset)):
                return list(obj)
            raise TypeError(f"Cannot serialize {obj}")

        return serialize

    @classmethod
    def _get_serialization_config(
        cls, class_fields: Tuple[Field, ...]  # type: ignore # Field does not have type hints
    ) -> Dict[str, FieldOverride]:
        serialization_configs = {}
        for field in class_fields:
            field_config = {}
            field_config.update(CDH_DATACLASS_JSON_CONFIG)
            field_config.update(field.metadata.get("dataclasses_json", {}))
            serialization_configs[field.name] = FieldOverride(
                *map(field_config.get, confs)  # type: ignore # we are sure the config matches the overrides
            )
        return serialization_configs

    @classmethod
    def _create_dataclass_serializer(cls, class_type: type) -> Callable[[object], Any]:
        class_fields = fields(class_type)
        field_configs = SerializerFactory._get_serialization_config(class_fields)
        getters = {field.name: attrgetter(field.name) for field in class_fields}

        def serializer(obj: object) -> Any:
            result = {}
            for original_key, field_config in field_configs.items():
                original_value = getters[original_key](obj)
                exclude = field_config.exclude
                if exclude and exclude(original_value):  # type: ignore[truthy-function]
                    continue

                serialized = (
                    field_config.encoder(original_value)
                    if field_config.encoder  # type: ignore[truthy-function]
                    else original_value
                )

                converted_key = field_config.letter_case(original_key)
                result[converted_key] = serialized
            return result

        return serializer
