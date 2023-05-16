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
import json
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set

import orjson
from dataclasses_json import config
from dataclasses_json import Exclude

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.entities.arn import Arn
from cdh_core.entities.serializer_factory import SerializerFactory


class TestSerializerFactory:
    @dataclass
    class _TestData(DataClassJsonCDHMixin):
        class SomeEnum(Enum):
            PROPERTY_A = "A"
            PROPERTY_B = "B"

        @dataclass
        class NestedData(DataClassJsonCDHMixin):
            data: str

        some_enum: SomeEnum
        additional_info: Dict[str, Dict[str, SomeEnum]]
        field_to_be_renamed: Arn = field(metadata=config(field_name="anotherName", encoder=str))
        nested_data: List[NestedData]
        field_to_be_removed: str = field(metadata=config(exclude=Exclude.ALWAYS))
        null_field: Optional[str]
        set_field: Set[str]
        frozenset_field: FrozenSet[str]

    def test_to_dict_with_complex_dataclass_body(self) -> None:
        serializer = SerializerFactory.create_serializer()
        data = TestSerializerFactory._TestData(
            some_enum=TestSerializerFactory._TestData.SomeEnum.PROPERTY_A,
            additional_info={
                "some_key": {"another_key": TestSerializerFactory._TestData.SomeEnum.PROPERTY_B},
            },
            field_to_be_renamed=Arn("arn:aws:s3:::something"),
            nested_data=[TestSerializerFactory._TestData.NestedData("some_data")],
            field_to_be_removed="should not be present",
            null_field=None,
            set_field={"one", "two"},
            frozenset_field=frozenset({"three", "four"}),
        )

        data_json = orjson.dumps(data, default=serializer, option=orjson.OPT_PASSTHROUGH_DATACLASS).decode("utf-8")

        # parse serialized json body string
        recovered = json.loads(data_json)
        # convert set -> list serialization back into sets to compare without order
        recovered["setField"] = set(recovered["setField"])
        recovered["frozensetField"] = set(recovered["frozensetField"])
        assert recovered == {
            "someEnum": "A",
            "additionalInfo": {"some_key": {"another_key": "B"}},
            "anotherName": "arn:aws:s3:::something",
            "nestedData": [{"data": "some_data"}],
            "nullField": None,
            "setField": {"one", "two"},
            "frozensetField": {"three", "four"},
        }

    @dataclass
    class _Base:
        name: str

    @dataclass
    class _Subclass(_Base):
        name: str = field(metadata=config(encoder=lambda name: 2 * name))

    def test_inheritance(self) -> None:
        @dataclass
        class BaseCollection:
            items: Sequence[TestSerializerFactory._Base]

        serializer = SerializerFactory.create_serializer()
        data = BaseCollection([TestSerializerFactory._Base("foo"), TestSerializerFactory._Subclass("bar")])

        data_json = orjson.dumps(data, default=serializer, option=orjson.OPT_PASSTHROUGH_DATACLASS).decode("utf-8")

        recovered = json.loads(data_json)
        assert len(recovered["items"]) == 2
        assert recovered["items"][0] == {"name": "foo"}
        assert recovered["items"][1] == {"name": "barbar"}
