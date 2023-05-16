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
from enum import Enum
from typing import Dict
from typing import List

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin


class TestDataClassJsonCDHMixin:
    @dataclass
    class _SimpleData(DataClassJsonCDHMixin):
        this_is_in_snake_case: bool

    def test_snake_case_to_camel_case(self) -> None:
        simple_data = TestDataClassJsonCDHMixin._SimpleData(this_is_in_snake_case=True)
        expected_snake_case_key = "thisIsInSnakeCase"
        assert expected_snake_case_key in simple_data.to_dict()
        assert expected_snake_case_key in simple_data.to_json()
        assert simple_data == simple_data.from_json(simple_data.to_json())

    @dataclass
    class _ComplexData(DataClassJsonCDHMixin):
        @dataclass
        class Point:
            x_coordinate: int
            y_coordinate: int

        class SomeEnum(Enum):
            PROPERTY_A = "A"
            PROPERTY_B = "B"

        some_enum: SomeEnum
        points: List[Point]
        additional_info: Dict[str, Dict[str, SomeEnum]]

    def test_to_plain_dict(self) -> None:
        data = TestDataClassJsonCDHMixin._ComplexData(
            some_enum=TestDataClassJsonCDHMixin._ComplexData.SomeEnum.PROPERTY_A,
            points=[
                TestDataClassJsonCDHMixin._ComplexData.Point(1, 2),
                TestDataClassJsonCDHMixin._ComplexData.Point(3, 4),
            ],
            additional_info={
                "nested": {"dictionary": TestDataClassJsonCDHMixin._ComplexData.SomeEnum.PROPERTY_B},
            },
        )

        assert data.to_plain_dict() == {
            "someEnum": "A",
            "points": [{"x_coordinate": 1, "y_coordinate": 2}, {"x_coordinate": 3, "y_coordinate": 4}],
            "additionalInfo": {"nested": {"dictionary": "B"}},
        }

    def test_to_plain_dict_json_dumps(self) -> None:
        data = TestDataClassJsonCDHMixin._ComplexData(
            some_enum=TestDataClassJsonCDHMixin._ComplexData.SomeEnum.PROPERTY_A,
            points=[
                TestDataClassJsonCDHMixin._ComplexData.Point(1, 2),
                TestDataClassJsonCDHMixin._ComplexData.Point(3, 4),
            ],
            additional_info={
                "nested": {"dictionary": TestDataClassJsonCDHMixin._ComplexData.SomeEnum.PROPERTY_B},
            },
        )
        stringified = json.dumps(data.to_plain_dict())

        assert stringified == data.to_json()
        assert TestDataClassJsonCDHMixin._ComplexData.from_json(stringified) == data
