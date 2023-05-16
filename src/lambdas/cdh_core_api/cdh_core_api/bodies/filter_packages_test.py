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
# pylint: disable=invalid-name
from typing import Any
from typing import Dict
from typing import Union

import pytest
from cdh_core_api.api.validation import SchemaValidator
from cdh_core_api.bodies.filter_packages import NewFilterBody
from cdh_core_api.bodies.filter_packages import NewFilterPackageBody
from cdh_core_api.config import ValidationContext
from cdh_core_api.config_test import build_config
from marshmallow import ValidationError

from cdh_core.entities.filter_package import ColumnName
from cdh_core.entities.filter_package import TableName
from cdh_core.enums.hubs_test import build_hub


class _TestBody:
    plain_body: Dict[str, Any]
    body: Union[NewFilterBody, NewFilterPackageBody]
    validator: SchemaValidator[Union[NewFilterBody, NewFilterPackageBody]]

    def _assert_valid(self) -> None:
        assert self.validator(self.plain_body) == self.body

    def test_valid(self) -> None:
        self._assert_valid()


class TestNewFilterPackageBody(_TestBody):
    def setup_method(self) -> None:
        self.plain_body = {
            "description": "test package description",
            "friendlyName": "test package name",
            "tableAccess": [],
        }
        self.body = NewFilterPackageBody(
            description="test package description", friendlyName="test package name", tableAccess=[]
        )
        self.validator = SchemaValidator(
            NewFilterPackageBody,
            context=ValidationContext(config=build_config(), current_hub=build_hub()),
        )

    @pytest.mark.parametrize("required_field", ["description", "friendlyName", "tableAccess"])
    def test_required_fields(self, required_field: str) -> None:
        self.plain_body.pop(required_field)

        with pytest.raises(ValidationError):
            self.validator(self.plain_body)


class TestNewFilterBody(_TestBody):
    def setup_method(self) -> None:
        self.plain_body = {"tableName": "table_1", "includedColumns": ["column.1_2"]}
        self.body = NewFilterBody(tableName=TableName("table_1"), includedColumns=[ColumnName("column.1_2")])
        self.validator = SchemaValidator(
            NewFilterBody,
            context=ValidationContext(config=build_config(), current_hub=build_hub()),
        )

    def test_invalid_column_name(self) -> None:
        self.plain_body["includedColumns"] = ["(invalid)"]

        with pytest.raises(ValidationError):
            self.validator(self.plain_body)
