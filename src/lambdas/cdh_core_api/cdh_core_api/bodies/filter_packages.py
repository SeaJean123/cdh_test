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
from dataclasses import dataclass
from typing import List
from typing import Optional

from cdh_core_api.api.validation import field
from cdh_core_api.validation.filter_packages import column_names_field
from cdh_core_api.validation.filter_packages import validate_filter_package_description
from cdh_core_api.validation.filter_packages import validate_filter_package_friendly_name
from cdh_core_api.validation.filter_packages import validate_table_name

from cdh_core.entities.filter_package import ColumnName
from cdh_core.entities.filter_package import RowFilter
from cdh_core.entities.filter_package import TableName


@dataclass(frozen=True)
class NewFilterBody:
    """Represents the attributes needed to build a new filter."""

    tableName: TableName = field(validator=validate_table_name)
    fullAccess: Optional[bool] = field(default=None)
    rowFilter: Optional[RowFilter] = None
    includedColumns: Optional[List[ColumnName]] = column_names_field()
    excludedColumns: Optional[List[ColumnName]] = column_names_field()


@dataclass(frozen=True)
class NewFilterPackageBody:
    """Represents the attributes needed to build a new filter package."""

    description: str = field(validator=validate_filter_package_description)
    friendlyName: str = field(validator=validate_filter_package_friendly_name)
    tableAccess: List[NewFilterBody] = field()
