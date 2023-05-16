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
from dataclasses import field
from datetime import datetime
from typing import List
from typing import NewType
from typing import Optional
from typing import TYPE_CHECKING

from dataclasses_json import config
from marshmallow import fields as mm_fields

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.dates import date_input
from cdh_core.dates import date_output
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_lakeformation.type_defs import DataCellsFilterTypeDef
else:
    DataCellsFilterTypeDef = object


FilterId = NewType("FilterId", str)
PackageId = NewType("PackageId", str)
TableName = NewType("TableName", str)
ColumnName = NewType("ColumnName", str)
RowFilter = NewType("RowFilter", str)


@dataclass(frozen=True)
class TableFilter(DataClassJsonCDHMixin):
    """Table filters contain the information on which parts of a table a consumer can see.

    They correspond to a filter object in Lake Formation.
    """

    filter_id: FilterId
    package_id: PackageId
    resource_account_id: AccountId
    database_name: DatabaseName
    table_name: TableName
    full_access: bool
    row_filter: Optional[RowFilter]
    included_columns: Optional[List[ColumnName]]
    excluded_columns: Optional[List[ColumnName]]
    creation_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )
    creator_user_id: Optional[str]
    update_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )

    @property
    def to_lake_formation_table_filter_resource(self) -> DataCellsFilterTypeDef:
        """Convert table filter to the format used for creating it."""
        resource: DataCellsFilterTypeDef = {
            "Name": self.filter_id,
            "TableCatalogId": self.resource_account_id,
            "DatabaseName": self.database_name,
            "TableName": self.table_name,
            "RowFilter": ({"FilterExpression": self.row_filter} if self.row_filter else {"AllRowsWildcard": {}}),
            "ColumnNames": self.included_columns or [],
        }
        if self.excluded_columns:
            resource["ColumnWildcard"] = {"ExcludedColumnNames": self.excluded_columns}
        return resource


@dataclass(frozen=True)
class FilterPackage(DataClassJsonCDHMixin):
    """Filter packages bundle several filters on tables in the same database.

    They are the base components of the CDH's fine grained access control.
    """

    id: PackageId  # pylint: disable=invalid-name
    friendly_name: str
    dataset_id: DatasetId
    stage: Stage
    region: Region
    description: str
    table_access: List[TableFilter]
    hub: Hub
    creation_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )
    creator_user_id: Optional[str]
    update_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )


@dataclass(frozen=True)
class FilterPackages(DataClassJsonCDHMixin):
    """This class is used to transfer multiple filter packages between the server and client."""

    filter_packages: List[FilterPackage]
