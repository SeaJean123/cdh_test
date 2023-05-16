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
from typing import List

from cdh_core_api.catalog.base import BaseTable
from cdh_core_api.catalog.base import create_model
from cdh_core_api.catalog.base import DateTimeAttribute
from cdh_core_api.catalog.base import LazyEnumAttribute
from pynamodb.attributes import BooleanAttribute
from pynamodb.attributes import ListAttribute
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.models import Model

from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.filter_package import ColumnName
from cdh_core.entities.filter_package import FilterId
from cdh_core.entities.filter_package import FilterPackage
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.filter_package import RowFilter
from cdh_core.entities.filter_package import TableFilter
from cdh_core.entities.filter_package import TableName
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId


class _TableFilterAttribute(MapAttribute[str, str]):
    filter_id: UnicodeAttribute = UnicodeAttribute()
    package_id: UnicodeAttribute = UnicodeAttribute()
    resource_account_id: UnicodeAttribute = UnicodeAttribute()
    database_name: UnicodeAttribute = UnicodeAttribute()
    table_name: UnicodeAttribute = UnicodeAttribute()
    full_access: BooleanAttribute = BooleanAttribute()
    row_filter: UnicodeAttribute = UnicodeAttribute(null=True)
    included_columns: ListAttribute[UnicodeAttribute] = ListAttribute(of=UnicodeAttribute, null=True)
    excluded_columns: ListAttribute[UnicodeAttribute] = ListAttribute(of=UnicodeAttribute, null=True)
    creation_date = DateTimeAttribute()
    update_date = DateTimeAttribute()
    creator_user_id = UnicodeAttribute(null=True)

    @property
    def table_filter(self) -> TableFilter:
        return TableFilter(
            filter_id=FilterId(self.filter_id),
            package_id=PackageId(self.package_id),
            resource_account_id=AccountId(self.resource_account_id),
            database_name=DatabaseName(self.database_name),
            table_name=TableName(self.table_name),
            full_access=self.full_access,
            row_filter=RowFilter(self.row_filter),
            included_columns=[ColumnName(column) for column in self.included_columns]  # type: ignore
            if self.included_columns is not None
            else None,
            excluded_columns=[ColumnName(column) for column in self.excluded_columns]  # type: ignore
            if self.excluded_columns is not None
            else None,
            creation_date=self.creation_date,
            creator_user_id=self.creator_user_id,
            update_date=self.update_date,
        )


class _FilterPackageModel(Model):
    datasetid_stage_region = UnicodeAttribute(hash_key=True)
    id = UnicodeAttribute(range_key=True)
    friendly_name = UnicodeAttribute()
    dataset_id = UnicodeAttribute()
    stage = LazyEnumAttribute[Stage](lambda: Stage)
    region = LazyEnumAttribute[Region](lambda: Region)
    description = UnicodeAttribute()
    table_access: ListAttribute[_TableFilterAttribute] = ListAttribute(of=_TableFilterAttribute)
    hub = LazyEnumAttribute[Hub](lambda: Hub)
    creation_date = DateTimeAttribute()
    update_date = DateTimeAttribute()
    creator_user_id = UnicodeAttribute(null=True)

    @property
    def filter_package(self) -> FilterPackage:
        return FilterPackage(
            id=PackageId(self.id),
            friendly_name=self.friendly_name,
            dataset_id=DatasetId(self.dataset_id),
            stage=self.stage,
            region=self.region,
            description=self.description,
            table_access=[table_filter.table_filter for table_filter in self.table_access],
            hub=self.hub,
            creation_date=self.creation_date,
            update_date=self.update_date,
            creator_user_id=self.creator_user_id,
        )


# pylint: disable=no-member
class FilterPackagesTable(BaseTable):
    """Represents the DynamoDB table for filter packages."""

    def __init__(self, prefix: str = "") -> None:
        self._model = create_model(f"{prefix}cdh-filter-packages", model=_FilterPackageModel, module=__name__)

    def get(self, dataset_id: DatasetId, stage: Stage, region: Region, package_id: PackageId) -> FilterPackage:
        """Return a single filter package."""
        hash_key = self._get_hash_key(dataset_id, stage, region)
        return self._get_by_range_key(hash_key, package_id).filter_package

    def _get_by_range_key(self, hash_key: str, package_id: PackageId) -> _FilterPackageModel:
        try:
            return self._model.get(hash_key=hash_key, range_key=package_id, consistent_read=True)
        except DoesNotExist as error:
            raise FilterPackageNotFound(hash_key, package_id) from error

    def list(
        self, dataset_id: DatasetId, stage: Stage, region: Region, consistent_read: bool = True
    ) -> List[FilterPackage]:
        """List all filter packages for a given dataset_id/stage/region combination."""
        return [
            model.filter_package
            for model in self._model.query(
                hash_key=self._get_hash_key(dataset_id, stage, region), consistent_read=consistent_read
            )
        ]

    @staticmethod
    def _get_hash_key(dataset_id: DatasetId, stage: Stage, region: Region) -> str:
        """Generate Dynamo hash key."""
        return f"{dataset_id}_{stage.value}_{region.value}"


class FilterPackageNotFound(Exception):
    """The filter package was not found."""

    def __init__(self, hash_key: str, package_id: PackageId):
        super().__init__(f"Package {package_id} was not found in {hash_key}")
