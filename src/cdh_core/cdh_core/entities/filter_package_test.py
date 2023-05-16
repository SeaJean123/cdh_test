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
import uuid
from datetime import datetime
from typing import List
from typing import Optional

from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.filter_package import ColumnName
from cdh_core.entities.filter_package import FilterId
from cdh_core.entities.filter_package import FilterPackage
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.filter_package import RowFilter
from cdh_core.entities.filter_package import TableFilter
from cdh_core.entities.filter_package import TableName
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.glue_database_test import build_database_name
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def build_table_filter(
    filter_id: Optional[FilterId] = None,
    package_id: Optional[PackageId] = None,
    resource_account_id: Optional[AccountId] = None,
    database_name: Optional[DatabaseName] = None,
    table_name: Optional[TableName] = None,
    full_access: Optional[bool] = None,
    row_filter: Optional[RowFilter] = None,
    included_columns: Optional[List[ColumnName]] = None,
    excluded_columns: Optional[List[ColumnName]] = None,
    creation_date: Optional[datetime] = None,
    creator_user_id: Optional[str] = None,
    update_date: Optional[datetime] = None,
) -> TableFilter:
    if full_access is None:
        full_access = Builder.get_random_bool()
    if full_access:
        row_filter = None
        included_columns = None
        excluded_columns = None
    else:
        if row_filter is None:
            row_filter = RowFilter(Builder.build_random_string())
        if included_columns is None:
            if excluded_columns is None:
                excluded_columns = [ColumnName(Builder.build_random_string()) for _ in range(5)]
        else:
            excluded_columns = None

    return TableFilter(
        filter_id=filter_id or FilterId(str(uuid.uuid4())),
        package_id=package_id or PackageId(str(uuid.uuid4())),
        resource_account_id=resource_account_id or build_account_id(),
        database_name=database_name or build_database_name(),
        table_name=table_name or TableName(Builder.build_random_string()),
        full_access=full_access,
        row_filter=row_filter,
        included_columns=included_columns,
        excluded_columns=excluded_columns,
        creation_date=creation_date or datetime.now(),
        creator_user_id=creator_user_id,
        update_date=update_date or datetime.now(),
    )


def build_filter_package(
    id: Optional[PackageId] = None,  # pylint: disable=invalid-name, redefined-builtin
    friendly_name: Optional[str] = None,
    dataset_id: Optional[DatasetId] = None,
    stage: Optional[Stage] = None,
    region: Optional[Region] = None,
    description: Optional[str] = None,
    table_access: Optional[List[TableFilter]] = None,
    hub: Optional[Hub] = None,
    creation_date: Optional[datetime] = None,
    creator_user_id: Optional[str] = None,
    update_date: Optional[datetime] = None,
) -> FilterPackage:
    id = id or PackageId(str(uuid.uuid4()))
    hub = hub or build_hub()
    return FilterPackage(
        id=id,
        friendly_name=friendly_name or Builder.build_random_string(),
        dataset_id=dataset_id or build_dataset(hub=hub).id,
        stage=stage or build_stage(),
        region=region or build_region(hub.partition),
        description=description or Builder.build_random_string(100),
        table_access=table_access or [build_table_filter(package_id=id) for _ in range(3)],
        hub=hub,
        creation_date=creation_date or datetime.now(),
        creator_user_id=creator_user_id,
        update_date=update_date or datetime.now(),
    )
