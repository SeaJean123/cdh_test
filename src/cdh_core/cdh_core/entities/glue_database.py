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
from typing import NewType
from typing import TYPE_CHECKING

from cdh_core.entities.arn import Arn
from cdh_core.entities.arn import build_arn_string
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_lakeformation.type_defs import ResourceTypeDef
else:
    ResourceTypeDef = object


DatabaseName = NewType("DatabaseName", str)


@dataclass(frozen=True)
class GlueDatabase:
    """Represents a local glue database or a resource-link to a remote glue database."""

    name: DatabaseName
    account_id: AccountId
    region: Region

    @property
    def arn(self) -> Arn:
        """Return the ARN of the glue database."""
        return Arn(
            build_arn_string(
                service="glue",
                region=self.region,
                account=self.account_id,
                resource=f"database/{self.name}",
                partition=self.region.partition,
            )
        )

    @property
    def tables_arn(self) -> Arn:
        """Return an ARN pattern that matches all tables of the glue database."""
        return Arn(
            build_arn_string(
                service="glue",
                region=self.region,
                account=self.account_id,
                resource=f"table/{self.name}/*",
                partition=self.region.partition,
            )
        )

    @property
    def catalog_arn(self) -> Arn:
        """Return the ARN of the catalog containing the database."""
        return Arn(
            build_arn_string(
                service="glue",
                region=self.region,
                account=self.account_id,
                resource="catalog",
                partition=self.region.partition,
            )
        )

    @property
    def to_lake_formation_database_resource(self) -> ResourceTypeDef:
        """Convert database to the format used for granting and revoking Lake Formation permissions to it."""
        return {"Database": {"Name": self.name, "CatalogId": self.account_id}}

    @property
    def to_lake_formation_tables_resource(self) -> ResourceTypeDef:
        """Convert database to the format used for granting and revoking Lake Formation permissions to its tables."""
        return {"Table": {"DatabaseName": self.name, "CatalogId": self.account_id, "TableWildcard": {}}}
