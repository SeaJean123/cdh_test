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
from typing import Optional

from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def build_database_name() -> DatabaseName:
    return DatabaseName(Builder.build_random_string())


def build_glue_database(
    name: Optional[DatabaseName] = None, account_id: Optional[AccountId] = None, region: Optional[Region] = None
) -> GlueDatabase:
    return GlueDatabase(
        name=name or build_database_name(), account_id=account_id or build_account_id(), region=region or build_region()
    )
