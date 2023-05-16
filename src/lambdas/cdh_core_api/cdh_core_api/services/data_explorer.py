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
from typing import Any
from typing import Dict

from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.kms_service import KmsService
from cdh_core_api.services.lock_service import LockService

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.entities.arn import Arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.entities.resource import S3Resource
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.primitives.account_id import AccountId


@dataclass(frozen=True)
class DataExplorer:
    """Holds information about the Data Explorer role."""

    role_name: str
    kms_policy_name: str
    kms_statement_sid: str
    bucket_policy_sid: str

    def get_arn(self, partition: Partition, resource_account_id: AccountId) -> str:
        """Get the data explorer role arn."""
        return f"arn:{partition.value}:iam::{resource_account_id}:role/{self.role_name}"


class DataExplorerSync:
    """Manages necessary changes to give the data explorer required access permissions."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        config: Config,
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        datasets_table: DatasetsTable,
        clients: AwsClientFactory,
        lock_service: LockService,
        kms_service: KmsService,
    ) -> None:
        self._resources_table = resources_table
        self._datasets_table = datasets_table
        self._aws_clients_factory = clients
        self._lock_service = lock_service
        self._kms_service = kms_service
        self.data_explorer = DataExplorer(
            role_name=f"{config.prefix}cdh-data-explorer",
            kms_policy_name=f"{config.prefix}data-explorer-provider-kms-access",
            kms_statement_sid="KmsAccessForDepersoBuckets",
            bucket_policy_sid="DataExplorerReadAccess",
        )

    def update_bucket_access_for_data_explorer(self, dataset: Dataset, bucket: S3Resource) -> None:
        """Update bucket policy to give the data explorer access."""
        s3_client = self._aws_clients_factory.s3_client(
            account_id=bucket.resource_account_id, account_purpose=AccountPurpose("resources"), region=bucket.region
        )
        policy = s3_client.get_bucket_policy(bucket.name)
        s3_client.set_bucket_policy(bucket.name, self._generate_new_policy(bucket, policy, dataset.preview_available))

    def update_lake_formation_access_for_data_explorer(self, dataset: Dataset, glue_db: GlueDatabase) -> None:
        """Update bucket policy to give the data explorer access."""
        lake_formation_client = self._aws_clients_factory.lake_formation_client(
            account_id=glue_db.account_id, account_purpose=AccountPurpose("resources"), region=glue_db.region
        )
        data_explorer_arn = Arn(
            self.data_explorer.get_arn(partition=glue_db.region.partition, resource_account_id=glue_db.account_id)
        )
        if dataset.preview_available:
            lake_formation_client.grant_read_access_for_database(
                principal=data_explorer_arn, database=glue_db, grantable=False
            )
        else:
            lake_formation_client.revoke_read_access_for_database(
                principal=data_explorer_arn, database=glue_db, grantable=False, fail_if_missing=False
            )

    def _generate_new_policy(
        self, bucket: S3Resource, policy: PolicyDocument, preview_available: bool
    ) -> PolicyDocument:
        if not preview_available:
            return policy.delete_statement_if_present(self.data_explorer.bucket_policy_sid)
        return policy.add_or_update_statement(self._get_bucket_policy_statement_for_data_explorer(bucket))

    def _get_bucket_policy_statement_for_data_explorer(self, bucket: S3Resource) -> Dict[str, Any]:
        return {
            "Sid": self.data_explorer.bucket_policy_sid,
            "Effect": "Allow",
            "Principal": {"AWS": self.data_explorer.get_arn(bucket.region.partition, bucket.resource_account_id)},
            "Action": ["s3:Get*", "s3:List*"],
            "Resource": [f"{bucket.arn}", f"{bucket.arn}/*"],
        }
