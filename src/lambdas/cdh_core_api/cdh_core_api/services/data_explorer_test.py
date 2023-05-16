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
from dataclasses import replace
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from unittest.mock import Mock

from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.kms_service import KmsService
from cdh_core_api.services.lock_service import LockService

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.lakeformation_client import LakeFormationClient
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.s3_client import S3Client
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_kms_key_arn
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.glue_database_test import build_glue_database
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.hubs_test import build_hub
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class DataExplorerTestBase:
    def setup_method(self) -> None:
        self.config = build_config()
        self.datasets_table = Mock(DatasetsTable)
        self.lock_service = Mock(LockService)
        self.kms_service = Mock(KmsService)
        self.resources_table = Mock(GenericResourcesTable)
        self.aws = Mock(spec=AwsClientFactory)
        self.data_explorer_sync = DataExplorerSync(
            config=self.config,
            resources_table=self.resources_table,
            datasets_table=self.datasets_table,
            clients=self.aws,
            lock_service=self.lock_service,
            kms_service=self.kms_service,
        )
        self.s3_client = Mock(spec=S3Client)
        self.aws.s3_client.return_value = self.s3_client
        self.lake_formation_client = Mock(spec=LakeFormationClient)
        self.aws.lake_formation_client.return_value = self.lake_formation_client
        self.datasets: Dict[str, Any] = {}
        self.datasets_table.get.side_effect = self.datasets.get
        self.kms_key_arn = build_kms_key_arn()

    def _build_dataset_bucket_pair(
        self,
        preview_available: bool,
        kms_key_arn: Optional[Arn] = None,
    ) -> Tuple[Dataset, S3Resource]:
        dataset = build_dataset(
            name=Builder.build_random_string(), preview_available=preview_available, hub=build_hub()
        )
        self.datasets[dataset.id] = dataset
        bucket = build_s3_resource(
            dataset=dataset,
            resource_account_id=build_resource_account().id,
            region=build_region(),
            kms_key_arn=kms_key_arn if kms_key_arn else self.kms_key_arn,
        )
        return dataset, bucket


class TestDataExplorerUpdateBucketAccess(DataExplorerTestBase):
    def test_without_preview(self) -> None:
        existing_bucket_policy = PolicyDocument.create_bucket_policy([])
        self.s3_client.get_bucket_policy.return_value = existing_bucket_policy
        dataset, bucket = self._build_dataset_bucket_pair(preview_available=False)

        self.data_explorer_sync.update_bucket_access_for_data_explorer(dataset, bucket)
        assert not self.datasets_table.method_calls
        assert not self.resources_table.method_calls
        self.s3_client.set_bucket_policy.assert_called_once()
        bucket_name, policy = self.s3_client.set_bucket_policy.call_args[0]
        assert bucket_name == bucket.name
        assert not policy.has_statement_with_sid("DataExplorerReadAccess")

    def test_with_preview(self) -> None:
        key = build_kms_key_arn()
        dataset, bucket = self._build_dataset_bucket_pair(preview_available=True, kms_key_arn=key)
        sid = Builder.build_random_string()
        bucket_policy = PolicyDocument.create_bucket_policy(
            [
                {
                    "Sid": sid,
                    "Effect": "Allow",
                    "Action": ["s3:Get*"],
                    "Principal": {"AWS": str(build_role_arn())},
                    "Resource": [str(bucket.arn)],
                }
            ]
        )
        self.s3_client.get_bucket_policy.return_value = bucket_policy

        self.data_explorer_sync.update_bucket_access_for_data_explorer(dataset, bucket)
        self.s3_client.set_bucket_policy.assert_called_once()
        bucket_name, policy = self.s3_client.set_bucket_policy.call_args[0]
        assert bucket_name == bucket.name
        assert policy.has_statement_with_sid(sid)
        data_explorer_statement = policy.get_policy_statement_by_sid(
            self.data_explorer_sync.data_explorer.bucket_policy_sid
        )
        assert data_explorer_statement["Principal"]["AWS"] == self.data_explorer_sync.data_explorer.get_arn(
            bucket.region.partition, bucket.resource_account_id
        )
        assert data_explorer_statement["Resource"] == [f"{bucket.arn}", f"{bucket.arn}/*"]

    def test_update_grants_access(self) -> None:
        key = build_kms_key_arn()
        dataset, bucket = self._build_dataset_bucket_pair(preview_available=False, kms_key_arn=key)
        bucket_policy = PolicyDocument.create_bucket_policy([])
        self.s3_client.get_bucket_policy.return_value = bucket_policy

        self.data_explorer_sync.update_bucket_access_for_data_explorer(dataset, bucket)
        self.data_explorer_sync.update_bucket_access_for_data_explorer(replace(dataset, preview_available=True), bucket)

        bucket_name, policy = self.s3_client.set_bucket_policy.call_args[0]
        assert bucket_name == bucket.name
        assert policy.has_statement_with_sid("DataExplorerReadAccess")

    def test_update_revokes_access(self) -> None:
        key = build_kms_key_arn()
        dataset, bucket = self._build_dataset_bucket_pair(preview_available=True, kms_key_arn=key)
        bucket_policy = PolicyDocument.create_bucket_policy([])
        self.s3_client.get_bucket_policy.return_value = bucket_policy

        self.data_explorer_sync.update_bucket_access_for_data_explorer(dataset, bucket)
        self.data_explorer_sync.update_bucket_access_for_data_explorer(
            replace(dataset, preview_available=False), bucket
        )

        bucket_name, policy = self.s3_client.set_bucket_policy.call_args[0]
        assert bucket_name == bucket.name
        assert not policy.has_statement_with_sid("DataExplorerReadAccess")


class TestDataExplorerUpdateLakeFormationAccess(DataExplorerTestBase):
    def test_with_preview(self) -> None:
        dataset = build_dataset(name=Builder.build_random_string(), preview_available=True, hub=build_hub())
        database_account_id = build_account_id()
        partition = build_partition()
        database = build_glue_database(account_id=database_account_id, region=build_region(partition))
        data_explorer_arn = Arn(self.data_explorer_sync.data_explorer.get_arn(partition, database_account_id))

        self.data_explorer_sync.update_lake_formation_access_for_data_explorer(dataset, database)

        self.lake_formation_client.grant_read_access_for_database.assert_called_once_with(
            principal=data_explorer_arn, database=database, grantable=False
        )

    def test_without_preview(self) -> None:
        dataset = build_dataset(name=Builder.build_random_string(), preview_available=False, hub=build_hub())
        database_account_id = build_account_id()
        partition = build_partition()
        database = build_glue_database(account_id=database_account_id, region=build_region(partition))
        data_explorer_arn = Arn(self.data_explorer_sync.data_explorer.get_arn(partition, database_account_id))

        self.data_explorer_sync.update_lake_formation_access_for_data_explorer(dataset, database)

        self.lake_formation_client.revoke_read_access_for_database.assert_called_once_with(
            principal=data_explorer_arn, database=database, grantable=False, fail_if_missing=False
        )
