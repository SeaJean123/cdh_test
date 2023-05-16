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
from contextlib import ExitStack
from datetime import datetime
from logging import getLogger
from typing import Generic
from typing import List
from typing import Set
from typing import Tuple
from typing import Type

from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.kms_service import KmsService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.lock_service import ResourceIsLocked
from cdh_core_api.services.s3_bucket_manager import S3BucketManager
from cdh_core_api.services.s3_bucket_manager import S3ResourceSpecification
from cdh_core_api.services.sns_topic_manager import SnsTopicManager

from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.aws_clients.s3_client import BucketNotEmpty
from cdh_core.aws_clients.sns_client import SnsTopic
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.dataset import Dataset
from cdh_core.enums.aws import Region
from cdh_core.enums.locking import LockingScope
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)


class S3ResourceManager(Generic[GenericS3Resource]):
    """Handles S3 resources."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        datasets_table: DatasetsTable,
        config: Config,
        s3_bucket_manager: S3BucketManager,
        kms_service: KmsService,
        sns_topic_manager: SnsTopicManager,
        lock_service: LockService,
        s3_resource_type: Type[GenericS3Resource],
        data_explorer_sync: DataExplorerSync,
    ):
        self._resources_table = resources_table
        self._datasets_table = datasets_table
        self._config = config
        self._s3_bucket_manager = s3_bucket_manager
        self._sns_topic_manager = sns_topic_manager
        self._lock_service = lock_service
        self._kms_service = kms_service
        self._s3_resource_type = s3_resource_type
        self._data_explorer_sync = data_explorer_sync

    def create_bucket(  # pylint: disable=too-many-arguments, too-many-locals
        self,
        dataset: Dataset,
        stage: Stage,
        region: Region,
        resource_account: ResourceAccount,
        owner_account_id: AccountId,
        user: str,
    ) -> GenericS3Resource:
        """Create and return an s3 resource for the given parameters, if it does not exist and is not locked."""
        if self._resources_table.exists(ResourceType.s3, dataset.id, stage, region):
            raise ConflictError(
                f"Dataset {dataset.id} already contains an S3 bucket for stage {stage.value} and region {region.value}"
            )

        lock = self._lock_service.acquire_lock(
            item_id=dataset.id,
            scope=LockingScope.s3_resource,
            region=region,
            stage=stage,
            data={"datasetId": dataset.id},
        )
        specification = S3ResourceSpecification(
            dataset=dataset,
            stage=stage,
            region=region,
            resource_account_id=resource_account.id,
            owner_id=owner_account_id,
        )
        kms_key = self._kms_service.get_shared_key(resource_account=resource_account, region=specification.region)
        kms_readers, kms_writers = self._get_reader_and_writer_account_ids(
            kms_key=kms_key, resource_account_id=resource_account.id
        )

        kms_writers.add(specification.owner_id)
        try:
            self._kms_service.regenerate_key_policy(
                kms_key=kms_key,
                resource_account=resource_account,
                account_ids_with_read_access=kms_readers,
                account_ids_with_write_access=kms_writers,
            )
        except ResourceIsLocked:
            # nothing was changed in this case so we can release the s3 lock
            self._lock_service.release_lock(lock)
            raise
        bucket_arn = self._s3_bucket_manager.create_bucket(spec=specification, kms_key=kms_key)
        topic = self._sns_topic_manager.create_topic(
            specification, topic_name=bucket_arn.identifier, kms_key_arn=kms_key.arn
        )
        self._s3_bucket_manager.link_to_s3_attribute_extractor_lambda(bucket_arn.identifier, topic)

        now = datetime.now()
        s3_resource = self._s3_resource_type(
            dataset_id=dataset.id,
            resource_account_id=specification.resource_account_id,
            hub=dataset.hub,
            arn=bucket_arn,
            creator_user_id=user,
            creation_date=now,
            region=region,
            stage=stage,
            sns_topic_arn=topic.arn,
            kms_key_arn=kms_key.arn,
            update_date=now,
            owner_account_id=owner_account_id,
        )
        self._resources_table.create(s3_resource)
        self._data_explorer_sync.update_bucket_access_for_data_explorer(dataset, s3_resource)
        self._lock_service.release_lock(lock)
        return s3_resource

    def delete_bucket(self, s3_resource: GenericS3Resource) -> None:
        """Delete an s3 resource and the associated bucket."""
        lock = self._lock_service.acquire_lock(
            item_id=s3_resource.dataset_id,
            scope=LockingScope.s3_resource,
            region=s3_resource.region,
            stage=s3_resource.stage,
            data=s3_resource.to_payload().to_plain_dict(),
        )
        try:
            self._s3_bucket_manager.delete_bucket(
                account_id=s3_resource.resource_account_id, region=s3_resource.region, bucket_name=s3_resource.name
            )
        except BucketNotEmpty as err:
            self._lock_service.release_lock(lock)
            raise ForbiddenError(f"S3 bucket {s3_resource.name} cannot be deleted as it is not empty.") from err

        self._resources_table.delete(
            resource_type=ResourceType.s3,
            dataset_id=s3_resource.dataset_id,
            stage=s3_resource.stage,
            region=s3_resource.region,
        )
        self._sns_topic_manager.delete_topic(topic_arn=s3_resource.sns_topic_arn)
        self._lock_service.release_lock(lock)

        resource_account = self._config.account_store.query_resource_account(
            account_ids=s3_resource.resource_account_id,
            environments=self._config.environment,
        )
        kms_key = KmsKey.parse_from_arn(s3_resource.kms_key_arn)
        kms_readers, kms_writers = self._get_reader_and_writer_account_ids(
            kms_key=kms_key, resource_account_id=resource_account.id
        )
        self._kms_service.regenerate_key_policy(
            kms_key=kms_key,
            resource_account=resource_account,
            account_ids_with_read_access=kms_readers,
            account_ids_with_write_access=kms_writers,
        )

    def update_bucket_read_access(
        self,
        s3_resource: GenericS3Resource,
        dataset: Dataset,
        resource_account: ResourceAccount,
    ) -> None:
        """Update the read access to the bucket associated with the given s3 resource."""
        account_ids_with_read_access = dataset.get_account_ids_with_read_access(
            stage=s3_resource.stage, region=s3_resource.region
        )
        sns_topic = SnsTopic(
            name=s3_resource.sns_topic_arn.identifier,
            arn=s3_resource.sns_topic_arn,
            region=s3_resource.region,
        )
        kms_key: KmsKey = KmsKey.parse_from_arn(s3_resource.kms_key_arn)
        kms_readers, kms_writers = self._get_reader_and_writer_account_ids(
            kms_key=kms_key, resource_account_id=resource_account.id
        )
        with ExitStack() as stack:
            stack.enter_context(
                self._s3_bucket_manager.update_bucket_policy_read_access_statement_transaction(
                    s3_resource=s3_resource, account_ids_with_read_access=account_ids_with_read_access
                )
            )
            stack.enter_context(
                self._sns_topic_manager.update_policy_transaction(
                    topic=sns_topic,
                    owner_account_id=s3_resource.owner_account_id,
                    account_ids_with_read_access=sorted(list(account_ids_with_read_access)),
                )
            )
            self._kms_service.regenerate_key_policy(
                kms_key=kms_key,
                resource_account=resource_account,
                account_ids_with_read_access=kms_readers,
                account_ids_with_write_access=kms_writers,
            )
            return

    def _get_reader_and_writer_account_ids(
        self, kms_key: KmsKey, resource_account_id: AccountId
    ) -> Tuple[Set[AccountId], Set[AccountId]]:
        s3_resources = self._resources_table.list_s3(resource_account=resource_account_id, region=kms_key.region)

        return self._get_reader_and_writer_account_ids_for_s3_resources(s3_resources)

    def _get_reader_and_writer_account_ids_for_s3_resources(
        self, s3_resources: List[GenericS3Resource]
    ) -> Tuple[Set[AccountId], Set[AccountId]]:
        account_ids_with_read_access_combined: Set[AccountId] = set()
        account_ids_with_write_access_combined: Set[AccountId] = set()

        datasets_dict = {dataset.id: dataset for dataset in self._datasets_table.list()}
        for s3_resource in s3_resources:
            account_ids_with_write_access_combined.update(
                {s3_resource.owner_account_id, s3_resource.resource_account_id}
            )
            try:
                dataset = datasets_dict[s3_resource.dataset_id]
            except KeyError:
                LOG.error(f"No dataset found for s3 resource {s3_resource}")
                continue

            account_ids_with_read_access_combined.update(
                dataset.get_account_ids_with_read_access(stage=s3_resource.stage, region=s3_resource.region)
            )
        return account_ids_with_read_access_combined, account_ids_with_write_access_combined
