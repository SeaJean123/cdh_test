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
import re
from logging import getLogger
from typing import Callable
from typing import Generic
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.catalog.base import LastEvaluatedKey
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.filter_packages_table import FilterPackageNotFound
from cdh_core_api.catalog.filter_packages_table import FilterPackagesTable
from cdh_core_api.catalog.resource_table import GenericResourceModel
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.visibility_check import VisibilityCheck

from cdh_core.entities.accounts import Account
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.filter_package import FilterPackage
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.resource import Resource
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import InternalError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)
BUCKET_NAME_REGEX = re.compile(r"(?:cdhx...)?cdh-(?P<dataset_part>[a-z0-9-_]*)-[a-z0-9]{4}")

ItemT = TypeVar("ItemT", bound=Union[Resource, Account, Dataset])


class VisibleDataLoader(Generic[GenericAccount, GenericS3Resource, GenericGlueSyncResource]):
    """Loads data from the catalog subject to visibility constraints."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        accounts_table: GenericAccountsTable[GenericAccount],
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        datasets_table: DatasetsTable,
        filter_packages_table: FilterPackagesTable,
        visibility_check: VisibilityCheck,
    ):
        self._resources_table = resources_table
        self._datasets_table = datasets_table
        self._accounts_table = accounts_table
        self._filter_packages_table = filter_packages_table
        self._visibility_check = visibility_check

    def get_account(self, account_id: AccountId) -> GenericAccount:
        """Get a single account, if visible."""
        account = self._accounts_table.get(account_id)
        if self._visibility_check.get_account_visibility_check(batch=False)(account):
            return account
        raise AccountNotFound(account_id)

    def get_accounts(
        self,
        limit: int,
        last_evaluated_key: Optional[LastEvaluatedKey],
    ) -> Tuple[Sequence[GenericAccount], Optional[LastEvaluatedKey]]:
        """Get all visible accounts."""
        accounts_iterator = self._accounts_table.get_accounts_iterator(
            last_evaluated_key=last_evaluated_key,
            consistent_read=False,
        )
        items = self._fill_from_iterator(
            iterator=accounts_iterator,
            is_visible=self._visibility_check.get_account_visibility_check(batch=True),
            limit=limit,
        )
        return items, accounts_iterator.last_evaluated_key

    def get_dataset(self, dataset_id: DatasetId) -> Dataset:
        """Get a single dataset, if visible."""
        if self._visibility_check.get_dataset_id_visibility_check(batch=False)(dataset_id):
            return self._datasets_table.get(dataset_id)
        raise DatasetNotFound(dataset_id)

    def get_filter_package(
        self, dataset_id: DatasetId, stage: Stage, region: Region, package_id: PackageId
    ) -> FilterPackage:
        """Get a single filter package, if the dataset is visible."""
        if self._visibility_check.get_dataset_id_visibility_check(batch=False)(dataset_id):
            return self._filter_packages_table.get(
                dataset_id=dataset_id, stage=stage, region=region, package_id=package_id
            )
        raise FilterPackageNotFound(f"{dataset_id}_{stage.value}_{region.value}", package_id)

    def get_filter_packages(self, dataset_id: DatasetId, stage: Stage, region: Region) -> List[FilterPackage]:
        """Get all filter packages, if the dataset is visible."""
        if self._visibility_check.get_dataset_id_visibility_check(batch=False)(dataset_id):
            return self._filter_packages_table.list(
                dataset_id=dataset_id, stage=stage, region=region, consistent_read=False
            )
        return []

    def get_datasets(
        self, hub: Hub, limit: int, last_evaluated_key: Optional[LastEvaluatedKey]
    ) -> Tuple[List[Dataset], Optional[LastEvaluatedKey]]:
        """Get all visible datasets within a hub.

        Depending on the specified `limit`, the response may be truncated and contain a `LastEvaluatedKey`.
        """
        datasets_iterator = self._datasets_table.get_datasets_iterator(
            hub=hub,
            last_evaluated_key=last_evaluated_key,
            consistent_read=False,
        )
        items = self._fill_from_iterator(
            iterator=datasets_iterator,
            is_visible=self._visibility_check.get_dataset_visibility_check(batch=True, hub=hub),
            limit=limit,
        )
        return items, datasets_iterator.last_evaluated_key

    def get_datasets_cross_hub(self, dataset_ids: List[DatasetId]) -> List[Dataset]:
        """Get all visible datasets among a specific list."""
        visibility_check = self._visibility_check.get_dataset_id_visibility_check(batch=True, dataset_ids=dataset_ids)
        return self._datasets_table.batch_get(
            [dataset_id for dataset_id in dataset_ids if visibility_check(dataset_id)]
        )

    def get_resource(
        self, resource_type: ResourceType, dataset_id: DatasetId, stage: Stage, region: Region
    ) -> Union[GenericS3Resource, GenericGlueSyncResource]:
        """Get a single dataset, if visible."""
        # a resource is considered visible if the dataset is visible
        if self._visibility_check.get_dataset_id_visibility_check(batch=False)(dataset_id):
            return self._resources_table.get(
                resource_type=resource_type,
                dataset_id=dataset_id,
                stage=stage,
                region=region,
            )
        range_key = GenericResourceModel.get_range_key(resource_type=resource_type, stage=stage, region=region)
        raise ResourceNotFound(dataset_id, range_key)

    def get_dataset_id_from_bucket_name(self, bucket_name: str) -> Optional[str]:
        """Get dataset ID for a bucket that corresponds to an S3 resource."""
        match = BUCKET_NAME_REGEX.fullmatch(bucket_name)
        if match:
            return match.group("dataset_part").replace("-", "_")
        return None

    def find_s3_resource_in_dataset_by_bucket_name(
        self, dataset_id: str, bucket_name: str
    ) -> Optional[GenericS3Resource]:
        """Find S3 resource a given bucket name belongs to in a specific dataset."""
        if self._visibility_check.get_dataset_id_visibility_check(batch=False)(DatasetId(dataset_id)):
            candidates = [r for r in self._resources_table.list_s3(dataset_id=dataset_id) if r.name == bucket_name]
            if len(candidates) > 1:
                raise InternalError(f"Found multiple resources with bucket name {bucket_name}")
            if len(candidates) == 1:
                return candidates[0]
        return None

    def get_resource_from_bucket_name(self, bucket_name: str) -> GenericS3Resource:
        """Find S3 resource a given bucket belongs to."""
        if dataset_id := self.get_dataset_id_from_bucket_name(bucket_name):
            if s3_resource := self.find_s3_resource_in_dataset_by_bucket_name(dataset_id, bucket_name):
                return s3_resource
        raise NotFoundError(f"Unable to find a resource for bucket name {bucket_name}.")

    def get_resources(  # pylint: disable=too-many-arguments
        self,
        hub: Hub,
        limit: int,
        last_evaluated_key: Optional[LastEvaluatedKey],
        dataset_id: Optional[str] = None,
        stage: Optional[Stage] = None,
        region: Optional[Region] = None,
        resource_account: Optional[AccountId] = None,
        resource_type: Optional[ResourceType] = None,
    ) -> Tuple[Sequence[Union[GenericS3Resource, GenericGlueSyncResource]], Optional[LastEvaluatedKey]]:
        """Get all visible resources that satisfy the specified conditions.

        Depending on the specified `limit`, the response may be truncated and contain a `LastEvaluatedKey`.
        """
        resources_iterator = self._resources_table.get_resources_iterator(
            hub=hub,
            dataset_id=dataset_id,
            stage=stage,
            region=region,
            resource_account=resource_account,
            resource_type=resource_type,
            last_evaluated_key=last_evaluated_key,
            consistent_read=False,
        )
        items = self._fill_from_iterator(
            iterator=resources_iterator,
            is_visible=self._visibility_check.get_resource_visibility_check(batch=True, hub=hub),
            limit=limit,
        )
        return items, resources_iterator.last_evaluated_key

    def get_hubs(self) -> List[Hub]:
        """Get all visible hubs."""
        visibility_check = self._visibility_check.get_hub_visibility_check(batch=True)
        return [hub for hub in Hub if visibility_check(hub)]

    @staticmethod
    def _fill_from_iterator(iterator: Iterator[ItemT], is_visible: Callable[[ItemT], bool], limit: int) -> List[ItemT]:
        items: List[ItemT] = []
        for item in iterator:
            if is_visible(item):
                items.append(item)
            if len(items) >= limit:
                break
        return items
