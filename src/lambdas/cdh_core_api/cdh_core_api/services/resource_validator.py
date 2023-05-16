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
from contextlib import suppress
from typing import cast
from typing import Optional
from typing import Type

from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.catalog.filter_packages_table import FilterPackagesTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.generic_types import GenericUpdateAccountBody
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.metadata_role_assumer import GenericAssumableAccountSpec
from cdh_core_api.services.utils import fetch_dataset
from cdh_core_api.services.utils import fetch_resource
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.account_store import QueryAccountNotFound
from cdh_core.entities.accounts import ResourceAccount
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.resource import Resource
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import BadRequestError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.primitives.account_id import AccountId


class ResourceValidator:
    """Validator for resources."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        accounts_table: GenericAccountsTable[GenericAccount],
        assumable_account_spec_cls: Type[GenericAssumableAccountSpec[GenericAccount, GenericUpdateAccountBody]],
        config: Config,
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        authorizer: Authorizer[GenericAccount],
        requester_identity: RequesterIdentity,
        visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
        filter_packages_table: FilterPackagesTable,
    ):
        self._accounts_table = accounts_table
        self._assumable_account_spec_cls = assumable_account_spec_cls
        self._config = config
        self._resources_table = resources_table
        self._authorizer = authorizer
        self._requester_identity = requester_identity
        self._visible_data_loader = visible_data_loader
        self._filter_packages_table = filter_packages_table

    def check_dataset_visible(self, hub: Hub, dataset_id: DatasetId) -> Dataset:
        """Return the dataset the resource should be associated to, if it is visible."""
        return fetch_dataset(hub=hub, dataset_id=dataset_id, visible_data_loader=self._visible_data_loader)

    def check_may_create_resource(  # pylint: disable=too-many-arguments
        self, dataset: Dataset, stage: Stage, region: Region, owner_account_id: AccountId, resource_type: ResourceType
    ) -> None:
        """Check that the requester is allowed to create the resource."""
        self._authorizer.check_requester_may_create_resource(
            dataset=dataset,
            stage=stage,
            region=region,
            resource_type=resource_type,
            owner_account_id=owner_account_id,
        )

    def check_glue_sync_resource_requirements(  # pylint: disable=too-many-arguments
        self,
        dataset: Dataset,
        stage: Stage,
        region: Region,
        owner_account_id: AccountId,
        sync_type: Optional[SyncType],
        partition: Partition,
    ) -> None:
        """Check requirements for new glue sync resources."""
        self._check_metadata_role_supported_by_provider(owner_account_id)
        self._check_sync_type_requirements(
            dataset=dataset, stage=stage, region=region, sync_type=sync_type, partition=partition
        )

    def _check_metadata_role_supported_by_provider(self, owner_account_id: AccountId) -> None:
        owner_account = self._accounts_table.get(owner_account_id)
        owner_account_spec = self._assumable_account_spec_cls.from_account(owner_account)
        if not owner_account_spec.supports_metadata_role():
            raise ForbiddenError(
                f"Account {owner_account_id} does not support metadata synchronisation and therefore cannot own "
                f"{ResourceType.glue_sync.value} resources."
            )

    def _check_sync_type_requirements(  # pylint: disable=too-many-arguments
        self, dataset: Dataset, stage: Stage, region: Region, sync_type: Optional[SyncType], partition: Partition
    ) -> None:
        permissions = dataset.filter_permissions(stage=stage, region=region)

        self._check_for_legacy_sync_type_and_partition(sync_type, partition)

        if sync_type is SyncType.lake_formation:
            if not self._config.environment.is_test_environment:
                raise ForbiddenError(f"syncType {SyncType.lake_formation.value} is not supported yet.")

            if permissions:
                raise ForbiddenError(
                    f"Cannot create glue resource of syncType {SyncType.lake_formation.value} as there are existing "
                    f"permissions for this stage and region. Please remove them first."
                )
        else:
            if any(permission.sync_type is SyncType.lake_formation for permission in permissions):
                raise ForbiddenError(
                    f"Can not create glue resource as there are existing permissions for this stage and region with "
                    f"syncType {SyncType.lake_formation.value} which is incompatible. Please remove them first."
                )

    def _check_for_legacy_sync_type_and_partition(
        self, sync_type: Optional[SyncType], partition: Partition  # pylint: disable=unused-argument
    ) -> None:
        if sync_type is SyncType.glue_sync:
            raise ForbiddenError(f"syncType {SyncType.glue_sync.value} is no longer supported.")

    def determine_account_for_new_resource(  # pylint: disable=too-many-arguments
        self,
        dataset: Dataset,
        hub: Hub,
        stage: Stage,
        region: Region,
        resource_type: ResourceType,
    ) -> ResourceAccount:
        """Make sure new resources reside in the correct resource account.

        An s3 resource must be created before the respective glue resource in the same stage and region. Take the
        current default resource account for the given hub, stage, and region.
        A glue resource must be created in the same resource account as the s3 resource with that stage and region.
        """
        if resource_type is ResourceType.s3:
            try:
                self._resources_table.get_glue_sync(dataset_id=dataset.id, region=region, stage=stage)
            except ResourceNotFound:
                return self._query_resource_account_for_new_resource(hub, stage)
            raise ForbiddenError(
                "Cannot create s3 resource with existing glue resource. Please delete the glue resource first."
            )
        try:
            s3_resource = self._resources_table.get_s3(dataset_id=dataset.id, region=region, stage=stage)
        except ResourceNotFound as err:
            raise ForbiddenError(
                "Cannot create glue resource without s3 resource. Please create a s3 resource first."
            ) from err
        return self._config.account_store.query_resource_account(
            account_ids=s3_resource.resource_account_id, environments=self._config.environment
        )

    def _query_resource_account_for_new_resource(self, hub: Hub, stage: Stage) -> ResourceAccount:
        try:
            return self._config.account_store.query_resource_account(
                hubs=hub, stages=stage, environments=self._config.environment, only_default=True
            )
        except QueryAccountNotFound as err:
            raise BadRequestError(
                f"Creating resources in hub {hub.value!r} and stage {stage.value!r} is not possible "
                f"in environment {self._config.environment.value!r}."
            ) from err

    def check_may_delete_s3_resource(
        self, dataset: Dataset, stage: Stage, region: Region
    ) -> GenericS3Resource:  # type: ignore
        """Make sure the requester is allowed to delete an s3 resource."""
        s3_resource = cast(
            GenericS3Resource,
            self._check_may_delete_resource(dataset=dataset, resource_type=ResourceType.s3, stage=stage, region=region),
        )
        if readers := dataset.get_account_ids_with_read_access(stage=s3_resource.stage, region=s3_resource.region):
            raise ForbiddenError(
                f"Accounts {list(readers)} have read access. " "Revoke their access before deleting the resource."
            )

        with suppress(ResourceNotFound):
            self._resources_table.get_glue_sync(dataset_id=dataset.id, region=region, stage=stage)
            raise ForbiddenError(
                "Cannot delete s3 resource with existing glue resource. Please delete the glue resource first."
            )

        return s3_resource

    def check_may_delete_glue_sync_resource(
        self, dataset: Dataset, stage: Stage, region: Region
    ) -> GenericGlueSyncResource:  # type: ignore
        """Make sure the requester is allowed to delete a glue-sync resource."""
        return cast(
            GenericGlueSyncResource,
            self._check_may_delete_resource(
                dataset=dataset, resource_type=ResourceType.glue_sync, stage=stage, region=region
            ),
        )

    def _check_may_delete_resource(
        self, dataset: Dataset, resource_type: ResourceType, stage: Stage, region: Region
    ) -> Resource:
        resource = fetch_resource(
            hub=dataset.hub,
            dataset_id=dataset.id,
            resource_type=resource_type,
            stage=stage,
            region=region,
            visible_data_loader=self._visible_data_loader,
        )
        self._authorizer.check_requester_may_delete_resource(resource=resource)

        return resource

    def check_glue_sync_resource_deletion_requirements(
        self, glue_resource: GenericGlueSyncResource, dataset: Dataset
    ) -> None:
        """Check requirements for deleting glue sync resources."""
        if glue_resource.sync_type is SyncType.lake_formation:
            if dataset.filter_permissions(stage=glue_resource.stage, region=glue_resource.region):
                raise ForbiddenError(
                    f"Cannot delete resource of syncType {SyncType.lake_formation.value} as there are existing "
                    f"permissions for this resource. Please remove them first."
                )
            if self._filter_packages_table.list(
                dataset_id=dataset.id, stage=glue_resource.stage, region=glue_resource.region
            ):
                raise ForbiddenError(
                    f"Cannot delete resource of syncType {SyncType.lake_formation.value} as there are existing "
                    f"filter packages for this resource. Please remove them first."
                )
