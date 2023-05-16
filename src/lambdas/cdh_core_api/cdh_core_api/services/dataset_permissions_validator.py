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
from typing import cast
from typing import Generic
from typing import Optional

from cdh_core_api.bodies.datasets import DatasetAccountPermissionBody
from cdh_core_api.bodies.datasets import DatasetAccountPermissionPostBody
from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.catalog.resource_table import GenericResourcesTable
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.utils import fetch_dataset
from cdh_core_api.services.utils import fetch_resource
from cdh_core_api.services.utils import find_permission
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetId
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id import AccountId

NON_SHAREABLE_ACCOUNT_TYPES = {AccountType.internal, AccountType.technical}


@dataclass(frozen=True)
class ValidatedDatasetAccessPermission(Generic[GenericAccount, GenericS3Resource]):
    """Dataset permission for an account which has already been validated."""

    permission: DatasetAccountPermission
    dataset: Dataset
    account: GenericAccount
    s3_resource: GenericS3Resource


class DatasetPermissionsValidator:
    """Validates dataset account permissions for resource access."""

    def __init__(
        self,
        authorizer: Authorizer[GenericAccount],
        accounts_table: GenericAccountsTable[GenericAccount],
        resources_table: GenericResourcesTable[GenericS3Resource, GenericGlueSyncResource],
        visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
    ):
        self._authorizer = authorizer
        self._accounts_table = accounts_table
        self._visible_data_loader = visible_data_loader
        self._resources_table = resources_table

    def validate_dataset_access_request(
        self, hub: Hub, dataset_id: DatasetId, body: DatasetAccountPermissionPostBody
    ) -> ValidatedDatasetAccessPermission[GenericAccount, GenericS3Resource]:
        """
        Check a specific dataset resource permission.

        Check if the permission can be granted to a given
        account and return the validated permission.
        """
        self._check_for_conflicting_resource(
            dataset_id=dataset_id, stage=body.stage, region=body.region, account_id=body.accountId
        )
        return self.get_validated_dataset_access_permission(
            hub=hub,
            dataset_id=dataset_id,
            stage=body.stage,
            region=body.region,
            account_id=body.accountId,
            sync_type=body.syncType,
        )

    def get_validated_dataset_access_permission(  # pylint: disable=too-many-arguments
        self,
        hub: Hub,
        dataset_id: DatasetId,
        account_id: AccountId,
        region: Region,
        stage: Stage,
        sync_type: Optional[SyncType] = None,
    ) -> ValidatedDatasetAccessPermission[GenericAccount, GenericS3Resource]:
        """Verify and return a new dataset account permission."""
        sync_type = self._infer_sync_type_from_glue_resource(
            hub=hub, dataset_id=dataset_id, stage=stage, region=region, sync_type=sync_type
        )

        dataset = self.fetch_dataset(dataset_id, hub)
        self._check_existing_permission(dataset, account_id, stage, region)

        resource: GenericS3Resource = self._validate_dataset_and_resource(
            dataset=dataset, hub=hub, stage=stage, region=region
        )
        permission = DatasetAccountPermission(account_id=account_id, region=region, stage=stage, sync_type=sync_type)
        account: GenericAccount = self._validate_account(permission_to_grant=permission)

        return ValidatedDatasetAccessPermission(
            permission=permission, dataset=dataset, account=account, s3_resource=resource
        )

    def _infer_sync_type_from_glue_resource(  # pylint: disable=too-many-arguments
        self, hub: Hub, dataset_id: DatasetId, stage: Stage, region: Region, sync_type: Optional[SyncType]
    ) -> SyncType:
        try:
            glue_resource = self.fetch_glue_resource(
                dataset_id=dataset_id, hub=hub, region=region, stage=stage
            )  # type: ignore
            resource_sync_type = glue_resource.sync_type
        except NotFoundError:
            resource_sync_type = None

        if resource_sync_type is SyncType.lake_formation:
            if sync_type in [SyncType.lake_formation, None]:
                return SyncType.lake_formation
            raise ForbiddenError(
                f"syncType {sync_type.value} is not allowed for existing glue resource with syncType "  # type: ignore
                f"{SyncType.lake_formation.value}."
            )

        if sync_type is SyncType.lake_formation:
            reason = (
                f"for existing glue resource with syncType {resource_sync_type.value}"
                if resource_sync_type
                else "because no corresponding glue resource exists yet"
            )
            raise ForbiddenError(f"syncType {sync_type.value} is not allowed {reason}")

        return sync_type or SyncType.resource_link

    def fetch_glue_resource(
        self, dataset_id: DatasetId, hub: Hub, region: Region, stage: Stage
    ) -> GenericGlueSyncResource:  # type: ignore
        """Fetch a glue resource from the data loader."""
        return cast(
            GenericGlueSyncResource,
            fetch_resource(
                hub=hub,
                dataset_id=dataset_id,
                resource_type=ResourceType.glue_sync,
                stage=stage,
                region=region,
                visible_data_loader=self._visible_data_loader,
            ),
        )

    def fetch_s3_resource(
        self, dataset_id: DatasetId, hub: Hub, region: Region, stage: Stage
    ) -> GenericS3Resource:  # type: ignore
        """Fetch a s3 resource from the data loader."""
        s3_resource = cast(
            GenericS3Resource,
            fetch_resource(
                hub=hub,
                dataset_id=dataset_id,
                resource_type=ResourceType.s3,
                stage=stage,
                region=region,
                visible_data_loader=self._visible_data_loader,
            ),
        )
        return s3_resource

    def fetch_dataset(self, dataset_id: DatasetId, hub: Hub) -> Dataset:
        """Fetch a dataset from the data loader."""
        return fetch_dataset(hub=hub, dataset_id=dataset_id, visible_data_loader=self._visible_data_loader)

    def validate_revoke(
        self, hub: Hub, dataset_id: DatasetId, body: DatasetAccountPermissionBody
    ) -> ValidatedDatasetAccessPermission[GenericAccount, GenericS3Resource]:
        """
        Check a specific dataset resource permission.

        Check if the permission can be removed from a given
        account and return the validated permission.
        """
        dataset = self.fetch_dataset(dataset_id, hub)
        resource: GenericS3Resource = self._validate_dataset_and_resource(
            dataset=dataset, hub=hub, stage=body.stage, region=body.region
        )
        account: GenericAccount = self._check_account_exists(account_id=body.accountId)
        permission = find_permission(account_id=body.accountId, dataset=dataset, region=body.region, stage=body.stage)
        return ValidatedDatasetAccessPermission(
            permission=permission, dataset=dataset, account=account, s3_resource=resource
        )

    def _check_existing_permission(self, dataset: Dataset, account_id: AccountId, stage: Stage, region: Region) -> None:
        if dataset.filter_permissions(account_id=account_id, stage=stage, region=region):
            raise ConflictError(
                f"Account {account_id} already has access to dataset {dataset.id} "
                f"in stage {stage.value} and region {region.value}."
            )

    def _validate_dataset_and_resource(
        self, dataset: Dataset, hub: Hub, stage: Stage, region: Region
    ) -> GenericS3Resource:  # type: ignore
        s3_resource: GenericS3Resource = self.fetch_s3_resource(
            dataset_id=dataset.id, hub=hub, region=region, stage=stage
        )
        self._authorizer.check_requester_may_manage_dataset_access(s3_resource.owner_account_id)
        return s3_resource

    def _validate_account(self, permission_to_grant: DatasetAccountPermission) -> GenericAccount:  # type: ignore
        account: GenericAccount = self._check_account_exists(permission_to_grant.account_id)

        if account.type in NON_SHAREABLE_ACCOUNT_TYPES:
            raise ForbiddenError(
                f"The account {account.id} is of type {account.type.value!r} and hence cannot be granted read access."
            )

        if permission_to_grant.region.partition is not account.hub.partition:
            raise ForbiddenError(
                f"Cannot share resources across AWS partitions. Account {account.id} is located in "
                f"partition {account.hub.partition}, but the resource is located "
                f"in partition {permission_to_grant.region.partition}."
            )
        return account

    def _check_account_exists(self, account_id: AccountId) -> GenericAccount:  # type: ignore
        try:
            return self._accounts_table.get(account_id)
        except AccountNotFound as err:
            raise ForbiddenError(f"The account {account_id} is not registered with the CDH.") from err

    def _check_for_conflicting_resource(
        self, dataset_id: DatasetId, account_id: AccountId, region: Region, stage: Stage
    ) -> None:
        resource_list = self._resources_table.list_glue_sync(dataset_id=dataset_id, region=region, stage=stage)
        if any(resource.owner_account_id == account_id for resource in resource_list):
            raise ConflictError(
                f"The account {account_id} is the owner account of the resource in stage {stage} and region {region} "
                f"for the dataset {dataset_id} and thus cannot be granted access."
            )
