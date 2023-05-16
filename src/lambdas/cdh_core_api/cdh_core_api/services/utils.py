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

from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.filter_packages_table import FilterPackageNotFound
from cdh_core_api.catalog.resource_table import GenericResourceModel
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.filter_package import FilterPackage
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.resource import Resource
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id import AccountId


def get_user(requester_identity: RequesterIdentity, config: Config, authorization_api: AuthorizationApi) -> str:
    """Resolve the user from the requester identity."""
    auth_user = None
    if config.using_authorization_api:
        auth_user = authorization_api.get_user_id()
    return auth_user or requester_identity.user


def fetch_dataset(
    hub: Hub,
    dataset_id: DatasetId,
    visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
) -> Dataset:
    """Fetch a dataset using the `VisibleDataLoader`. If the hub does not match, raise a 404 error."""
    with suppress(DatasetNotFound):
        dataset = visible_data_loader.get_dataset(dataset_id)
        if dataset.hub is hub:
            return dataset
    raise NotFoundError(DatasetNotFound(dataset_id))


def fetch_resource(  # pylint: disable=too-many-arguments
    hub: Hub,
    dataset_id: DatasetId,
    resource_type: ResourceType,
    stage: Stage,
    region: Region,
    visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
) -> Resource:
    """Fetch a resource using the `VisibleDataLoader`. If the hub does not match, raise a 404 error."""
    with suppress(ResourceNotFound):
        resource = visible_data_loader.get_resource(
            resource_type=resource_type, dataset_id=dataset_id, stage=stage, region=region
        )
        if resource.hub is hub:
            return resource
    range_key = GenericResourceModel.get_range_key(resource_type=resource_type, stage=stage, region=region)
    raise NotFoundError(ResourceNotFound(dataset_id, range_key))


def fetch_filter_package(
    dataset_id: DatasetId,
    stage: Stage,
    region: Region,
    package_id: PackageId,
    visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
) -> FilterPackage:
    """Fetch a filter package using the `VisibleDataLoader`."""
    with suppress(FilterPackageNotFound):
        filter_package = visible_data_loader.get_filter_package(
            dataset_id=dataset_id, stage=stage, region=region, package_id=package_id
        )
        return filter_package
    raise NotFoundError(FilterPackageNotFound(f"{dataset_id}_{stage.value}_{region.value}", package_id))


def find_permission(account_id: AccountId, dataset: Dataset, region: Region, stage: Stage) -> DatasetAccountPermission:
    """Return the dataset permission that matches the provided account_id, stage and region.

    The result is retrieved from the permissions field of the dataset.
    """
    try:
        return next(iter(dataset.filter_permissions(account_id=account_id, stage=stage, region=region)))
    except StopIteration as err:
        raise ConflictError(
            f"Account {account_id} does not have access to dataset {dataset.id} "
            f"in stage {stage.value} and region {region.value}."
        ) from err
