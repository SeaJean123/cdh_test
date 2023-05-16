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
from http import HTTPStatus

from cdh_core_api.api.openapi_spec.openapi import OpenApiSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiTypes
from cdh_core_api.api.openapi_spec.openapi_schemas import HUB_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import REGION_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import STAGE_SCHEMA
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.services.utils import fetch_filter_package
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.accounts import Account
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.filter_package import FilterPackages
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage


_TABLE_FILTER_SCHEMA = OpenApiSchema(
    "TableFilter",
    {
        "filterId": OpenApiTypes.STRING,
        "packageId": OpenApiTypes.STRING,
        "resourceAccountId": OpenApiTypes.STRING,
        "databaseName": OpenApiTypes.STRING,
        "tableName": OpenApiTypes.STRING,
        "fullAccess": OpenApiTypes.BOOLEAN,
        "rowFilter": OpenApiTypes.STRING,
        "includedColumns": OpenApiTypes.array_of(OpenApiTypes.STRING),
        "excludedColumns": OpenApiTypes.array_of(OpenApiTypes.STRING),
        "creationDate": OpenApiTypes.STRING,
        "creatorUserId": OpenApiTypes.STRING,
        "updateDate": OpenApiTypes.DATE_TIME,
    },
)

FILTER_PACKAGE_SCHEMA = OpenApiSchema(
    "FilterPackage",
    {
        "id": OpenApiTypes.STRING,
        "datasetId": OpenApiTypes.STRING,
        "stage": openapi.link(STAGE_SCHEMA),
        "region": openapi.link(REGION_SCHEMA),
        "friendlyName": OpenApiTypes.STRING,
        "description": OpenApiTypes.STRING,
        "tableAccess": OpenApiTypes.array_of(openapi.link(_TABLE_FILTER_SCHEMA)),
        "hub": openapi.link(HUB_SCHEMA),
        "creationDate": OpenApiTypes.STRING,
        "creatorUserId": OpenApiTypes.STRING,
        "updateDate": OpenApiTypes.DATE_TIME,
    },
)


FILTER_PACKAGE_LIST_SCHEMA = OpenApiSchema(
    "FilterPackages",
    {"filterPackages": OpenApiTypes.array_of(openapi.link(FILTER_PACKAGE_SCHEMA))},
)


@dataclass(frozen=True)
class FilterPackagesPath:
    """Base class for filter package paths."""

    hub: Hub
    stage: Stage
    region: Region
    datasetId: DatasetId  # pylint: disable=invalid-name


@dataclass(frozen=True)
class FilterPackageByIdPath(FilterPackagesPath):
    """Path for filter packages."""

    packageId: PackageId  # pylint: disable=invalid-name


@coreapi.route(
    "/{hub}/resources/glue-sync/{datasetId}/{stage}/{region}/filter-packages/{packageId}",
    ["GET"],
)
@openapi.response(HTTPStatus.OK, FILTER_PACKAGE_SCHEMA)
def get_filter_package(
    path: FilterPackageByIdPath,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> JsonResponse:
    """Get a filter package, if it is visible."""
    filter_package = fetch_filter_package(
        dataset_id=path.datasetId,
        stage=path.stage,
        region=path.region,
        visible_data_loader=visible_data_loader,
        package_id=path.packageId,
    )
    return JsonResponse(body=filter_package)


@coreapi.route("/{hub}/resources/glue-sync/{datasetId}/{stage}/{region}/filter-packages", ["GET"])
@openapi.response(HTTPStatus.OK, FILTER_PACKAGE_LIST_SCHEMA)
def get_filter_packages(
    path: FilterPackagesPath,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> JsonResponse:
    """Get all visible filter packages."""
    filter_packages = visible_data_loader.get_filter_packages(
        dataset_id=path.datasetId,
        stage=path.stage,
        region=path.region,
    )
    return JsonResponse(body=FilterPackages(filter_packages=filter_packages))
