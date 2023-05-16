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
from cdh_core_api.api.openapi_spec.openapi_schemas import REGION_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import STAGE_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import SYNC_TYPE_SCHEMA
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.bodies.datasets import DatasetAccountPermissionBody
from cdh_core_api.bodies.datasets import DatasetAccountPermissionPostBody
from cdh_core_api.endpoints.utils import remap_dynamo_internal_errors
from cdh_core_api.endpoints.utils import throttleable
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.dataset_permissions_validator import DatasetPermissionsValidator
from cdh_core_api.services.dataset_permissions_validator import ValidatedDatasetAccessPermission
from cdh_core_api.services.utils import fetch_dataset
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.accounts import Account
from cdh_core.entities.dataset import DatasetAccountPermissionAction
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import ResponseDatasetPermissions
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.hubs import Hub

DATASET_PERMISSION_SCHEMA = OpenApiSchema(
    "DatasetPermission",
    {
        "accountId": OpenApiTypes.STRING,
        "region": openapi.link(REGION_SCHEMA),
        "stage": openapi.link(STAGE_SCHEMA),
        "syncType": openapi.link(SYNC_TYPE_SCHEMA),
    },
)

DATASET_PERMISSION_LIST_SCHEMA = OpenApiSchema(
    "DatasetPermissions", {"permissions": OpenApiTypes.array_of(openapi.link(DATASET_PERMISSION_SCHEMA))}
)


@dataclass(frozen=True)
class PermissionsPath:
    """Represents the path parameters required to call the GET /{hub}/datasets/{datasetId}/permissions endpoint."""

    hub: Hub
    datasetId: DatasetId  # pylint: disable=invalid-name


@coreapi.route("/{hub}/datasets/{datasetId}/permissions", ["GET"])
@openapi.response(HTTPStatus.OK, DATASET_PERMISSION_LIST_SCHEMA)
@remap_dynamo_internal_errors
@throttleable
def get_permissions(
    path: PermissionsPath,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> JsonResponse:
    """
    Return the permissions of the dataset with the ID `datasetId`, if it is visible.

    The response may be truncated and contain only a subset of all permissions.
    In that case, a 'nextPageToken' is returned in the response's header.
    This token can be used as a query parameter in a subsequent request to fetch the next 'page' of permissions.
    """
    dataset = fetch_dataset(hub=path.hub, dataset_id=path.datasetId, visible_data_loader=visible_data_loader)
    return JsonResponse(body=ResponseDatasetPermissions(permissions=dataset.permissions))


@coreapi.route("/{hub}/datasets/{datasetId}/permissions", ["POST"])
@openapi.response(HTTPStatus.CREATED, DATASET_PERMISSION_SCHEMA)
def grant_access(
    body: DatasetAccountPermissionPostBody,
    path: PermissionsPath,
    dataset_permissions_validator: DatasetPermissionsValidator,
    dataset_permissions_manager: DatasetPermissionsManager[Account, S3Resource, GlueSyncResource],
) -> JsonResponse:
    """Grant read access to a dataset for one stage and region."""
    validated_permission: ValidatedDatasetAccessPermission[
        Account, S3Resource
    ] = dataset_permissions_validator.validate_dataset_access_request(
        hub=path.hub, dataset_id=path.datasetId, body=body
    )

    dataset_permissions_manager.add_or_remove_permission_handle_errors(
        validated_permission, DatasetAccountPermissionAction.add
    )

    return JsonResponse(body=validated_permission.permission, status_code=HTTPStatus.CREATED)


@coreapi.route("/{hub}/datasets/{datasetId}/permissions", ["DELETE"])
@openapi.response(HTTPStatus.OK, DATASET_PERMISSION_SCHEMA)
def revoke_access(
    body: DatasetAccountPermissionBody,
    path: PermissionsPath,
    dataset_permissions_validator: DatasetPermissionsValidator,
    dataset_permissions_manager: DatasetPermissionsManager[Account, S3Resource, GlueSyncResource],
) -> JsonResponse:
    """Revoke read access to a dataset for one stage and region."""
    validated_permission: ValidatedDatasetAccessPermission[
        Account, S3Resource
    ] = dataset_permissions_validator.validate_revoke(hub=path.hub, dataset_id=path.datasetId, body=body)

    dataset_permissions_manager.add_or_remove_permission_handle_errors(
        validated_permission, DatasetAccountPermissionAction.remove
    )

    return JsonResponse(body=validated_permission.permission)
