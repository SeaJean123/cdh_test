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
from typing import Optional

from cdh_core_api.api.openapi_spec.openapi import OpenApiEnum
from cdh_core_api.api.openapi_spec.openapi import OpenApiSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiTypes
from cdh_core_api.api.openapi_spec.openapi_schemas import HUB_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import REGION_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import STAGE_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import SYNC_TYPE_SCHEMA
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.bodies.resources import NewGlueSyncBody
from cdh_core_api.bodies.resources import NewS3BucketBody
from cdh_core_api.config import Config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.glue_resource_manager import GlueResourceManager
from cdh_core_api.services.glue_resource_manager import GlueSyncAlreadyExists
from cdh_core_api.services.pagination_service import NextPageTokenContext
from cdh_core_api.services.pagination_service import PaginationService
from cdh_core_api.services.resource_payload_builder import ResourcePayloadBuilder
from cdh_core_api.services.resource_validator import ResourceValidator
from cdh_core_api.services.s3_resource_manager import S3ResourceManager
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.utils import fetch_resource
from cdh_core_api.services.utils import get_user
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from cdh_core_api.validation.base import next_page_token_field
from cdh_core_api.validation.common_paths import HubPath

from cdh_core.entities.accounts import Account
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import ResourcesPayload
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.primitives.account_id import AccountId

_RESOURCE_SCHEMA_COMMON = {
    "datasetId": OpenApiTypes.STRING,
    "arn": OpenApiTypes.STRING,
    "creationDate": OpenApiTypes.DATE_TIME,
    "creatorUserId": OpenApiTypes.OPTIONAL_STRING,
    "hub": openapi.link(HUB_SCHEMA),
    "name": OpenApiTypes.STRING,
    "ownerAccountId": OpenApiTypes.STRING,
    "region": openapi.link(REGION_SCHEMA),
    "resourceAccountId": OpenApiTypes.STRING,
    "stage": openapi.link(STAGE_SCHEMA),
    "updateDate": OpenApiTypes.DATE_TIME,
}
GLUE_SYNC_ATTRIBUTES_SCHEMA = OpenApiSchema(
    "GlueSyncAttributes",
    {
        "syncType": openapi.link(SYNC_TYPE_SCHEMA),
    },
)
GLUE_SYNC_RESOURCE_SCHEMA = OpenApiSchema(
    "GlueSyncResource",
    {
        **_RESOURCE_SCHEMA_COMMON,
        "type": OpenApiTypes.constant_string(ResourceType.glue_sync.value),
        "attributes": openapi.link(GLUE_SYNC_ATTRIBUTES_SCHEMA),
    },
)
S3_ATTRIBUTES_SCHEMA = OpenApiSchema(
    "S3Attributes",
    {
        "kmsKeyArn": OpenApiTypes.STRING,
        "snsTopicArn": OpenApiTypes.STRING,
    },
)
S3_RESOURCE_SCHEMA = OpenApiSchema(
    "S3Resource",
    {
        **_RESOURCE_SCHEMA_COMMON,
        "attributes": openapi.link(S3_ATTRIBUTES_SCHEMA),
        "type": OpenApiTypes.constant_string(ResourceType.s3.value),
    },
)

openapi.link(OpenApiEnum.from_enum_type(ResourceType))

RESOURCE_SCHEMA = OpenApiTypes.union(openapi.link(GLUE_SYNC_RESOURCE_SCHEMA), openapi.link(S3_RESOURCE_SCHEMA))
RESOURCE_LIST_SCHEMA = OpenApiSchema("Resources", {"resources": OpenApiTypes.array_of(RESOURCE_SCHEMA)})


@dataclass(frozen=True)
class ResourcesQuerySchema:
    """Represents the query parameters that can be used when calling the GET /{hub}/resources endpoint."""

    region: Optional[Region] = None
    resourceAccountId: Optional[AccountId] = None  # pylint: disable=invalid-name
    stage: Optional[Stage] = None
    datasetId: Optional[DatasetId] = None  # pylint: disable=invalid-name
    nextPageToken: Optional[str] = next_page_token_field  # pylint: disable=invalid-name


@dataclass(frozen=True)
class ResourceByBucketNameQuerySchema:
    """Represents the query parameters that can be used when calling the GET /resources/s3 endpoint."""

    bucketName: str  # pylint: disable=invalid-name


@coreapi.route("/{hub}/resources", ["GET"])
@openapi.response(HTTPStatus.OK, RESOURCE_LIST_SCHEMA)
def get_resources(
    path: HubPath,
    query: ResourcesQuerySchema,
    config: Config,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
    pagination_service: PaginationService,
) -> JsonResponse:
    """Return a list of visible resources.

    The response may be truncated and contain only a subset of all visible resources.
    In that case, a 'nextPageToken' is returned in the response's header.
    This token can be used as a query parameter in a subsequent request to fetch the next 'page' of resources.
    """
    last_evaluated_key = pagination_service.decode_token(
        next_page_token=query.nextPageToken,
        context=NextPageTokenContext.RESOURCES,
    )
    resources, new_last_evaluated_key = visible_data_loader.get_resources(
        hub=path.hub,
        dataset_id=query.datasetId,
        stage=query.stage,
        region=query.region,
        resource_account=query.resourceAccountId,
        limit=config.result_page_size,
        last_evaluated_key=last_evaluated_key,
    )
    next_page_token = pagination_service.issue_token(
        last_evaluated_key=new_last_evaluated_key,
        context=NextPageTokenContext.RESOURCES,
    )
    return JsonResponse(
        body=ResourcesPayload.from_resources(resources),
        next_page_token=next_page_token,
    )


@coreapi.route("/resources/s3", ["GET"])
@openapi.response(HTTPStatus.OK, S3_RESOURCE_SCHEMA)
def get_resource_by_bucket_name(
    query: ResourceByBucketNameQuerySchema,
    resource_payload_builder: ResourcePayloadBuilder,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> JsonResponse:
    """Find the S3 resource corresponding to a specified bucket name, if it is visible."""
    resource = visible_data_loader.get_resource_from_bucket_name(query.bucketName)
    return JsonResponse(body=resource_payload_builder(resource))


@dataclass(frozen=True)
class ResourcePath:
    """Represents the path parameters for the DELETE /{hub}/resources/s3/{datasetId}/{stage}/{region} endpoint."""

    hub: Hub
    stage: Stage
    region: Region
    datasetId: DatasetId  # pylint: disable=invalid-name


@dataclass(frozen=True)
class TypedResourcePath(ResourcePath):
    """Represents the path parameters for the GET /{hub}/resources/{type}/{datasetId}/{stage}/{region} endpoint."""

    type: ResourceType


@coreapi.route("/{hub}/resources/{type}/{datasetId}/{stage}/{region}", ["GET"])
@openapi.response(HTTPStatus.OK, RESOURCE_SCHEMA)
def get_resource(
    path: TypedResourcePath,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
    resource_payload_builder: ResourcePayloadBuilder,
) -> JsonResponse:
    """Get a resource, if it is visible."""
    resource = fetch_resource(
        hub=path.hub,
        dataset_id=path.datasetId,
        resource_type=path.type,
        stage=path.stage,
        region=path.region,
        visible_data_loader=visible_data_loader,
    )
    return JsonResponse(body=resource_payload_builder(resource))


@coreapi.route("/{hub}/resources/s3", ["POST"])
@openapi.response(HTTPStatus.CREATED, S3_RESOURCE_SCHEMA)
def create_s3_bucket(  # pylint: disable=too-many-arguments
    config: Config,
    requester_identity: RequesterIdentity,
    path: HubPath,
    body: NewS3BucketBody,
    s3_resource_manager: S3ResourceManager[S3Resource],
    sns_publisher: SnsPublisher,
    authorization_api: AuthorizationApi,
    resource_validator: ResourceValidator,
    resource_payload_builder: ResourcePayloadBuilder,
) -> JsonResponse:
    """Create a new S3 bucket resource, if the associated dataset is visible.

    Also creates an SNS topic for updates to the bucket.
    """
    dataset = resource_validator.check_dataset_visible(hub=path.hub, dataset_id=body.datasetId)
    owner_account_id = body.ownerAccountId or requester_identity.account_id

    resource_validator.check_may_create_resource(
        dataset=dataset,
        stage=body.stage,
        region=body.region,
        resource_type=ResourceType.s3,
        owner_account_id=owner_account_id,
    )

    resource_account = resource_validator.determine_account_for_new_resource(
        dataset=dataset,
        hub=path.hub,
        stage=body.stage,
        region=body.region,
        resource_type=ResourceType.s3,
    )

    s3_bucket = s3_resource_manager.create_bucket(
        dataset=dataset,
        stage=body.stage,
        region=body.region,
        resource_account=resource_account,
        user=get_user(requester_identity=requester_identity, config=config, authorization_api=authorization_api),
        owner_account_id=owner_account_id,
    )

    sns_publisher.publish(
        entity_type=EntityType.RESOURCE,
        operation=Operation.CREATE,
        payload=s3_bucket.to_payload(),
    )

    return JsonResponse(body=resource_payload_builder(s3_bucket), status_code=HTTPStatus.CREATED)


@coreapi.route("/{hub}/resources/glue-sync", ["POST"])
@openapi.response(HTTPStatus.CREATED, GLUE_SYNC_RESOURCE_SCHEMA)
def create_glue_sync(  # pylint: disable=too-many-arguments
    requester_identity: RequesterIdentity,
    path: HubPath,
    body: NewGlueSyncBody,
    glue_resource_manager: GlueResourceManager[GlueSyncResource, NewGlueSyncBody],
    config: Config,
    sns_publisher: SnsPublisher,
    authorization_api: AuthorizationApi,
    dataset_permissions_manager: DatasetPermissionsManager[Account, S3Resource, GlueSyncResource],
    resource_validator: ResourceValidator,
) -> JsonResponse:
    """Create a new Glue Sync resource, if the associated dataset is visible."""
    dataset = resource_validator.check_dataset_visible(hub=path.hub, dataset_id=body.datasetId)
    owner_account_id = body.ownerAccountId or requester_identity.account_id
    resource_validator.check_may_create_resource(
        dataset=dataset,
        stage=body.stage,
        region=body.region,
        resource_type=ResourceType.glue_sync,
        owner_account_id=owner_account_id,
    )
    resource_validator.check_glue_sync_resource_requirements(
        dataset=dataset,
        stage=body.stage,
        region=body.region,
        owner_account_id=owner_account_id,
        sync_type=body.syncType,
        partition=path.hub.partition,
    )
    resource_account = resource_validator.determine_account_for_new_resource(
        dataset=dataset,
        hub=path.hub,
        stage=body.stage,
        region=body.region,
        resource_type=ResourceType.glue_sync,
    )

    try:
        glue_sync = glue_resource_manager.create_glue_sync(
            dataset=dataset,
            body=body,
            resource_account=resource_account,
            owner_account_id=owner_account_id,
            user=get_user(requester_identity=requester_identity, config=config, authorization_api=authorization_api),
        )
    except GlueSyncAlreadyExists as error:
        raise ConflictError(error) from error

    sns_publisher.publish(
        entity_type=EntityType.RESOURCE,
        operation=Operation.CREATE,
        payload=glue_sync.to_payload(),
    )

    dataset_permissions_manager.create_missing_resource_links(dataset, body.stage, body.region)

    return JsonResponse(
        body=glue_sync.to_payload(),
        status_code=HTTPStatus.CREATED,
    )


@coreapi.route("/{hub}/resources/s3/{datasetId}/{stage}/{region}", ["DELETE"])
def delete_s3_resource(
    path: ResourcePath,
    s3_resource_manager: S3ResourceManager[S3Resource],
    sns_publisher: SnsPublisher,
    resource_validator: ResourceValidator,
) -> JsonResponse:
    """Delete a s3 resource, if the associated dataset is visible."""
    dataset = resource_validator.check_dataset_visible(hub=path.hub, dataset_id=path.datasetId)
    resource: S3Resource = resource_validator.check_may_delete_s3_resource(
        dataset=dataset, stage=path.stage, region=path.region
    )
    s3_resource_manager.delete_bucket(s3_resource=resource)
    sns_publisher.publish(
        entity_type=EntityType.RESOURCE,
        operation=Operation.DELETE,
        payload=resource.to_payload(),
    )
    return JsonResponse(status_code=HTTPStatus.NO_CONTENT)


@coreapi.route("/{hub}/resources/glue-sync/{datasetId}/{stage}/{region}", ["DELETE"])
def delete_glue_sync_resource(
    path: ResourcePath,
    glue_resource_manager: GlueResourceManager[GlueSyncResource, NewGlueSyncBody],
    sns_publisher: SnsPublisher,
    dataset_permissions_manager: DatasetPermissionsManager[Account, S3Resource, GlueSyncResource],
    resource_validator: ResourceValidator,
) -> JsonResponse:
    """Delete a glue sync resource, if the associated dataset is visible."""
    dataset = resource_validator.check_dataset_visible(hub=path.hub, dataset_id=path.datasetId)
    resource: GlueSyncResource = resource_validator.check_may_delete_glue_sync_resource(
        dataset=dataset, stage=path.stage, region=path.region
    )
    resource_validator.check_glue_sync_resource_deletion_requirements(resource, dataset)

    glue_resource_manager.delete_glue_sync(resource)

    dataset_permissions_manager.delete_metadata_syncs_for_glue_sync(resource, dataset)

    sns_publisher.publish(
        entity_type=EntityType.RESOURCE,
        operation=Operation.DELETE,
        payload=resource.to_payload(),
    )
    return JsonResponse(status_code=HTTPStatus.NO_CONTENT)
