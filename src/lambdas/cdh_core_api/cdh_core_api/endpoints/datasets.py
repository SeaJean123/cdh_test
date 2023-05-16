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
from dataclasses import field
from http import HTTPStatus
from typing import List
from typing import Optional

from cdh_core_api.api.openapi_spec.openapi import DataclassSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiTypes
from cdh_core_api.api.openapi_spec.openapi_schemas import BUSINESS_OBJECT_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import CONFIDENTIALITY_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import DATASET_EXTERNAL_LINK_TYPE_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import DATASET_PURPOSE_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import DATASET_STATUS_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import HUB_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import INGEST_FREQUENCY_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import LAYER_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import RETENTION_PERIOD_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import SUPPORT_LEVEL_SCHEMA
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.bodies.datasets import DatasetParticipantBodyPart
from cdh_core_api.bodies.datasets import ExternalLinkBody
from cdh_core_api.bodies.datasets import NewDatasetBody
from cdh_core_api.bodies.datasets import UpdateDatasetBody
from cdh_core_api.config import Config
from cdh_core_api.endpoints.dataset_account_permissions import DATASET_PERMISSION_SCHEMA
from cdh_core_api.endpoints.utils import remap_dynamo_internal_errors
from cdh_core_api.endpoints.utils import throttleable
from cdh_core_api.services.dataset_manager import DatasetManager
from cdh_core_api.services.dataset_participants_manager import DatasetParticipantsManager
from cdh_core_api.services.dataset_validator import DatasetValidator
from cdh_core_api.services.pagination_service import NextPageTokenContext
from cdh_core_api.services.pagination_service import PaginationService
from cdh_core_api.services.response_dataset_builder import ResponseDatasetBuilder
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.utils import fetch_dataset
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from cdh_core_api.validation.base import next_page_token_field
from cdh_core_api.validation.common_paths import HubPath
from cdh_core_api.validation.datasets import DatasetIdField
from marshmallow import fields
from marshmallow import validate

from cdh_core.entities.accounts import Account
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import ResponseDatasets
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.hubs import Hub
from cdh_core.iterables import unwrap_singleton

DATASET_PARTICIPANT_SCHEMA = OpenApiSchema(
    "DatasetParticipant",
    {
        "id": OpenApiTypes.STRING,
        "idp": OpenApiTypes.STRING,
    },
)
DATASET_EXTERNAL_LINK_SCHEMA = OpenApiSchema(
    "DatasetExternalLink",
    {
        "name": OpenApiTypes.OPTIONAL_STRING,
        "type": openapi.link(DATASET_EXTERNAL_LINK_TYPE_SCHEMA),
        "url": OpenApiTypes.STRING,
    },
)
DATASET_LINEAGE_SCHEMA = OpenApiSchema(
    "DatasetLineage",
    {
        "upstream": OpenApiTypes.array_of(OpenApiTypes.STRING),
    },
)
DATASET_SCHEMA = OpenApiSchema(
    "Dataset",
    {
        "id": OpenApiTypes.STRING,
        "businessObject": openapi.link(BUSINESS_OBJECT_SCHEMA),
        "confidentiality": openapi.link(CONFIDENTIALITY_SCHEMA),
        "containsPii": OpenApiTypes.BOOLEAN,
        "creatorUserId": OpenApiTypes.OPTIONAL_STRING,
        "creationDate": OpenApiTypes.DATE_TIME,
        "description": OpenApiTypes.STRING,
        "documentation": OpenApiTypes.OPTIONAL_STRING,
        "engineers": OpenApiTypes.array_of(openapi.link(DATASET_PARTICIPANT_SCHEMA)),
        "externalLinks": OpenApiTypes.array_of(openapi.link(DATASET_EXTERNAL_LINK_SCHEMA)),
        "friendlyName": OpenApiTypes.STRING,
        "hub": openapi.link(HUB_SCHEMA),
        "hubVisibility": OpenApiTypes.array_of(openapi.link(HUB_SCHEMA)),
        "ingestFrequency": openapi.link(INGEST_FREQUENCY_SCHEMA),
        "labels": OpenApiTypes.array_of(OpenApiTypes.STRING),
        "layer": openapi.link(LAYER_SCHEMA),
        "lineage": openapi.link(DATASET_LINEAGE_SCHEMA),
        "name": OpenApiTypes.STRING,
        "ownerAccountId": OpenApiTypes.STRING,
        "permissions": OpenApiTypes.array_of(openapi.link(DATASET_PERMISSION_SCHEMA)),
        "previewAvailable": OpenApiTypes.BOOLEAN,
        "purpose": OpenApiTypes.array_of(openapi.link(DATASET_PURPOSE_SCHEMA)),
        "qualityScore": OpenApiTypes.OPTIONAL_INTEGER,
        "retentionPeriod": openapi.link(RETENTION_PERIOD_SCHEMA),
        "sourceIdentifier": OpenApiTypes.OPTIONAL_STRING,
        "status": openapi.link(DATASET_STATUS_SCHEMA),
        "stewards": OpenApiTypes.array_of(openapi.link(DATASET_PARTICIPANT_SCHEMA)),
        "supportGroup": OpenApiTypes.OPTIONAL_STRING,
        "supportLevel": openapi.link(SUPPORT_LEVEL_SCHEMA),
        "tags": OpenApiTypes.dictionary("Custom tags for further dataset classification."),
        "updateDate": OpenApiTypes.DATE_TIME,
    },
    optional_keys=[
        "creatorUserId",
        "documentation",
        "qualityScore",
        "sourceIdentifier",
        "status",
        "supportGroup",
    ],
)

DATASET_LIST_SCHEMA = OpenApiSchema("Datasets", {"datasets": OpenApiTypes.array_of(openapi.link(DATASET_SCHEMA))})

openapi.link(DataclassSchema(DatasetParticipantBodyPart))
openapi.link(DataclassSchema(ExternalLinkBody))


@dataclass(frozen=True)
class DatasetCrossHubQueryParams:
    """Represents the query parameters required to call the GET /datasets endpoint."""

    ids: List[DatasetId] = field(
        metadata={
            "marshmallow_field": fields.List(DatasetIdField, required=True, validate=validate.Length(max=100)),
        }
    )


@coreapi.route("/datasets", ["GET"])
@openapi.response(HTTPStatus.OK, DATASET_LIST_SCHEMA)
@remap_dynamo_internal_errors
@throttleable
def get_datasets_cross_hub(
    requester_identity: RequesterIdentity,
    query: DatasetCrossHubQueryParams,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
    response_dataset_builder: ResponseDatasetBuilder,
) -> JsonResponse:
    """List visible datasets specified in query parameter `ids`."""
    return JsonResponse(
        body=ResponseDatasets(
            datasets=response_dataset_builder(
                datasets=visible_data_loader.get_datasets_cross_hub(query.ids),
                requester_identity=requester_identity,
            )
        )
    )


@dataclass(frozen=True)
class DatasetPath:
    """Represents the path parameters required to call the GET /{hub}/datasets/{datasetId} endpoint."""

    hub: Hub
    datasetId: DatasetId  # pylint: disable=invalid-name


@coreapi.route("/{hub}/datasets/{datasetId}", ["GET"])
@openapi.response(HTTPStatus.OK, DATASET_SCHEMA)
def get_dataset(
    requester_identity: RequesterIdentity,
    path: DatasetPath,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
    response_dataset_builder: ResponseDatasetBuilder,
) -> JsonResponse:
    """Return the dataset with the ID `dataset_id`, if it is visible."""
    dataset = fetch_dataset(hub=path.hub, dataset_id=path.datasetId, visible_data_loader=visible_data_loader)
    return JsonResponse(
        body=unwrap_singleton(response_dataset_builder(datasets=[dataset], requester_identity=requester_identity))
    )


@dataclass(frozen=True)
class DatasetsQuerySchema:
    """Represents the query parameters that may be provided when querying all datasets."""

    nextPageToken: Optional[str] = next_page_token_field  # pylint: disable=invalid-name


@coreapi.route("/{hub}/datasets", ["GET"])
@openapi.response(HTTPStatus.OK, DATASET_LIST_SCHEMA)
@remap_dynamo_internal_errors
@throttleable
def get_all_datasets(  # pylint: disable=too-many-arguments
    config: Config,
    requester_identity: RequesterIdentity,
    path: HubPath,
    query: DatasetsQuerySchema,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
    response_dataset_builder: ResponseDatasetBuilder,
    pagination_service: PaginationService,
) -> JsonResponse:
    """
    List all visible datasets.

    The response may be truncated and contain only a subset of all visible datasets.
    In that case, a 'nextPageToken' is returned in the response's header.

    This token can be used as a query parameter in a subsequent request to fetch the next 'page' of datasets.'
    """
    last_evaluated_key = pagination_service.decode_token(
        next_page_token=query.nextPageToken,
        context=NextPageTokenContext.DATASETS,
    )
    datasets, new_last_evaluated_key = visible_data_loader.get_datasets(
        hub=path.hub,
        limit=config.result_page_size,
        last_evaluated_key=last_evaluated_key,
    )
    next_page_token = pagination_service.issue_token(
        last_evaluated_key=new_last_evaluated_key,
        context=NextPageTokenContext.DATASETS,
    )
    return JsonResponse(
        body=ResponseDatasets(
            response_dataset_builder(
                datasets=datasets,
                requester_identity=requester_identity,
            )
        ),
        next_page_token=next_page_token,
    )


@coreapi.route("/{hub}/datasets", ["POST"])
@openapi.response(HTTPStatus.CREATED, DATASET_SCHEMA)
def create_new_dataset(  # pylint: disable=too-many-arguments
    requester_identity: RequesterIdentity,
    path: HubPath,
    body: NewDatasetBody,
    response_dataset_builder: ResponseDatasetBuilder,
    sns_publisher: SnsPublisher,
    dataset_validator: DatasetValidator,
    dataset_manager: DatasetManager,
    dataset_participants_manager: DatasetParticipantsManager,
) -> JsonResponse:
    """Create a new dataset."""
    dataset = dataset_validator.validate_new_dataset_body(body=body, hub=path.hub)
    participants = dataset_participants_manager.validate_new_participants(
        layer=body.layer,
        engineers=body.engineers,
        stewards=body.stewards,
    )

    dataset_manager.create_dataset(dataset=dataset)
    dataset_participants_manager.create_dataset_participants(
        dataset=dataset, participants=participants, requester_identity=requester_identity
    )

    sns_publisher.publish(
        entity_type=EntityType.DATASET,
        operation=Operation.CREATE,
        payload=dataset,
        message_consistency=MessageConsistency.CONFIRMED,
    )
    return JsonResponse(
        body=unwrap_singleton(response_dataset_builder(datasets=[dataset], requester_identity=requester_identity)),
        status_code=HTTPStatus.CREATED,
    )


@coreapi.route("/{hub}/datasets/{datasetId}", ["DELETE"])
def delete_dataset(
    path: DatasetPath,
    sns_publisher: SnsPublisher,
    dataset_validator: DatasetValidator,
    dataset_manager: DatasetManager,
    dataset_participants_manager: DatasetParticipantsManager,
) -> JsonResponse:
    """Delete a dataset, if it is visible."""
    dataset = dataset_validator.validate_deletion(hub=path.hub, dataset_id=path.datasetId)
    dataset_manager.delete_dataset(dataset, sns_publisher)
    dataset_participants_manager.delete_dataset_participants(dataset)
    sns_publisher.publish(
        entity_type=EntityType.DATASET,
        operation=Operation.DELETE,
        payload=dataset,
        message_consistency=MessageConsistency.CONFIRMED,
    )
    return JsonResponse(status_code=HTTPStatus.NO_CONTENT)


@coreapi.route("/{hub}/datasets/{datasetId}", ["PUT"])
@openapi.response(HTTPStatus.OK, DATASET_SCHEMA)
def update_dataset(  # pylint: disable=too-many-arguments
    path: DatasetPath,
    body: UpdateDatasetBody,
    response_dataset_builder: ResponseDatasetBuilder,
    sns_publisher: SnsPublisher,
    requester_identity: RequesterIdentity,
    dataset_validator: DatasetValidator,
    dataset_manager: DatasetManager,
    dataset_participants_manager: DatasetParticipantsManager,
) -> JsonResponse:
    """Update a dataset, if it is visible."""
    old_dataset = dataset_validator.validate_update_dataset_body(
        dataset_id=path.datasetId,
        body=body,
        hub=path.hub,
    )
    participants = dataset_participants_manager.get_updated_participants(
        old_dataset=old_dataset,
        body_engineers=body.engineers,
        body_stewards=body.stewards,
    )

    updated_dataset = dataset_manager.update_dataset(old_dataset=old_dataset, body=body)
    dataset_participants_manager.update_dataset_participants(
        dataset=updated_dataset, participants=participants, requester_identity=requester_identity
    )
    sns_publisher.publish(
        entity_type=EntityType.DATASET,
        operation=Operation.UPDATE,
        payload=updated_dataset,
        message_consistency=MessageConsistency.CONFIRMED,
    )

    return JsonResponse(
        body=unwrap_singleton(
            response_dataset_builder(datasets=[updated_dataset], requester_identity=requester_identity)
        )
    )
