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
from cdh_core_api.api.openapi_spec.openapi_schemas import BUSINESS_OBJECT_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import HUB_SCHEMA
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.config import Config
from cdh_core_api.services.users_api import UsersApi
from cdh_core_api.services.visibility_check import VisibilityCheck
from cdh_core_api.validation.common_paths import HubPath

from cdh_core.entities.hub_business_object import HubBusinessObject
from cdh_core.entities.hub_business_object import HubBusinessObjectList
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.hubs import Hub
from cdh_core.exceptions.http import NotFoundError

HUB_BUSINESS_OBJECT_SCHEMA = OpenApiSchema(
    "HubBusinessObject",
    {
        "hub": openapi.link(HUB_SCHEMA),
        "businessObject": openapi.link(BUSINESS_OBJECT_SCHEMA),
        "friendlyName": OpenApiTypes.STRING,
        "responsibles": OpenApiTypes.array_of(OpenApiTypes.STRING),
    },
)

HUB_BUSINESS_OBJECT_LIST_SCHEMA = OpenApiSchema(
    "HubBusinessObjects", {"businessObjects": OpenApiTypes.array_of(openapi.link(HUB_BUSINESS_OBJECT_SCHEMA))}
)


@coreapi.route("/{hub}/businessObjects", ["GET"])
@openapi.response(HTTPStatus.OK, HUB_BUSINESS_OBJECT_LIST_SCHEMA)
def get_business_objects(
    config: Config, users_api: UsersApi, visibility_check: VisibilityCheck, path: HubPath
) -> JsonResponse:
    """
    List all business objects in a hub.

    The response may be truncated and contain only a subset of all visible business objects.
    In that case, a 'nextPageToken' is returned in the response's header.
    This token can be used as a query parameter in a subsequent request to fetch the next 'page' of business objects.
    """
    if not visibility_check.get_hub_visibility_check(batch=False)(path.hub):
        raise NotFoundError(f"Hub {path.hub.value} not found.")

    hub_business_objects = users_api.get_all_hub_business_objects(path.hub) if config.using_authorization_api else {}

    return JsonResponse(
        body=HubBusinessObjectList(
            [
                hub_business_object
                if (hub_business_object := hub_business_objects.get(business_object.value))
                else HubBusinessObject.get_default_hub_business_object(path.hub, business_object)
                for business_object in BusinessObject
            ]
        )
    )


@dataclass(frozen=True)
class BusinessObjectPath:
    """Represents the path parameters required to call the GET /{hub}/businessObjects/{businessObject} endpoint."""

    hub: Hub
    businessObject: BusinessObject  # pylint: disable=invalid-name


@coreapi.route("/{hub}/businessObjects/{businessObject}", ["GET"])
@openapi.response(HTTPStatus.OK, HUB_BUSINESS_OBJECT_SCHEMA)
def get_business_object(
    config: Config, users_api: UsersApi, visibility_check: VisibilityCheck, path: BusinessObjectPath
) -> JsonResponse:
    """Return a specific businessObject in a hub."""
    if not visibility_check.get_hub_visibility_check(batch=False)(path.hub):
        raise NotFoundError(f"Hub {path.hub.value} not found.")
    hub_business_object = (
        users_api.get_hub_business_object(hub=path.hub, business_object=path.businessObject)
        if config.using_authorization_api
        else None
    )
    if hub_business_object is None:
        hub_business_object = HubBusinessObject.get_default_hub_business_object(
            hub=path.hub, business_object=path.businessObject
        )
    return JsonResponse(body=hub_business_object)
