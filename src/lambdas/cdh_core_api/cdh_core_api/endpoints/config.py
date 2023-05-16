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
from http import HTTPStatus
from typing import Any
from typing import Dict

from cdh_core_api.api.openapi_spec.openapi import OpenApiSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiTypes
from cdh_core_api.api.openapi_spec.openapi_schemas import HUB_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import REGION_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import STAGE_SCHEMA
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.config import Config
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.accounts import Account
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.hubs import Hub

ENUM_KEY_SCHEMA = OpenApiSchema("EnumKey", {"value": OpenApiTypes.STRING, "friendlyName": OpenApiTypes.STRING})
HUB_DESCRIPTION_SCHEMA = OpenApiSchema(
    "HubDescription",
    {
        "name": openapi.link(HUB_SCHEMA),
        "friendlyName": OpenApiTypes.STRING,
        "resourceAccounts": OpenApiTypes.array_of(
            {"type": "object", "properties": {"id": OpenApiTypes.STRING, "stage": openapi.link(STAGE_SCHEMA)}}
        ),
        "regions": OpenApiTypes.array_of(openapi.link(REGION_SCHEMA)),
    },
)
CONFIG_SCHEMA = OpenApiSchema(
    "Config",
    {
        "enums": {"type": "object", "additionalProperties": OpenApiTypes.array_of(openapi.link(ENUM_KEY_SCHEMA))},
        "hubs": OpenApiTypes.array_of(openapi.link(HUB_DESCRIPTION_SCHEMA)),
    },
)


@coreapi.route("/config", ["GET"])
@openapi.internal_endpoint()
@openapi.response(HTTPStatus.OK, CONFIG_SCHEMA)
def get_config(
    config: Config, visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource]
) -> JsonResponse:
    """Return the configuration of the CDH Core API."""
    enums = {
        enum.__name__: [
            {"value": value.value, "friendlyName": value.friendly_name}  # type: ignore
            if enum.__name__ != "Affiliation"
            else {
                "value": value.value,
                "friendlyName": value.friendly_name,  # type: ignore
                "accessManagement": value.access_management,  # type: ignore
            }
            for value in enum
        ]
        for enum in config.ENUMS_TO_EXPOSE
    }
    body = {
        "enums": enums,
        "hubs": [_hub_to_dict(config, hub) for hub in visible_data_loader.get_hubs()],
    }
    return JsonResponse(body=body)


def _hub_to_dict(config: Config, hub: Hub) -> Dict[str, Any]:
    resource_accounts_in_hub = set(
        config.account_store.query_resource_accounts(environments=config.environment, hubs=hub)
    )
    return {
        "name": hub.value,
        "friendlyName": hub.friendly_name,
        "resourceAccounts": sorted(
            [{"id": account.id, "stage": account.stage.value} for account in resource_accounts_in_hub],
            key=lambda x: x["id"],  # type:ignore
        ),
        "regions": sorted([region.value for region in hub.regions]),
        "partition": hub.partition.value,
    }
