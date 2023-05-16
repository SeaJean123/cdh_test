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
from typing import Sequence
from typing import Tuple

from cdh_core_api.api.openapi_spec.openapi import DataclassSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiTypes
from cdh_core_api.api.openapi_spec.openapi_schemas import ACCOUNT_ROLE_TYPE_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import ACCOUNT_TYPE_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import AFFILIATION_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import BUSINESS_OBJECT_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import HUB_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import LAYER_SCHEMA
from cdh_core_api.api.openapi_spec.openapi_schemas import STAGE_SCHEMA
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.bodies.accounts import AccountRoleBody
from cdh_core_api.bodies.accounts import NewAccountBody
from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.account_manager import AccountManager
from cdh_core_api.services.account_validator import AccountValidator
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.pagination_service import NextPageTokenContext
from cdh_core_api.services.pagination_service import PaginationService
from cdh_core_api.services.response_account_builder import ResponseAccountBuilder
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts import ResponseAccounts
from cdh_core.entities.accounts import ResponseAccountWithoutCosts
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.response import JsonResponse
from cdh_core.primitives.account_id import AccountId

ACCOUNT_ROLE_SCHEMA = OpenApiSchema(
    "AccountRole",
    {
        "name": OpenApiTypes.STRING,
        "path": OpenApiTypes.STRING,
        "type": openapi.link(ACCOUNT_ROLE_TYPE_SCHEMA),
        "friendlyName": OpenApiTypes.STRING,
    },
)

ACCOUNT_SCHEMA = OpenApiSchema(
    "Account",
    {
        "id": OpenApiTypes.STRING,
        "adminRoles": OpenApiTypes.deprecate(OpenApiTypes.array_of(OpenApiTypes.STRING)),
        "affiliation": openapi.link(AFFILIATION_SCHEMA),
        "businessObjects": OpenApiTypes.array_of(openapi.link(BUSINESS_OBJECT_SCHEMA)),
        "costHistory": OpenApiTypes.optional_dictionary(
            description="Historical costs on a per month basis to two decimal places in dollars or yuan",
            value_type="number",
        ),
        "creationDate": OpenApiTypes.DATE_TIME,
        "data": OpenApiTypes.dictionary(
            description="Contains unspecified account data",
            value_type=None,
        ),
        "estimatedCost": OpenApiTypes.optional_float_with_description(
            description="Estimated costs of current month to two decimal places in dollars or yuan"
        ),
        "forecastedCost": OpenApiTypes.optional_float_with_description(
            description="Forecasted costs of current month to two decimal places in dollars or yuan"
        ),
        "friendlyName": OpenApiTypes.STRING,
        "group": OpenApiTypes.optional_string_with_description(
            description="Used for grouping accounts, e.g. in Data Portal Providers or Usecases."
        ),
        "hub": openapi.link(HUB_SCHEMA),
        "layers": OpenApiTypes.array_of(openapi.link(LAYER_SCHEMA)),
        "responsibles": OpenApiTypes.array_of(OpenApiTypes.STRING),
        "requestId": OpenApiTypes.optional_string_with_description(
            description="Id to identify account creation request from Data Portal."
        ),
        "roles": OpenApiTypes.array_of(openapi.link(ACCOUNT_ROLE_SCHEMA)),
        "stages": OpenApiTypes.array_of(openapi.link(STAGE_SCHEMA)),
        "type": openapi.link(ACCOUNT_TYPE_SCHEMA),
        "updateDate": OpenApiTypes.DATE_TIME,
        "visibleInHubs": OpenApiTypes.array_of(openapi.link(HUB_SCHEMA)),
    },
)

ACCOUNT_SCHEMA_WITHOUT_COSTS = OpenApiSchema(
    "AccountWithoutCosts",
    {
        k: v
        for k, v in ACCOUNT_SCHEMA.properties.items()
        if k
        not in {
            "costHistory",
            "estimatedCost",
            "forecastedCost",
        }
    },
)

ACCOUNT_LIST_SCHEMA = OpenApiSchema(
    "Accounts", {"accounts": OpenApiTypes.array_of(openapi.link(ACCOUNT_SCHEMA_WITHOUT_COSTS))}
)

openapi.link(DataclassSchema(AccountRoleBody))


@dataclass(frozen=True)
class AccountQuerySchema:
    """Represents the query parameters that can be used when calling the GET /accounts endpoint."""

    nextPageToken: Optional[str] = None  # pylint: disable=invalid-name


def get_accounts_and_next_page_token(
    query: AccountQuerySchema,
    config: Config,
    visible_data_loader: VisibleDataLoader[GenericAccount, GenericS3Resource, GenericGlueSyncResource],
    pagination_service: PaginationService,
) -> Tuple[Sequence[Account], Optional[str]]:
    """Get the accounts and the next page token."""
    last_evaluated_key = pagination_service.decode_token(
        next_page_token=query.nextPageToken,
        context=NextPageTokenContext.ACCOUNTS,
    )
    accounts, new_last_evaluated_key = visible_data_loader.get_accounts(
        limit=config.result_page_size, last_evaluated_key=last_evaluated_key
    )
    next_page_token = pagination_service.issue_token(
        last_evaluated_key=new_last_evaluated_key,
        context=NextPageTokenContext.ACCOUNTS,
    )
    return accounts, next_page_token


@coreapi.route("/accounts", ["GET"])
@openapi.response(HTTPStatus.OK, ACCOUNT_LIST_SCHEMA)
def get_accounts(
    query: AccountQuerySchema,
    config: Config,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
    pagination_service: PaginationService,
) -> JsonResponse:
    """
    Return all stored information from all visible accounts.

    The response may be truncated and contain only a subset of all visible accounts.
    In that case, a 'nextPageToken' is returned in the response's header.
    This token can be used as a query parameter in a subsequent request to fetch the next 'page' of accounts.
    """
    accounts, next_page_token = get_accounts_and_next_page_token(
        query=query, config=config, visible_data_loader=visible_data_loader, pagination_service=pagination_service
    )
    return JsonResponse(
        body=ResponseAccounts(
            accounts=[account.to_response_account(ResponseAccountWithoutCosts) for account in accounts],
        ),
        next_page_token=next_page_token,
    )


@dataclass(frozen=True)
class AccountPath:
    """Represents the path parameters required to call the GET /accounts/{accountID} endpoint."""

    accountId: AccountId  # pylint: disable=invalid-name


@coreapi.route("/accounts/{accountId}", ["GET"])
@openapi.response(HTTPStatus.OK, ACCOUNT_SCHEMA)
def get_account(
    path: AccountPath,
    account_validator: AccountValidator[Account, UpdateAccountBody],
    response_account_builder: ResponseAccountBuilder[Account],
) -> JsonResponse:
    """Return all stored information of a specific account id, if the account is visible."""
    account = account_validator.get_account(path.accountId)
    response_account = response_account_builder.get_response_account(account)
    return JsonResponse(body=response_account)


@coreapi.route("/accounts", ["POST"])
@openapi.response(HTTPStatus.CREATED, ACCOUNT_SCHEMA)
@openapi.internal_endpoint()
def register_account(  # pylint: disable=too-many-arguments
    body: NewAccountBody,
    account_manager: AccountManager[Account, UpdateAccountBody],
    account_validator: AccountValidator[Account, UpdateAccountBody],
    authorizer: Authorizer[Account],
    sns_publisher: SnsPublisher,
    response_account_builder: ResponseAccountBuilder[Account],
) -> JsonResponse:
    """Register a new account with the CDH."""
    account = body.to_account()
    authorizer.check_requester_may_manage_accounts()
    account_validator.validate_new_account(account)

    account_manager.create(account)

    response_account = response_account_builder.get_response_account(account)
    sns_publisher.publish(
        entity_type=EntityType.ACCOUNT,
        operation=Operation.CREATE,
        payload=response_account,
    )

    return JsonResponse(body=response_account, status_code=HTTPStatus.CREATED)


@coreapi.route("/accounts/{accountId}", ["PUT"])
@openapi.response(HTTPStatus.OK, ACCOUNT_SCHEMA)
@openapi.internal_endpoint()
def update_account(  # pylint: disable=too-many-arguments
    path: AccountPath,
    body: UpdateAccountBody,
    account_manager: AccountManager[Account, UpdateAccountBody],
    account_validator: AccountValidator[Account, UpdateAccountBody],
    authorizer: Authorizer[Account],
    sns_publisher: SnsPublisher,
    response_account_builder: ResponseAccountBuilder[Account],
) -> JsonResponse:
    """Update an account that is registered with the CDH."""
    authorizer.check_requester_may_manage_accounts()
    account = account_validator.get_account(path.accountId)
    account_validator.validate_update_body(account, body)

    updated_account = account_manager.update(path.accountId, body)

    response_account = response_account_builder.get_response_account(updated_account)
    sns_publisher.publish(
        entity_type=EntityType.ACCOUNT,
        operation=Operation.UPDATE,
        payload=response_account,
    )

    return JsonResponse(body=response_account)


@coreapi.route("/accounts/{accountId}", ["DELETE"])
@openapi.internal_endpoint()
def deregister_account(  # pylint: disable=too-many-arguments
    path: AccountPath,
    account_manager: AccountManager[Account, UpdateAccountBody],
    account_validator: AccountValidator[Account, UpdateAccountBody],
    authorizer: Authorizer[Account],
    sns_publisher: SnsPublisher,
    response_account_builder: ResponseAccountBuilder[Account],
) -> JsonResponse:
    """Deregister an account from the CDH."""
    authorizer.check_requester_may_delete_accounts()
    account = account_validator.get_account(path.accountId)
    account_validator.check_account_can_be_deregistered(account)

    account_manager.delete(account)

    response_account = response_account_builder.get_response_account(account)

    sns_publisher.publish(
        entity_type=EntityType.ACCOUNT,
        operation=Operation.DELETE,
        payload=response_account,
    )
    return JsonResponse(status_code=HTTPStatus.NO_CONTENT)
