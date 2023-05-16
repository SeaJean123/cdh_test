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
import logging
from http import HTTPStatus
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeVar

from requests import HTTPError
from requests.exceptions import ConnectionError as RequestsConnectionError

from cdh_core.decorators import decorate_class
from cdh_core.entities.arn import Arn
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.request import Cookie
from cdh_core.entities.resource import Resource
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import TooManyRequestsError
from cdh_core.exceptions.http import UnauthorizedError
from cdh_core.primitives.account_id import AccountId
from cdh_core.services.external_api import ExternalApiSession

LOG = logging.getLogger(__name__)
AnyFunc = TypeVar("AnyFunc", bound=Callable[..., Any])


def catch_exceptions(func: AnyFunc) -> AnyFunc:
    """Construct a decorator to convert certain exceptions.

    conversions:
    * ConnectionError to TooManyRequestsError (as this error occurs when the auth api receives a request burst that
      causes high latencies)
    * UNAUTHORIZED status code to UnauthorizedError
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except RequestsConnectionError as err:
            raise TooManyRequestsError("Too many requests. Please try again in a few seconds.") from err
        except HTTPError as err:
            if err.response.status_code == HTTPStatus.UNAUTHORIZED:
                raise UnauthorizedError("Authorization failed due to invalid credentials") from err
            raise

    return cast(AnyFunc, wrapper)


class BaseAuthorizationApi:
    """All subclasses of BaseAuthorizationApi automatically convert certain errors."""

    def __init_subclass__(cls, **kwargs: Any) -> None:  # pylint: disable=unused-argument
        """Decorate all subclasses of BaseAuthorizationApi with catch_exceptions."""
        decorate_class(cls=cls, decorator=catch_exceptions)


class AuthorizationApi(BaseAuthorizationApi):
    """Handles calls to the Authorization API which manages the permissions and visibility of cdh core entities.

    The AuthorizationApi will be instantiated once per request to include the request's JWT whereas its session will be
    instantiated only once per lambda instance to hold a persistent HTTP session (important for HTTP keep-alives).
    """

    def __init__(self, session: ExternalApiSession, requester: Arn, jwt: Optional[Cookie]):
        self._session = session
        self._headers = self._get_headers(requester, jwt)

    def _get(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        return self._session.get(path, self._headers, params)

    def get_visible_account_ids(self) -> Set[AccountId]:
        """Return the ids of all accounts that the requester is allowed to see."""
        response = self._get("/accounts/view")
        return set(response["accountIds"])

    def is_account_visible(self, account_id: AccountId) -> bool:
        """Check whether the requester is allowed to see the given account."""
        response = self._get(
            f"/accounts/view/{account_id}",
        )
        return bool(response["granted"])

    def is_account_group_updatable(self, group_id: str) -> Tuple[bool, str]:
        """Check whether the requester is allowed to manage the given account group."""
        response = self._get(f"/environmentGroups/managePermissions/{group_id}")
        return bool(response["granted"]), response["message"]

    def get_visible_dataset_ids(
        self, hub: Optional[Hub] = None, dataset_ids: Optional[Iterable[DatasetId]] = None
    ) -> Set[DatasetId]:
        """Return the ids of all datasets that the requester is allowed to see."""
        if hub is not None and dataset_ids is not None:
            raise ForbiddenError("Filtering by both Hub and DatasetIds is not allowed.")
        if dataset_ids is not None:
            response = self._get("/datasets", {"dataset_ids": ",".join(dataset_ids)})
            return set(DatasetId(dataset["id"]) for dataset in response["datasets"])
        response = self._get("/datasets/view", {"hub": hub.value} if hub else None)
        return set(DatasetId(dataset_id) for dataset_id in response["datasetIds"])

    def is_dataset_visible(self, dataset_id: DatasetId) -> bool:
        """Check whether the requester is allowed to see the given dataset."""
        response = self._get(f"/datasets/view/{dataset_id}")
        return bool(response["granted"])

    def get_visible_hubs(self) -> Set[Hub]:
        """Return all hubs that the requester is allowed to see."""
        response = self._get("/hubs/view")
        result = set()
        for hub_name in response["hubNames"]:
            try:
                hub = Hub(hub_name)
                result.add(hub)
            except ValueError:
                LOG.warning(f"Unknown value for hub name: {hub_name!r}")
        return result

    def is_hub_visible(self, hub: Hub) -> bool:
        """Check whether the requester is allowed to see the given hub."""
        response = self._get(f"/hubs/view/{hub.value}")
        return bool(response["granted"])

    def is_dataset_creatable(  # pylint: disable=too-many-arguments
        self,
        layer: Layer,
        hub: Hub,
        business_object: BusinessObject,
        owner_account_id: AccountId,
    ) -> Tuple[bool, str]:
        """Check whether a dataset with the given parameters may be created by the requester."""
        query_params: Dict[str, str] = {
            "layer": layer.value,
            "hub": hub.value,
            "business_object": business_object.value,
            "owner_id": owner_account_id,
        }
        response = self._get("/datasets/create", query_params)
        return response["granted"], response["message"]

    def is_dataset_updatable(self, dataset_id: DatasetId) -> Tuple[bool, str]:
        """Check whether a dataset may be updated by the requester."""
        response = self._get(
            f"/datasets/update/{dataset_id}",
        )
        return response["granted"], response["message"]

    def is_dataset_deletable(self, dataset_id: DatasetId) -> Tuple[bool, str]:
        """Check whether a dataset may be deleted by the requester."""
        response = self._get(f"/datasets/delete/{dataset_id}")
        return response["granted"], response["message"]

    def is_resource_updatable(self, resource: Resource) -> Tuple[bool, str]:
        """Check whether a resource may be updated by the requester."""
        response = self._get(
            f"/resources/update/{resource.type.value}/{resource.dataset_id}/{resource.stage.value}/"
            f"{resource.region.value}",
        )
        return response["granted"], response["message"]

    def is_resource_deletable(self, resource: Resource) -> Tuple[bool, str]:
        """Check whether a resource may be deleted by the requester."""
        response = self._get(
            f"/resources/delete/{resource.type.value}/{resource.dataset_id}/{resource.stage.value}/"
            f"{resource.region.value}",
        )
        return response["granted"], response["message"]

    def is_resource_creatable(  # pylint: disable=too-many-arguments
        self,
        resource_type: ResourceType,
        dataset_id: DatasetId,
        stage: Stage,
        region: Region,
        owner_account_id: AccountId,
    ) -> Tuple[bool, str]:
        """Check whether a resource with the given parameters may be created by the requester."""
        query_params: Dict[str, str] = {
            "type": resource_type.value,
            "dataset_id": dataset_id,
            "stage": stage.value,
            "region": region.value,
            "ignore_existing": "True",
            "owner_id": owner_account_id,
        }
        response = self._get("/resources/create", query_params)
        return response["granted"], response["message"]

    def get_user_id(self) -> Optional[str]:
        """Get the user id of the requester from the jwt."""
        response = self._get(
            "/requester",
        )
        if response.get("user"):
            return str(response["user"]["id"])
        return None

    def get_datasets_participants(
        self, dataset_ids: Optional[List[DatasetId]] = None, hub: Optional[Hub] = None
    ) -> Dict[DatasetId, DatasetParticipants]:
        """Get the participants for the given datasets."""
        if dataset_ids is not None and hub:
            raise ForbiddenError("Filtering by both Hub and DatasetIds is not allowed.")
        if not dataset_ids and not hub:
            return {}

        body = {
            "datasetIds": dataset_ids,
            "hub": hub.value if hub else None,
            "participantTypes": ["steward", "engineer"],
        }
        response = self._session.post(path="/users/dataset/participants", headers=self._headers, json=body)
        return {
            dataset_id: DatasetParticipants(
                engineers=[
                    DatasetParticipant(engineer["mail"], engineer["idp"]) for engineer in participants["engineers"]
                ],
                stewards=[DatasetParticipant(steward["mail"], steward["idp"]) for steward in participants["stewards"]],
            )
            for dataset_id, participants in response["participants"].items()
        }

    @classmethod
    def _get_headers(cls, requester: Arn, jwt: Optional[Cookie]) -> Dict[str, str]:
        cookie_header = {"Cookie": jwt.encode()} if jwt else {}
        return {"X-FORWARDED-CALLER-ARN": str(requester), **cookie_header}
