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
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from urllib.parse import quote

from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.hub_business_object import HubBusinessObject
from cdh_core.entities.request import RequesterIdentity
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.hubs import Hub
from cdh_core.services.external_api import ExternalApiSession

LOG = getLogger(__name__)


class UsersApi:
    """Handle calls to the Users API which is responsible for storing and retrieving permissions for users."""

    def __init__(self, session: ExternalApiSession):
        self._session = session

    def get_all_hub_business_objects(self, hub: Hub) -> Dict[str, HubBusinessObject]:
        """Get all business objects in a certain hub."""
        response = self._session.get(
            f"/namespaces/business_object/permissions?param={quote(f'hub:{hub.value}')}", headers={}
        )
        return self._response_to_hub_business_object_dict(response["permissions"])

    def get_hub_business_object(self, hub: Hub, business_object: BusinessObject) -> Optional[HubBusinessObject]:
        """
        Get a certain business object in a certain hub.

        Method returns None if the business object does not exist in the hub.
        """
        response = self._session.get(
            (
                f"/namespaces/business_object/permissions?param={quote(f'hub:{hub.value}')}"
                f"&param={quote(f'business_object:{business_object.value}')}"
            ),
            headers={},
        )
        hub_business_objects = self._response_to_hub_business_object_dict(response["permissions"])
        return hub_business_objects.get(business_object.value)

    @staticmethod
    def _response_to_hub_business_object_dict(permissions: List[Dict[str, Any]]) -> Dict[str, HubBusinessObject]:
        hub_business_objects = {}
        for permission in permissions:
            business_object = permission["permission"]["parameters"]["business_object"]

            if business_object not in [bo.value for bo in BusinessObject]:
                LOG.error(f"Unknown business object in auth api response: {business_object}. Ignoring it...")
                continue

            if business_object not in hub_business_objects:
                hub_business_objects[business_object] = HubBusinessObject.get_default_hub_business_object(
                    hub=Hub(permission["permission"]["parameters"]["hub"]),
                    business_object=BusinessObject(business_object),
                )

            hub_business_objects[business_object].responsibles.append(permission["subject"]["id"])

        return hub_business_objects

    @staticmethod
    def _build_permission_body(dataset_id: DatasetId, action: str) -> Dict[str, Any]:
        return {
            "namespace": "dataset",
            "action": action,
            "parameters": {
                "platform": "cdh",
                "id": dataset_id,
            },
        }

    def put_dataset_participants(
        self,
        dataset_id: DatasetId,
        engineers: List[DatasetParticipant],
        stewards: List[DatasetParticipant],
        requester_identity: RequesterIdentity,
    ) -> None:
        """Update the participants of a given dataset."""

        def build_update_body(action: str, participants: List[DatasetParticipant]) -> Dict[str, Any]:
            return {
                "exclusive": True,
                "permission": UsersApi._build_permission_body(dataset_id, action),
                "subjects": [
                    {
                        "id": participant.id,
                        "idp": participant.idp,
                        "type": "user",
                    }
                    for participant in participants
                ],
            }

        requester_information = requester_identity.jwt_user_id or requester_identity.arn
        self._session.post(
            path="/bulk",
            headers={"X-Portal-Reason": f"CDH Core API called by {requester_information}"},
            json={"updates": [build_update_body("engineer", engineers), build_update_body("steward", stewards)]},
        )

    def delete_dataset_participants(self, dataset_id: DatasetId) -> None:
        """
        Delete all dataset participants for a dataset.

        Should usually only be called during dataset deletion.
        """

        def build_delete_body(action: str) -> Dict[str, Any]:
            return {"permission": UsersApi._build_permission_body(dataset_id, action), "subjects": None}

        self._session.delete(
            path="/bulk", headers={}, json={"deletes": [build_delete_body("engineer"), build_delete_body("steward")]}
        )
