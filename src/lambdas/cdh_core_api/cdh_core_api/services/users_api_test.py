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
from typing import Any
from typing import Dict
from typing import Union
from unittest.mock import ANY
from unittest.mock import Mock
from urllib.parse import quote

import pytest
from cdh_core_api.services.users_api import LOG
from cdh_core_api.services.users_api import UsersApi

from cdh_core.entities.dataset_participants_test import build_dataset_participants
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.hub_business_object import HubBusinessObject
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core_dev_tools.testing.builder import Builder

IDP = Builder.build_random_string()


@pytest.fixture(name="external_api_session")
def fixture_external_api_session() -> Mock:
    return Mock()


@pytest.fixture(name="users_api")
def fixture_users_api(external_api_session: Mock) -> UsersApi:
    return UsersApi(external_api_session)


class TestUsersApi:
    def _build_business_object_permission(
        self, user: str, hub: Hub, business_object: Union[BusinessObject, str]
    ) -> Dict[str, Any]:
        business_object_value = business_object if isinstance(business_object, str) else business_object.value
        return {
            "subject": {"type": "user", "id": user, "idp": IDP},
            "permission": {
                "namespace": "business_object",
                "action": "governor",
                "parameters": {"hub": hub.value, "business_object": business_object_value},
            },
        }

    def test_get_hub_business_object(self, external_api_session: Mock, users_api: UsersApi) -> None:
        user_1 = Builder.build_random_string()
        user_2 = Builder.build_random_string()
        hub = build_hub()
        business_object = build_business_object()

        external_api_session.get.return_value = {
            "permissions": [
                self._build_business_object_permission(user_1, hub, business_object),
                self._build_business_object_permission(user_2, hub, business_object),
            ]
        }

        result = users_api.get_hub_business_object(hub, business_object)

        expected = HubBusinessObject(
            hub=hub,
            business_object=business_object,
            friendly_name=business_object.friendly_name,
            responsibles=[user_1, user_2],
        )

        assert result == expected
        external_api_session.get.assert_called_once_with(
            (
                f"/namespaces/business_object/permissions?param={quote(f'hub:{hub.value}')}"
                f"&param={quote(f'business_object:{business_object.value}')}"
            ),
            headers={},
        )

    def test_get_nonexistent_hub_business_object(self, external_api_session: Mock, users_api: UsersApi) -> None:
        hub = build_hub()
        business_object = build_business_object()

        external_api_session.get.return_value = {"permissions": []}

        result = users_api.get_hub_business_object(hub=hub, business_object=business_object)

        assert result is None
        external_api_session.get.assert_called_once_with(
            (
                f"/namespaces/business_object/permissions?param={quote(f'hub:{hub.value}')}"
                f"&param={quote(f'business_object:{business_object.value}')}"
            ),
            headers={},
        )

    def test_get_all_hub_business_objects(self, external_api_session: Mock, users_api: UsersApi) -> None:
        LOG.error = Mock()  # type: ignore
        user_1 = Builder.build_random_string()
        user_2 = Builder.build_random_string()
        hub = build_hub()
        business_object_1 = build_business_object()
        business_object_2 = business_object_1
        while business_object_1 == business_object_2:
            business_object_2 = build_business_object()

        external_api_session.get.return_value = {
            "permissions": [
                self._build_business_object_permission(user_1, hub, business_object_1),
                self._build_business_object_permission(user_2, hub, business_object_1),
                self._build_business_object_permission(user_1, hub, business_object_2),
            ]
        }

        result = users_api.get_all_hub_business_objects(hub)

        expected = {
            business_object_1.value: HubBusinessObject(
                hub=hub,
                business_object=business_object_1,
                friendly_name=business_object_1.friendly_name,
                responsibles=[user_1, user_2],
            ),
            business_object_2.value: HubBusinessObject(
                hub=hub,
                business_object=business_object_2,
                friendly_name=business_object_2.friendly_name,
                responsibles=[user_1],
            ),
        }

        assert result == expected
        external_api_session.get.assert_called_once_with(
            f"/namespaces/business_object/permissions?param={quote(f'hub:{hub.value}')}",
            headers={},
        )
        LOG.error.assert_not_called()

    def test_get_all_ignores_unknown_business_object(self, external_api_session: Mock, users_api: UsersApi) -> None:
        LOG.error = Mock()  # type: ignore

        user = Builder.build_random_string()
        hub = build_hub()
        known_business_object = build_business_object()
        unknown_business_object_value = Builder.build_random_string()

        external_api_session.get.return_value = {
            "permissions": [
                self._build_business_object_permission(user, hub, known_business_object),
                self._build_business_object_permission(user, hub, unknown_business_object_value),
            ]
        }

        result = users_api.get_all_hub_business_objects(hub)

        expected = {
            known_business_object.value: HubBusinessObject(
                hub=hub,
                business_object=known_business_object,
                friendly_name=known_business_object.friendly_name,
                responsibles=[user],
            ),
        }

        assert result == expected
        external_api_session.get.assert_called_once_with(
            f"/namespaces/business_object/permissions?param={quote(f'hub:{hub.value}')}",
            headers={},
        )
        LOG.error.assert_called_once()

    def test_put_dataset_participants(self, external_api_session: Mock, users_api: UsersApi) -> None:
        dataset_id = build_dataset_id()
        participants = build_dataset_participants()
        requester_information = build_requester_identity(jwt_user_id=None)
        expected_body = {
            "updates": [
                {
                    "exclusive": True,
                    "permission": {
                        "namespace": "dataset",
                        "action": "engineer",
                        "parameters": {
                            "platform": "cdh",
                            "id": dataset_id,
                        },
                    },
                    "subjects": [
                        {
                            "id": participant.id,
                            "idp": participant.idp,
                            "type": "user",
                        }
                        for participant in participants.engineers
                    ],
                },
                {
                    "exclusive": True,
                    "permission": {
                        "namespace": "dataset",
                        "action": "steward",
                        "parameters": {
                            "platform": "cdh",
                            "id": dataset_id,
                        },
                    },
                    "subjects": [
                        {
                            "id": participant.id,
                            "idp": participant.idp,
                            "type": "user",
                        }
                        for participant in participants.stewards
                    ],
                },
            ]
        }

        users_api.put_dataset_participants(
            dataset_id, participants.engineers, participants.stewards, requester_information
        )

        external_api_session.post.assert_called_once_with(
            path="/bulk",
            headers={"X-Portal-Reason": ANY},
            json=expected_body,
        )

    @pytest.mark.parametrize(
        "requester_information",
        [
            build_requester_identity(jwt_user_id=None),
            build_requester_identity(jwt_user_id=Builder.build_random_string()),
        ],
    )
    def test_put_dataset_participants_fills_reason_header(
        self, external_api_session: Mock, users_api: UsersApi, requester_information: RequesterIdentity
    ) -> None:
        dataset_id = build_dataset_id()
        participants = build_dataset_participants()

        users_api.put_dataset_participants(
            dataset_id, participants.engineers, participants.stewards, requester_information
        )

        expected_reason_header = (
            f"CDH Core API called by {requester_information.jwt_user_id}"
            if requester_information.jwt_user_id
            else f"CDH Core API called by {requester_information.arn}"
        )
        external_api_session.post.assert_called_once_with(
            path="/bulk",
            headers={"X-Portal-Reason": expected_reason_header},
            json=ANY,
        )

    def test_delete_dataset_participants(self, external_api_session: Mock, users_api: UsersApi) -> None:
        dataset_id = build_dataset_id()
        expected_body = {
            "deletes": [
                {
                    "permission": {
                        "namespace": "dataset",
                        "action": "engineer",
                        "parameters": {
                            "platform": "cdh",
                            "id": dataset_id,
                        },
                    },
                    "subjects": None,
                },
                {
                    "permission": {
                        "namespace": "dataset",
                        "action": "steward",
                        "parameters": {
                            "platform": "cdh",
                            "id": dataset_id,
                        },
                    },
                    "subjects": None,
                },
            ]
        }

        users_api.delete_dataset_participants(dataset_id)

        external_api_session.delete.assert_called_once_with(path="/bulk", headers={}, json=expected_body)
