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
# pylint: disable=redefined-outer-name
from typing import Optional
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest
import requests
from cdh_core_api.services.authorization_api import AuthorizationApi
from requests import HTTPError
from requests.exceptions import ConnectionError as RequestsConnectionError

from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.dataset_participants_test import build_dataset_participant
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.request import Cookie
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import TooManyRequestsError
from cdh_core.exceptions.http import UnauthorizedError
from cdh_core.services.external_api import ExternalApiSession
from cdh_core_dev_tools.testing.builder import Builder


@pytest.fixture()
def requests_session() -> Mock:
    return MagicMock()


@pytest.fixture()
def auth_api(requests_session: Mock) -> AuthorizationApi:
    return AuthorizationApi(
        session=ExternalApiSession(
            request_session_factory=lambda: requests_session,
            api_url="https://authorization.test",
            timeout=(0.4, 0.4),
        ),
        requester=build_arn("iam"),
        jwt=Cookie("jwt", "xyz"),
    )


class TestAuthorizationApi:
    def test_get_visible_hubs_filters_unknown_hubs(self, requests_session: Mock, auth_api: AuthorizationApi) -> None:
        known_hubs = list(Builder.choose_without_repetition(list(Hub), 2))
        requests_session.get.return_value.json.return_value = {
            "hubNames": ["unknown_hub"] + [hub.value for hub in known_hubs]
        }

        result = auth_api.get_visible_hubs()

        assert result == set(known_hubs)
        requests_session.get.assert_called_once_with(
            url="https://authorization.test/hubs/view", params=None, headers=ANY, timeout=ANY, auth=ANY
        )

    @pytest.mark.parametrize("build_dataset_ids", [True, False])
    def test_get_datasets_participants_with_hub_and_list_raises(
        self, requests_session: Mock, auth_api: AuthorizationApi, build_dataset_ids: bool
    ) -> None:
        dataset_ids = [build_dataset_id()] if build_dataset_ids else []
        with pytest.raises(ForbiddenError):
            auth_api.get_datasets_participants(dataset_ids=dataset_ids, hub=build_hub())
        requests_session.assert_not_called()

    def test_get_datasets_participants_with_empty_list(
        self, requests_session: Mock, auth_api: AuthorizationApi
    ) -> None:
        assert auth_api.get_datasets_participants(dataset_ids=[]) == {}
        requests_session.assert_not_called()

    @pytest.mark.parametrize("request_dataset_ids, hub", [(True, None), (False, Hub("global"))])
    def test_get_datasets_participants(
        self, requests_session: Mock, auth_api: AuthorizationApi, request_dataset_ids: bool, hub: Optional[Hub]
    ) -> None:
        dataset_ids = [build_dataset_id() for _ in range(2)]
        participants = [build_dataset_participant() for _ in range(3)]
        requested_dataset_ids = dataset_ids if request_dataset_ids else None
        body = {
            "datasetIds": requested_dataset_ids,
            "hub": hub.value if hub else None,
            "participantTypes": ["steward", "engineer"],
        }
        requests_session.post.return_value.json.return_value = {
            "participants": {
                dataset_ids[0]: {
                    "stewards": [],
                    "engineers": [
                        {"mail": participants[0].id, "idp": participants[0].idp},
                        {"mail": participants[1].id, "idp": participants[1].idp},
                    ],
                    "contributors": None,
                },
                dataset_ids[1]: {
                    "stewards": [{"mail": participants[2].id, "idp": participants[2].idp}],
                    "engineers": [],
                    "contributors": None,
                },
            }
        }

        result = auth_api.get_datasets_participants(dataset_ids=requested_dataset_ids, hub=hub)

        assert result == {
            dataset_ids[0]: DatasetParticipants(
                stewards=[],
                engineers=participants[:2],
            ),
            dataset_ids[1]: DatasetParticipants(
                stewards=participants[2:],
                engineers=[],
            ),
        }
        requests_session.post.assert_called_once_with(
            url="https://authorization.test/users/dataset/participants",
            json=body,
            headers=ANY,
            timeout=ANY,
            auth=ANY,
        )

    def test_user_token_invalid_rewrite_exception(self, requests_session: Mock, auth_api: AuthorizationApi) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.content = "Unauthorized for url"
        requests_session.get.side_effect = requests.HTTPError(response=mock_response)

        with pytest.raises(UnauthorizedError):
            auth_api.get_visible_account_ids()

    def test_other_http_exception_not_rewritten(self, requests_session: Mock, auth_api: AuthorizationApi) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.content = "error"
        requests_session.get.side_effect = requests.HTTPError(response=mock_response)

        with pytest.raises(HTTPError):
            auth_api.get_visible_account_ids()

    def test_connection_error_rewritten_to_too_many_requests_error(
        self, requests_session: Mock, auth_api: AuthorizationApi
    ) -> None:
        requests_session.get.side_effect = RequestsConnectionError()

        with pytest.raises(TooManyRequestsError):
            auth_api.get_visible_account_ids()

    def test_general_exception_not_rewritten(self, requests_session: Mock, auth_api: AuthorizationApi) -> None:
        error = Exception("my error")
        requests_session.get.side_effect = error

        with pytest.raises(Exception) as exc_info:
            auth_api.get_visible_account_ids()
        assert exc_info.value == error

    def test_get_visibile_dataset_ids_via_hub_and_list_fails(self, auth_api: AuthorizationApi) -> None:
        with pytest.raises(ForbiddenError):
            auth_api.get_visible_dataset_ids(Hub("global"), [build_dataset_id()])

    def test_get_visibile_dataset_ids_via_hub(self, requests_session: Mock, auth_api: AuthorizationApi) -> None:
        dataset_ids = {build_dataset_id() for _ in range(3)}
        requests_session.get.return_value.json.return_value = {"datasetIds": dataset_ids}
        assert auth_api.get_visible_dataset_ids(Hub("global")) == dataset_ids

    def test_get_visibile_dataset_ids_via_list(self, requests_session: Mock, auth_api: AuthorizationApi) -> None:
        dataset_ids = {build_dataset_id() for _ in range(3)}
        requests_session.get.return_value.json.return_value = {
            "datasets": [{"id": dataset_id} for dataset_id in dataset_ids]
        }
        assert auth_api.get_visible_dataset_ids(dataset_ids=dataset_ids) == dataset_ids
