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
import random
from http import HTTPStatus
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from urllib3.util.retry import Retry

from cdh_core.entities.accounts import Credentials
from cdh_core.enums.http import HttpVerb
from cdh_core.services.external_api import ApiSessionBuilder
from cdh_core.services.external_api import ExternalApiSession
from cdh_core.services.external_api import get_retry_config
from cdh_core.services.external_api import UnusableSession
from cdh_core.services.external_api import VerboseSession
from cdh_core_dev_tools.testing.builder import Builder


class TestVerboseSession:
    def setup_method(self) -> None:
        self.url = Builder.build_random_url()
        self.identifier_key = Builder.build_random_string()
        self.identifier_value = Builder.build_random_string()
        self.verbose_session = VerboseSession(
            log_identifier_key=self.identifier_key, log_identifier_value=self.identifier_value
        )

    @pytest.mark.parametrize("method", sorted(HttpVerb, key=str))
    def test_response_unaltered(self, requests_mock: Mock, method: HttpVerb) -> None:
        requests_mock.request(
            method=method.value,
            url=self.url,
            json={"foo": "bar"},
            status_code=HTTPStatus.OK,
        )

        response = self.verbose_session.request(method=method.value, url=self.url)
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"foo": "bar"}

    def test_log_statement(self, requests_mock: Mock) -> None:
        status_code = Builder.get_random_element(list(HTTPStatus))
        requests_mock.request(
            method="GET",
            url=self.url,
            status_code=status_code,
        )
        with patch("cdh_core.services.external_api.LOG") as logmock:
            self.verbose_session.get(self.url)
            logmock.info.assert_called_once()
            log_entry = logmock.info.call_args.args[0]
            assert log_entry[self.identifier_key] == self.identifier_value
            assert log_entry["http_method"] == "GET"
            assert log_entry["status_code"] == status_code
            assert log_entry["url"] in {self.url, self.url + "/"}
            assert log_entry["elapsed_ms"] > 0


def test_api_session_builder() -> None:
    api_name = Builder.build_random_string()
    retry_config = Mock(Retry)
    api_session_builder = ApiSessionBuilder(api_name=api_name, retry_config=retry_config)

    session = api_session_builder.get_session()

    assert isinstance(session, VerboseSession)
    assert api_session_builder.get_session().adapters["https://"].max_retries == retry_config


def test_get_retry_config_no_retry_on_post() -> None:
    retries = random.randint(1, 5)
    backoff_factor = random.uniform(0.5, 2)
    assert "POST" not in (
        get_retry_config(retries, backoff_factor, retry_on_post=False).allowed_methods  # type: ignore
    )


def test_get_retry_config_with_retry_on_post() -> None:
    retries = random.randint(1, 5)
    backoff_factor = random.uniform(0.5, 2)
    assert "POST" in iter(get_retry_config(retries, backoff_factor, retry_on_post=True).allowed_methods)  # type: ignore


def test_unusable_session() -> None:
    some_mock = Mock()
    some_mock.method = "POST"
    some_mock.url = "test"
    with pytest.raises(RuntimeError, match="Should not have called POST on test"):
        UnusableSession().send(some_mock)


class TestExternalApiSession:
    def setup_method(self) -> None:
        self.requests = Mock()
        self.request_session_factory = lambda: self.requests
        self.api_url = Builder.build_random_url()
        self.timeout_config = (random.uniform(0.5, 2), random.uniform(0.5, 2))
        self.external_api_session = ExternalApiSession(
            self.request_session_factory, self.api_url, self.timeout_config, None
        )
        self.path = Builder.build_random_string()
        self.headers = {Builder.build_random_string(): Builder.build_random_string() for _ in range(3)}
        self.json = {Builder.build_random_string(): Builder.build_random_string() for _ in range(3)}

    @pytest.mark.parametrize("with_credentials", [True, False])
    def test_get(self, with_credentials: bool) -> None:
        if with_credentials:
            credentials = Credentials(
                Builder.build_random_string(), Builder.build_random_string(), Builder.build_random_string()
            )
            self.external_api_session = ExternalApiSession(
                self.request_session_factory, self.api_url, self.timeout_config, credentials
            )
        parameters = {"a": "b"}
        mocked_response = Mock()
        self.requests.get.return_value = mocked_response
        response = self.external_api_session.get(self.path, self.headers, parameters)

        assert response == mocked_response.json.return_value
        self.requests.get.assert_called_once_with(
            url=self.api_url + self.path,
            headers=self.headers,
            timeout=self.timeout_config,
            auth=ANY,
            params=parameters,
        )
        mocked_response.raise_for_status.assert_called_once()

    def test_post_http_status_no_content(self) -> None:
        mocked_response = MagicMock(status_code=HTTPStatus.NO_CONTENT)
        self.requests.post.return_value = mocked_response
        response = self.external_api_session.post(self.path, self.headers, self.json)

        assert response == {}
        self.requests.post.assert_called_once_with(
            url=self.api_url + self.path, headers=self.headers, timeout=self.timeout_config, auth=ANY, json=self.json
        )
        mocked_response.raise_for_status.assert_called_once()
        mocked_response.json.assert_not_called()

    def test_post_other_http_status(self) -> None:
        mocked_response = Mock()
        self.requests.post.return_value = mocked_response
        response = self.external_api_session.post(self.path, self.headers, self.json)

        assert response == mocked_response.json.return_value
        self.requests.post.assert_called_once_with(
            url=self.api_url + self.path, headers=self.headers, timeout=self.timeout_config, auth=ANY, json=self.json
        )
        mocked_response.raise_for_status.assert_called_once()

    def test_delete(self) -> None:
        mocked_response = Mock()
        self.requests.delete.return_value = mocked_response
        self.external_api_session.delete(self.path, self.headers, self.json)

        self.requests.delete.assert_called_once_with(
            url=self.api_url + self.path, headers=self.headers, timeout=self.timeout_config, auth=ANY, json=self.json
        )
        mocked_response.raise_for_status.assert_called_once()
