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
from typing import Any
from typing import Dict
from typing import List
from typing import Union
from unittest.mock import Mock

import pytest
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ProxyError
from requests.exceptions import Timeout
from requests_mock.mocker import Mocker

from cdh_core.clients.http_client import HttpClient
from cdh_core.clients.http_client import HttpStatusCodeNotInExpectedCodes
from cdh_core.clients.http_client import MaximumFetchesExceeded
from cdh_core.clients.http_client import NonRetryableConflictError
from cdh_core.clients.http_client import ResponseTooSmall
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


# pylint: disable=protected-access
class TestHttpClient:
    def setup_method(self) -> None:
        self.base_url = "https://example.com"
        self.auth = Mock(BotoAWSRequestsAuth)
        self.http_client = HttpClient(base_url=self.base_url, credentials=self.auth)
        self.http_client.default_seconds_between_retries = 0
        self.params: Dict[str, Union[str, List[str]]] = {
            **{Builder.build_random_string(): Builder.build_random_string() for _ in range(random.randint(1, 5))},
            **{
                Builder.build_random_string(): [Builder.build_random_string() for _ in range(random.randint(1, 5))]
                for _ in range(random.randint(1, 5))
            },
        }
        self.path = f"/{Builder.build_random_string()}"
        self.full_url = self.base_url + self.path

    def test_get(self, requests_mock: Mocker) -> None:
        expected_response_data = {
            Builder.build_random_string(): Builder.build_random_string() for _ in range(random.randint(1, 5))
        }
        requests_mock.request(method="GET", url=self.full_url, status_code=HTTPStatus.OK, json=expected_response_data)
        actual_response = self.http_client.get(
            path=self.path,
            expected_status_codes=[HTTPStatus.OK],
            retry_status_codes=None,
            min_bytes=None,
            params=self.params,
        )
        assert actual_response == expected_response_data
        self._assert_params_were_applied(requests_mock.last_request)

    def test_post_should_retry(self, requests_mock: Mocker) -> None:
        requests_mock.request(
            method="POST",
            url=self.full_url,
            response_list=[
                {"status_code": status_code, "json": {"Code": "ResourceIsLocked", "Message": "", "Retryable": "True"}}
                for status_code in [HTTPStatus.CONFLICT, HTTPStatus.NOT_FOUND, HTTPStatus.NOT_FOUND, HTTPStatus.OK]
            ],
        )

        actual_response = self.http_client.raw(
            method="POST",
            path=self.path,
            expected_status_codes=[HTTPStatus.OK],
            retry_status_codes=[HTTPStatus.CONFLICT, HTTPStatus.NOT_FOUND],
            min_bytes=0,
            body={},
            params=self.params,
            retries=8,
        )
        assert actual_response.status_code == HTTPStatus.OK

    def test_post_should_raise_exceptions(self, requests_mock: Mocker) -> None:
        min_size = 100
        requests_mock.request(method="POST", url=self.full_url, status_code=HTTPStatus.OK)
        with assert_raises(ResponseTooSmall(actual_size=0, min_size=min_size)):
            self.http_client.post(
                path=self.path,
                expected_status_codes=[HTTPStatus.OK],
                retry_status_codes=[HTTPStatus.NOT_FOUND],
                min_bytes=min_size,
                body={},
                params=self.params,
            )

        with assert_raises(
            HttpStatusCodeNotInExpectedCodes(
                expected_status_codes=[HTTPStatus.CONFLICT], status_code=HTTPStatus.OK, content=""
            )
        ):
            self.http_client.post(
                path=self.path,
                expected_status_codes=[HTTPStatus.CONFLICT],
                retry_status_codes=[HTTPStatus.NOT_FOUND],
                min_bytes=0,
                body={},
                params=self.params,
            )

    def test_post_nonretryable_conflict(self, requests_mock: Mocker) -> None:
        requests_mock.request(
            method="POST",
            url=self.full_url,
            response_list=[
                {"status_code": status_code, "json": {"Code": "ResourceIsLocked", "Message": "", "Retryable": "False"}}
                for status_code in [HTTPStatus.CONFLICT, HTTPStatus.NOT_FOUND]
            ],
        )

        with pytest.raises(NonRetryableConflictError):
            self.http_client.post(
                path=self.path,
                expected_status_codes=[HTTPStatus.OK],
                min_bytes=0,
                body={},
                params=self.params,
            )
        assert requests_mock.call_count == 1

    def test_post_gateway_timout_retry_with_following_nonretryable_conflict(self, requests_mock: Mocker) -> None:
        status_codes = [
            HTTPStatus.GATEWAY_TIMEOUT,
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.LOCKED,
            HTTPStatus.CONFLICT,
        ]
        requests_mock.request(
            method="POST",
            url=self.full_url,
            response_list=[
                {
                    "status_code": status_code,
                    "json": {"Code": "ResourceIsLocked", "Message": "", "Retryable": "False"}
                    if status_code == HTTPStatus.CONFLICT
                    else {"Code": "ResourceIsLocked", "Message": "", "Retryable": "True"},
                }
                for status_code in status_codes
            ],
        )

        with pytest.raises(NonRetryableConflictError):
            self.http_client.post(
                path=self.path,
                expected_status_codes=[HTTPStatus.OK],
                min_bytes=0,
                body={},
                params=self.params,
            )
        assert requests_mock.call_count == len(status_codes)

    def test_post_retryable_conflict_until_timeout_reached(self, requests_mock: Mocker) -> None:
        status_codes = [HTTPStatus.CONFLICT, HTTPStatus.CONFLICT, HTTPStatus.CONFLICT]
        requests_mock.request(
            method="POST",
            url=self.full_url,
            response_list=[
                {"status_code": status_code, "json": {"Code": "ResourceIsLocked", "Message": "", "Retryable": "True"}}
                for status_code in status_codes
            ],
        )
        self.http_client.raw(
            method="POST",
            path=self.path,
            expected_status_codes=[HTTPStatus.CONFLICT],
            min_bytes=0,
            body={},
            params=self.params,
            retries=len(status_codes),
        )
        assert requests_mock.call_count == len(status_codes)

    def test_should_retry_service_lock_errors(self, requests_mock: Mocker) -> None:
        requests_mock.request(
            method="POST",
            url=self.full_url,
            status_code=HTTPStatus.CONFLICT,
            json={"Code": "ResourceIsLocked", "Message": "", "Retryable": "True"},
        )

        with pytest.raises(HttpStatusCodeNotInExpectedCodes):
            actual_response = self.http_client.raw(
                method="POST",
                path=self.path,
                expected_status_codes=[HTTPStatus.OK],
                min_bytes=0,
                body={},
                params=self.params,
            )
            assert actual_response.status_code == HTTPStatus.CONFLICT
        assert requests_mock.call_count == self.http_client.default_retries

    def test_request_exceptions_should_be_retried(self, requests_mock: Mocker) -> None:
        requests_mock.request(
            method="POST",
            url=self.full_url,
            response_list=[{"exc": ProxyError("some error")}, {"status_code": HTTPStatus.OK}],
        )

        actual_response = self.http_client.raw(
            method="POST",
            path=self.path,
            expected_status_codes=[HTTPStatus.OK],
            retry_status_codes=[],
            min_bytes=0,
            body={},
            params=self.params,
        )
        assert actual_response.status_code == HTTPStatus.OK
        assert requests_mock.call_count == 2

    def test_request_exceptions_should_be_retried_and_reraised(self, requests_mock: Mocker) -> None:
        retries = 3
        requests_mock.request(
            method="POST",
            url=self.full_url,
            response_list=[
                {"exc": RequestsConnectionError()},
                {"exc": Timeout()},
                {"exc": ProxyError("some error")},
            ],
        )
        with pytest.raises(ProxyError):
            self.http_client.raw(
                method="POST",
                path=self.path,
                expected_status_codes=[HTTPStatus.OK],
                retry_status_codes=[],
                min_bytes=0,
                body={},
                params=self.params,
                retries=retries,
            )
        assert requests_mock.call_count == retries

    def test_get_with_pagination_single_page(self, requests_mock: Mocker) -> None:
        expected_items = [
            {Builder.build_random_string(): Builder.build_random_string()} for _ in range(random.randint(1, 5))
        ]
        item_key = Builder.build_random_string()
        requests_mock.request(
            method="GET", url=self.full_url, status_code=HTTPStatus.OK, json={item_key: expected_items}
        )
        actual_response = self.http_client.get_with_pagination(
            path=self.path,
            retry_status_codes=None,
            min_bytes=None,
            params=self.params,
            next_page_key=Builder.build_random_string(),
            item_key=item_key,
        )
        assert actual_response == {item_key: expected_items}

    @pytest.mark.parametrize("include_empty_token", [False, True])
    def test_get_with_pagination_multiple_pages(self, requests_mock: Mocker, include_empty_token: bool) -> None:
        item_key = Builder.build_random_string()
        next_page_key = Builder.build_random_string()
        pages = [
            [{Builder.build_random_string(): Builder.build_random_string()} for _ in range(random.randint(1, 5))]
            for _ in range(3)
        ]
        headers = [
            {next_page_key: "first"},
            {next_page_key: "second"},
            {next_page_key: ""} if include_empty_token else {},
        ]
        requests_mock.request(
            method="GET",
            url=self.full_url,
            response_list=[
                {"status_code": HTTPStatus.OK, "json": {item_key: page}, "headers": header}
                for page, header in zip(pages, headers)
            ],
        )
        actual_response = self.http_client.get_with_pagination(
            path=self.path,
            retry_status_codes=None,
            min_bytes=None,
            params=self.params,
            next_page_key=next_page_key,
            item_key=item_key,
        )
        assert actual_response == {item_key: sum(pages, [])}
        assert requests_mock.call_count == 3
        for request in requests_mock.request_history:
            self._assert_params_were_applied(request)
        assert next_page_key not in self.params

        assert next_page_key not in requests_mock.request_history[0].qs
        assert requests_mock.request_history[1].qs[next_page_key] == ["first"]
        assert requests_mock.request_history[2].qs[next_page_key] == ["second"]

    @pytest.mark.parametrize("exceed", [False, True])
    def test_bound_number_of_pagination_fetches(self, requests_mock: Mocker, exceed: bool) -> None:
        item_key = Builder.build_random_string()
        next_page_key = Builder.build_random_string()
        pages = [
            [{Builder.build_random_string(): Builder.build_random_string()} for _ in range(random.randint(1, 5))]
            for _ in range(2)
        ]
        headers = [{next_page_key: "first"}, {next_page_key: ""}]
        requests_mock.request(
            method="GET",
            url=self.full_url,
            response_list=[
                {"status_code": HTTPStatus.OK, "json": {item_key: page}, "headers": header}
                for page, header in zip(pages, headers)
            ],
        )
        if exceed:
            with pytest.raises(MaximumFetchesExceeded):
                self.http_client.get_with_pagination(
                    path=self.path,
                    retry_status_codes=None,
                    min_bytes=None,
                    params=self.params,
                    next_page_key=next_page_key,
                    item_key=item_key,
                    max_number_of_fetches=1,
                )
        else:
            assert self.http_client.get_with_pagination(
                path=self.path,
                retry_status_codes=None,
                min_bytes=None,
                params=self.params,
                next_page_key=next_page_key,
                item_key=item_key,
                max_number_of_fetches=2,
            ) == {item_key: sum(pages, [])}

    def _assert_params_were_applied(self, request: Any) -> None:
        for param_key, param_value in self.params.items():
            expected_value = param_value if isinstance(param_value, list) else [param_value]
            assert request.qs[param_key] == expected_value
