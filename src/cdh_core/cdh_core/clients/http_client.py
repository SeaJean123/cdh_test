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
import os
import random
import time
from copy import deepcopy
from http import HTTPStatus
from http.client import HTTPConnection
from json import JSONDecodeError
from logging import getLogger
from time import sleep
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Union

from aws_requests_auth.aws_auth import AWSRequestsAuth
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from requests import request
from requests import Response
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ProxyError
from requests.exceptions import Timeout

LOG = getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


if os.environ.get("LOG_LEVEL_HTTP_CLIENT", "").upper() == "DEBUG":
    HTTPConnection.debuglevel = 1
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel("DEBUG")
    requests_log.propagate = True

ResponseJson = Dict[str, Any]
ResponseHeaders = Mapping[str, str]


class HttpClient:
    """
    Makes HTTP requests based on the library 'requests' extended by retries and more.

    The debug log of the client and requests can be enable via the env entry LOG_LEVEL_HTTP_CLIENT=DEBUG.
    """

    def __init__(
        self,
        base_url: str,
        credentials: Optional[Union[AWSRequestsAuth, BotoAWSRequestsAuth]],
    ) -> None:
        self._base_url = _ensure_https_and_remove_last_slash(base_url)
        self.default_retries = 4
        self.default_seconds_between_retries = 2
        self.retry_sleep_jitter_factor_min = 0.8
        self.retry_sleep_jitter_factor_max = 1.2
        self._auth = credentials

    def get(
        self,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        min_bytes: Optional[int] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> ResponseJson:
        """Make a HTTP request of the type 'GET'."""
        return self._fetch(
            "GET",
            path,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            min_bytes=min_bytes,
            body=None,
            params=params,
            headers=headers,
        )[0]

    def post(
        self,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        seconds_between_retries: Optional[int] = None,
        min_bytes: Optional[int] = None,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> ResponseJson:
        """Make a HTTP request of the type 'POST'."""
        return self._fetch(
            method="POST",
            path=path,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            min_bytes=min_bytes,
            body=body,
            params=params,
            seconds_between_retries=seconds_between_retries,
            headers=headers,
        )[0]

    def put(
        self,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        min_bytes: Optional[int] = None,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> ResponseJson:
        """Make a HTTP request of the type 'PUT'."""
        return self._fetch(
            "PUT",
            path,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            min_bytes=min_bytes,
            body=body,
            params=params,
            headers=headers,
        )[0]

    def patch(
        self,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        min_bytes: Optional[int] = None,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> ResponseJson:
        """Make a HTTP request of the type 'PATCH'."""
        return self._fetch(
            "PATCH",
            path,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            min_bytes=min_bytes,
            body=body,
            params=params,
            headers=headers,
        )[0]

    def delete(
        self,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        min_bytes: Optional[int] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> ResponseJson:
        """Make a HTTP request of the type 'DELETE'."""
        return self._fetch(
            "DELETE",
            path,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            min_bytes=min_bytes,
            body=body,
            headers=headers,
        )[0]

    def _fetch(
        self,
        method: str,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        seconds_between_retries: Optional[int] = None,
        min_bytes: Optional[int] = None,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Tuple[ResponseJson, ResponseHeaders]:
        response = self.raw(
            method=method,
            path=path,
            expected_status_codes=expected_status_codes,
            retry_status_codes=retry_status_codes,
            min_bytes=min_bytes,
            body=body,
            params=params,
            seconds_between_retries=seconds_between_retries,
            headers=headers,
        )
        response_json = {}
        if response.content:
            response_json = response.json()
            if not isinstance(response_json, dict):
                raise TypeError("Only responses which are dict shaped are supported.")
        return response_json, response.headers

    def _wait_with_jitter(self, seconds_between_retries: int) -> None:
        sleep_factor = random.uniform(self.retry_sleep_jitter_factor_min, self.retry_sleep_jitter_factor_max)
        sleep(seconds_between_retries * sleep_factor)

    def raw(  # pylint: disable=too-many-locals
        self,
        method: str,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        min_bytes: Optional[int] = None,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        retries: Optional[int] = None,
        seconds_between_retries: Optional[int] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Response:
        """Make a HTTP where everything can be configured."""
        retry_status_codes = retry_status_codes or []
        expected_status_codes = expected_status_codes or []
        connect_timeout = 12
        read_timeout = 42

        if retries is None:
            retries = self.default_retries

        if seconds_between_retries is None:
            seconds_between_retries = self.default_seconds_between_retries

        # Default retryable status codes
        retry_status_codes.append(HTTPStatus.LOCKED)
        retry_status_codes.append(HTTPStatus.GATEWAY_TIMEOUT)
        retry_status_codes.append(HTTPStatus.TOO_MANY_REQUESTS)
        response = Response()
        for attempt in range(retries):
            LOG.info(f"REQUEST (#{attempt}) {method}: {self._base_url + path}")
            start = time.perf_counter()
            try:
                response = request(
                    method,
                    self._base_url + path,
                    # mypy does not acknowledge an BotoAwsRequestsAuth as an AuthBase
                    auth=self._auth,  # type: ignore
                    json=body,
                    timeout=(connect_timeout, read_timeout),
                    params=dict(params) if params is not None else None,
                    headers=headers,
                )
            except (ProxyError, RequestsConnectionError, Timeout) as err:
                end = time.perf_counter()
                LOG.info(f"RESPONSE Exception (total={int((end - start) * 1000)}ms / response=-ms): {err}")
                if attempt == retries - 1:
                    raise
                self._wait_with_jitter(seconds_between_retries)
                continue
            end = time.perf_counter()
            response_time = int(response.elapsed.microseconds / 1000)
            LOG.info(
                " ".join(
                    [
                        f"RESPONSE {response.status_code}",
                        f"(total time={int((end-start)*1000)}ms / response time={response_time}ms)",
                        f"(total content size={len(response.content)}bytes)",
                    ]
                )
            )
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug(response.text)

            if not _is_retryable_conflict(response):
                # non retry-able Conflicts may also be listed in expected_status_codes
                if response.status_code in expected_status_codes:
                    break
                if response.status_code == HTTPStatus.CONFLICT:
                    raise NonRetryableConflictError(response.text)
                # stop loop when status_code is not set as retryable
                if response.status_code not in retry_status_codes:
                    break

            self._wait_with_jitter(seconds_between_retries)

        if response is not Response() and response.status_code not in expected_status_codes:
            raise HttpStatusCodeNotInExpectedCodes(
                status_code=HTTPStatus(response.status_code),
                expected_status_codes=expected_status_codes,
                content=response.text,
            )

        if min_bytes and len(response.content) < min_bytes:
            raise ResponseTooSmall(actual_size=len(response.content), min_size=min_bytes)

        return response

    def options(self, path: str, *, expected_status_codes: List[HTTPStatus], min_bytes: int = 0) -> ResponseJson:
        """Make a HTTP request of the type 'OPTIONS'."""
        return self._fetch(
            "OPTIONS",
            path,
            expected_status_codes=expected_status_codes,
            min_bytes=min_bytes,
            body=None,
        )[0]

    def get_with_pagination(
        self,
        path: str,
        *,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        min_bytes: Optional[int] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        next_page_key: str,
        item_key: str,
        max_number_of_fetches: int = 100,
    ) -> ResponseJson:
        """Perform a bounded sequence of 'GET' requests and concatenate the results along the provided 'item_key'."""
        params = {k: deepcopy(v) for k, v in params.items()} if params else {}
        items = []
        for _ in range(max_number_of_fetches):
            json, headers = self._fetch(
                "GET",
                path,
                expected_status_codes=[HTTPStatus.OK],
                retry_status_codes=retry_status_codes,
                min_bytes=min_bytes,
                body=None,
                params=params,
            )
            new_items = json[item_key]
            if not isinstance(new_items, list):
                raise TypeError("Cannot paginate over non-list values")
            items.extend(new_items)
            next_page_token = headers.get(next_page_key)
            if not next_page_token:
                return {item_key: items}
            params[next_page_key] = next_page_token
        raise MaximumFetchesExceeded(f"Maximum number of fetches (={max_number_of_fetches}) exceeded")


def _is_retryable_conflict(response: Response) -> bool:
    if response.status_code != HTTPStatus.CONFLICT:
        return False
    try:
        error_dict = response.json()
    except JSONDecodeError:
        return False
    if "Retryable" not in error_dict:
        return False
    if error_dict["Retryable"] != "True":
        return False
    return True


def _ensure_https_and_remove_last_slash(url: str) -> str:
    return (url if url.startswith("https://") else "https://" + url).rstrip("/")


class HttpStatusCodeNotInExpectedCodes(Exception):
    """Only a certain amount of HTTP codes are allowed, but the actual code differs."""

    def __init__(
        self,
        status_code: HTTPStatus,
        expected_status_codes: List[HTTPStatus],
        content: Optional[str] = None,
    ):
        self.status_code = status_code
        error_msg = f"Status code was {status_code} but one of {str(expected_status_codes)} expected."
        if content and content != "":
            error_msg = error_msg + f" Content was: {content!r}"

        super().__init__(error_msg)


class ResponseTooSmall(Exception):
    """The expected response body should be bigger."""

    def __init__(self, actual_size: int, min_size: int):
        super().__init__(f"Response size was {actual_size} but size of minimal size of {min_size} was expected.")


class NonRetryableConflictError(Exception):
    """There is a conflict but it cannot be retried."""


class MaximumFetchesExceeded(Exception):
    """The maximum number of fetches has been exceeded."""
