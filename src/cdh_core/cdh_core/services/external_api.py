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
import time
from dataclasses import dataclass
from http import HTTPStatus
from logging import getLogger
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Mapping
from typing import Optional
from typing import Tuple
from urllib.parse import urlparse

from aws_requests_auth.aws_auth import AWSRequestsAuth
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from requests import PreparedRequest
from requests import Response
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from cdh_core.entities.accounts import Credentials
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region

LOG = getLogger(__name__)


class VerboseSession(Session):
    """Extends the requests.Session class by enforcing a timeout and writing the elapsed time to the logs."""

    def __init__(self, log_identifier_key: str, log_identifier_value: str, timeout_fallback: int = 20) -> None:
        super().__init__()
        self._log_identifier_key = log_identifier_key
        self._log_identifier_value = log_identifier_value
        self._timeout_fallback = timeout_fallback

    def send(self, request: PreparedRequest, **kwargs: Any) -> Response:
        """Send the request with the given parameters.

        If no timeout is provided, the timeout fallback value is used.
        The statistics about the call are written to the logs afterwards.
        """
        start = time.perf_counter()
        if "timeout" not in kwargs:
            kwargs["timeout"] = self._timeout_fallback
        response = super().send(request, **kwargs)
        self._write_logs(response, 1000 * (time.perf_counter() - start))
        return response

    def _write_logs(self, response: Response, elapsed_ms: float) -> None:
        LOG.info(
            {
                self._log_identifier_key: self._log_identifier_value,
                "elapsed_ms": elapsed_ms,
                "http_method": response.request.method,
                "url": response.request.url,
                "status_code": response.status_code,
            }
        )


class ApiSessionBuilder:
    """Builds a VerboseSession instance."""

    def __init__(self, api_name: str, retry_config: Retry) -> None:
        self._api_name = api_name
        self._retry_config = retry_config

    def get_session(self) -> VerboseSession:
        """Return a VerboseSession instance to which a HTTPAdapter with the specified retry_config is mounted."""
        session = VerboseSession(log_identifier_key="api_name", log_identifier_value=self._api_name)
        adapter = HTTPAdapter(max_retries=self._retry_config)
        session.mount("https://", adapter)
        return session


def get_retry_config(retries: int, backoff_factor: float, retry_on_post: bool = False) -> Retry:
    """Return a Retry instance with the specified parameters."""
    allowed_methods = Retry.DEFAULT_ALLOWED_METHODS | ({"POST"} if retry_on_post else set())
    return Retry(
        total=retries,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=backoff_factor,
        allowed_methods=allowed_methods,
    )


class UnusableSession(Session):
    """Extends the requests.Session class to a new session class that must not be used to send requests."""

    def send(self, request: PreparedRequest, **kwargs: Any) -> Response:
        """Raise a runtime error if this method is called."""
        raise RuntimeError(f"Should not have called {request.method} on {request.url}")


@dataclass
class ExternalApiResponse:
    """Represents the response received by an external API."""

    data: Dict[str, Any]
    headers: Mapping[str, str]


class ExternalApiSession:
    """Wraps the calls to an external api."""

    def __init__(
        self,
        request_session_factory: Callable[[], Session],
        api_url: str,
        timeout: Tuple[float, float],
        credentials: Optional[Credentials] = None,
    ):
        self._requests = request_session_factory()
        self._api_url = api_url
        region_name = Region.preferred(Partition.default()).value
        self._auth = (
            AWSRequestsAuth(
                aws_access_key=credentials.aws_access_key_id,
                aws_secret_access_key=credentials.aws_secret_access_key,
                aws_host=urlparse(api_url).hostname,
                aws_region=region_name,
                aws_service="execute-api",
                aws_token=credentials.aws_session_token,
            )
            if credentials
            else BotoAWSRequestsAuth(urlparse(api_url).hostname, region_name, "execute-api")
        )

        self._timeout_config = timeout

    def get(self, path: str, headers: Dict[str, Any], params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make a request of the type 'GET' to the given path and return the received data."""
        return self.get_response(path=path, headers=headers, params=params).data

    def get_response(
        self, path: str, headers: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, str]] = None
    ) -> ExternalApiResponse:
        """Make a request of the type 'GET' to the given path and return the response including headers."""
        response = self._requests.get(
            url=self._api_url + path,
            headers=headers,
            timeout=self._timeout_config,
            # mypy does not acknowledge an BotoAwsRequestsAuth as an AuthBase
            auth=self._auth,  # type: ignore
            params=params,
        )
        response.raise_for_status()
        return ExternalApiResponse(cast(Dict[str, Any], response.json()), response.headers)

    def post(self, path: str, headers: Dict[str, Any], json: Dict[str, Any]) -> Dict[str, Any]:
        """Make a request of the type 'POST' to the given path with the specified headers and json body."""
        response = self._requests.post(
            url=self._api_url + path,
            headers=headers,
            timeout=self._timeout_config,
            # mypy does not acknowledge an BotoAwsRequestsAuth as an AuthBase
            auth=self._auth,  # type: ignore
            json=json,
        )
        response.raise_for_status()
        if response.status_code == HTTPStatus.NO_CONTENT:
            return {}
        return cast(Dict[str, Any], response.json())

    def delete(self, path: str, headers: Dict[str, Any], json: Dict[str, Any]) -> None:
        """Make a request of the type 'DELETE' to the given path with the specified headers and json body."""
        response = self._requests.delete(
            url=self._api_url + path,
            headers=headers,
            timeout=self._timeout_config,
            # mypy does not acknowledge an BotoAwsRequestsAuth as an AuthBase
            auth=self._auth,  # type: ignore
            json=json,
        )
        response.raise_for_status()
