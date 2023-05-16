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
from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from http.cookies import SimpleCookie
from json import JSONDecodeError
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import overload

from dataclasses_json import config

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.entities.arn import Arn
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.enums.http import HttpVerb
from cdh_core.exceptions.http import BadRequestError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import UnsupportedMediaTypeError
from cdh_core.primitives.account_id import AccountId


@dataclass(frozen=True)
class Cookie:
    """A single HTTP cookie."""

    key: str
    value: str

    def encode(self) -> str:
        """Return the cookie as encoded string."""
        simple_cookie: SimpleCookie[Any] = SimpleCookie()  # pylint: disable=unsubscriptable-object
        simple_cookie[self.key] = self.value
        return simple_cookie[self.key].OutputString()


class Headers:
    """Abstracts the HTTP headers."""

    def __init__(self, headers: Dict[str, str]):
        self._headers = {key.lower(): value for key, value in headers.items()}

    @classmethod
    def from_lambda_event(cls, event: Dict[str, Any]) -> Headers:
        """Build a new Headers instance from a lambda event."""
        return cls(event.get("headers") or {})

    def __contains__(self, key: str) -> bool:
        """Check if the key exists, case insensitive."""
        return key.lower() in self._headers

    def __getitem__(self, key: str) -> str:
        """Return the item, case insensitive."""
        return self._headers[key.lower()]

    def __eq__(self, other: object) -> bool:
        """Compare two Headers instances based on their internal dict."""
        if isinstance(other, Headers):
            return self._headers == other._headers
        return NotImplemented

    @overload
    def get(self, key: str, default: str) -> str:
        ...

    @overload
    def get(self, key: str) -> Optional[str]:
        ...

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Return the entry value for a given key."""
        return self._headers.get(key.lower(), default)

    def to_dict(self) -> Dict[str, str]:
        """Return the header as dict."""
        return self._headers.copy()


@dataclass(frozen=True)
class Request(DataClassJsonCDHMixin):
    """Represents a HTTP request."""

    id: str  # pylint: disable=invalid-name
    http_verb: HttpVerb
    route: str
    path: str
    path_params: Dict[str, str]
    query_params: Dict[str, str]
    query_params_multi_value: Dict[str, List[str]]
    headers: Headers = field(metadata=config(encoder=lambda x: x.to_dict()))
    body: Dict[str, Any]
    _requester_arn: Optional[Arn] = field(metadata=config(field_name="requesterArn", encoder=str))
    user: str
    api_request_id: str

    @classmethod
    def from_lambda_event(cls, event: Dict[str, Any], context: LambdaContext, ignore_body: bool = False) -> Request:
        """Build a request from a lambda event."""
        # The format of events sent to a Lambda function by API gateway is documented here:
        # https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
        requester_arn_string = event["requestContext"]["identity"]["userArn"]
        headers = Headers.from_lambda_event(event)
        return Request(
            id=context.aws_request_id,
            http_verb=HttpVerb[event["httpMethod"].upper()],
            route=event["resource"],
            path=event["path"],
            headers=headers,
            query_params=event["queryStringParameters"] or {},
            query_params_multi_value=event["multiValueQueryStringParameters"] or {},
            path_params=event["pathParameters"] or {},
            body=cls._get_body(event, headers) if not ignore_body else {},
            _requester_arn=Arn(requester_arn_string) if requester_arn_string else None,
            user=event["requestContext"]["identity"]["user"],
            api_request_id=event["requestContext"]["requestId"],
        )

    @classmethod
    def _get_body(cls, event: Dict[str, Any], headers: Headers) -> Dict[str, Any]:
        if headers.get("Content-Type", "application/json") != "application/json":
            raise UnsupportedMediaTypeError("Invalid Content-Type header. Must be application/json.")

        if body := event.get("body"):
            try:
                if (json_body := json.loads(body)) and isinstance(json_body, dict):
                    return json_body
                raise BadRequestError("Invalid JSON body. Body is not a dictionary.")
            except JSONDecodeError as error:
                raise BadRequestError("Invalid JSON body") from error
        return {}

    @property
    def requester_arn(self) -> Arn:
        """Return the requester ARN, if the request contains authentication."""
        if self._requester_arn is None:
            raise ForbiddenError("Must be authenticated")
        return self._requester_arn

    @property
    def requester_account_id(self) -> AccountId:
        """Return the requester aws account id, if the request contains authentication."""
        return self.requester_arn.account_id

    @property
    def origin(self) -> Optional[str]:
        """Return the Origin field of the headers."""
        return self.headers.get("Origin")

    def get_cookie(self, name: str) -> Optional[Cookie]:
        """Return the cookie based on the name."""
        cookie: SimpleCookie[Any] = SimpleCookie(  # pylint: disable=unsubscriptable-object
            self.headers.get("Cookie", "")
        )
        return Cookie(name, cookie[name].value) if name in cookie else None


@dataclass(frozen=True)
class RequesterIdentity:
    """Represents information on the requester's identity."""

    arn: Arn
    user: str
    jwt_user_id: Optional[str]

    @property
    def account_id(self) -> AccountId:
        """Return the requester's AWS account ID."""
        return self.arn.account_id
