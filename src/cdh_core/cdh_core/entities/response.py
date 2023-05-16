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
from abc import ABC
from abc import abstractmethod
from http import HTTPStatus
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import orjson

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.entities.serializer_factory import Serializer
from cdh_core.entities.serializer_factory import SerializerFactory


class Response(ABC):
    """Meta class for HTTP responses."""

    status_code: HTTPStatus
    headers: Dict[str, str]

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Return the content as dict."""
        raise NotImplementedError


class JsonResponse(Response):
    """Formats the HTTP body as JSON."""

    def __init__(
        self,
        body: Optional[Union[Dict[str, Any], DataClassJsonCDHMixin]] = None,
        status_code: HTTPStatus = HTTPStatus.OK,
        headers: Optional[Dict[str, str]] = None,
        next_page_token: Optional[str] = None,
    ):
        self.body = body
        self.status_code = status_code
        self.headers = headers or {}
        if next_page_token:
            self.headers["nextPageToken"] = next_page_token

    def to_dict(self, serializer: Optional[Serializer] = None) -> Dict[str, Any]:  # pylint: disable=arguments-differ
        """Return the content as dict where the body is a json string.

        The input body can be a Dict containing only primitive types or a DataClassJsonCDHMixin object which implements
        the to_json() method.
        """
        # The necessary output format for Lambdas invoked by API Gateway is documented here:
        # https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-output-format
        body = None
        if self.body is not None:
            serializer = serializer or SerializerFactory.create_serializer()
            body = orjson.dumps(self.body, default=serializer, option=orjson.OPT_PASSTHROUGH_DATACLASS).decode("utf-8")

        result: Dict[str, Any] = {
            "isBase64Encoded": False,
            "statusCode": self.status_code.value,
            "body": body,
        }
        if self.headers:
            result["headers"] = self.headers

        return result


class CsvResponse(Response):
    """Returns the result as CSV."""

    def __init__(
        self,
        body: str,
        status_code: HTTPStatus = HTTPStatus.OK,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.body = body
        self.status_code = status_code
        self.headers = {"Content-Type": "text/csv", **(headers or {})}

    def to_dict(self) -> Dict[str, Any]:
        """Return the CsvResponse in dict representation."""
        return {
            "isBase64Encoded": False,
            "statusCode": self.status_code.value,
            "body": self.body,
            "headers": self.headers,
        }
