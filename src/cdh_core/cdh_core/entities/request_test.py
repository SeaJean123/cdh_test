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
from dataclasses import replace
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from unittest.mock import Mock

import pytest

from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.entities.request import Cookie
from cdh_core.entities.request import Headers
from cdh_core.entities.request import Request
from cdh_core.entities.request import RequesterIdentity
from cdh_core.enums.http import HttpVerb
from cdh_core.exceptions.http import BadRequestError
from cdh_core.exceptions.http import UnsupportedMediaTypeError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def build_request(
    http_verb: HttpVerb = HttpVerb.GET,
    route: str = "/route",
    path: str = "/route",
    path_params: Optional[Dict[str, str]] = None,
    query_params: Optional[Dict[str, str]] = None,
    query_params_multi_value: Optional[Dict[str, List[str]]] = None,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
    requester_arn: Optional[Arn] = None,
    user: Optional[str] = None,
) -> Request:
    return Request(
        id=Builder.build_request_id(),
        http_verb=http_verb,
        route=route,
        path=path,
        path_params=path_params or {},
        query_params=query_params or {},
        query_params_multi_value=query_params_multi_value or {},
        headers=Headers(headers or {}),
        body=body or {},
        _requester_arn=requester_arn or Arn(f"arn:aws:iam::{build_account_id()}:user/{Builder.build_random_string()}"),
        user=user or Builder.build_random_string(),
        api_request_id=Builder.build_request_id(),
    )


def build_requester_identity(
    arn: Optional[Arn] = None, user: Optional[str] = None, jwt_user_id: Optional[str] = None
) -> RequesterIdentity:
    return RequesterIdentity(
        arn=arn or Arn(f"arn:aws:iam::{build_account_id()}:user/{Builder.build_random_string()}"),
        user=user or Builder.build_random_string(),
        jwt_user_id=jwt_user_id,
    )


class TestHeaders:
    def test_header_keys_can_be_accessed_case_insensitively(self) -> None:
        headers = Headers({"Origin": "Frontend"})
        assert "Origin" in headers
        assert "origin" in headers
        assert headers["Origin"] == "Frontend"
        assert headers["origin"] == "Frontend"

    def test_equality(self) -> None:
        assert Headers({"Origin": "Frontend"}) == Headers({"origin": "Frontend"})
        assert Headers({"Origin": "Frontend"}) != Headers({"Origin": "frontend"})

    def test_get(self) -> None:
        headers = Headers({"Origin": "Frontend"})
        assert headers.get("origin") == "Frontend"
        assert headers.get("content-type") is None
        assert headers.get("content-type", "application/json") == "application/json"


class TestRequest:
    arn = build_arn(service="iam")
    EVENT = {
        "resource": "/{hub}/resources",
        "path": "/global/resources",
        "httpMethod": "get",
        "headers": {"Content-Type": "application/json"},
        "multiValueHeaders": {},
        "queryStringParameters": {"stage": "dev", "some_list": "value2"},
        "multiValueQueryStringParameters": {"stage": ["dev"], "some_list": ["value1", "value2"]},
        "pathParameters": {"hub": "global"},
        "stageVariables": {},
        "requestContext": {
            "identity": {
                "userArn": str(arn),
                "user": "Hans",
            },
            "requestId": "746dbb58-9839-49b3-b79d-77caccf2b479",
        },
        "body": '{"key": "value"}',
        "isBase64Encoded": "A boolean flag to indicate if the applicable request payload is Base64-encode",
    }
    CONTEXT: LambdaContext = Mock(aws_request_id="deef4878-7910-11e6-8f14-25afc3e9ae33")
    EXPECTED_REQUEST = Request(
        id="deef4878-7910-11e6-8f14-25afc3e9ae33",
        http_verb=HttpVerb.GET,
        route="/{hub}/resources",
        path="/global/resources",
        path_params={"hub": "global"},
        query_params={"stage": "dev", "some_list": "value2"},
        query_params_multi_value={"stage": ["dev"], "some_list": ["value1", "value2"]},
        headers=Headers({"Content-Type": "application/json"}),
        body={"key": "value"},
        _requester_arn=arn,
        user="Hans",
        api_request_id="746dbb58-9839-49b3-b79d-77caccf2b479",
    )

    def test_to_dict(self) -> None:
        request = Request.from_lambda_event(self.EVENT, self.CONTEXT)
        assert request.to_plain_dict() == {
            "id": self.EXPECTED_REQUEST.id,
            "httpVerb": self.EXPECTED_REQUEST.http_verb.value,
            "route": self.EXPECTED_REQUEST.route,
            "path": self.EXPECTED_REQUEST.path,
            "pathParams": self.EXPECTED_REQUEST.path_params,
            "queryParams": self.EXPECTED_REQUEST.query_params,
            "queryParamsMultiValue": self.EXPECTED_REQUEST.query_params_multi_value,
            "headers": self.EXPECTED_REQUEST.headers.to_dict(),
            "body": self.EXPECTED_REQUEST.body,
            "requesterArn": str(self.EXPECTED_REQUEST.requester_arn),
            "user": self.EXPECTED_REQUEST.user,
            "apiRequestId": self.EXPECTED_REQUEST.api_request_id,
        }

    def test_from_lambda_event(self) -> None:
        assert Request.from_lambda_event(self.EVENT, self.CONTEXT) == self.EXPECTED_REQUEST

    def test_without_content_type(self) -> None:
        request = Request.from_lambda_event({**self.EVENT, "headers": {}}, self.CONTEXT)
        assert request == replace(self.EXPECTED_REQUEST, headers=Headers({}))

    def test_invalid_content_type(self) -> None:
        with pytest.raises(UnsupportedMediaTypeError):
            Request.from_lambda_event({**self.EVENT, "headers": {"Content-Type": "image/jpeg"}}, self.CONTEXT)

    def test_empty_body(self) -> None:
        request = Request.from_lambda_event({**self.EVENT, "body": None}, self.CONTEXT)
        assert request == replace(self.EXPECTED_REQUEST, body={})

    def test_from_lambda_event_missing_header(self) -> None:
        """Not every lambda call contains a headers information."""
        event = dict(self.EVENT)
        event["headers"] = None  # type: ignore
        assert Request.from_lambda_event(event, self.CONTEXT) == replace(self.EXPECTED_REQUEST, headers=Headers({}))

    def test_invalid_body(self) -> None:
        with pytest.raises(BadRequestError):
            Request.from_lambda_event({**self.EVENT, "body": "this ain't no JSON"}, self.CONTEXT)

    def test_invalid_body_with_ignored_body(self) -> None:
        request = Request.from_lambda_event(
            {**self.EVENT, "body": "this ain't no JSON"}, self.CONTEXT, ignore_body=True
        )
        assert request == replace(self.EXPECTED_REQUEST, body={})

    def test_get_cookie(self) -> None:
        request = Request.from_lambda_event({**self.EVENT, "headers": {"Cookie": "jwt=123;abc=456"}}, self.CONTEXT)
        assert request.get_cookie("jwt") == Cookie("jwt", "123")

    def test_get_missing_cookie(self) -> None:
        request = Request.from_lambda_event({**self.EVENT, "headers": {"Cookie": "jwt=123;abc=456"}}, self.CONTEXT)
        assert request.get_cookie("does-not-exist") is None
