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
# pylint: disable=protected-access
import json
import random
from http import HTTPStatus
from typing import Any
from typing import Dict
from typing import Optional
from unittest.mock import Mock
from unittest.mock import patch

import orjson
import pytest
from cdh_core_api.api import router
from cdh_core_api.api.router import AUDIT_VERBS
from cdh_core_api.api.router import CORS_HEADER
from cdh_core_api.api.router import CORS_METHODS
from cdh_core_api.api.router import Router
from cdh_core_api.api.router import SECURITY_HEADERS
from cdh_core_api.config_test import build_config
from marshmallow import ValidationError

from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.entities.request import Request
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.http import HttpVerb
from cdh_core.exceptions.http import BadRequestError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.manager.dependency_manager import DependencyManager
from cdh_core_dev_tools.testing.builder import Builder

STANDARD_HEADERS = {"Access-Control-Allow-Credentials": "true", **SECURITY_HEADERS}


class RequestEventBuilder:
    ALLOWED_ORIGINS = (Builder.build_random_string(), Builder.build_random_string())
    PATH = "/test"
    REGION = build_region().value

    @staticmethod
    def build_event(
        method: str,
        *,
        path: str = PATH,
        origin: str = ALLOWED_ORIGINS[0],
        additional_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        return {
            "resource": path,
            "path": path,
            "httpMethod": method,
            "requestContext": {
                "identity": {"userArn": "arn:aws:iam::123456789012:user/Bob", "user": "Bob"},
                "requestId": "123456789",
            },
            "pathParameters": {},
            "multiValueQueryStringParameters": {},
            "headers": {"Content-Type": "application/json", "Origin": origin, **(additional_headers or {})},
            "body": None,
            "stageVariables": {
                "region": RequestEventBuilder.REGION,
            },
            "queryStringParameters": {},
        }


@pytest.mark.usefixtures("mock_xray")
class TestRouter:
    CONTEXT: LambdaContext = Mock(
        aws_request_id="deef4878-7910-11e6-8f14-25afc3e9ae33", get_remaining_time_in_millis=lambda: 0
    )

    def setup_method(self) -> None:
        self.dependency_manager = DependencyManager()
        self.router = Router(RequestEventBuilder.ALLOWED_ORIGINS, self.dependency_manager)
        self.router._audit_logger = Mock()
        self.config = build_config()

    def test_error_during_request_parsing(self) -> None:
        event = RequestEventBuilder.build_event(method="GET", origin=RequestEventBuilder.ALLOWED_ORIGINS[1])
        event["body"] = "this ain't no JSON"

        response = self.router.handle_request(event, self.CONTEXT, self.config)

        body = {"Code": "BadRequestError", "Message": "Invalid JSON body", "RequestId": self.CONTEXT.aws_request_id}
        assert response == {
            "headers": {"Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[1], **STANDARD_HEADERS},
            "body": orjson.dumps(body).decode(),
            "statusCode": HTTPStatus.BAD_REQUEST.value,
            "isBase64Encoded": False,
        }

    @pytest.mark.parametrize("http_verb", sorted(AUDIT_VERBS))
    def test_logger_is_called(self, http_verb: str) -> None:
        event = RequestEventBuilder.build_event(http_verb, origin=RequestEventBuilder.ALLOWED_ORIGINS[1])
        request = Request.from_lambda_event(event, self.CONTEXT)
        log_message = {
            "request": request.to_plain_dict(),
            "response": self.router.handle_request(event, self.CONTEXT, self.config),
        }
        assert json.loads(self.router._audit_logger.write_log.call_args.args[0][0]) == log_message  # type: ignore

    @patch.object(router, "get_jwt_user_id")
    @pytest.mark.parametrize("jwt_user_id", [None, "my-jwt"])
    def test_logger_with_and_without_jwt_user_id(self, get_jwt_user_id: Mock, jwt_user_id: Optional[str]) -> None:
        get_jwt_user_id.return_value = jwt_user_id

        event = RequestEventBuilder.build_event(
            method=random.choice(list(AUDIT_VERBS)),
            additional_headers={"Cookie": Builder.build_random_string()},
        )

        self.router.handle_request(event, self.CONTEXT, self.config)

        expected_request_dict = Request.from_lambda_event(event, self.CONTEXT).to_plain_dict()
        expected_request_dict["headers"].pop("cookie")
        if jwt_user_id:
            expected_request_dict["jwtUserId"] = jwt_user_id
        logged_request_dict = json.loads(self.router._audit_logger.write_log.call_args.args[0][0])[  # type: ignore
            "request"
        ]
        assert logged_request_dict == expected_request_dict
        assert "cookie" not in logged_request_dict["headers"]

    @pytest.mark.parametrize("http_verb", sorted(AUDIT_VERBS))
    def test_logger_in_case_of_error_during_request_parsing(self, http_verb: str) -> None:
        event = RequestEventBuilder.build_event(http_verb, origin=RequestEventBuilder.ALLOWED_ORIGINS[1])
        event["body"] = "this ain't no JSON"

        log_message = {
            "request": None,
            "event": event,
            "response": self.router.handle_request(event, self.CONTEXT, self.config),
        }
        assert json.loads(self.router._audit_logger.write_log.call_args.args[0][0]) == log_message  # type: ignore

    @pytest.mark.parametrize("http_verb", sorted(AUDIT_VERBS))
    def test_handler_finishes_even_audit_logger_fails(self, http_verb: str) -> None:
        self.router._audit_logger.write_log.side_effect = Exception  # type: ignore
        event = RequestEventBuilder.build_event(http_verb, origin=RequestEventBuilder.ALLOWED_ORIGINS[1])
        self.router.handle_request(event, self.CONTEXT, self.config)

    def test_logger_is_not_called(self) -> None:
        all_http_verbs = {verb.value for verb in HttpVerb}
        non_audit_logging_actions = all_http_verbs - AUDIT_VERBS
        for http_verb in non_audit_logging_actions:
            local_router = Router(RequestEventBuilder.ALLOWED_ORIGINS, self.dependency_manager)
            local_router._audit_logger = Mock()
            event = RequestEventBuilder.build_event(http_verb, origin=RequestEventBuilder.ALLOWED_ORIGINS[1])
            local_router.handle_request(event, self.CONTEXT, self.config)
            local_router._audit_logger.write_log.assert_not_called()

    def test_handler_is_called(self) -> None:
        call_check = Mock()

        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            call_check()
            return JsonResponse(headers={"header": "bla"}, body={"some": "body"}, status_code=HTTPStatus.CREATED)

        response = self.router.handle_request(
            RequestEventBuilder.build_event("GET", origin=RequestEventBuilder.ALLOWED_ORIGINS[1]),
            self.CONTEXT,
            self.config,
        )

        call_check.assert_called_once()
        assert response == {
            "headers": {
                "header": "bla",
                "Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[1],
                **STANDARD_HEADERS,
            },
            "statusCode": HTTPStatus.CREATED.value,
            "body": '{"some":"body"}',
            "isBase64Encoded": False,
        }

    def test_arguments_are_injected(self) -> None:
        call_check = Mock()

        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler(injected_value: str) -> JsonResponse:
            call_check(injected_value)
            return JsonResponse(status_code=HTTPStatus.NO_CONTENT)

        self.dependency_manager.register("injected_value", DependencyManager.TimeToLive.PER_REQUEST)(
            lambda: "injected!"
        )
        self.router.handle_request(RequestEventBuilder.build_event("GET"), self.CONTEXT, self.config)

        call_check.assert_called_once()
        assert call_check.call_args[0][0] == "injected!"  # pylint: disable=unsubscriptable-object

    def test_view_error(self) -> None:
        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            raise ForbiddenError("You shall not pass!")

        response = self.router.handle_request(RequestEventBuilder.build_event("GET"), self.CONTEXT, self.config)

        body = {"Code": "ForbiddenError", "Message": "You shall not pass!", "RequestId": self.CONTEXT.aws_request_id}
        assert response == {
            "headers": {"Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[0], **STANDARD_HEADERS},
            "statusCode": HTTPStatus.FORBIDDEN.value,
            "body": orjson.dumps(body).decode(),
            "isBase64Encoded": False,
        }

    def test_validation_error(self) -> None:
        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            raise ValidationError("test")

        response = self.router.handle_request(RequestEventBuilder.build_event("GET"), self.CONTEXT, self.config)

        body = {"Code": "BadRequestError", "Message": "test", "RequestId": self.CONTEXT.aws_request_id}
        assert response == {
            "headers": {"Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[0], **STANDARD_HEADERS},
            "statusCode": HTTPStatus.BAD_REQUEST.value,
            "body": orjson.dumps(body).decode(),
            "isBase64Encoded": False,
        }

    def test_validation_error_with_dict_error_messages(self) -> None:
        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            raise ValidationError({"field1": "is broken", "field2": "is also broken"})

        response = self.router.handle_request(RequestEventBuilder.build_event("GET"), self.CONTEXT, self.config)

        body = {
            "Code": "BadRequestError",
            "Message": "field1: is broken; field2: is also broken",
            "RequestId": self.CONTEXT.aws_request_id,
        }
        assert response == {
            "headers": {"Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[0], **STANDARD_HEADERS},
            "statusCode": HTTPStatus.BAD_REQUEST.value,
            "body": orjson.dumps(body).decode(),
            "isBase64Encoded": False,
        }

    def test_unhandled_error(self) -> None:
        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            raise ValueError("Internal error info")

        response = self.router.handle_request(RequestEventBuilder.build_event("GET"), self.CONTEXT, self.config)

        body = {
            "Code": "InternalError",
            "Message": "Something went wrong. If this error persists please contact the CDH support.",
            "RequestId": self.CONTEXT.aws_request_id,
        }
        assert response == {
            "headers": {"Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[0], **STANDARD_HEADERS},
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            "body": orjson.dumps(body).decode(),
            "isBase64Encoded": False,
        }

    def test_error_raised_in_inject_handler_arguments(self) -> None:
        dependency_manager = Mock(DependencyManager)
        dependency_manager.build_dependencies_for_callable.side_effect = BadRequestError("invalid arguments")
        local_router = Router(RequestEventBuilder.ALLOWED_ORIGINS, dependency_manager)

        @local_router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            raise AssertionError()

        response = local_router.handle_request(RequestEventBuilder.build_event("GET"), self.CONTEXT, self.config)

        body = {"Code": "BadRequestError", "Message": "invalid arguments", "RequestId": self.CONTEXT.aws_request_id}
        assert response == {
            "headers": {"Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[0], **STANDARD_HEADERS},
            "statusCode": HTTPStatus.BAD_REQUEST.value,
            "body": orjson.dumps(body).decode(),
            "isBase64Encoded": False,
        }

    def test_cors_preflight(self) -> None:
        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            raise AssertionError()

        response = self.router.handle_request(
            RequestEventBuilder.build_event("OPTIONS", origin=RequestEventBuilder.ALLOWED_ORIGINS[1]),
            self.CONTEXT,
            self.config,
        )

        assert response == {
            "headers": {
                "Access-Control-Allow-Methods": CORS_METHODS,
                "Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[1],
                "Access-Control-Allow-Headers": CORS_HEADER,
                **STANDARD_HEADERS,
            },
            "statusCode": HTTPStatus.OK.value,
            "body": None,
            "isBase64Encoded": False,
        }

    @pytest.mark.parametrize("http_verb", [HttpVerb.GET, HttpVerb.OPTIONS])
    def test_cors_preflight_works_with_lowercase_header(self, http_verb: HttpVerb) -> None:
        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            return JsonResponse()

        event = RequestEventBuilder.build_event(http_verb.value)
        del event["headers"]["Origin"]
        event["headers"]["origin"] = RequestEventBuilder.ALLOWED_ORIGINS[1]
        response = self.router.handle_request(event, self.CONTEXT, self.config)

        assert response["headers"]["Access-Control-Allow-Origin"] == RequestEventBuilder.ALLOWED_ORIGINS[1]

    def test_cors_preflight_on_route_that_does_not_exist(self) -> None:
        response = self.router.handle_request(
            RequestEventBuilder.build_event("OPTIONS", origin=RequestEventBuilder.ALLOWED_ORIGINS[1]),
            self.CONTEXT,
            self.config,
        )

        body = {
            "Code": "NotFoundError",
            "Message": "Route " + RequestEventBuilder.PATH + " does not exist",
            "RequestId": self.CONTEXT.aws_request_id,
        }
        assert response == {
            "headers": {"Access-Control-Allow-Origin": RequestEventBuilder.ALLOWED_ORIGINS[1], **STANDARD_HEADERS},
            "statusCode": HTTPStatus.NOT_FOUND.value,
            "body": orjson.dumps(body).decode(),
            "isBase64Encoded": False,
        }

    @pytest.mark.parametrize("http_verb", [HttpVerb.GET, HttpVerb.OPTIONS])
    def test_response_for_wrong_origin_has_no_allow_origin_header(self, http_verb: HttpVerb) -> None:
        call_check = Mock()

        @self.router.route(RequestEventBuilder.PATH, HttpVerb.GET)
        def handler() -> JsonResponse:
            call_check()
            return JsonResponse()

        response = self.router.handle_request(
            RequestEventBuilder.build_event(http_verb.value, origin="wrong origin"), self.CONTEXT, self.config
        )

        assert call_check.call_count == (1 if http_verb is HttpVerb.GET else 0)
        assert "Access-Control-Allow-Origin" not in response.get("headers", {})
        assert response["statusCode"] == HTTPStatus.OK.value
