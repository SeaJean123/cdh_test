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
import inspect
import json
import os
from copy import deepcopy
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any
from typing import Dict
from typing import get_origin
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecCollector
from cdh_core_api.api.router import Router
from cdh_core_api.app import Application
from cdh_core_api.app import coreapi
from cdh_core_api.validation.common_paths import HubPath

from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.http import HttpVerb
from cdh_core.enums.hubs import Hub
from cdh_core.manager.dependency_manager import DependencyManager
from cdh_core_dev_tools.testing.builder import Builder

EXAMPLE_DIR = os.path.join(os.path.dirname(__file__), "examples")
EXAMPLE_FILES = sorted(file_name for file_name in os.listdir(EXAMPLE_DIR) if file_name.endswith(".json"))


@pytest.fixture()
def initialize_env_variables_for_config(monkeypatch: Any) -> None:
    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setenv("RESOURCE_NAME_PREFIX", "")
    monkeypatch.setenv("GLUE_SYNC_LAMBDA_ARN", "")
    monkeypatch.setenv("PROCESS_AD_GROUPS_QUEUE_URL", "")
    monkeypatch.setenv("BOOTSTRAP_ACCOUNTS_QUEUE_URL", "")
    monkeypatch.setenv("BOOTSTRAP_ACCOUNTS_LAMBDA_ARN", str(build_arn("lambda")))
    monkeypatch.setenv("AD_GROUP_CREATION_QUEUE_URL", "")
    monkeypatch.setenv("DATASET_NOTIFICATION_TOPIC", str(build_arn("sns")))
    monkeypatch.setenv("NOTIFICATION_TOPIC", str(build_arn("sns")))
    monkeypatch.setenv("AWS_REGION", Region.preferred(Partition.default()).value)
    monkeypatch.setenv("ROLES_SYNC_QUEUE_URL", "")
    monkeypatch.setenv("ORCHESTRATION_STATE_MACHINE_ARN", str(build_arn("states")))
    monkeypatch.setenv("ACCOUNT_ORDERING_API_URL", "")
    monkeypatch.setenv("ACCOUNT_ORDERING_API_KEY", "")
    monkeypatch.setenv("BILLING_API_URL", "")
    monkeypatch.setenv("BILLING_API_BEARER_TOKEN", "")
    monkeypatch.setenv("USE_CN_PROXY", "False")
    monkeypatch.setenv("AUTHORIZATION_API_URL", f"https://{Builder.build_random_string()}.com")
    monkeypatch.setenv("USERS_API_URL", f"https://{Builder.build_random_string()}.com")
    monkeypatch.setenv("AUTHORIZATION_API_COOKIE_NAME", Builder.build_random_string())
    monkeypatch.setenv("ENCRYPTION_KEY_NAME", Builder.build_random_string())
    monkeypatch.setenv("RESULT_PAGE_SIZE", "42")


def build_event(
    path: str,
    method: str,
    path_parameters: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    query_parameters: Optional[Mapping[str, Union[str, List[str]]]] = None,
) -> Dict[str, Any]:
    return {
        "resource": path,
        "path": path,
        "httpMethod": method,
        "requestContext": {
            "identity": {
                "userArn": "arn:aws:iam::123456789012:user/Bob",
                "user": None,
            },
            "apiId": "1234apiId",
            "stage": "default",
            "requestId": Builder.build_random_digit_string(10),
        },
        "pathParameters": path_parameters or {},
        "headers": {"Content-Type": "application/json", "Origin": "West"},
        "body": json.dumps(body) if body else None,
        **build_event_query_parameters(query_parameters),
        "stageVariables": {
            "region": Region.preferred(Partition.default()).value,
        },
    }


def build_event_query_parameters(
    query_parameters: Optional[Mapping[str, Union[str, List[str]]]] = None
) -> Dict[str, Mapping[str, Union[str, List[str]]]]:
    """Mimic the way query parameters are passed to Lambda by AWS Gateway."""
    query_parameters = query_parameters or {}
    return {
        "queryStringParameters": {
            key: (value if isinstance(value, str) else value[-1]) for key, value in query_parameters.items()
        },
        "multiValueQueryStringParameters": {
            key: (value if isinstance(value, list) else [value]) for key, value in query_parameters.items()
        },
    }


def build_lambda_context() -> LambdaContext:
    lambda_region = Region.preferred(Partition.default()).value
    lambda_context = MagicMock(
        LambdaContext,
        invoked_function_arn=f"arn:aws:lambda:{lambda_region}::function:test",
        aws_request_id=Builder.build_request_id(),
        log_group_name="foo",
        log_stream_name="bar",
    )
    lambda_context.get_remaining_time_in_millis.return_value = 10000
    return lambda_context


@pytest.mark.usefixtures("mock_xray")
@pytest.mark.usefixtures("initialize_env_variables_for_config")
class TestApp:
    def setup_method(self) -> None:
        self.app = Application(OpenApiSpecCollector())
        self.aws = Mock()
        self.config = Mock()
        self.config.disabled = False
        self.config.hubs = list(Hub)
        self.lock_service = Mock()
        self.app.dependency("aws", DependencyManager.TimeToLive.FOREVER)(lambda: self.aws)
        self.app.dependency("config", DependencyManager.TimeToLive.FOREVER)(lambda: self.config)
        self.app.dependency("lock_service", DependencyManager.TimeToLive.FOREVER)(lambda: self.lock_service)

    def test_disabled(self) -> None:
        self.config.disabled = True

        @self.app.route("/test", ["GET"])
        def handler() -> JsonResponse:
            raise AssertionError()

        event = build_event("/test", "GET")
        response = self.app.handle_request(event, build_lambda_context())
        assert response["statusCode"] == HTTPStatus.SERVICE_UNAVAILABLE.value

    def test_valid_hub(self) -> None:
        call_check = Mock()

        @self.app.route("/{hub}/items", ["GET"])
        def handler(path: HubPath) -> JsonResponse:
            call_check(path.hub)
            return JsonResponse(status_code=HTTPStatus.OK)

        event = build_event("/{hub}/items", "GET")
        event["pathParameters"] = {"hub": "global"}
        response = self.app.handle_request(event, build_lambda_context())
        assert response["statusCode"] == HTTPStatus.OK.value
        call_check.assert_called_once_with(Hub("global"))

    def test_invalid_hub(self) -> None:
        call_check = Mock()

        @self.app.route("/{hub}/items", ["GET"])
        def handler(path: HubPath) -> JsonResponse:
            call_check(path.hub)
            return JsonResponse()

        event = build_event("/{hub}/items", "GET")
        event["pathParameters"] = {"hub": "jupiter"}
        response = self.app.handle_request(event, build_lambda_context())
        assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
        assert json.loads(response["body"])["Code"] == "BadRequestError"
        call_check.assert_not_called()

    def test_injection_of_query_parameters(self) -> None:
        call_check = Mock()

        @dataclass(frozen=True)
        class QuerySchema:
            value: str
            value_list: List[str]

        @self.app.route("/items", ["GET"])
        def handler(query: QuerySchema) -> JsonResponse:
            call_check(query)
            return JsonResponse()

        event = build_event("/items", "GET")
        event["queryStringParameters"]["value"] = "queryparam"
        event["queryStringParameters"]["value_list"] = "b"
        event["multiValueQueryStringParameters"] = {"value": ["queryparam"], "value_list": ["a", "b"]}

        self.app.handle_request(event, build_lambda_context())
        call_check.assert_called_once_with(QuerySchema(value="queryparam", value_list=["a", "b"]))

    def test_injection_of_body(self) -> None:
        call_check = Mock()

        @dataclass(frozen=True)
        class BodySchema:
            value: str

        @self.app.route("/items", ["POST"])
        def handler(body: BodySchema) -> JsonResponse:
            call_check(body)
            return JsonResponse()

        event = build_event("/items", "POST")
        event["body"] = '{"value": "bodyparam"}'

        self.app.handle_request(event, build_lambda_context())
        call_check.assert_called_once_with(BodySchema(value="bodyparam"))

    def test_dependency_failure_recovery(self) -> None:
        return_value = 42
        flaky_calls: List[int] = []
        error = Exception("my error")

        @self.app.dependency("flaky", DependencyManager.TimeToLive.FOREVER)
        def fail_on_first_call() -> int:
            flaky_calls.append(0)
            if len(flaky_calls) == 1:
                raise error
            return return_value

        @self.app.route("/items", ["GET"])
        def handler(flaky: int) -> JsonResponse:
            return JsonResponse(body={"value": flaky})

        event = build_event("/items", "GET")
        with pytest.raises(Exception) as exc_info:
            self.app.handle_request(event, build_lambda_context())
        assert exc_info.value == error

        response = self.app.handle_request(event, build_lambda_context())
        assert json.loads(response["body"]) == {"value": return_value}


@pytest.mark.usefixtures("mock_xray")
@pytest.mark.usefixtures("initialize_env_variables_for_config")
class CoreApiTestCase:
    real_core_api: Application = coreapi
    items_to_skip: Set[Tuple[str, HttpVerb]] = set()
    dependencies_to_mock: Set[Tuple[str, DependencyManager.TimeToLive]] = {
        ("aws", DependencyManager.TimeToLive.FOREVER),
        ("sns_client", DependencyManager.TimeToLive.FOREVER),
    }

    def setup_method(self) -> None:
        self.dependency_manager = deepcopy(self.real_core_api._dependency_manager)
        self.router = Router({Builder.build_random_string()}, self.dependency_manager)
        self.router._routes = deepcopy(self.real_core_api._router._routes)
        self.core_api = Application(OpenApiSpecCollector())
        self.core_api.dependency = self.dependency_manager.register
        self.core_api._dependency_manager = self.dependency_manager
        self.core_api._router = self.router

        for dependency, ttl in self.dependencies_to_mock:
            self.dependency_manager.register_constant(dependency, ttl, value=Mock(), force=True)

    def run_test(self, example_data: Dict[str, Any]) -> None:
        http_verb = HttpVerb[example_data["method"]]
        default_path_parameters = example_data.get("path")
        if (default_path_parameters, http_verb) in self.items_to_skip:
            return

        handler = self.router.get_route(path=example_data["path"], method=http_verb)
        handler_signature = inspect.signature(handler)
        event = build_event(
            example_data["path"],
            example_data["method"],
            path_parameters=example_data.get("defaultPathParameters"),
            body=example_data.get("body"),
            query_parameters=example_data.get("defaultQueryParameters"),
        )

        def _validation_handler(*args: Any, **kwargs: Dict[str, Any]) -> JsonResponse:
            assert not args
            mocked = {dep for (dep, _) in self.dependencies_to_mock}
            for key, arg in kwargs.items():
                if key in mocked:
                    continue
                annotation = handler_signature.parameters[key].annotation
                assert isinstance(arg, get_origin(annotation) or annotation)
            assert len(kwargs) == len(handler_signature.parameters)
            return JsonResponse(status_code=HTTPStatus.UNAVAILABLE_FOR_LEGAL_REASONS)

        _validation_handler.__signature__ = handler_signature  # type: ignore
        self.router._routes._handlers[example_data["path"]][http_verb] = _validation_handler
        response = self.core_api.handle_request(event, build_lambda_context())
        assert response["statusCode"] == HTTPStatus.UNAVAILABLE_FOR_LEGAL_REASONS


class TestCoreApi(CoreApiTestCase):
    @pytest.mark.parametrize("example_file", EXAMPLE_FILES)
    def test_each_endpoint_individually(self, example_file: str) -> None:
        with open(os.path.join(EXAMPLE_DIR, example_file), "r", encoding="UTF-8") as file:
            self.run_test(json.load(file))

    def test_all_endpoints_in_succession(self) -> None:
        # reuse the same dependency manager and check it updates all PER_REQUEST dependencies as necessary
        for example_file in EXAMPLE_FILES:
            with open(os.path.join(EXAMPLE_DIR, example_file), "r", encoding="UTF-8") as file:
                self.run_test(json.load(file))
