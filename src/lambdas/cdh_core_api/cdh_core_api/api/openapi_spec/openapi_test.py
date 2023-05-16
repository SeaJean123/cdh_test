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
# pylint: disable=unused-argument
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import Optional
from unittest.mock import ANY

import pytest
import yaml
from cdh_core_api.api.openapi_spec.openapi import _HandlerInfo
from cdh_core_api.api.openapi_spec.openapi import ConflictingPathSchema
from cdh_core_api.api.openapi_spec.openapi import ConflictingSchemas
from cdh_core_api.api.openapi_spec.openapi import DataclassSchema
from cdh_core_api.api.openapi_spec.openapi import DuplicateBody
from cdh_core_api.api.openapi_spec.openapi import DuplicateResponseStatus
from cdh_core_api.api.openapi_spec.openapi import DuplicateRoute
from cdh_core_api.api.openapi_spec.openapi import format_docstring
from cdh_core_api.api.openapi_spec.openapi import get_path_parameters
from cdh_core_api.api.openapi_spec.openapi import get_spec_header
from cdh_core_api.api.openapi_spec.openapi import HandlerAlreadyExists
from cdh_core_api.api.openapi_spec.openapi import MissingPath
from cdh_core_api.api.openapi_spec.openapi import OpenApiSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecCollector
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecGenerator

from cdh_core.entities.response import JsonResponse
from cdh_core.enums.http import HttpVerb
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder

URL = Builder.build_random_url()
SPEC_HEADER = get_spec_header(URL)

TEST_SCHEMA = OpenApiSchema("test-schema", {"param": "string"})


SECURITY_SCHEMES_YAML = """\
    sigv4:
      in: header
      name: Authorization
      type: apiKey
      x-amazon-apigateway-authtype: awsSigv4
"""


class TestOpenApiSpecCollector:
    def test_collect_handlers(self) -> None:
        openapi = OpenApiSpecCollector()

        @openapi.route("/items", HttpVerb.POST)
        def handler1() -> JsonResponse:
            return JsonResponse()

        @openapi.route("/items/{id}", HttpVerb.GET)
        def handler2() -> JsonResponse:
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {
            "/items": [_HandlerInfo("/items", HttpVerb.POST)],
            "/items/{id}": [_HandlerInfo("/items/{id}", HttpVerb.GET)],
        }

    def test_must_not_add_same_route_twice_same_handler(self) -> None:
        openapi = OpenApiSpecCollector()

        with pytest.raises(DuplicateRoute):

            @openapi.route("/items", HttpVerb.GET)
            @openapi.route("/items", HttpVerb.GET)
            def handler() -> JsonResponse:
                return JsonResponse()

    def test_must_not_add_several_routes(self) -> None:
        openapi = OpenApiSpecCollector()

        with pytest.raises(HandlerAlreadyExists):

            @openapi.route("/items1", HttpVerb.GET)
            @openapi.route("/items2", HttpVerb.GET)
            def handler() -> JsonResponse:
                return JsonResponse()

    def test_must_not_add_several_verbs(self) -> None:
        openapi = OpenApiSpecCollector()

        with pytest.raises(HandlerAlreadyExists):

            @openapi.route("/items", HttpVerb.GET)
            @openapi.route("/items", HttpVerb.POST)
            def handler() -> JsonResponse:
                return JsonResponse()

    def test_may_add_several_handlers_for_the_same_path_but_different_verb(self) -> None:
        openapi = OpenApiSpecCollector()

        @openapi.route("/items", HttpVerb.GET)
        @openapi.body(TEST_SCHEMA)
        def handler1() -> JsonResponse:
            return JsonResponse()

        @openapi.route("/items", HttpVerb.POST)
        def handler2() -> JsonResponse:
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {
            "/items": [
                _HandlerInfo("/items", HttpVerb.GET, body=TEST_SCHEMA),
                _HandlerInfo("/items", HttpVerb.POST, body=None),
            ]
        }

    def test_must_not_add_conflicting_handlers(self) -> None:
        openapi = OpenApiSpecCollector()

        @openapi.route("/items", HttpVerb.GET)
        def handler1() -> JsonResponse:
            return JsonResponse()

        with pytest.raises(DuplicateRoute):

            @openapi.route("/items", HttpVerb.GET)
            def handler2() -> JsonResponse:
                return JsonResponse()

    def test_overwrite_handler_with_force(self) -> None:
        openapi = OpenApiSpecCollector()
        path = "/items"

        @dataclass(frozen=True)
        class TestSchema:
            param: str

        @openapi.route(path, HttpVerb.GET)
        def handler1(body: TestSchema) -> JsonResponse:
            return JsonResponse()

        body = openapi.get_schemas()[0]
        assert body.name == TestSchema.__name__
        assert openapi.get_handlers_by_path() == {path: [_HandlerInfo(path, HttpVerb.GET, body=body)]}  # type: ignore

        @openapi.route(path, HttpVerb.GET, force=True)
        def handler2() -> JsonResponse:
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {
            path: [_HandlerInfo(path, HttpVerb.GET, body=None)],
        }

    def test_fail_on_handler_without_path(self) -> None:
        openapi = OpenApiSpecCollector()

        @openapi.body(TEST_SCHEMA)
        def handler() -> JsonResponse:
            return JsonResponse()

        with assert_raises(MissingPath(handler)):
            openapi.get_handlers_by_path()

    def test_collect_docstring(self) -> None:
        openapi = OpenApiSpecCollector()

        @openapi.route("/items", HttpVerb.PUT)
        def handler() -> JsonResponse:
            """Do whatever you want."""
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {
            "/items": [_HandlerInfo("/items", HttpVerb.PUT, description="Do whatever you want.")]
        }

    def test_collect_body_with_decorator(self) -> None:
        openapi = OpenApiSpecCollector()

        @openapi.route("/items", HttpVerb.GET)
        @openapi.body(TEST_SCHEMA)
        def handler() -> JsonResponse:
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {"/items": [_HandlerInfo("/items", HttpVerb.GET, body=TEST_SCHEMA)]}
        assert openapi.get_schemas() == [TEST_SCHEMA]

    def test_collect_body_from_parameter(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class TestSchema:
            param: str

        @openapi.route("/items", HttpVerb.GET)
        def handler(body: TestSchema) -> JsonResponse:
            return JsonResponse()

        assert len(openapi.get_schemas()) == 1
        schema = openapi.get_schemas()[0]
        assert isinstance(schema, DataclassSchema)
        assert openapi.get_handlers_by_path() == {"/items": [_HandlerInfo("/items", HttpVerb.GET, body=schema)]}

    def test_must_not_specify_two_request_bodies(self) -> None:
        openapi = OpenApiSpecCollector()

        with pytest.raises(DuplicateBody):

            @openapi.body(TEST_SCHEMA)
            @openapi.body(TEST_SCHEMA)
            def handler() -> JsonResponse:
                return JsonResponse()

    def test_may_use_same_body_in_several_handlers(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class TestSchema:
            param: str

        @openapi.route("/items1", HttpVerb.GET)
        def handler1(body: TestSchema) -> JsonResponse:
            return JsonResponse()

        @openapi.route("/items2", HttpVerb.GET)
        def handler2(body: TestSchema) -> JsonResponse:
            return JsonResponse()

    def test_must_not_specify_two_bodies_with_same_name(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class Schema1:
            param1: str

        @openapi.route("/items1", HttpVerb.GET)
        def handler1(body: Schema1) -> JsonResponse:
            return JsonResponse()

        @dataclass(frozen=True)
        class Schema2:
            param2: str

        Schema2.__name__ = Schema1.__name__

        with pytest.raises(ConflictingSchemas):

            @openapi.route("/items2", HttpVerb.GET)
            def handler2(body: Schema2) -> JsonResponse:
                return JsonResponse()

    def test_overwrite_body_and_route(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class Schema1:
            param1: str

        @openapi.route("/items", HttpVerb.GET)
        def handler1(body: Schema1) -> JsonResponse:
            return JsonResponse()

        @openapi.overwrite_existing_schema(Schema1.__name__)
        @dataclass(frozen=True)
        class Schema2:
            param2: str

        @openapi.route("/items", HttpVerb.GET, force=True)
        def handler2(body: Schema2) -> JsonResponse:
            return JsonResponse()

        assert len(openapi.get_schemas()) == 1
        schema = openapi.get_schemas()[0]
        assert isinstance(schema, DataclassSchema)
        assert schema.name == Schema1.__name__
        assert schema == DataclassSchema(Schema2)
        assert openapi.get_handlers_by_path() == {"/items": [_HandlerInfo("/items", HttpVerb.GET, body=schema)]}

    def test_collect_query_from_parameter(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class TestSchema:
            param: str

        @openapi.route("/items", HttpVerb.GET)
        def handler(query: TestSchema) -> JsonResponse:
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {"/items": [_HandlerInfo("/items", HttpVerb.GET, query=ANY)]}
        schema = openapi.get_handlers_by_path()["/items"][0].query
        assert schema is not None and schema.dataclass is TestSchema

    def test_collect_path_from_parameter(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class TestSchema:
            param: str

        @openapi.route("/items/{param}", HttpVerb.GET)
        def handler(path: TestSchema) -> JsonResponse:
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {
            "/items/{param}": [_HandlerInfo("/items/{param}", HttpVerb.GET, path_schema=ANY)]
        }
        schema = openapi.get_handlers_by_path()["/items/{param}"][0].path_schema
        assert schema is not None and schema.dataclass is TestSchema

    def test_missing_path_param_schema(self) -> None:
        openapi = OpenApiSpecCollector()

        with pytest.raises(TypeError):

            @openapi.route("/items/{path}", HttpVerb.GET)
            def handler(path: str) -> JsonResponse:
                return JsonResponse()

    def test_missing_path_param_in_schema(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class TestSchema:
            param1: str

        with pytest.raises(ValueError):

            @openapi.route("/items/{param1}/{param2}", HttpVerb.GET)
            def handler(path: TestSchema) -> JsonResponse:
                return JsonResponse()

    def test_missing_path_param_in_path(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class TestSchema:
            param1: str
            param2: str

        with pytest.raises(ValueError):

            @openapi.route("/items/{param1}", HttpVerb.GET)
            def handler(path: TestSchema) -> JsonResponse:
                return JsonResponse()

    def test_conflicting_path_param(self) -> None:
        openapi = OpenApiSpecCollector()

        @dataclass(frozen=True)
        class TestSchema1:
            param1: str

        @openapi.route("/items/{param1}", HttpVerb.GET)
        def handler1(path: TestSchema1) -> JsonResponse:
            return JsonResponse()

        @dataclass(frozen=True)
        class TestSchema2:
            param1: str

        @openapi.route("/items/{param1}", HttpVerb.POST)
        def handler2(path: TestSchema2) -> JsonResponse:
            return JsonResponse()

        with assert_raises(ConflictingPathSchema("/items/{param1}", {TestSchema1, TestSchema2})):
            openapi.get_handlers_by_path()

    def test_collect_responses(self) -> None:
        openapi = OpenApiSpecCollector()

        @openapi.route("/items", HttpVerb.GET)
        @openapi.response(HTTPStatus.CREATED, TEST_SCHEMA)
        @openapi.response(HTTPStatus.NO_CONTENT)
        def handler() -> JsonResponse:
            return JsonResponse()

        assert openapi.get_handlers_by_path() == {
            "/items": [
                _HandlerInfo(
                    path="/items",
                    http_verb=HttpVerb.GET,
                    responses={HTTPStatus.CREATED: TEST_SCHEMA, HTTPStatus.NO_CONTENT: None},
                )
            ]
        }
        assert openapi.get_schemas() == [TEST_SCHEMA]

    def test_must_not_specify_two_responses_for_same_status_code(self) -> None:
        openapi = OpenApiSpecCollector()

        with pytest.raises(DuplicateResponseStatus):

            @openapi.response(HTTPStatus.CREATED, TEST_SCHEMA)
            @openapi.response(HTTPStatus.CREATED)
            def handler() -> JsonResponse:
                return JsonResponse()

    def test_link_collects_schema(self) -> None:
        openapi = OpenApiSpecCollector()
        assert openapi.link(TEST_SCHEMA) == {"$ref": "#/components/schemas/test-schema"}
        assert openapi.get_schemas() == [TEST_SCHEMA]

    def test_cannot_link_two_different_schemas_with_same_name(self) -> None:
        openapi = OpenApiSpecCollector()
        openapi.link(TEST_SCHEMA)
        with pytest.raises(ConflictingSchemas):
            openapi.link(OpenApiSchema(TEST_SCHEMA.name, {"param": "int"}))

    def test_can_overwrite_link_schema_with_force_flag(self) -> None:
        openapi = OpenApiSpecCollector()
        openapi.link(TEST_SCHEMA)
        other_schema = OpenApiSchema(TEST_SCHEMA.name, {"param": "int"})
        openapi.link(other_schema, force=True)
        assert openapi.get_schemas() == [other_schema]

    def test_can_link_same_schema_twice(self) -> None:
        openapi = OpenApiSpecCollector()
        openapi.link(TEST_SCHEMA)
        openapi.link(TEST_SCHEMA)

    def test_use_linked_schema_in_response(self) -> None:
        openapi = OpenApiSpecCollector()
        openapi.link(TEST_SCHEMA)

        @openapi.response(HTTPStatus.CREATED, TEST_SCHEMA)
        def handler() -> JsonResponse:
            return JsonResponse()

        assert openapi.get_schemas() == [TEST_SCHEMA]

    def test_response_schema_conflicting_with_linked_schema(self) -> None:
        openapi = OpenApiSpecCollector()
        other_schema = OpenApiSchema(TEST_SCHEMA.name, {"param": "int"})
        openapi.link(TEST_SCHEMA)

        with pytest.raises(ConflictingSchemas):

            @openapi.response(HTTPStatus.CREATED, other_schema)
            def handler() -> JsonResponse:
                return JsonResponse()

    def test_linked_schema_conflicting_with_response_schema(self) -> None:
        openapi = OpenApiSpecCollector()
        other_schema = OpenApiSchema(TEST_SCHEMA.name, {"param": "int"})

        @openapi.response(HTTPStatus.CREATED, TEST_SCHEMA)
        def handler() -> JsonResponse:
            return JsonResponse()

        with pytest.raises(ConflictingSchemas):
            openapi.link(other_schema)

    def test_linked_schema_overwrites_response_schema_with_force(self) -> None:
        openapi = OpenApiSpecCollector()
        other_schema = OpenApiSchema(TEST_SCHEMA.name, {"param": "int"})

        @openapi.response(HTTPStatus.CREATED, TEST_SCHEMA)
        def handler() -> JsonResponse:
            return JsonResponse()

        openapi.link(other_schema, force=True)

        assert openapi.get_schemas() == [other_schema]


class TestOpenApiSpecGenerator:
    def test_empty_spec(self) -> None:
        spec = OpenApiSpecGenerator(handlers_by_path={}, schemas=[]).generate(URL)
        assert spec == (
            SPEC_HEADER + "paths: {}\ncomponents:\n  schemas: {}\n  securitySchemes:\n" + SECURITY_SCHEMES_YAML
        )

    def test_path_without_parameters_or_body_or_responses(self) -> None:
        handler_info = _HandlerInfo("/items", HttpVerb.GET, description="Hello Openapi")
        spec = OpenApiSpecGenerator(handlers_by_path={"/items": [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert list(data.keys()) == ["/items"]
        assert data["/items"] == {
            "get": {
                "description": "Hello Openapi",
                "responses": {
                    "204": {
                        "description": "NO_CONTENT",
                        "headers": {
                            "Access-Control-Allow-Headers": {"schema": {"type": "string"}},
                            "Access-Control-Allow-Origin": {"schema": {"type": "string"}},
                        },
                    }
                },
                "x-amazon-apigateway-request-validator": "validateBodyAndParameters",
                "x-amazon-apigateway-integration": ANY,
            },
            "options": {
                "responses": {
                    "200": {
                        "description": "OK",
                        "headers": {
                            "Access-Control-Allow-Headers": {"schema": {"type": "string"}},
                            "Access-Control-Allow-Methods": {"schema": {"type": "string"}},
                            "Access-Control-Allow-Origin": {"schema": {"type": "string"}},
                        },
                    }
                },
                "security": [],
                "tags": ["options"],
                "x-amazon-apigateway-integration": ANY,
            },
        }

    def test_path_with_parameters(self) -> None:
        path = "/items/{param1}/{param2}"

        @dataclass
        class TestSchema:
            param1: str
            param2: str

        path_schema = DataclassSchema(TestSchema)

        handler_info = _HandlerInfo(path, HttpVerb.GET, path_schema=path_schema)
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["parameters"] == [
            {
                "name": "param1",
                "in": "path",
                "required": True,
                "schema": {"title": "param1", "type": "string"},
            },
            {
                "name": "param2",
                "in": "path",
                "required": True,
                "schema": {"title": "param2", "type": "string"},
            },
        ]

    def test_path_with_body_as_openapi_schema(self) -> None:
        path = "/items"
        handler_info = _HandlerInfo(path, HttpVerb.GET, body=TEST_SCHEMA)
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["get"]["x-amazon-apigateway-request-validator"] == "validateBodyAndParameters"
        assert data[path]["get"]["requestBody"] == {
            "content": {
                "application/json": {
                    "schema": {
                        "$ref": f"#/components/schemas/{TEST_SCHEMA.name}",
                    },
                },
            },
            "required": True,
        }

    def test_path_with_body_as_dataclass(self) -> None:
        path = "/items"

        @dataclass(frozen=True)
        class TestSchema:
            param: str

        body_schema = DataclassSchema(TestSchema)

        handler_info = _HandlerInfo(path, HttpVerb.GET, body=body_schema)
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["get"]["x-amazon-apigateway-request-validator"] == "NONE"
        assert data[path]["get"]["requestBody"] == {
            "content": {
                "application/json": {
                    "schema": {
                        "$ref": "#/components/schemas/TestSchema",
                    },
                },
            },
            "required": True,
        }

    def test_path_with_required_query_param(self) -> None:
        path = "/items"

        @dataclass(frozen=True)
        class TestSchema:
            required_param: str

        handler_info = _HandlerInfo(path, HttpVerb.GET, query=DataclassSchema(TestSchema))
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["get"]["x-amazon-apigateway-request-validator"] == "validateBodyAndParameters"
        assert data[path]["get"]["parameters"] == [
            {
                "name": "required_param",
                "schema": {"type": "string", "title": "required_param"},
                "in": "query",
                "required": True,
            },
        ]

    def test_path_with_optional_query_params(self) -> None:
        path = "/items"

        @dataclass(frozen=True)
        class TestSchema:
            with_default_none: Optional[str] = None
            with_default_string: str = "default-value"

        handler_info = _HandlerInfo(path, HttpVerb.GET, query=DataclassSchema(TestSchema))
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["get"]["x-amazon-apigateway-request-validator"] == "validateBodyAndParameters"
        assert data[path]["get"]["parameters"] == [
            {
                "name": "with_default_none",
                "schema": {"type": "string", "title": "with_default_none", "nullable": True},
                "in": "query",
                "required": False,
            },
            {
                "name": "with_default_string",
                "schema": {"type": "string", "title": "with_default_string", "default": "default-value"},
                "in": "query",
                "required": False,
            },
        ]

    def test_path_with_enum_query_params(self) -> None:
        path = "/items"

        class MyEnum(Enum):
            """My enum doc."""

            RED = "red"
            GREEN = "green"

        @dataclass(frozen=True)
        class TestSchema:
            enum_param: MyEnum = MyEnum.RED

        handler_info = _HandlerInfo(path, HttpVerb.GET, query=DataclassSchema(TestSchema))
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["get"]["x-amazon-apigateway-request-validator"] == "validateBodyAndParameters"
        assert data[path]["get"]["parameters"] == [
            {
                "name": "enum_param",
                # no default: None here because None is not a valid string
                "schema": {"$ref": "#/components/schemas/MyEnum"},
                "in": "query",
                "required": False,
            },
        ]

    def test_path_with_responses(self) -> None:
        path = "/items"
        handler_info = _HandlerInfo(
            path,
            HttpVerb.GET,
            responses={
                HTTPStatus.CREATED: TEST_SCHEMA,
                HTTPStatus.NO_CONTENT: None,
            },
        )
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["get"]["responses"] == {
            str(HTTPStatus.CREATED.value): {
                "description": "CREATED",
                "headers": {
                    "Access-Control-Allow-Headers": {"schema": {"type": "string"}},
                    "Access-Control-Allow-Origin": {"schema": {"type": "string"}},
                },
                "content": {
                    "application/json": {
                        "schema": {
                            "$ref": f"#/components/schemas/{TEST_SCHEMA.name}",
                        },
                    },
                },
            },
            str(HTTPStatus.NO_CONTENT.value): {
                "description": "NO_CONTENT",
                "headers": {
                    "Access-Control-Allow-Headers": {"schema": {"type": "string"}},
                    "Access-Control-Allow-Origin": {"schema": {"type": "string"}},
                },
            },
        }

    def test_internal_endpoints(self) -> None:
        path = "/items"
        handler_info = _HandlerInfo(path, HttpVerb.GET, responses={}, internal=True)
        spec = OpenApiSpecGenerator(handlers_by_path={path: [handler_info]}, schemas=[]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        data = yaml.safe_load(spec)["paths"]
        assert data[path]["get"]["tags"] == ["internal"]

    def test_schema(self) -> None:
        schema = OpenApiSchema(
            "test-schema", {"param_required": "string", "param_optional": "string"}, optional_keys=["param_optional"]
        )
        spec = OpenApiSpecGenerator(handlers_by_path={}, schemas=[schema]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        assert yaml.safe_load(spec)["components"] == {
            "schemas": {
                TEST_SCHEMA.name: {
                    "type": "object",
                    "required": ["param_required"],
                    "properties": schema.properties,
                }
            },
            "securitySchemes": yaml.safe_load(SECURITY_SCHEMES_YAML),
        }

    def test_schema_with_all_keys_optional(self) -> None:
        schema = OpenApiSchema("test-schema", {"param_optional": "string"}, optional_keys=["param_optional"])
        spec = OpenApiSpecGenerator(handlers_by_path={}, schemas=[schema]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        assert yaml.safe_load(spec)["components"]["schemas"][TEST_SCHEMA.name] == {
            "type": "object",
            "properties": schema.properties,
        }

    def test_nested_schema(self) -> None:
        @dataclass(frozen=True)
        class SubSchema:
            param: str

        @dataclass(frozen=True)
        class MySchema:
            sub: SubSchema

        schema = DataclassSchema(MySchema)
        spec = OpenApiSpecGenerator(handlers_by_path={}, schemas=[schema]).generate(URL)
        spec = spec[len(SPEC_HEADER) :]

        assert yaml.safe_load(spec)["components"] == {
            "schemas": {
                "MySchema": {
                    "type": "object",
                    "required": ["sub"],
                    "additionalProperties": False,
                    "properties": {"sub": {"type": "object", "$ref": "#/components/schemas/SubSchema"}},
                }
            },
            "securitySchemes": yaml.safe_load(SECURITY_SCHEMES_YAML),
        }


def test_get_path_parameters() -> None:
    path = "/datasets/{dataset_id}/resources/{resource_id}"
    assert get_path_parameters(path) == ["dataset_id", "resource_id"]


class TestFormatDocstring:
    def test_without_docstring(self) -> None:
        assert format_docstring(lambda x: x) == ""

    def test_with_single_line_docstring(self) -> None:
        def handler() -> JsonResponse:
            """Create a foo bar."""
            return JsonResponse()

        assert format_docstring(handler) == "Create a foo bar."

    def test_with_multi_line_docstring(self) -> None:
        def handler() -> JsonResponse:
            """
            Create a foo bar.

            Also does some magic to make it work.
            """
            return JsonResponse()

        assert format_docstring(handler) == "Create a foo bar.\n\nAlso does some magic to make it work."
