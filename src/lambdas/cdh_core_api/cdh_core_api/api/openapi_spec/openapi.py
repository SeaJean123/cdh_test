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
import re
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from dataclasses import is_dataclass
from enum import Enum
from http import HTTPStatus
from inspect import signature
from io import StringIO
from typing import Any
from typing import Callable
from typing import cast
from typing import Collection
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Type
from typing import TypeVar
from typing import Union

import marshmallow_dataclass
import yaml
from cdh_core_api.api.validation import BaseSchema
from marshmallow_enum import EnumField
from marshmallow_jsonschema import JSONSchema
from marshmallow_jsonschema.base import PY_TO_JSON_TYPES_MAP  # type: ignore

from cdh_core.enums.http import HttpVerb

# flake8: noqa: B028


AnyHandler = Callable[..., Any]
Handler = TypeVar("Handler", bound=AnyHandler)
ClassType = TypeVar("ClassType", bound=type)  # pylint: disable=invalid-name
Json = Dict[str, Any]

_DEFAULT_ENUM_DOCSTRING = "An enumeration."


# We monkeypatch marshmallow_enum for two reasons:
#   - We always want to deserialize enums by value. Enum names have to be valid Python identifiers,
#     values can be chosen arbitrarily.
#   - EnumField does not support JSON schema generation.
# Unfortunately, we cannot use a proper subclass of EnumField, because marshmallow_dataclass cannot be configured
# to use a class different than marshmallow_enum.EnumField.
assert EnumField.__init__.__defaults__[0] is False  # type: ignore
EnumField.__init__.__defaults__ = (True,) + EnumField.__init__.__defaults__[1:]  # type: ignore
EnumField._jsonschema_type_mapping = lambda self: {  # pylint: disable=protected-access
    "$ref": "#/components/schemas/" + self.enum.__name__
}

# The json schema type and format combinations used by marshmallow jsonschema does not match the OpenAPI specification.
# This workaround changes the mapping to the correct types: https://swagger.io/specification/#dataTypes
PY_TO_JSON_TYPES_MAP[float] = {"type": "number", "format": "double"}
PY_TO_JSON_TYPES_MAP[int] = {"type": "integer", "format": "int64"}


@dataclass(frozen=True)
class OpenApiSchema:
    """Represents the openapi schema."""

    name: str
    properties: Json
    optional_keys: Optional[List[str]] = None  # By default, all keys are required

    def to_dict(self) -> Json:
        """Return the object as dict."""
        optional_keys = self.optional_keys or []
        required_keys = [key for key in self.properties.keys() if key not in optional_keys]
        properties = self.properties
        if "default" in properties and properties["default"] is None:
            properties["nullable"] = True
        result = {
            "type": "object",
            "properties": properties,
        }
        if required_keys:  # an empty "required" list does not conform to the OpenAPI spec
            result["required"] = required_keys
        return result


@dataclass(frozen=True)
class CsvSchema:
    """Interface class to export as CSV."""

    name: str

    @staticmethod
    def to_dict() -> Json:
        """Return the object as dict."""
        return {}


class DataclassSchema:
    """Enable a dataclass to have a marshmallow schema."""

    _json_schema = JSONSchema()

    def __init__(self, dataclass_type: type):
        if not is_dataclass(dataclass_type):
            raise TypeError("DataclassSchema must be used with dataclasses")
        self.name = dataclass_type.__name__
        self.dataclass = dataclass_type
        self._schema_instance = marshmallow_dataclass.class_schema(dataclass_type, base_schema=BaseSchema)()
        self._enrich_enum_description()

    def _enrich_enum_description(self) -> None:
        for field_descriptor in self._schema_instance.fields.values():
            if isinstance(field_descriptor, EnumField):
                if "description" not in field_descriptor.metadata:
                    # mypy does not know the enum field in the field_descriptor
                    doc = str(field_descriptor.enum.__doc__)
                    if doc == _DEFAULT_ENUM_DOCSTRING:
                        raise ValueError(f"Doc string of {field_descriptor} must be set")
                    field_descriptor.metadata["description"] = doc

    def to_dict(self) -> Json:
        """Return the object as dict."""
        result = self._json_schema.dump(self._schema_instance)["definitions"][self.name]
        self._fix_json_schema(result)
        return cast(Json, result)

    def _fix_json_schema(self, data: Dict[Any, Any]) -> None:
        # marshmallow_jsonschema includes all field metadata into the output. Thus we have to remove our own keys here.
        data.pop("validator_with_context", None)

        # marshmallow_jsonschema hard-codes the prefix "#/definitions" for schema-references.
        # Unfortunately, "definitions" is not valid anymore in OpenAPI v3.
        # Thus, we recursively replace it by "#/components/schemas".
        if "$ref" in data and data["$ref"].startswith("#/definitions/"):
            data["$ref"] = data["$ref"].replace("#/definitions/", "#/components/schemas/", 1)
        else:
            if "default" in data and data["default"] is None:
                data["nullable"] = True
            for value in data.values():
                if isinstance(value, dict):
                    self._fix_json_schema(value)

    def to_parameters(self, parameter_type: str) -> List[Json]:
        """Return a list of the parameters based on the given type."""
        schema_dict = self.to_dict()
        result = []
        for field_name, schema in schema_dict["properties"].items():
            parameter = {
                "name": field_name,
                "in": parameter_type,
                "required": field_name in schema_dict.get("required", []),
                "schema": self._to_schema(schema),
            }
            if "description" in schema:
                parameter["description"] = schema["description"]
            result.append(parameter)
        return result

    @staticmethod
    def _to_schema(schema: Json) -> Json:
        # Without this fix
        # field: Optional[str] = None noqa: E800
        # would generate a schema {'type': 'string', 'default': None, 'required': False}
        # But None is not a valid default value for type 'string'.
        if schema.get("default", "") is None:
            schema.pop("default")
        return schema

    def __eq__(self, other: Any) -> bool:
        """Return True if the other is the same object."""
        if isinstance(other, DataclassSchema):
            return other.dataclass is self.dataclass
        return NotImplemented


ResponseSchema = Union[None, Json, OpenApiSchema, CsvSchema]


@dataclass(frozen=True)
class OpenApiEnum:
    """Represents an enum in openapi spec syntax."""

    name: str
    values: Collection[str]
    description: str

    def to_dict(self) -> Json:
        """Return the object as dict."""
        return {"type": "string", "enum": self.values, "description": self.description}

    @classmethod
    def from_enum_type(cls, enum_type: Type[Enum]) -> "OpenApiEnum":
        """Build from an enum type."""
        doc = str(enum_type.__doc__)
        assert doc != _DEFAULT_ENUM_DOCSTRING, f"Doc string of {enum_type} must be set"
        return cls(enum_type.__name__, [item.value for item in enum_type], str(enum_type.__doc__))


@dataclass(frozen=True)
class OpenApiEnumAsString:
    """This can be used if the values of the enum must not be available in the spec."""

    name: str
    description: str

    def to_dict(self) -> Json:
        """Return the object as dict."""
        return {"type": "string", "description": self.description}

    @classmethod
    def from_enum_type(cls, enum_type: Type[Enum]) -> "OpenApiEnumAsString":
        """Build from an enum type."""
        doc = str(enum_type.__doc__)
        assert doc != _DEFAULT_ENUM_DOCSTRING, f"Doc string of {enum_type} must be set"
        return cls(enum_type.__name__, str(enum_type.__doc__))


OpenApiSchemas = Union[OpenApiEnum, OpenApiEnumAsString, OpenApiSchema, DataclassSchema, CsvSchema]


@dataclass()
class _HandlerInfo:
    path: Optional[str] = None
    http_verb: Optional[HttpVerb] = None
    responses: Dict[HTTPStatus, ResponseSchema] = field(default_factory=dict)
    path_schema: Optional[DataclassSchema] = None
    body: Union[None, OpenApiSchema, DataclassSchema] = None
    query: Optional[DataclassSchema] = None
    description: str = ""
    internal: bool = False


class OpenApiSpecCollector:
    """Stores the routes and schemas per endpoint."""

    def __init__(self) -> None:
        self._handlers: Dict[AnyHandler, _HandlerInfo] = {}
        self._schemas: Dict[str, OpenApiSchemas] = {}

    def get_handlers_by_path(self) -> Mapping[str, List[_HandlerInfo]]:
        """Return all handlers as dict grouped by their path."""
        result: Dict[str, List[_HandlerInfo]] = defaultdict(list)
        for handler, handler_info in self._handlers.items():
            path = handler_info.path
            http_verb = handler_info.http_verb
            if not path:
                raise MissingPath(handler)
            if result[path] and result[path][0].path_schema != handler_info.path_schema:
                base_handler = result[path][0]
                assert base_handler.path_schema
                assert handler_info.path_schema
                raise ConflictingPathSchema(
                    path, {base_handler.path_schema.dataclass, handler_info.path_schema.dataclass}
                )
            if http_verb and http_verb in [info.http_verb for info in result[path]]:
                raise ConflictingHandlers(path, http_verb)
            result[path].append(handler_info)
        return result

    def get_schemas(self) -> List[OpenApiSchemas]:
        """Return the a list of Openapi schemas."""
        return list(self._schemas.values())

    def _get_or_create_handler_info(self, handler: AnyHandler, raise_if_exists: bool = False) -> _HandlerInfo:
        if handler not in self._handlers:
            self._handlers[handler] = _HandlerInfo()
        else:
            existing_handler_info = self._handlers[handler]
            if raise_if_exists and (existing_handler_info.path or existing_handler_info.http_verb):
                raise HandlerAlreadyExists(handler, self._handlers[handler])
        return self._handlers[handler]

    def _get_handler(self, path: str, http_verb: HttpVerb) -> Optional[AnyHandler]:
        return next(
            (
                handler
                for handler, info in self._handlers.items()
                if (path == info.path and http_verb == info.http_verb)
            ),
            None,
        )

    def _remove_handler(self, handler: AnyHandler) -> None:
        self._handlers.pop(handler)

    def route(self, path: str, http_verb: HttpVerb, force: bool = False) -> Callable[[Handler], Handler]:
        """Decorate a handler to set the path and method."""

        def decorator(handler: Handler) -> Handler:
            if conflicting_handler := self._get_handler(path, http_verb):
                if not force:
                    raise DuplicateRoute(path)
                self._remove_handler(conflicting_handler)
            handler_info = self._get_or_create_handler_info(handler, raise_if_exists=True)
            handler_info.path = path
            handler_info.http_verb = http_verb
            handler_info.description = format_docstring(handler)
            self._add_body_from_annotation(handler)
            self._add_query_from_annotation(handler)
            self._add_path_parameters_from_annotation(path, handler)
            return handler

        return decorator

    def response(self, status: HTTPStatus, schema: ResponseSchema = None) -> Callable[[Handler], Handler]:
        """Decorate a handler to add a response."""

        def decorator(handler: Handler) -> Handler:
            handler_info = self._get_or_create_handler_info(handler)
            if status in handler_info.responses:
                raise DuplicateResponseStatus(handler, status)
            handler_info.responses[status] = schema
            if isinstance(schema, OpenApiSchema):
                self._add_schema(schema)
            return handler

        return decorator

    def body(self, schema: Union[OpenApiSchema, DataclassSchema]) -> Callable[[Handler], Handler]:
        """Decorate a handler to add a request body."""

        def decorator(handler: Handler) -> Handler:
            self._add_body(handler, schema)
            return handler

        return decorator

    def _add_body(self, handler: Handler, schema: Union[OpenApiSchema, DataclassSchema]) -> None:
        handler_info = self._get_or_create_handler_info(handler)
        if handler_info.body:
            raise DuplicateBody(handler)
        handler_info.body = schema
        self._add_schema(schema)

    def _add_body_from_annotation(self, handler: Handler) -> None:
        sig = signature(handler)
        if "body" in sig.parameters:
            body_type = sig.parameters["body"].annotation
            if not is_dataclass(body_type):
                raise TypeError(f"Parameter 'body' of handler {handler.__name__} must be annotated with a dataclass")
            self._add_body(handler, DataclassSchema(body_type))

    def _add_query_from_annotation(self, handler: Handler) -> None:
        sig = signature(handler)
        if "query" in sig.parameters:
            query_type = sig.parameters["query"].annotation
            if not is_dataclass(query_type):
                raise TypeError(f"Parameter 'query' of handler {handler.__name__} must be annotated with a dataclass")

            handler_info = self._get_or_create_handler_info(handler)
            assert not handler_info.query
            handler_info.query = DataclassSchema(query_type)

    def _add_path_parameters_from_annotation(self, path: str, handler: Handler) -> None:
        sig = signature(handler)
        if "path" in sig.parameters:
            path_type = sig.parameters["path"].annotation
            if not is_dataclass(path_type):
                raise TypeError(f"Parameter 'path' of handler {handler.__name__} must be annotated with a dataclass")
            handler_info = self._get_or_create_handler_info(handler)
            schema = DataclassSchema(path_type)
            path_parameters = get_path_parameters(path)
            if set(path_parameters) != set(schema.to_dict()["properties"].keys()):
                raise ValueError(f"Parameters and type annotations do not match for path {path}")
            assert not handler_info.path_schema or handler_info.path_schema == schema
            handler_info.path_schema = schema

    def internal_endpoint(self) -> Callable[[Handler], Handler]:
        """Decorate a handler to signal it is only for internal usage."""

        def decorator(handler: Handler) -> Handler:
            handler_info = self._get_or_create_handler_info(handler)
            handler_info.internal = True
            return handler

        return decorator

    def _add_schema(self, schema: OpenApiSchemas, force: bool = False) -> None:
        if schema.name not in self._schemas or force:
            self._schemas[schema.name] = schema
        elif schema != self._schemas[schema.name]:
            raise ConflictingSchemas(schema.name)

    def overwrite_existing_schema(self, name: str) -> Callable[[ClassType], ClassType]:
        """Decorate a class that should overwrite the schema with the given name."""

        def decorator(cls: ClassType) -> ClassType:
            cls.__name__ = name
            if not is_dataclass(cls):
                raise TypeError("The decorator 'overwrite_existing_schema' can only be used to decorate a dataclass")
            self.link(DataclassSchema(cls), force=True)
            return cls  # type: ignore[return-value]

        return decorator

    def link(self, schema: OpenApiSchemas, nullable: bool = False, force: bool = False) -> Json:
        """Add a link to another schema."""
        self._add_schema(schema, force)
        if nullable:
            # this is hack based on: https://github.com/OAI/OpenAPI-Specification/issues/1368
            return {"nullable": True, "allOf": [{"$ref": "#/components/schemas/" + schema.name}]}
        return {"$ref": "#/components/schemas/" + schema.name}


class OpenApiSpecGenerator:
    """Creates a Openapi Spec based on the registered routes."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        handlers_by_path: Mapping[str, List[_HandlerInfo]],
        schemas: List[OpenApiSchemas],
        remap_integration_timeout: bool = False,
        partition: Optional[str] = None,
        region: Optional[str] = None,
        lambda_arn: Optional[str] = None,
        accounts: Optional[str] = None,
        org_ids: Optional[str] = None,
        options_arn: Optional[str] = None,
    ):
        self._handlers_by_path = handlers_by_path
        self._schemas = schemas
        self._buffer = StringIO()
        self._remap_integration_timeout = remap_integration_timeout
        self._partition = partition
        self._region = region
        self._lambda_arn = lambda_arn
        self._accounts = accounts
        self._org_ids = org_ids
        self._options_arn = options_arn

    @classmethod
    def from_collector(  # pylint: disable=too-many-arguments
        cls,
        collector: OpenApiSpecCollector,
        remap_integration_timeout: bool = False,
        partition: Optional[str] = None,
        region: Optional[str] = None,
        lambda_arn: Optional[str] = None,
        accounts: Optional[str] = None,
        org_ids: Optional[str] = None,
        options_arn: Optional[str] = None,
    ) -> "OpenApiSpecGenerator":
        """Build a new generator based on a collector."""
        return cls(
            handlers_by_path=collector.get_handlers_by_path(),
            schemas=collector.get_schemas(),
            partition=partition,
            region=region,
            lambda_arn=lambda_arn,
            accounts=accounts,
            org_ids=org_ids,
            options_arn=options_arn,
            remap_integration_timeout=remap_integration_timeout,
        )

    def generate(self, url: str) -> str:
        """Return openapi spec as whole as string."""
        self._buffer.write(
            get_spec_header(
                url=url,
                remap_integration_timeout=self._remap_integration_timeout,
                accounts=self._accounts,
                org_ids=self._org_ids,
                options_arn=self._options_arn,
            )
        )
        self._write_paths()
        self._write_components()
        return self._buffer.getvalue()

    def _write_paths(self) -> None:
        self._dump(
            {
                "paths": {
                    path: self._generate_path_section(path, handler_infos)
                    for path, handler_infos in self._handlers_by_path.items()
                }
            }
        )

    def _generate_path_section(self, path: str, handler_infos: List[_HandlerInfo]) -> Json:
        result: Dict[str, Any] = {"options": self._generate_cors_section()}
        parameters_section = self._generate_parameters_section(path, handler_infos[0].path_schema)
        if parameters_section:
            result["parameters"] = parameters_section

        for handler_info in handler_infos:
            operation_section = self._generate_operation_section(handler_info)
            if http_verb := handler_info.http_verb:
                assert http_verb.value not in result  # Should be guaranteed by OpenApiSpecCollector
                result[http_verb.value] = operation_section

        return result

    @staticmethod
    def _generate_parameters_section(path: str, types: Optional[DataclassSchema]) -> List[Json]:
        parameters = get_path_parameters(path)
        if parameters:
            assert types, "Parameter types not available, but parameters set"
            return types.to_parameters("path")
        return []

    def _generate_cors_section(self) -> Dict[str, Any]:
        return {
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
            "x-amazon-apigateway-integration": self._get_aws_integration_section(HTTPStatus.OK),
        }

    def _generate_operation_section(self, handler_info: _HandlerInfo) -> Json:
        # API gateway's built-in validation is simple to use, but gives pretty bad error messages.
        # (While it is possible to extend error output, even "$context.error.validationErrorString" does not contain
        # e.g. the name of the infringing field when validating a dict).
        # If the handler defines a dataclass schema for the body we disable API gateway's validation to benefit
        # from the nice error messages generated by Marshmallow. For handlers that do not yet use Marshmallow,
        # we continue to use API gateway's validation.
        validator = "NONE" if isinstance(handler_info.body, DataclassSchema) else "validateBodyAndParameters"
        result: Dict[str, Any] = {
            "responses": {
                str(http_status.value): self._generate_response_section(http_status, response)
                for http_status, response in handler_info.responses.items()
            },
            "x-amazon-apigateway-request-validator": validator,
            "x-amazon-apigateway-integration": self._get_aws_integration_section(
                self._get_default_status(handler_info)
            ),
        }
        if not result["responses"]:
            result["responses"]["204"] = self._generate_response_section(HTTPStatus.NO_CONTENT, None)
        if handler_info.description:
            result["description"] = handler_info.description
        if handler_info.query:
            result["parameters"] = handler_info.query.to_parameters("query")
        if handler_info.body:
            result["requestBody"] = {
                "content": self._render_schema(handler_info.body),
                "required": True,
            }
        if handler_info.internal:
            result["tags"] = ["internal"]
        return result

    def _get_aws_integration_section(self, default_status: HTTPStatus) -> Json:
        return {
            "uri": f"arn:{self._partition or '${partition}'}:apigateway:{self._region or '${region}'}:"
            f"lambda:path/2015-03-31/functions/{self._lambda_arn or '${lambda_arn}'}/invocations",
            # The AWS docs don't mention what this 'responses' section is used for,
            # but the statusCode should be one of the keys in 'responses' above.
            # https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-swagger-extensions-integration-responses.html  # noqa: E501
            "responses": {"default": {"statusCode": str(default_status.value)}},
            "passthroughBehavior": "NEVER",
            "httpMethod": "POST",
            "contentHandling": "CONVERT_TO_TEXT",
            "type": "AWS_PROXY",
        }

    @staticmethod
    def _get_default_status(handler_info: _HandlerInfo) -> HTTPStatus:
        if not handler_info.responses:
            return HTTPStatus.NO_CONTENT
        if HTTPStatus.OK in handler_info.responses:
            return HTTPStatus.OK
        if HTTPStatus.ACCEPTED in handler_info.responses:
            return HTTPStatus.ACCEPTED
        if HTTPStatus.CREATED in handler_info.responses:
            return HTTPStatus.CREATED
        raise ValueError("Missing default response")

    def _generate_response_section(self, status: HTTPStatus, schema: ResponseSchema) -> Json:
        result: Dict[str, Any] = {
            "description": status.name,
            "headers": {
                "Access-Control-Allow-Headers": {"schema": {"type": "string"}},
                "Access-Control-Allow-Origin": {"schema": {"type": "string"}},
            },
        }
        if schema is not None:
            result["content"] = self._render_schema(schema)
        return result

    @staticmethod
    def _render_schema(schema: Union[Json, OpenApiSchema, DataclassSchema, CsvSchema]) -> Json:
        if isinstance(schema, dict):
            return {"application/json": {"schema": schema}}
        if isinstance(schema, CsvSchema):
            return {"text/csv": {}}
        return {"application/json": {"schema": {"$ref": "#/components/schemas/" + schema.name}}}

    def _write_components(self) -> None:
        self._dump(
            {
                "components": {
                    "schemas": {schema.name: schema.to_dict() for schema in self._schemas},
                    "securitySchemes": SECURITY_SCHEME,
                }
            }
        )

    def _dump(self, data: Json) -> None:
        dumper = yaml.dumper.SafeDumper
        # By default yaml.dump will use YAML references. But API Gateway does not support this feature.
        dumper.ignore_aliases = lambda self, data: True  # type: ignore
        yaml.dump(data, Dumper=dumper, stream=self._buffer)


def get_path_parameters(path: str) -> List[str]:
    """Extract the parameters from the path."""
    matches_with_braces = re.findall("{[A-Za-z0-9_]+}", path)
    return [match[1:-1] for match in matches_with_braces]


def format_docstring(handler: Handler) -> str:
    """Return the cleaned doc string of the handler."""
    docstring = handler.__doc__ or ""
    if docstring.startswith("\n"):  # Strip the initial \n after opening a docstring with """
        docstring = docstring[1:]
    return textwrap.dedent(docstring).strip()


class DuplicateRoute(Exception):
    """Signals that a route has been registered already."""

    def __init__(self, path: str):
        super().__init__(f"Route {path} was added twice")


class DuplicateBody(Exception):
    """Signals that a handler has multiple annotations for the body."""

    def __init__(self, handler: Handler):
        super().__init__(
            f"Handler {handler.__name__} carries two body annotations (either using @body or using the body parameter)"
        )


class DuplicateResponseStatus(Exception):
    """Signals that a handler has multiple annotations for the response status."""

    def __init__(self, handler: Handler, status: HTTPStatus):
        super().__init__(f"Handler {handler.__name__} carries two response annotations for status {status.name}")


class HandlerAlreadyExists(Exception):
    """Signals that a handler is already registered."""

    def __init__(self, handler: Handler, info: _HandlerInfo):
        self.info = info
        super().__init__(f"Handler {handler.__name__} is already registered.")


class ConflictingHandlers(Exception):
    """Signals that there is already a handler defined for a path/httpVerb combination."""

    def __init__(self, path: str, verb: HttpVerb):
        super().__init__(f"More than one handler was defined for {path} and {verb}")


class ConflictingPathSchema(Exception):
    """Signals that there are already parameters defined for a path."""

    def __init__(self, path: str, dataclasses: Set[type]):
        super().__init__(f"Different path parameter schemas for {path}: {dataclasses}")


class ConflictingSchemas(Exception):
    """Signals that there is already a schema registered."""

    def __init__(self, schema_name: str):
        super().__init__(f"Cannot register a second schema with name {schema_name}.")


class MissingPath(Exception):
    """Signals that a handler does not have path defined."""

    def __init__(self, handler: Handler):
        super().__init__(f"No path was defined for handler {handler.__name__}")


def get_spec_header(  # pylint: disable=too-many-arguments
    url: str,
    remap_integration_timeout: bool = False,
    accounts: Optional[str] = None,
    org_ids: Optional[str] = None,
    options_arn: Optional[str] = None,
) -> str:
    """Build the openapi spec header."""
    enable_access_via_org_id = org_ids is not None
    accounts = accounts or "${accounts}"
    org_ids = org_ids or "${org_ids}"
    options_arn = options_arn or "${options_arn}"

    return (
        (
            f"""\
openapi: "3.0.1"
info:
  title: "Core API"
  version: "2.0.0"
  description: "The CDH API constitutes the central API of the Cloud Data Hub"
security:
  - sigv4: []
servers:
  # The gateway id in this URL is ignored when uploading to AWS.
  - url: "{url}"
x-amazon-apigateway-minimum-compression-size: 0
x-amazon-apigateway-request-validators:
  validateBodyAndParameters:
    validateRequestParameters: true
    validateRequestBody: true
x-amazon-apigateway-gateway-responses:
  # "Gateway responses" are error responses generated by API Gateway itself, without invoking the Core API Lambda.
  # The following lines make sure that these error responses include the Access-Control-Allow-Origin header.
  # Configuring DEFAULT_4XX and DEFAULT_5XX is enough to configure this for all gateway responses.
  DEFAULT_4XX:
    responseParameters:
      gatewayresponse.header.Access-Control-Allow-Credentials: "'true'"
      gatewayresponse.header.Access-Control-Allow-Origin: "method.request.header.origin"
    responseTemplates:
      application/json: "{{\\"message\\":$context.error.messageString}}"
  DEFAULT_5XX:
    responseParameters:
      gatewayresponse.header.Access-Control-Allow-Credentials: "'true'"
      gatewayresponse.header.Access-Control-Allow-Origin: "method.request.header.origin"
    responseTemplates:
      application/json: "{{\\"message\\":$context.error.messageString}}"
"""
        )
        + (
            """\
  INTEGRATION_TIMEOUT:
    statusCode: 429
    responseParameters:
      gatewayresponse.header.Access-Control-Allow-Credentials: "'true'"
      gatewayresponse.header.Access-Control-Allow-Origin: "method.request.header.origin"
    responseTemplates:
      application/json: "{\\"message\\":$context.error.messageString}"
"""
            if remap_integration_timeout
            else ""
        )
        + (
            f"""\
x-amazon-apigateway-policy:
  # This key does not seem to be documented in the API Gateway docs.
  # It sets the gateway's resource policy and must equal the one set by Terraform:
  # If we remove the policy from here, we end up without any resource policy,
  # because this is apparently applied after Terraform has set the resource policy.
  # If we remove the policy from Terraform, we risk that a later run of Terraform will delete our policy.
  Version: '2012-10-17'
  Statement:
    - Effect: Allow
      Principal:
        AWS: ["{accounts}"]
      Action: execute-api:Invoke
      Resource:
        - "*"
"""
        )
        + (
            f"""\
    - Effect: Allow
      Principal:
        AWS: ["*"]
      Action: execute-api:Invoke
      Resource:
        - "*"
      Condition:
        StringEquals:
          "aws:PrincipalOrgID": ["{org_ids}"]
"""
            if enable_access_via_org_id
            else ""
        )
        + """\
    - Effect: Allow
      Principal:
        AWS: "*"
      Action: execute-api:Invoke
      Resource:
"""
        + (
            f"""\
        - "{options_arn}"
"""
        )
    )


# See https://swagger.io/docs/specification/authentication/
# and https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-swagger-extensions-authtype.html
SECURITY_SCHEME = {
    "sigv4": {
        "type": "apiKey",
        "name": "Authorization",
        "in": "header",
        "x-amazon-apigateway-authtype": "awsSigv4",
    }
}


class OpenApiTypes:
    """Contains basic types in an openapi spec representation."""

    BOOLEAN: Json = {"type": "boolean"}
    INTEGER: Json = {"type": "integer", "format": "int64"}
    DOUBLE: Json = {"type": "number", "format": "double"}
    STRING: Json = {"type": "string"}
    DATE_TIME: Json = {"type": "string", "format": "date-time"}
    OPTIONAL_BOOLEAN: Json = {"type": "boolean", "nullable": True}
    OPTIONAL_INTEGER: Json = {"type": "integer", "nullable": True}
    OPTIONAL_NUMBER: Json = {"type": "number", "nullable": True}
    OPTIONAL_STRING: Json = {"type": "string", "nullable": True}
    OPTIONAL_DATE_TIME: Json = {"type": "string", "format": "date-time", "nullable": True}

    @staticmethod
    def array_of(item_type: Json, description: Optional[str] = None) -> Json:
        """Return a array with a type and a description as openapi spec."""
        open_api_type = {"type": "array", "items": item_type}
        if description:
            open_api_type["description"] = description
        return open_api_type

    @staticmethod
    def optional_string_with_description(description: str) -> Json:
        """Return a optional string with a description as openapi spec."""
        return {"type": "string", "nullable": True, "description": description}

    @staticmethod
    def optional_integer_with_description(description: str) -> Json:
        """Return a optional integer with a description as openapi spec."""
        return {"type": "integer", "nullable": True, "description": description}

    @staticmethod
    def optional_float_with_description(description: str) -> Json:
        """Return a optional float with a description as openapi spec."""
        return {"type": "number", "format": "double", "nullable": True, "description": description}

    @staticmethod
    def union(*types: Json) -> Json:
        """Return a union of multiple types as openapi spec."""
        return {"oneOf": types}

    @staticmethod
    def constant_string(string: str) -> Json:
        """Return a string enum as openapi spec."""
        return {"type": "string", "enum": [string]}

    @staticmethod
    def dictionary(description: str, value_type: Optional[str] = "string") -> Json:
        """Return a mandatory dictionary as openapi spec."""
        return {
            "type": "object",
            "description": description,
            "additionalProperties": {"type": value_type} if value_type else {},
        }

    @staticmethod
    def optional_dictionary(description: str, value_type: str = "string") -> Json:
        """Return a optional dictionary as openapi spec."""
        return {
            "type": "object",
            "description": description,
            "nullable": True,
            "additionalProperties": {
                "type": value_type,
            },
        }

    @staticmethod
    def deprecate(open_api_type: Json) -> Json:
        """Set the tag 'deprecated'."""
        open_api_type["deprecated"] = True
        return open_api_type
