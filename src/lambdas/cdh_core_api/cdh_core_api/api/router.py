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
import inspect
import json
import logging
import os
from datetime import datetime
from http import HTTPStatus
from logging import getLogger
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import Optional

from aws_xray_sdk.core import xray_recorder
from cdh_core_api.api.openapi_spec.openapi import Handler
from cdh_core_api.api.route_collection import AnyHandler
from cdh_core_api.api.route_collection import RouteCollection
from cdh_core_api.config import Config
from cdh_core_api.jwt_helper import get_jwt_user_id
from marshmallow import ValidationError

from cdh_core.aws_clients.cloudwatch_log_writer import CloudwatchLogWriter
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.entities.request import Headers
from cdh_core.entities.request import Request
from cdh_core.entities.response import JsonResponse
from cdh_core.entities.response import Response
from cdh_core.enums.http import HttpVerb
from cdh_core.exceptions.http import BadRequestError
from cdh_core.exceptions.http import HttpError
from cdh_core.exceptions.http import InternalError
from cdh_core.exceptions.http import ServiceUnavailableError
from cdh_core.log.xray import XRayMiddleware
from cdh_core.manager.dependency_manager import DependencyManager

LOG = getLogger(__name__)

CORS_HEADER = "Content-Type,X-Amz-Date,Authorization,Host,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent"
CORS_METHODS = "GET,HEAD,POST,PUT,DELETE,PATCH,OPTIONS"
AUDIT_VERBS = {HttpVerb.PUT.name, HttpVerb.DELETE.name, HttpVerb.POST.name, HttpVerb.PATCH.name}

SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "Content-Security-Policy": "frame-ancestors 'none'",
    "Vary": "Origin",
}

LAMBDA_TIMEOUT_SECONDS = int(os.environ.get("AWS_LAMBDA_TIMEOUT", "0"))


class Router:
    """Calls functions based on the incoming HTTP requests."""

    def __init__(
        self,
        allowed_origins: Collection[str],
        dependency_manager: DependencyManager,
    ) -> None:
        self._routes = RouteCollection()
        self._allowed_origins = allowed_origins
        self._dependency_manager = dependency_manager
        self._xray = XRayMiddleware(
            xray_recorder=xray_recorder, services_to_patch=["boto3", "botocore", "requests", "pynamodb"]
        )
        self._audit_logger: CloudwatchLogWriter = None  # type: ignore

    def set_audit_logger(self, audit_logger: CloudwatchLogWriter) -> None:
        """Set an audit logger, which has to be done in an AWS context."""
        self._audit_logger = audit_logger

    def handle_request(self, event: Dict[str, Any], context: LambdaContext, config: Config) -> Dict[str, Any]:
        """Handle an AWS request and call the handler based on the request."""
        request: Optional[Request] = None
        try:
            if config.disabled:
                raise ServiceUnavailableError("The Core API is currently unavailable due to maintenance")
            self._log_request(event, context)
            request = Request.from_lambda_event(event, context)
            if request.http_verb is not HttpVerb.OPTIONS:
                response = self._handle_normal_request(request)
            else:
                response = self._handle_cors_preflight(request)
        except Exception as error:  # pylint: disable=broad-except
            response = self._handle_error(event, context, error)

        response.headers.update(self._get_mandatory_response_headers(event))
        if event["httpMethod"] in AUDIT_VERBS:
            self._write_audit_log(event=event, request=request, response=response, config=config)
        self._xray.log_response(response)
        xray_recorder.begin_subsegment("response to dict")
        response_as_dict = response.to_dict()
        xray_recorder.end_subsegment()
        self._log_latency_info(
            event=event,
            context=context,
            request=request,
            status_code=response.status_code,
            response_size=len(response_as_dict["body"]) if response_as_dict and response_as_dict.get("body") else 0,
        )
        return response_as_dict

    @staticmethod
    def _log_latency_info(
        event: Dict[str, Any],
        context: LambdaContext,
        request: Optional[Request],
        status_code: HTTPStatus,
        response_size: int,
    ) -> None:
        latency_info: Dict[str, Any] = {
            "elapsed_lambda_ms": LAMBDA_TIMEOUT_SECONDS * 1000 - context.get_remaining_time_in_millis(),
            "status_code": status_code.value,
            "response_size": response_size,
        }
        if request:
            latency_info.update(
                {
                    "http_method": request.http_verb.name,
                    "path": request.path,
                    "query_params": request.query_params,
                    "route": request.route,
                }
            )
        if (request_context := event.get("requestContext")) and (
            request_time_epoch := request_context.get("requestTimeEpoch")
        ):
            latency_info["elapsed_total_ms"] = datetime.now().timestamp() * 1000 - request_time_epoch
        LOG.info(json.dumps(latency_info))

    def get_route(self, path: str, method: HttpVerb) -> AnyHandler:
        """Return the handler based on the path/method."""
        return self._routes.get(route=path, http_verb=method)

    def _handle_normal_request(self, request: Request) -> Response:
        handler = self._routes.get(request.route, request.http_verb)
        xray_recorder.begin_subsegment(f"build_dependencies for {handler.__qualname__}")
        self._dependency_manager.register_constant(
            "request", DependencyManager.TimeToLive.PER_REQUEST, value=request, force=True
        )
        signature = inspect.signature(handler)
        for name in ["body", "path", "query"]:
            annotation = signature.parameters[name].annotation if signature.parameters.get(name) else None
            self._dependency_manager.register_constant(
                f"{name}_annotation",
                DependencyManager.TimeToLive.PER_REQUEST,
                value=annotation,
                force=True,
            )
        self._dependency_manager.validate_dependencies()
        handler_arguments = self._dependency_manager.build_dependencies_for_callable(any_callable=handler)
        xray_recorder.end_subsegment()
        xray_recorder.begin_subsegment(handler.__qualname__)
        result = handler(**handler_arguments)
        xray_recorder.end_subsegment()
        return result

    def _handle_cors_preflight(self, request: Request) -> Response:
        # This method does not work as expected, see CDHX-4448.
        # Therefore we do not use the result of the method, but the CORS_METHODS value.
        # It is only called for handling non existing routes.
        self._routes.get_available_http_verbs(request.route)
        return JsonResponse(
            headers={
                "Access-Control-Allow-Methods": CORS_METHODS,
                "Access-Control-Allow-Headers": CORS_HEADER,
            }
        )

    def _handle_error(self, event: Dict[str, Any], context: LambdaContext, error: Exception) -> Response:
        if isinstance(error, HttpError):
            http_error = error
        elif isinstance(error, ValidationError):
            http_error = BadRequestError(self._format_validation_error(error))
        else:
            http_error = InternalError("Something went wrong. If this error persists please contact the CDH support.")

        self._log_request_on_error(event, context, error, http_error.STATUS)
        return JsonResponse(body=http_error.to_dict(request_id=context.aws_request_id), status_code=http_error.STATUS)

    @staticmethod
    def _format_validation_error(error: ValidationError) -> str:
        if isinstance(error.messages, list):
            return "; ".join(error.messages)
        if isinstance(error.messages, dict):
            return "; ".join(f"{key}: {value}" for key, value in error.messages.items())
        return str(error.messages)

    def _get_mandatory_response_headers(self, event: Dict[str, Any]) -> Dict[str, str]:
        origin = Headers.from_lambda_event(event).get("Origin", "")
        headers = SECURITY_HEADERS.copy()
        if origin in self._allowed_origins:
            headers["Access-Control-Allow-Origin"] = origin
            headers["Access-Control-Allow-Credentials"] = "true"
        return headers

    def route(self, path: str, http_verb: HttpVerb, force: bool = False) -> Callable[[Handler], Handler]:
        """Decorate a handler to register a new route."""

        def decorator(handler: Handler) -> Handler:
            self._routes.add(path, http_verb, handler, force)
            return handler

        return decorator

    def _log_request(self, event: Dict[str, Any], context: LambdaContext) -> None:
        request = Request.from_lambda_event(event, context, ignore_body=True)
        self._xray.log_request(request)
        parameter_info = f" with parameters {request.path_params}" if request.path_params else ""
        LOG.info(f"{request.http_verb.value} to {request.route}{parameter_info} ({request.id})")

    def _log_request_on_error(
        self, event: Dict[str, Any], context: LambdaContext, error: Exception, status: HTTPStatus
    ) -> None:
        log_level = logging.ERROR if status >= HTTPStatus.INTERNAL_SERVER_ERROR else logging.WARNING
        self._xray.log_exception(status)
        LOG.log(  # noqa: FKA01
            log_level,
            "Message: %s \rPath: %s Id: %s, \rRequest-Body: %s",
            str(error),
            event["path"],
            context.aws_request_id,
            event["body"],
            exc_info=True,
        )

    def _write_audit_log(
        self, event: Dict[str, Any], request: Optional[Request], response: Response, config: Config
    ) -> None:
        audit_info: Dict[str, Optional[Dict[str, Any]]] = {
            "response": response.to_dict() if response is not None else {}
        }
        if request is not None:
            audit_info["request"] = self._extract_request_info(request, config)
        else:  # if request cannot be parsed, log event instead
            audit_info.update({"request": None, "event": event})
        try:
            self._audit_logger.write_log([json.dumps(audit_info)])
        except Exception:  # pylint: disable=broad-except
            LOG.exception(f"Lost the following audit log information: {audit_info}")

    @staticmethod
    def _extract_request_info(request: Request, config: Config) -> Dict[str, Any]:
        request_info = request.to_plain_dict()
        if "cookie" in request_info["headers"]:
            request_info["headers"].pop("cookie")
        if jwt_user_id := get_jwt_user_id(request, config):
            request_info["jwtUserId"] = jwt_user_id
        return request_info
