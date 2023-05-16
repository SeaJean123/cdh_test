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
import sys
from contextlib import suppress
from logging import getLogger
from typing import List

from aws_xray_sdk.core import AWSXRayRecorder
from aws_xray_sdk.core import patch
from aws_xray_sdk.core.lambda_launcher import check_in_lambda
from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.utils import stacktrace
from aws_xray_sdk.ext.util import calculate_segment_name
from aws_xray_sdk.ext.util import construct_xray_header
from aws_xray_sdk.ext.util import prepare_response_header

from cdh_core.entities.request import Request
from cdh_core.entities.response import Response

LOG = getLogger(__name__)


class XRayMiddleware:
    """Abstracts the AWS XRay as middleware for HTTP."""

    def __init__(self, xray_recorder: AWSXRayRecorder, services_to_patch: List[str]) -> None:
        self._recorder = xray_recorder
        if check_in_lambda():
            self.in_lambda_ctx = True
            patch(services_to_patch)
        else:
            self.in_lambda_ctx = False

    def log_request(self, request: Request) -> None:
        """Log the HTTP request to xray."""
        if not self.in_lambda_ctx:
            return
        headers = request.headers
        xray_header = construct_xray_header(headers.to_dict())
        host = headers.get("Host", "")

        name = calculate_segment_name(host, self._recorder)
        if name is None:
            LOG.warning("Could not find name for segment")
            name = "default segment"
        segment = self._recorder.begin_subsegment(name)
        if segment is None:
            LOG.error("x-ray log request failed")
            return

        segment.save_origin_trace_header(xray_header)
        segment.put_http_meta(http.URL, host + request.path)
        segment.put_http_meta(http.METHOD, request.http_verb.value)
        segment.put_http_meta(http.USER_AGENT, headers.get("User-Agent"))
        for key, value in request.path_params.items():
            segment.put_metadata(key, value, "path_params")
        for key, value in request.query_params.items():
            segment.put_metadata(key, value, "query_params")
        for key, value2 in request.query_params_multi_value.items():
            segment.put_metadata(key, value2, "query_params_multi_value")

        client_ip = headers.get("X-Forwarded-For") or headers.get("HTTP_X_FORWARDED_FOR")
        if client_ip:
            segment.put_http_meta(http.CLIENT_IP, client_ip)
            segment.put_http_meta(http.X_FORWARDED_FOR, True)

    def log_response(self, response: Response) -> None:
        """Write a HTTP response to xray."""
        if not self.in_lambda_ctx:
            return
        segment = self._recorder.current_subsegment()
        if segment is None:
            LOG.error("x-ray log response failed")
            return
        segment.put_http_meta(http.STATUS, response.status_code)

        origin_header = segment.get_origin_trace_header()
        resp_header_str = prepare_response_header(origin_header, segment)
        response.headers[http.XRAY_HEADER] = resp_header_str

        cont_len = response.headers.get("Content-Length")
        if cont_len:
            segment.put_http_meta(http.CONTENT_LENGTH, int(cont_len))

        self._recorder.end_subsegment()
        return

    def log_exception(self, error_code: int) -> None:
        """Log the stack which contains the exception."""
        if not self.in_lambda_ctx:
            return
        exception = sys.exc_info()[1]
        if not exception:
            return

        with suppress(Exception):
            segment = self._recorder.current_subsegment()
        if not segment:
            LOG.error("x-ray log exception failed")
            return

        segment.put_http_meta(http.STATUS, error_code)
        stack = stacktrace.get_stacktrace(limit=self._recorder.max_trace_back)
        segment.add_exception(exception, stack)
