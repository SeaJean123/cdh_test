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
from typing import Any
from typing import Dict
from unittest.mock import Mock

from aws_xray_sdk.core import AWSXRayRecorder

from cdh_core.entities.request import Headers
from cdh_core.entities.request import Request
from cdh_core.entities.response import Response
from cdh_core.log.xray import XRayMiddleware
from cdh_core_dev_tools.testing.builder import Builder


def _request_mock(headers_dict: Dict[str, Any]) -> Mock:
    request = Mock()
    request.headers = Headers(headers_dict)
    request.path = "my_path"
    request.path_params = {"param1": "value1"}
    request.query_params = {"param1": "value1"}
    request.query_params_multi_value = {"param1": ["value1"]}
    return request


def _response_mock(headers_dict: Dict[str, Any]) -> Mock:
    request = Mock(Response)
    request.headers = headers_dict
    request.path = "my_path"
    request.path_params = {"param1": "value1"}
    request.query_params = {"param1": "value1"}
    request.status_code = 200
    request.root = ""
    return request


def _prepare_xray(segment: Mock) -> XRayMiddleware:
    xray_recorder = Mock()
    xray_recorder.begin_subsegment.return_value = segment
    xray_recorder.current_subsegment.return_value = segment
    xray = XRayMiddleware(xray_recorder, ["boto3", "botocore", "requests", "pynamodb"])
    xray.in_lambda_ctx = True
    return xray


def test_log_request_when_xray_recorder_normal_without_client_ip() -> None:
    segment = Mock()
    xray = _prepare_xray(segment)
    xray.log_request(_request_mock({"Host": "my_host"}))
    assert segment.put_http_meta.call_count == 3
    assert segment.put_metadata.call_count == 3


def test_log_request_when_xray_recorder_normal_with_client_ip_x_forwarded() -> None:
    segment = Mock()
    xray = _prepare_xray(segment)
    xray.log_request(_request_mock({"Host": "my_host", "X-Forwarded-For": "127.0.0.1"}))
    assert segment.put_http_meta.call_count == 5
    assert segment.put_metadata.call_count == 3


def test_log_request_when_xray_recorder_normal_with_client_ip_http_x_forwarded() -> None:
    segment = Mock()
    xray = _prepare_xray(segment)
    xray.log_request(_request_mock({"Host": "my_host", "HTTP_X_FORWARDED_FOR": "127.0.0.1"}))
    assert segment.put_http_meta.call_count == 5
    assert segment.put_metadata.call_count == 3


def test_log_request_when_xray_recorder_with_in_lambda_ctx_false() -> None:
    segment = Mock()
    xray = _prepare_xray(segment)
    xray.in_lambda_ctx = False
    xray.log_request(_request_mock({"Host": "my_host"}))
    assert segment.put_http_meta.call_count == 0
    assert segment.put_metadata.call_count == 0


def test_log_request_when_xray_recorder_returns_none() -> None:
    xray_recorder = Mock(AWSXRayRecorder)
    xray_recorder.begin_subsegment.return_value = None
    xray = XRayMiddleware(xray_recorder, ["boto3", "botocore", "requests", "pynamodb"])
    xray.in_lambda_ctx = True
    request = Mock(Request)
    request.attach_mock(Mock(Headers), "headers")
    path_params_mock = Mock(dict)
    query_params_mock = Mock(dict)
    request.attach_mock(path_params_mock, "path_params")
    request.attach_mock(query_params_mock, "query_params")
    xray.log_request(request)
    assert path_params_mock.items.call_count == 0
    assert query_params_mock.items.call_count == 0
    xray_recorder.begin_subsegment.assert_called_once()


def test_log_response_without_content_length() -> None:
    segment = Mock()
    segment.trace_id = Builder.build_random_string()
    xray = _prepare_xray(segment)
    xray.log_response(_response_mock({"Host": "my_host"}))
    segment.get_origin_trace_header.assert_called_once()
    assert segment.put_http_meta.call_count == 1


def test_log_response_with_content_length() -> None:
    segment = Mock()
    segment.trace_id = Builder.build_random_string()
    xray = _prepare_xray(segment)
    xray.log_response(_response_mock({"Host": "my_host", "Content-Length": 123}))
    segment.get_origin_trace_header.assert_called_once()
    assert segment.put_http_meta.call_count == 2


def test_log_response_when_xray_recorder_with_in_lambda_ctx_false() -> None:
    segment = Mock()
    xray = _prepare_xray(segment)
    xray.in_lambda_ctx = False
    segment.get_origin_trace_header.assert_not_called()
    xray.log_request(_response_mock({"Host": "my_host"}))
    assert segment.put_http_meta.call_count == 0


def test_log_response_when_xray_recorder_returns_none() -> None:
    xray_recorder = Mock()
    xray_recorder.current_subsegment.return_value = None
    xray = XRayMiddleware(xray_recorder, ["boto3", "botocore", "requests", "pynamodb"])
    response = Mock()
    xray.in_lambda_ctx = True
    xray.log_response(response)
    xray_recorder.current_subsegment.assert_called_once()
    assert response.headers.get.call_count == 0
