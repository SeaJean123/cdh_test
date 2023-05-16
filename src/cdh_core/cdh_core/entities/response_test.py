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
import json
from http import HTTPStatus
from unittest.mock import Mock

from cdh_core.entities.response import CsvResponse
from cdh_core.entities.response import JsonResponse
from cdh_core_dev_tools.testing.builder import Builder


class TestJsonResponse:
    def test_to_dict_without_body_and_headers(self) -> None:
        response = JsonResponse(status_code=HTTPStatus.LOOP_DETECTED)
        assert response.to_dict() == {
            "isBase64Encoded": False,
            "statusCode": HTTPStatus.LOOP_DETECTED.value,
            "body": None,
        }

    def test_to_dict_with_dict_body_and_headers(self) -> None:
        response = JsonResponse(
            status_code=HTTPStatus.LOOP_DETECTED,
            headers={"WWW-Authenticate": "Basic"},
            body={"mass": "index"},
        )
        assert response.to_dict() == {
            "isBase64Encoded": False,
            "statusCode": HTTPStatus.LOOP_DETECTED.value,
            "headers": {"WWW-Authenticate": "Basic"},
            "body": '{"mass":"index"}',
        }

    def test_to_dict_employs_serializer(self) -> None:
        class CustomClass:
            pass

        not_json_serializable = CustomClass()

        response = JsonResponse(
            status_code=HTTPStatus.LOOP_DETECTED,
            headers={"WWW-Authenticate": "Basic"},
            body={"foo": not_json_serializable, "hello": "world"},
        )
        serializer = Mock()
        serializer.return_value = "bar"

        response_body = response.to_dict(serializer=serializer)["body"]

        assert json.loads(response_body) == {"foo": "bar", "hello": "world"}
        serializer.assert_called_once_with(not_json_serializable)

    def test_include_next_page_token_no_headers(self) -> None:
        next_page_token = Builder.build_random_string()
        response = JsonResponse(
            status_code=HTTPStatus.LOOP_DETECTED,
            next_page_token=next_page_token,
        )
        assert response.to_dict() == {
            "isBase64Encoded": False,
            "statusCode": HTTPStatus.LOOP_DETECTED.value,
            "headers": {"nextPageToken": next_page_token},
            "body": None,
        }

    def test_include_next_page_token_with_headers(self) -> None:
        headers = {"WWW-Authenticate": "Basic"}
        next_page_token = Builder.build_random_string()
        response = JsonResponse(
            status_code=HTTPStatus.LOOP_DETECTED,
            headers=headers,
            next_page_token=next_page_token,
        )
        assert response.to_dict() == {
            "isBase64Encoded": False,
            "statusCode": HTTPStatus.LOOP_DETECTED.value,
            "headers": {**headers, "nextPageToken": next_page_token},
            "body": None,
        }


class TestCsvResponse:
    def test_to_dict(self) -> None:
        body = Builder.build_random_string()
        status_code = HTTPStatus.LOOP_DETECTED
        headers = {Builder.build_random_string(): Builder.build_random_string()}

        csv_response = CsvResponse(body, status_code, headers)
        assert csv_response.to_dict() == {
            "isBase64Encoded": False,
            "statusCode": status_code,
            "body": body,
            "headers": {"Content-Type": "text/csv", **headers},
        }
