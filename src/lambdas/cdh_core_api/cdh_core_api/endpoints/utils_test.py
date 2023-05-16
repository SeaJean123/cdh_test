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
import pytest
from cdh_core_api.catalog.base import DynamoInternalServerError
from cdh_core_api.catalog.base import ThrottlingException
from cdh_core_api.endpoints.utils import remap_dynamo_internal_errors
from cdh_core_api.endpoints.utils import throttleable

from cdh_core.exceptions.http import ServiceUnavailableError
from cdh_core.exceptions.http import TooManyRequestsError
from cdh_core_dev_tools.testing.builder import Builder


class TestRouteDynamoAnnotations:
    def setup_method(self) -> None:
        self.table_name = Builder.build_random_string()

    def test_throttleable_throttling(self) -> None:
        @throttleable
        def testfunc() -> None:
            raise ThrottlingException()

        with pytest.raises(TooManyRequestsError):
            testfunc()

    def test_throttleable_other(self) -> None:
        @throttleable
        def testfunc() -> None:
            raise DynamoInternalServerError()

        with pytest.raises(DynamoInternalServerError):
            testfunc()

    def test_dynamo_recoverable(self) -> None:
        @remap_dynamo_internal_errors
        def testfunc() -> None:
            raise DynamoInternalServerError()

        with pytest.raises(ServiceUnavailableError):
            testfunc()

    def test_dynamo_recoverable_other(self) -> None:
        @remap_dynamo_internal_errors
        def testfunc() -> None:
            raise ThrottlingException()

        with pytest.raises(ThrottlingException):
            testfunc()
