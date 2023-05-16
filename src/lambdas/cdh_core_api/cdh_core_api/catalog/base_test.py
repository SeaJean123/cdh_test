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
from typing import Set
from typing import Type
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from cdh_core_api.catalog.base import BaseTable
from cdh_core_api.catalog.base import catch_dynamo_errors
from cdh_core_api.catalog.base import DynamoInternalServerError
from cdh_core_api.catalog.base import DynamoItemIterator
from cdh_core_api.catalog.base import LastEvaluatedKey
from cdh_core_api.catalog.base import NUM_RETRIES
from cdh_core_api.catalog.base import SECONDS_BETWEEN_RETRIES
from cdh_core_api.catalog.base import ThrottlingException
from pynamodb.attributes import Attribute
from pynamodb.exceptions import GetError
from pynamodb.exceptions import QueryError
from pynamodb.exceptions import ScanError
from pynamodb.models import Model

from cdh_core_dev_tools.testing.builder import Builder


def build_last_evaluated_key() -> LastEvaluatedKey:
    return LastEvaluatedKey(
        {Builder.build_random_string(): {Builder.build_random_string(): Builder.build_random_string()}}
    )


class TestDynamoErrorDecorator:
    @patch("time.sleep")
    def test_retryable_dynamo_error(self, mocked_sleep: Mock) -> None:
        mocked_function = Mock()

        @catch_dynamo_errors
        def testfunc() -> None:
            client_error = ClientError(error_response={"Error": {"Code": "InternalServerError"}}, operation_name="")
            mocked_function.side_effect = GetError("", client_error)
            mocked_function()

        with pytest.raises(DynamoInternalServerError):
            testfunc()

        mocked_function.assert_has_calls([call() for _ in range(NUM_RETRIES)])
        mocked_sleep.assert_has_calls([call(SECONDS_BETWEEN_RETRIES) for _ in range(NUM_RETRIES - 1)])

    @pytest.mark.parametrize("caught_exception", [ScanError, QueryError])
    @pytest.mark.parametrize(
        "error_code, error",
        [("InternalServerError", DynamoInternalServerError), ("ThrottlingException", ThrottlingException)],
    )
    def test_non_retryable_dynamo_errors(
        self, caught_exception: Type[Exception], error_code: str, error: Type[Exception]
    ) -> None:
        mocked_function = Mock()

        @catch_dynamo_errors
        def testfunc(error_code: str) -> None:
            client_error = ClientError(error_response={"Error": {"Code": error_code}}, operation_name="")
            mocked_function.side_effect = caught_exception("", client_error)
            mocked_function()

        with pytest.raises(error):
            testfunc(error_code)

        mocked_function.assert_called_once()

    def test_other_error_raised(self) -> None:
        mocked_function = Mock()

        @catch_dynamo_errors
        def testfunc() -> None:
            mocked_function.side_effect = KeyError()
            mocked_function()

        with pytest.raises(KeyError):
            testfunc()

        mocked_function.assert_called_once()


class TestBaseTable:
    @patch("time.sleep")
    def test_retryable_dynamo_error_in_subclass(self, mocked_sleep: Mock) -> None:
        mocked_function = Mock()

        class BaseTableSubclass(BaseTable):
            def testfunc(self) -> None:
                client_error = ClientError(error_response={"Error": {"Code": "InternalServerError"}}, operation_name="")
                mocked_function.side_effect = GetError("", client_error)
                mocked_function()

        with pytest.raises(DynamoInternalServerError):
            BaseTableSubclass().testfunc()

        mocked_function.assert_has_calls([call() for _ in range(NUM_RETRIES)])
        mocked_sleep.assert_has_calls([call(SECONDS_BETWEEN_RETRIES) for _ in range(NUM_RETRIES - 1)])

    @pytest.mark.parametrize("caught_exception", [ScanError, QueryError])
    @pytest.mark.parametrize(
        "error_code, error",
        [("InternalServerError", DynamoInternalServerError), ("ThrottlingException", ThrottlingException)],
    )
    def test_non_retryable_dynamo_error_in_subclass(
        self, caught_exception: Type[Exception], error_code: str, error: Type[Exception]
    ) -> None:
        class BaseTableSubclass(BaseTable):
            def testfunc(self, error_code: str) -> None:
                client_error = ClientError(error_response={"Error": {"Code": error_code}}, operation_name="")
                error = caught_exception("", client_error)
                raise error

        with pytest.raises(error):
            BaseTableSubclass().testfunc(error_code)

    def test_other_error_in_subclass(self) -> None:
        class BaseTableSubclass(BaseTable):
            def testfunc(self) -> None:
                raise KeyError()

        with pytest.raises(KeyError):
            BaseTableSubclass().testfunc()


class TestDynamoItemIterator:
    def test_iterate_items(self) -> None:
        items = [Builder.build_random_string() for _ in range(5)]

        iterator = DynamoItemIterator[str](items=iter(items), get_last_evaluated_key=build_last_evaluated_key)

        assert list(iterator) == items

    def test_last_evaluated_key(self) -> None:
        get_last_evaluated_key = Mock()

        iterator = DynamoItemIterator[int](items=iter([1, 2, 3]), get_last_evaluated_key=get_last_evaluated_key)

        assert iterator.last_evaluated_key == get_last_evaluated_key.return_value

    @pytest.mark.parametrize("caught_exception", [ScanError, QueryError])
    @pytest.mark.parametrize(
        "error_code, error",
        [("InternalServerError", DynamoInternalServerError), ("ThrottlingException", ThrottlingException)],
    )
    def test_dynamo_error_converted(
        self, caught_exception: Type[Exception], error_code: str, error: Type[Exception]
    ) -> None:
        items = MagicMock()
        client_error = ClientError(error_response={"Error": {"Code": error_code}}, operation_name="")
        items.__next__.side_effect = caught_exception("", client_error)

        iterator = DynamoItemIterator(items=items, get_last_evaluated_key=build_last_evaluated_key)

        with pytest.raises(error):
            next(iterator)

    def test_other_error_passes_through(self) -> None:
        items = MagicMock()
        items.__next__.side_effect = ValueError

        iterator = DynamoItemIterator(items=items, get_last_evaluated_key=build_last_evaluated_key)

        with pytest.raises(ValueError):
            next(iterator)


def get_nullable_attributes(model: Type[Model]) -> Set[str]:
    """Return all nullable attributes."""
    return {name for name, attr in model.get_attributes().items() if attr.null}


def get_attributes_of_type(model: Type[Model], attr_type: Type[Attribute[Any]]) -> Set[str]:
    """Return all attributes of a given type."""
    return {name for name, attr in model.get_attributes().items() if isinstance(attr, attr_type)}
