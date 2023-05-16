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
import logging
import traceback
from typing import Any
from typing import Dict
from typing import List
from unittest.mock import Mock

import pytest

from cdh_core.log.log_safe import GenericLambdaException
from cdh_core.log.log_safe import LambdaHandler
from cdh_core.log.log_safe import log_safe
from cdh_core_dev_tools.testing.builder import Builder


def test_output_is_preserved() -> None:
    output = Builder.build_random_string()

    @log_safe()
    def handler(event: Dict[str, Any], context: Any) -> str:  # pylint: disable=unused-argument
        return output

    assert handler({}, {}) == output


def test_input_is_preserved() -> None:
    mock = Mock()
    event = Mock()
    context = Mock()

    @log_safe()
    def handler(event: Dict[str, Any], context: Any) -> None:
        mock(event, context)

    handler(event, context)

    mock.assert_called_once_with(event, context)


def test_convert_to_generic_exception() -> None:
    @log_safe()
    def handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
        raise AttributeError("sensitive information")

    with pytest.raises(GenericLambdaException):
        handler({}, {})


def test_exception_pass_through() -> None:
    class MyError(Exception):
        ...

    @log_safe([MyError])
    def handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
        raise MyError("foo")

    with pytest.raises(MyError):
        handler({}, {})


def test_exception_pass_through_does_not_match() -> None:
    class MyError(Exception):
        ...

    class OtherError(Exception):
        ...

    @log_safe([MyError])
    def handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
        raise OtherError("foo")

    with pytest.raises(GenericLambdaException):
        handler({}, {})


class TestFullAwsRuntime:
    """Mimics the way AWS Lambda invokes the handler and logs errors."""

    class _CustomException(Exception):
        pass

    def setup_method(self) -> None:
        self.secret = Builder.build_random_string()
        secret = self.secret
        self.log_messages: List[str] = []
        local_messages = self.log_messages

        class AbsorbingLogger(logging.Handler):
            def format(self, record: logging.LogRecord) -> str:
                # we can modify the formatting of LogRecords by configuring the root logger
                formatted = super().format(record)
                return formatted.replace(secret, "but I cannot tell you")

            def emit(self, record: logging.LogRecord) -> None:
                formatted = self.format(record)
                local_messages.append(formatted.replace(secret, "but I cannot tell you"))

        self.original_handlers = logging.root.handlers
        handler = AbsorbingLogger()
        handler.setFormatter(logging.Formatter(fmt="%(levelname)s %(module)s %(funcName)s %(message)s"))
        logging.root.handlers = [handler]  # simulate AWS

    def teardown_method(self) -> None:
        logging.root.handlers = self.original_handlers

    def run(self, handler: LambdaHandler, event: Dict[str, Any], context: Any) -> Any:
        try:
            return handler(event, context)
        except Exception as error:  # pylint: disable=broad-except
            # AWS Lambda writes error logs straight to the "Sink", bypassing the logging module
            error_message = f"[ERROR] {type(error).__name__}: {str(error)}. Traceback: {traceback.format_exc()}"
            self.log_messages.append(error_message)
            return error

    def test_tainted_logs_default(self) -> None:
        """This test just assures that the test setup works as intended."""

        def handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
            raise TestFullAwsRuntime._CustomException(f"I have a secret: {self.secret}")

        result = self.run(handler, Mock(), Mock())

        assert isinstance(result, TestFullAwsRuntime._CustomException)
        assert any(self.secret in message for message in self.log_messages)

    def test_logs_stay_clean(self) -> None:
        @log_safe()
        def myhandler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
            raise TestFullAwsRuntime._CustomException(f"I have a secret: {self.secret}")

        result = self.run(myhandler, Mock(), Mock())

        assert isinstance(result, GenericLambdaException)
        assert all(self.secret not in message for message in self.log_messages)
        assert any(
            f"{TestFullAwsRuntime._CustomException.__name__}: I have a secret: but I cannot tell you" in message
            for message in self.log_messages
        )

    def test_original_error_reference_is_kept(self) -> None:
        def helper() -> None:
            raise TestFullAwsRuntime._CustomException(Builder.build_random_string())

        @log_safe()
        def my_handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
            helper()

        self.run(my_handler, Mock(), Mock())

        module = inspect.getmodulename(__file__)
        assert self.log_messages[0].startswith(f"ERROR {module} {helper.__name__}")
        assert my_handler.__name__ in self.log_messages[0]

    def test_pass_through_exceptions_are_preserved(self) -> None:
        @log_safe([TestFullAwsRuntime._CustomException])
        def handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
            raise TestFullAwsRuntime._CustomException(f"I have a secret: {self.secret}")

        result = self.run(handler, Mock(), Mock())

        assert isinstance(result, TestFullAwsRuntime._CustomException)
        assert any(self.secret in message for message in self.log_messages)

    def test_no_error(self) -> None:
        @log_safe()
        def handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
            return

        result = self.run(handler, Mock(), Mock())

        assert result is None
        assert len(self.log_messages) == 0
