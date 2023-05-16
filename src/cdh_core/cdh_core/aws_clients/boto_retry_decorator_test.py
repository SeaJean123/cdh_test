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
from unittest.mock import call
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError as BotoCoreConnectionError
from botocore.exceptions import ConnectTimeoutError
from botocore.exceptions import ProxyConnectionError

from cdh_core.aws_clients.boto_retry_decorator import create_boto_retry_decorator
from cdh_core.aws_clients.boto_retry_decorator import FunctionHasNoSleeperAttribute
from cdh_core.aws_clients.boto_retry_decorator import FunctionIsNotMemberOfClass
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class TestCreateRetryDecorator:
    def test_instant_success(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper

            @decorator(num_attempts=5, wait_between_attempts=3)
            def decorated(self) -> str:
                return "success"

        cls = WithRetries()
        result = cls.decorated()

        assert result == "success"
        assert not sleeper.call_args_list

    def test_eventual_success(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper
                self._counter = 0

            @decorator(num_attempts=5, wait_between_attempts=3, retryable_error_codes=["e1", "e2", "e0"])
            def decorated(self) -> str:
                if self._counter < 3:
                    self._counter = self._counter + 1
                    raise Builder.build_client_error(f"e{self._counter % 2}")
                return "success"

        cls = WithRetries()
        result = cls.decorated()

        assert result == "success"
        assert sleeper.call_args_list == [((3,),), ((3,),), ((3,),)]

    def test_always_exception(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper

            @decorator(num_attempts=3, wait_between_attempts=4)
            def decorated(self) -> None:
                raise Builder.build_client_error("e", message="some-message", operation_name="some-operation")

        cls = WithRetries()
        with assert_raises(ClientError({"Error": {"Code": "e", "Message": "some-message"}}, "some-operation")):
            cls.decorated()

        assert sleeper.call_args_list == [((4,),), ((4,),)]

    def test_non_caught_exception(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()
        exception = Builder.build_client_error("e2")

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper
                self._counter = 0

            @decorator(num_attempts=3, wait_between_attempts=4, retryable_error_codes=["e"])
            def decorated(self) -> None:
                if self._counter == 0:
                    self._counter = self._counter + 1
                    raise Builder.build_client_error("e")
                raise exception

        cls = WithRetries()
        with assert_raises(exception):
            cls.decorated()

        assert sleeper.call_args_list == [((4,),)]

    def test_non_client_error(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper

            @decorator(num_attempts=3, wait_between_attempts=4)
            def decorated(self) -> None:
                raise RuntimeError("whatever")

        cls = WithRetries()
        with assert_raises(RuntimeError("whatever")):
            cls.decorated()

        assert sleeper.call_count == 0

    def test_eventual_success_with_non_client_errors(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper
                self._counter = 0

            @decorator(num_attempts=5, wait_between_attempts=3, retryable_errors=[ConnectTimeoutError])
            def decorated(self) -> str:
                if self._counter < 3:
                    self._counter = self._counter + 1
                    raise ConnectTimeoutError(endpoint_url="my_endpoint")
                return "success"

        cls = WithRetries()
        result = cls.decorated()
        assert sleeper.call_args_list == [call(3), call(3), call(3)]

        assert result == "success"

    def test_eventual_success_with_all_connection_errors(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper
                self._counter = 0

            @decorator(num_attempts=5, wait_between_attempts=3, retryable_errors="ALL")
            def decorated(self) -> str:
                if self._counter < 3:
                    self._counter = self._counter + 1
                    raise BotoCoreConnectionError(error=BotoCoreError())
                return "success"

        cls = WithRetries()
        result = cls.decorated()
        assert sleeper.call_args_list == [call(3), call(3), call(3)]

        assert result == "success"

    def test_non_caught_exception_non_client_errors(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper
                self._counter = 0

            @decorator(num_attempts=3, wait_between_attempts=4, retryable_errors=[ConnectTimeoutError])
            def decorated(self) -> None:
                if self._counter == 0:
                    self._counter = self._counter + 1
                    raise ConnectTimeoutError(endpoint_url="my_endpoint")
                raise ProxyConnectionError(proxy_url="another endpoint")

        cls = WithRetries()
        with pytest.raises(ProxyConnectionError):
            cls.decorated()
        assert sleeper.call_args_list == [call(4)]

    def test_non_client_errors_over_retry_limit(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper

            @decorator(num_attempts=3, wait_between_attempts=4, retryable_errors=[ConnectTimeoutError])
            def decorated(self) -> None:
                raise ConnectTimeoutError(endpoint_url="my_endpoint")

        cls = WithRetries()
        with pytest.raises(ConnectTimeoutError):
            cls.decorated()
        assert sleeper.call_args_list == [((4,),), ((4,),)]

    def test_num_attempts_too_low(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()
        with pytest.raises(ValueError):

            class WithRetries:  # pylint: disable=unused-variable
                def __init__(self) -> None:
                    self._sleeper = sleeper

                @decorator(num_attempts=0, wait_between_attempts=3)
                def decorated(self) -> str:
                    return "success"

    def test_wait_time_too_low(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper")
        sleeper = Mock()
        with pytest.raises(ValueError):

            class WithRetries:  # pylint: disable=unused-variable
                def __init__(self) -> None:
                    self._sleeper = sleeper

                @decorator(num_attempts=1, wait_between_attempts=-1)
                def decorated(self) -> str:
                    return "success"

    def test_fails_if_incorrect_sleeper_attribute_is_set(self) -> None:
        decorator = create_boto_retry_decorator("_sleeper123")
        sleeper = Mock()

        class WithRetries:
            def __init__(self) -> None:
                self._sleeper = sleeper

            @decorator(num_attempts=1, wait_between_attempts=1)
            def decorated(self) -> str:
                return "success"

        cls = WithRetries()
        with pytest.raises(FunctionHasNoSleeperAttribute):
            cls.decorated()

    def test_use_time_sleep_if_sleeper_attribute_is_none(self) -> None:
        decorator = create_boto_retry_decorator()

        class WithRetries:
            def __init__(self) -> None:
                self._counter = 0

            @decorator(num_attempts=5, wait_between_attempts=10, retryable_error_codes=["e1", "e2", "e0"])
            def decorated(self) -> str:
                if self._counter < 3:
                    self._counter = self._counter + 1
                    raise Builder.build_client_error(f"e{self._counter % 2}")
                return "success"

        cls = WithRetries()
        with patch("time.sleep") as patched_sleep:
            result = cls.decorated()
            assert patched_sleep.call_args_list == [((10,),), ((10,),), ((10,),)]

        assert result == "success"

    def test_decorate_no_class_member_function(self) -> None:
        decorator = create_boto_retry_decorator()

        with pytest.raises(FunctionIsNotMemberOfClass):

            @decorator(num_attempts=5, wait_between_attempts=10, retryable_error_codes=["e1", "e2", "e0"])
            def decorated() -> str:
                return "success"
