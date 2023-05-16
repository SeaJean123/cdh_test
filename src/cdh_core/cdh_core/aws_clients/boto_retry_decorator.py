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
import functools
import time
from inspect import signature
from typing import Any
from typing import Callable
from typing import cast
from typing import List
from typing import Literal
from typing import Optional
from typing import Protocol
from typing import Type
from typing import TypeVar
from typing import Union

from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError  # pylint: disable=redefined-builtin

from cdh_core.aws_clients.utils import get_error_code

AnyFunc = TypeVar("AnyFunc", bound=Callable[..., Any])


class BotoRetryDecorator(Protocol):
    """Decorates a function to retry boto calls."""

    def __call__(
        self,
        num_attempts: int,
        wait_between_attempts: float,
        retryable_error_codes: Union[List[str], Literal["ALL"]] = "ALL",
        retryable_errors: Union[List[Type[ConnectionError]], Literal["ALL"], None] = None,
    ) -> Callable[[AnyFunc], AnyFunc]:
        """Do nothing."""


def create_boto_retry_decorator(sleeper_attribute_name: Optional[str] = None) -> BotoRetryDecorator:
    """Construct a decorator to retry the decorated function."""

    def client_error_retrying(
        num_attempts: int,
        wait_between_attempts: float,
        retryable_error_codes: Union[List[str], Literal["ALL"]] = "ALL",
        retryable_errors: Union[List[Type[ConnectionError]], Literal["ALL"], None] = None,
    ) -> Callable[[AnyFunc], AnyFunc]:
        if num_attempts < 1:
            raise ValueError("num_attempts should be at least 2")
        if wait_between_attempts < 0:
            raise ValueError("you cannot wait less than 0 seconds")

        def _retry_sleeper(self: Any, sleeper_attribute_name: Optional[str], waiting_time: float) -> None:
            if sleeper_attribute_name is None:
                time.sleep(waiting_time)
            else:
                getattr(self, sleeper_attribute_name)(waiting_time)

        def decorator(func: AnyFunc) -> AnyFunc:
            if "self" not in signature(func).parameters:
                raise FunctionIsNotMemberOfClass("The annotated function is not a member of a class!")

            @functools.wraps(func)
            def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
                if sleeper_attribute_name is not None and not hasattr(self, sleeper_attribute_name):
                    raise FunctionHasNoSleeperAttribute(
                        f"The class of the annotated function {func} has no sleeper attribute {sleeper_attribute_name}"
                    )
                for attempt in range(1, num_attempts + 1):
                    try:
                        return func(self, *args, **kwargs)
                    except ClientError as error:
                        code = get_error_code(error)
                        is_retryable = retryable_error_codes == "ALL" or code in retryable_error_codes
                        if not is_retryable:
                            raise error

                        if attempt == num_attempts:
                            raise error
                        _retry_sleeper(self, sleeper_attribute_name, wait_between_attempts)
                    except ConnectionError as error:
                        is_retryable = retryable_errors == "ALL" or any(
                            isinstance(error, retryable_error) for retryable_error in retryable_errors or []
                        )
                        if not is_retryable:
                            raise error

                        if attempt == num_attempts:
                            raise error
                        _retry_sleeper(self, sleeper_attribute_name, wait_between_attempts)
                raise MaximumRetriesExceeded(f"Failed after {attempt} attempts, this should never happen.")

            return cast(AnyFunc, wrapper)

        return decorator

    return client_error_retrying


class FunctionIsNotMemberOfClass(Exception):
    """Signals the function is not within a class."""


class FunctionHasNoSleeperAttribute(Exception):
    """Signals that the function has no sleeper attribute."""


class MaximumRetriesExceeded(Exception):
    """The maximum number of rertries has been exceeded."""
