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
from functools import wraps
from typing import Any
from typing import cast

import pytest

from cdh_core.decorators import C
from cdh_core.decorators import decorate_class
from cdh_core.decorators import T


class ErrorRaisedInFunction(Exception):
    pass


class ErrorRaisedInDecorator(Exception):
    pass


def class_decorator_for_testing(cls: C) -> C:
    def function_decorator_for_testing(func: T) -> T:
        @wraps(func)
        def with_error(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except ErrorRaisedInFunction as exception:
                raise ErrorRaisedInDecorator from exception

        return cast(T, with_error)

    return decorate_class(cls, function_decorator_for_testing)


@class_decorator_for_testing
class FailedClass:
    def __init__(self, should_fail: bool) -> None:
        if should_fail:
            raise ErrorRaisedInFunction

    def some_func(self) -> None:
        raise ErrorRaisedInFunction


def test_constructor_is_not_decorated() -> None:
    with pytest.raises(ErrorRaisedInFunction):
        FailedClass(should_fail=True)


def test_class_method_is_decorated() -> None:
    with pytest.raises(ErrorRaisedInDecorator):
        FailedClass(should_fail=False).some_func()
