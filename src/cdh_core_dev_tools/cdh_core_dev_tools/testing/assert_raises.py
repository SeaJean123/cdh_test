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
from contextlib import contextmanager
from typing import Generator

import pytest
from asserts import assert_equal


@contextmanager
def assert_raises(expected_exception: Exception) -> Generator[None, None, None]:
    """
    Assert that a specific exception is raised.

    This context manager will compare not only the type of the exception (as pytest.raises does),
    but will also make sure the string representations of expected and actual exception are the same.

    Usage:

    >>> with assert_raises(MyExceptionClass('expected error message')):
    ...     ... # code under test

    """
    with pytest.raises(type(expected_exception)) as context:
        yield
    actual_exception = context.value
    assert_equal(str(actual_exception), str(expected_exception))
