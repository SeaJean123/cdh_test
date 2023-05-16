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
from unittest.mock import Mock

import pytest

from cdh_core.optionals import apply_if_not_none


def test_simple_function() -> None:
    def add_one(num: int) -> int:
        return num + 1

    add_one_if_not_none = apply_if_not_none(add_one)

    assert add_one_if_not_none(None) is None
    assert add_one_if_not_none(41) == 42


def test_original_method_not_called() -> None:
    not_to_be_called = Mock()
    augmented_function = apply_if_not_none(not_to_be_called)

    assert augmented_function(None) is None
    not_to_be_called.assert_not_called()


@pytest.mark.parametrize("value", [0, False, "", []])
def test_original_method_called(value: Any) -> None:
    call_me_maybe = Mock()
    augmented_function = apply_if_not_none(call_me_maybe)

    augmented_function(value)
    call_me_maybe.assert_called_once_with(value)
