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
from typing import Callable
from typing import Optional
from typing import TypeVar

S = TypeVar("S")  # pylint: disable=invalid-name
T = TypeVar("T")  # pylint: disable=invalid-name


def apply_if_not_none(single_arg_func: Callable[[S], T]) -> Callable[[Optional[S]], Optional[T]]:
    """
    Augment a given single-argument function by allowing it to receive an optional input.

    The returned function returns 'None' if the input is 'None' and calls the given function otherwise.
    """

    def inner_func(single_arg: Optional[S]) -> Optional[T]:
        if single_arg is None:
            return None
        return single_arg_func(single_arg)

    return inner_func
