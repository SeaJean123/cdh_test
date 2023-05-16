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
from typing import Callable
from typing import TypeVar

T = TypeVar("T", bound=Callable[..., Any])  # pylint: disable=invalid-name
C = TypeVar("C")  # pylint: disable=invalid-name


def decorate_class(cls: C, decorator: T) -> C:
    """Decorate every function within a class, except dunder functions."""
    for attr_name, attr in cls.__dict__.items():
        if callable(attr) and not attr_name.startswith("__"):
            setattr(cls, attr_name, decorator(attr))
    return cls
