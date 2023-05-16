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
import io
import os
import sys
from contextlib import contextmanager
from typing import Any
from typing import Generator
from unittest.mock import Mock

from _pytest.monkeypatch import MonkeyPatch

from cdh_core_dev_tools.testing.builder import Builder


@contextmanager
def disable_stdout() -> Generator[None, None, None]:
    """Disable the stdout for this context."""
    default_stdout = sys.stdout
    sys.stdout = io.StringIO()
    yield
    sys.stdout = default_stdout


def build_and_set_moto_account_id() -> str:
    """Build a random MOTO Account Id and saves it to the environment variables for the session."""
    moto_account_id = os.getenv("MOTO_ACCOUNT_ID")
    if not moto_account_id:
        moto_account_id = Builder.build_random_digit_string(length=12)
        MonkeyPatch().setenv("MOTO_ACCOUNT_ID", moto_account_id)
    return moto_account_id


class UnusableMock(Mock):
    """Mocks objects that should not be used in unit tests."""

    def __getattr__(self, attr: str) -> None:
        """Raise a TypeError."""
        raise TypeError(f"Must not access attribute {attr!r} of UnusableMock")

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        """Raise a TypeError."""
        raise TypeError("Must not call UnusableMock")
