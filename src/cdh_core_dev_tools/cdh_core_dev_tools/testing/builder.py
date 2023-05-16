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
import random
import string
from datetime import datetime
from datetime import timedelta
from random import choices
from random import randint
from typing import Collection
from typing import Optional
from typing import Tuple
from typing import TypeVar
from uuid import uuid4

from botocore.exceptions import ClientError

T = TypeVar("T")  # pylint: disable=invalid-name


class Builder:
    """Build random things which can be used for testing."""

    DEFAULT_TIMESTAMP_1970 = datetime.utcfromtimestamp(0)
    DT_NOW = datetime.now()

    @classmethod
    def build_random_string(cls, length: int = 8) -> str:
        """Build a random string based on ascii lowercase chars."""
        return "".join(choices(string.ascii_lowercase, k=length))

    @classmethod
    def append_random_string(cls, prefix: str, length: int = 4) -> str:
        """Append a random string to a given string."""
        return prefix + "-" + cls.build_random_string(length)

    @classmethod
    def get_random_element(cls, to_choose_from: Collection[T], exclude: Collection[T] = frozenset()) -> T:
        """Accept a collection and return one element except the ones excluded."""
        return random.choice(list(set(to_choose_from) - set(exclude)))

    @classmethod
    def choose_without_repetition(cls, to_choose_from: Collection[T], number_of_elements: int) -> Tuple[T, ...]:
        """Accept a collection and return any number of items of it."""
        return tuple(random.sample(list(to_choose_from), k=number_of_elements))

    @classmethod
    def build_random_digit_string(cls, length: int = 8) -> str:
        """Build a string containing only digits."""
        return "".join([f"{randint(0, 9)}" for _ in range(0, length)])

    @classmethod
    def build_client_error(
        cls, code: str, operation_name: Optional[str] = None, message: Optional[str] = None
    ) -> ClientError:
        """Create a botocore ClientError."""
        return ClientError(
            {"Error": {"Code": code, "Message": message if message else cls.build_random_string()}},
            operation_name if operation_name else cls.build_random_string(),
        )

    @classmethod
    def build_random_url(cls, scheme: str = "https") -> str:
        """Build a random URL-like string consisting of a scheme and a random part."""
        return f"{scheme}://{Builder.build_random_string()}.example.com"

    @staticmethod
    def build_random_email() -> str:
        """Build a random email address."""
        return f"{Builder.build_random_string()}@{Builder.build_random_string()}.{Builder.build_random_string(3)}"

    @staticmethod
    def build_resource_name_prefix() -> str:
        """Build a random resource name prefix."""
        return f"cdhx{Builder.build_random_string(3)}"

    @staticmethod
    def build_request_id() -> str:
        """Build a random AWS lambda request id."""
        return str(uuid4())

    @staticmethod
    def get_random_bool() -> bool:
        """Get a random boolean."""
        return bool(random.getrandbits(1))

    @staticmethod
    def build_random_datetime(start: datetime = DEFAULT_TIMESTAMP_1970, end: datetime = DT_NOW) -> datetime:
        """Build a random date."""
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + timedelta(seconds=random_second)
