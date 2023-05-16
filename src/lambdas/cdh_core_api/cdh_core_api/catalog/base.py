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
import time
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Generic
from typing import Iterator
from typing import NewType
from typing import Optional
from typing import Type
from typing import TypeVar

import pynamodb
from pynamodb.attributes import Attribute
from pynamodb.constants import STRING
from pynamodb.exceptions import GetError
from pynamodb.exceptions import PynamoDBException
from pynamodb.exceptions import QueryError
from pynamodb.exceptions import ScanError
from pynamodb.models import Model

from cdh_core.decorators import decorate_class
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region

M = TypeVar("M", bound=Model)  # pylint: disable=invalid-name
T = TypeVar("T", bound=Callable[..., Any])  # pylint: disable=invalid-name
LastEvaluatedKey = NewType("LastEvaluatedKey", Dict[str, Dict[str, Any]])

NUM_RETRIES = 5
SECONDS_BETWEEN_RETRIES = 1


def catch_dynamo_errors(func: T) -> T:
    """Decorate a function to catch dynamo specific errors and convert them to python like errors.

    Any GetError caused by an InternalServerError is retried 5 times.
    """

    @wraps(func)
    def with_dynamo_error(*args: Any, **kwargs: Any) -> Any:
        for attempt in range(1, NUM_RETRIES + 1):
            try:
                return func(*args, **kwargs)

            except GetError as error:
                if _is_internal_dynamodb_server_error(error):
                    if attempt == NUM_RETRIES:
                        raise DynamoInternalServerError() from error
                    time.sleep(SECONDS_BETWEEN_RETRIES)
                else:
                    raise error

            except (ScanError, QueryError) as error:
                if _is_internal_dynamodb_server_error(error):
                    raise DynamoInternalServerError() from error
                if _is_request_throttled(error):
                    raise ThrottlingException() from error
                raise error

        raise Exception(  # pylint: disable=broad-exception-raised
            f"Failed after {attempt} attempts, this should never happen."
        )

    return cast(T, with_dynamo_error)


class BaseTable:
    """Base class for all DynamoDB tables."""

    def __init_subclass__(cls) -> None:
        """Catch all dynamo errors within the subclasses."""
        decorate_class(cls=cls, decorator=catch_dynamo_errors)


Thing = TypeVar("Thing")


class DynamoItemIterator(Generic[Thing], Iterator[Thing]):
    """Iterates over the items in a Dynamo table, fetching new pages if necessary."""

    def __init__(
        self, items: Iterator[Thing], get_last_evaluated_key: Callable[[], Optional[LastEvaluatedKey]]
    ) -> None:
        self._items = items
        self._get_last_evaluated_key = get_last_evaluated_key

    @catch_dynamo_errors
    def __next__(self) -> Thing:
        """Fetch the next item from DynamoDB."""
        return next(self._items)

    @property
    def last_evaluated_key(self) -> Optional[LastEvaluatedKey]:
        """Retrieve the current LastEvaluatedKey to resume the iteration at a later point."""
        return self._get_last_evaluated_key()


class DateTimeAttribute(Attribute[datetime]):
    """Represents a datetime attribute."""

    attr_type = STRING
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

    def serialize(self, value: datetime) -> str:
        """Convert the date object to a string."""
        return value.strftime(self.DATETIME_FORMAT)

    def deserialize(self, value: str) -> datetime:
        """Convert the given string to a date object."""
        return datetime.strptime(value, self.DATETIME_FORMAT)


def create_model(table: str, model: Type[M], module: str) -> Type[M]:
    """Create a database model of the specific type."""

    class Meta:
        region = Region.preferred(Partition.default()).value
        table_name = table
        connect_timeout_seconds = 1
        read_timeout_seconds = 3

    # We must set __module__ because otherwise PynamoDB will set it to None which makes Pytest crash on DoesNotExist.
    return type(model.__name__, (model,), {"Meta": Meta, "__module__": module})


def conditional_check_failed(error: PynamoDBException) -> bool:
    """Return if the error is ConditionalCheckFailedException."""
    return error.cause_response_code == "ConditionalCheckFailedException"


def _is_request_throttled(error: PynamoDBException) -> bool:
    return error.cause_response_code in {"ProvisionedThroughputExceededException", "ThrottlingException"}


def _is_internal_dynamodb_server_error(error: PynamoDBException) -> bool:
    return error.cause_response_code in {"InternalServerError"}


class DynamoInternalServerError(Exception):
    """Signals that a internal error occurred."""

    def __init__(self) -> None:
        super().__init__("DynamoDB experienced an internal server error. Please try again later.")


class ThrottlingException(Exception):
    """Signals that too many requests are send to AWS."""

    def __init__(self) -> None:
        super().__init__("The requested table has been throttled by DynamoDB, try again later.")


DynamicEnum = TypeVar("DynamicEnum", bound=Enum)


class LazyEnumAttribute(Attribute[DynamicEnum], Generic[DynamicEnum]):
    """Lazy Enum Attribute class to support the mocking of enums in config files."""

    attr_type = pynamodb.constants.STRING

    def __init__(self, get_class: Callable[[], Type[DynamicEnum]]):
        super().__init__()
        self._get_class = get_class

    def serialize(self, value: DynamicEnum) -> Any:
        """Convert the enum object to a string."""
        return value.value

    def deserialize(self, value: str) -> DynamicEnum:
        """Convert the given string to an enum object."""
        return self._get_class()(value)
