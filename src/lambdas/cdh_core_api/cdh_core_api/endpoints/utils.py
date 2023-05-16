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
from typing import Callable
from typing import cast
from typing import TypeVar

from cdh_core_api.catalog.base import DynamoInternalServerError
from cdh_core_api.catalog.base import ThrottlingException

from cdh_core.exceptions.http import ServiceUnavailableError
from cdh_core.exceptions.http import TooManyRequestsError

AnyFunc = TypeVar("AnyFunc", bound=Callable[..., Any])


def throttleable(func: AnyFunc) -> AnyFunc:
    """Convert any DynamoDB throttling errors that may occur during function execution to 429 errors."""

    @wraps(func)
    def with_throttling(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except ThrottlingException as error:
            raise TooManyRequestsError(error) from error

    return cast(AnyFunc, with_throttling)


def remap_dynamo_internal_errors(func: AnyFunc) -> AnyFunc:
    """Convert any DynamoDB internal server errors that may occur during function execution to 503 errors."""

    @wraps(func)
    def with_dynamo_recovering(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except DynamoInternalServerError as error:
            raise ServiceUnavailableError(error) from error

    return cast(AnyFunc, with_dynamo_recovering)
