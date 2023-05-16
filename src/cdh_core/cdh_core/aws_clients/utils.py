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
from typing import Dict
from typing import Generator
from typing import Mapping

from botocore.exceptions import ClientError


def repeat_while_truncated(
    function: Callable[..., Dict[str, Any]], result_key: str, **kwargs: Any
) -> Generator[Any, None, None]:
    """Call the given function as long the result signals there is a Marker."""
    truncated = True
    while truncated:
        result = function(**kwargs)
        truncated = result["IsTruncated"]
        kwargs["Marker"] = result.get("Marker")
        yield from result[result_key]


def repeat_while_truncated_nextmarker(
    function: Callable[..., Dict[str, Any]], result_key: str, **kwargs: Any
) -> Generator[Any, None, None]:
    """Call the given function as long the result signals it is Truncated and there is a NextToken."""
    truncated = True
    while truncated:
        result = function(**kwargs)
        truncated = result["Truncated"]
        kwargs["Marker"] = result.get("NextMarker")
        yield from result[result_key]


def repeat_continuation_call(
    function: Callable[..., Mapping[str, Any]], result_key: str, **kwargs: Any
) -> Generator[Any, None, None]:
    """Call the given function as long the result signals there is a NextToken."""
    truncated = True
    while truncated:
        result = function(**kwargs)
        truncated = "NextToken" in result
        kwargs["NextToken"] = result.get("NextToken")
        yield from result[result_key]


def get_error_code(error: ClientError) -> str:
    """Extract the error code from the ClientError."""
    return str(error.response.get("Error", {}).get("Code", "UnknownError"))


def get_error_message(error: ClientError) -> str:
    """Extract the error message from the ClientError."""
    return str(error.response.get("Error", {}).get("Message", "UnknownMessage"))


class FailedToDeleteResourcesStillAssociating(Exception):
    """Signals a resource could not be deleted because RAM is still associating resources."""
