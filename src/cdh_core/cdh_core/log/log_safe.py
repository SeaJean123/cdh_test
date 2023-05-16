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
import inspect
import logging
import sys
from functools import wraps
from types import TracebackType
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

LOG = logging.getLogger(__name__)

LambdaHandler = TypeVar("LambdaHandler", bound=Callable[..., Any])
_UNHANDLED_EXCEPTION = "unhandled_exception"


class GenericLambdaException(Exception):
    """Generic exception raised by a Lambda handler to avoid sensitive data ending up in logs.

    Contains a generic error message without any additional info, making it safe to use.
    May be raised actively by a Lambda handler method, but *NOT* during error handling (traceback might be compromised).
    To avoid duplicate alerts, these errors will be ignored by the LogsSubscriptionLambda.
    """

    def __init__(self) -> None:
        super().__init__("Lambda invocation failed")


def log_safe(exceptions_to_pass: Optional[List[Type[Exception]]] = None) -> Callable[[LambdaHandler], LambdaHandler]:
    """Ensure exceptions not handled by the LambdaHandler itself do not expose sensitive information."""
    safe_exceptions = tuple((exceptions_to_pass or []) + [GenericLambdaException])

    def decorator(lambda_handler: LambdaHandler) -> LambdaHandler:
        @wraps(lambda_handler)
        def inner(event: Dict[str, Any], context: Any) -> Any:
            _redirect_unhandled_exception_logging()
            try:
                return lambda_handler(event, context)
            except safe_exceptions:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                # don't raise GenericLambdaException here, otherwise secrets may make it into the traceback
                exc_type, exc_value, exc_traceback = sys.exc_info()
                assert exc_type is not None and exc_value is not None and exc_traceback is not None
                innermost_exception = _extract_innermost_unhandled_exception(exc_traceback)
                LOG.exception(
                    "Lambda invocation failed",
                    exc_info=(exc_type, exc_value, exc_traceback.tb_next),  # discard info about wrapper
                    extra={_UNHANDLED_EXCEPTION: innermost_exception},
                )
            raise GenericLambdaException()

        return cast(LambdaHandler, inner)

    return decorator


def _extract_innermost_unhandled_exception(exc_traceback: TracebackType) -> TracebackType:
    while exc_traceback.tb_next is not None:
        exc_traceback = exc_traceback.tb_next
    return exc_traceback


class RedirectionFilter(logging.Filter):
    """Log filter that redirects a LogRecord to the original unhandled exception."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redirects the logged function name and line number to that of the call that caused the exception."""
        if unhandled_exception := getattr(record, _UNHANDLED_EXCEPTION, None):
            frame = inspect.getframeinfo(unhandled_exception)
            record.funcName = frame.function
            record.lineno = frame.lineno
            record.filename = frame.filename
            record.module = inspect.getmodulename(frame.filename)  # type: ignore
        return True


def _redirect_unhandled_exception_logging() -> None:
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(RedirectionFilter())
