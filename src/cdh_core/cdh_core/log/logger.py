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
import logging
import os
import re
import time
import traceback
from contextlib import suppress
from sys import exc_info
from typing import Any

LOG = logging.getLogger(__name__)

_JWT_REGEX = re.compile(r"jwt[\w]*=(eyJ[A-Za-z\d/]*\.eyJ[A-Za-z\d+/]*\.[A-Za-z\d/]*)")
_AWS_ACCESS_KEY_ID = re.compile(r"(?<![A-Z\d])[A-Z\d]{20}(?![A-Z\d])")
_AWS_SECRET_ACCESS_KEY = re.compile(r"(?<![A-Za-z\d/+=])[A-Za-z\d/+=]{40}(?![A-Za-z\d/+=])")
_AWS_SESSION_TOKEN = re.compile(r"(?<![A-Za-z\d/+=])[A-Za-z\d/+=]{360}(?![A-Za-z\d/+=])")

_URL_CHAR = r"[^\s()<>]"
_USER_NAME_CHAR = r"\w"
_PASSWORD_CHAR = r"[A-Za-z\d_.~\-]"  # unreserved characters according to RFC 3986 Uniform Resource Identifier
_USER_PASSWORD_AT = re.compile(rf"{_USER_NAME_CHAR}+:({_PASSWORD_CHAR}+)@{_URL_CHAR}+")


def configure_logging(base_package: str) -> None:
    """
    Set the internal log level to info, third party to error and the format of the log line in an AWS context.

    This function has to be called within the lambda handler not on top level.
    This configures also filtering for passwords and other sensitive data.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.ERROR)
    logging.getLogger(base_package).setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    logging.getLogger("cdh_core").setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

    for handler in root_logger.handlers:
        if type(handler).__name__ == "LambdaLoggerHandler":
            handler.setFormatter(
                CredentialHandlingLogger(
                    fmt="[%(levelname)s]\t[%(module)s]\t[%(funcName)s:%(lineno)d]\t"
                    "%(asctime)sZ\t%(aws_request_id)s\t%(message)s",
                )
            )
        elif type(handler.formatter).__name__ == "ColoredLevelFormatter":
            handler.setFormatter(CredentialHandlingLogger(colored_level_formatter=handler.formatter))


# We sometimes log entries with a lot (100M+) of characters causing the credentials removal to take too long (10+ min).
# Therefore we want to truncate log entries, there should not be a reason to log that much anyway.
# 1000000 seems to be sensible limit, should be enough in all cases with useful log outputs and is still fast _enough_.
MAX_RECORD_LENGTH = 10**6
RECORD_TRUNCATION_OVERHEAD = 1000
MAX_LOG_DURATION = float(os.environ.get("MAX_LOG_DURATION", "1"))


class CredentialHandlingLogger(logging.Formatter):
    """Class that removes sensitive data from log lines."""

    replacement_string = "***"

    def __init__(self, *args: Any, colored_level_formatter: Any = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._colored_level_formatter = colored_level_formatter

    def format(self, record: logging.LogRecord) -> str:
        """Override the format method and filter for sensitive data."""
        if self._colored_level_formatter:
            formatter = self._colored_level_formatter
        else:
            formatter = super()
        formatted_record = formatter.format(record)
        start = time.perf_counter()
        filtered = self.apply_all_filters(formatted_record)
        if (duration := time.perf_counter() - start) > MAX_LOG_DURATION:
            LOG.error(f"Applying log filters took too long ({duration:.3f}s)!")
        return filtered

    @classmethod
    def apply_all_filters(cls, to_check: str) -> str:
        """Replace sensitive data within the string with a predefined string."""
        try:
            truncate = len(to_check) > MAX_RECORD_LENGTH
            if truncate:
                to_check = to_check[: MAX_RECORD_LENGTH + RECORD_TRUNCATION_OVERHEAD]

            to_check = cls.remove_passwords_from_urls(to_check)
            to_check = cls.remove_aws_credentials(to_check)
            to_check = cls.remove_jwt_token(to_check)

            if truncate:
                # need to truncate again to not expose passwords at the bounds of the truncation
                return to_check[:-RECORD_TRUNCATION_OVERHEAD] + " ... (truncated by core logger)"
            return to_check
        except Exception as error:  # pylint: disable=broad-except
            return f"Something went wrong while formatting the log string: {error}" + "".join(traceback.format_stack())

    @classmethod
    def remove_passwords_from_urls(cls, to_check: str) -> str:
        """Replace all passwords within urls with a predefined string."""
        for match in _USER_PASSWORD_AT.finditer(to_check):
            url = match.group(0)
            if url.startswith("git::ssh://git@github.com"):
                continue
            password = match.group(1)
            to_check = to_check.replace(url, url.replace(password, cls.replacement_string, 1))
        return to_check

    @classmethod
    def remove_aws_credentials(cls, to_check: str) -> str:
        """Replace all AWS credentials with a predefined string."""
        with suppress(Exception):
            for match in _AWS_ACCESS_KEY_ID.finditer(to_check):
                to_check = to_check.replace(match.group(0), cls.replacement_string)
            for match in _AWS_SECRET_ACCESS_KEY.finditer(to_check):
                to_check = to_check.replace(match.group(0), cls.replacement_string)
            for match in _AWS_SESSION_TOKEN.finditer(to_check):
                to_check = to_check.replace(match.group(0), cls.replacement_string)
        return to_check

    @classmethod
    def remove_jwt_token(cls, to_check: str) -> str:
        """Replace all JWT tokens with a predefined string."""
        with suppress(Exception):
            for match in _JWT_REGEX.finditer(to_check):
                to_check = to_check.replace(match.group(1), cls.replacement_string)
        return to_check


def log_exception_frame(log: logging.Logger) -> None:
    """
    Log all current variables to error.

    Be aware that this method may log unexpectedly large amounts of data depending on the local variables in scope.
    """
    _, _, tb_last = exc_info()
    while tb_last.tb_next:  # type: ignore
        tb_last = tb_last.tb_next  # type: ignore
    log.error(tb_last.tb_frame.f_locals)  # type: ignore
