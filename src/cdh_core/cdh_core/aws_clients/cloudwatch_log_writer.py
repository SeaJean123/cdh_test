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
from __future__ import annotations

from typing import Callable
from typing import Dict
from typing import List

from cdh_core.aws_clients.logs_client import LogsClient


def make_cloudwatch_logger_singleton(
    cls: Callable[[LogsClient, str, str], CloudwatchLogWriter]
) -> Callable[[LogsClient, str, str], CloudwatchLogWriter]:
    """Ensure there is only one CloudwatchLogWriter."""
    existing_audit_loggers: Dict[str, CloudwatchLogWriter] = {}

    def get_instance(logs_client: LogsClient, log_group_name: str, log_stream_name: str) -> CloudwatchLogWriter:
        key = f"{log_group_name} + {log_stream_name}"
        if not existing_audit_loggers.get(key):
            existing_audit_loggers[key] = cls(logs_client, log_group_name, log_stream_name)
        return existing_audit_loggers[key]

    return get_instance


@make_cloudwatch_logger_singleton
class CloudwatchLogWriter:
    """Abstracts the boto3 logs client regarding writing logs."""

    def __init__(self, logs_client: LogsClient, log_group_name: str, log_stream_name: str) -> None:
        self._logs_client = logs_client
        self._log_group_name = log_group_name
        self._log_stream_name = log_stream_name
        self._next_sequence_token = ""
        self._created_log_stream = False

    def write_log(self, messages: List[str]) -> None:
        """Write a single line to the log."""
        if not self._created_log_stream:
            self._logs_client.create_log_stream(self._log_group_name, self._log_stream_name)
            self._created_log_stream = True

        self._next_sequence_token = self._logs_client.write_log(
            log_group_name=self._log_group_name,
            log_stream_name=self._log_stream_name,
            messages=messages,
            next_sequence_token=self._next_sequence_token,
        )
