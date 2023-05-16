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
import datetime
import time
from contextlib import suppress
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_logs import CloudWatchLogsClient
    from mypy_boto3_logs.type_defs import FilteredLogEventTypeDef
    from mypy_boto3_logs.type_defs import InputLogEventTypeDef
else:
    CloudWatchLogsClient = object
    FilteredLogEventTypeDef = Dict[str, Any]
    InputLogEventTypeDef = object


class FailedToWriteToLog(Exception):
    """Signals that it was not possible to write to a log stream."""


class LogsClient:
    """Abstracts the boto3 logs client."""

    def __init__(self, boto3_logs_client: CloudWatchLogsClient) -> None:
        self._client = boto3_logs_client

    def create_log_stream(self, log_group_name: str, log_stream_name: str) -> None:
        """Create a new log stream."""
        with suppress(self._client.exceptions.ResourceAlreadyExistsException):
            self._client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)

    def write_log(
        self, log_group_name: str, log_stream_name: str, messages: List[str], next_sequence_token: str
    ) -> str:
        """Try to write to log stream with retires and if so return the next sequence token."""
        retry_counter = 0
        while retry_counter < 20:
            try:
                if next_sequence_token == "":
                    response = self._client.put_log_events(
                        logGroupName=log_group_name,
                        logStreamName=log_stream_name,
                        logEvents=LogsClient.get_log_events_from_messages(messages),
                    )
                else:
                    response = self._client.put_log_events(
                        logGroupName=log_group_name,
                        logStreamName=log_stream_name,
                        logEvents=LogsClient.get_log_events_from_messages(messages),
                        sequenceToken=next_sequence_token,
                    )
                return str(response["nextSequenceToken"])
            except self._client.exceptions.InvalidSequenceTokenException as error:
                next_sequence_token = error.response["expectedSequenceToken"]
            retry_counter += 1
        raise FailedToWriteToLog("Failed to write to log. Could not find correct sequence token.")

    @staticmethod
    def get_log_events_from_messages(messages: List[str]) -> List[InputLogEventTypeDef]:
        """Create log events from a list of messages."""
        return [{"timestamp": int(time.time() * 1000), "message": msg} for msg in messages]

    def filter_log_events_by_log_stream_name_prefix(  # pylint: disable=too-many-arguments
        self,
        log_group_name: str,
        log_stream_name_prefix: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        filter_pattern: Optional[str] = None,
        limit: Optional[int] = 1000,
        interleaved: Optional[bool] = True,
    ) -> List[FilteredLogEventTypeDef]:
        """Return the events of a log stream filtered by the prefix of the stream."""
        return self._client.filter_log_events(
            logGroupName=log_group_name,
            logStreamNamePrefix=log_stream_name_prefix,
            startTime=int(start_time.timestamp() * 1000) if start_time else None,  # type: ignore
            endTime=int(end_time.timestamp() * 1000) if end_time else None,  # type: ignore
            filterPattern=filter_pattern,  # type: ignore
            limit=limit,  # type: ignore
            interleaved=interleaved,  # type: ignore
        )["events"]
