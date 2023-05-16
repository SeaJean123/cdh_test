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
from datetime import timedelta
from typing import Any
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest

from cdh_core.aws_clients.logs_client import FailedToWriteToLog
from cdh_core.aws_clients.logs_client import LogsClient
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.builder import Builder


class TestLogsClient:
    LOG_GROUP_NAME = "foo"
    LOG_STREAM_NAME = "bar"
    MESSAGES = ["Hallo Welt", "Hello World"]

    @pytest.fixture(autouse=True)
    def service_setup(self, mock_logs: Any) -> None:  # pylint: disable=unused-argument
        self.boto_client = boto3.client("logs", region_name=build_region().value)

    def test_create_log_stream_does_not_fail_on_existing_stream(self) -> None:
        self.boto_client.create_log_group(logGroupName=self.LOG_GROUP_NAME)
        logs_client = LogsClient(self.boto_client)
        logs_client.create_log_stream(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME)
        logs_client.create_log_stream(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME)

    def test_messages_are_written_to_log(self) -> None:
        self.boto_client.create_log_group(logGroupName=self.LOG_GROUP_NAME)

        logs_client = LogsClient(self.boto_client)
        logs_client.create_log_stream(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME)
        sequence_token = logs_client.write_log(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME, [self.MESSAGES[0]], "")
        assert sequence_token == "00000000000000000000000000000000000000000000000000000001"
        logs_client.write_log(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME, [self.MESSAGES[1]], sequence_token)

        stored_messages = self.boto_client.get_log_events(
            logGroupName=self.LOG_GROUP_NAME,
            logStreamName=self.LOG_STREAM_NAME,
            startTime=0,
            endTime=int(time.time() * 1000) + 100000,
            limit=123,
            startFromHead=True,
        )["events"]

        assert stored_messages[0]["message"] == self.MESSAGES[0]
        assert stored_messages[1]["message"] == self.MESSAGES[1]

    def test_no_sequence_token_in_put_log_events(self) -> None:
        boto_client = Mock()
        boto_client.put_log_events = Mock(return_value={"nextSequenceToken": "next-token"})
        logs_client = LogsClient(boto_client)
        fixed_time = 100
        with patch("cdh_core.aws_clients.logs_client.time.time", return_value=fixed_time):
            logs_client.write_log(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME, [self.MESSAGES[0]], "")
        boto_client.put_log_events.assert_called_with(
            logGroupName=self.LOG_GROUP_NAME,
            logStreamName=self.LOG_STREAM_NAME,
            logEvents=[{"timestamp": fixed_time * 1000, "message": self.MESSAGES[0]}],
        )

    def test_sequence_token_in_put_log_events(self) -> None:
        boto_client = Mock()
        boto_client.put_log_events = Mock(return_value={"nextSequenceToken": "next-token"})
        logs_client = LogsClient(boto_client)
        fixed_time = 100

        next_sequence_token = "the-next-token"
        with patch("cdh_core.aws_clients.logs_client.time.time", return_value=fixed_time):
            logs_client.write_log(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME, [self.MESSAGES[0]], next_sequence_token)
        boto_client.put_log_events.assert_called_with(
            logGroupName=self.LOG_GROUP_NAME,
            logStreamName=self.LOG_STREAM_NAME,
            logEvents=[{"timestamp": fixed_time * 1000, "message": self.MESSAGES[0]}],
            sequenceToken=next_sequence_token,
        )

    def test_write_log_retries_successfully(self) -> None:
        boto_client = Mock()
        invalid_sequence_token = self.boto_client.exceptions.InvalidSequenceTokenException
        boto_client.exceptions.InvalidSequenceTokenException = invalid_sequence_token
        next_token = "next-token"
        boto_client.put_log_events.side_effect = [
            invalid_sequence_token(error_response={"expectedSequenceToken": "123"}, operation_name="foo"),
            {"nextSequenceToken": next_token},
        ]
        logs_client = LogsClient(boto_client)
        assert next_token == logs_client.write_log(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME, [self.MESSAGES[0]], "")
        assert boto_client.put_log_events.call_count == 2

    def test_write_log_raises_error_if_next_sequence_token_is_not_valid_in_a_row(self) -> None:
        boto_client = Mock()
        invalid_sequence_token = self.boto_client.exceptions.InvalidSequenceTokenException
        boto_client.exceptions.InvalidSequenceTokenException = invalid_sequence_token
        boto_client.put_log_events.side_effect = invalid_sequence_token(
            error_response={"expectedSequenceToken": "123"}, operation_name="foo"
        )
        logs_client = LogsClient(boto_client)
        with pytest.raises(FailedToWriteToLog):
            logs_client.write_log(self.LOG_GROUP_NAME, self.LOG_STREAM_NAME, [self.MESSAGES[0]], "")
        assert boto_client.put_log_events.call_count == 20

    def test_log_messages_are_filtered_correctly(self) -> None:
        events = [{"foo": "bar"}]

        boto_client = Mock()
        boto_client.filter_log_events.return_value = {"events": events}

        now = datetime.now()
        pattern = Builder.build_random_string()
        response = LogsClient(boto_client).filter_log_events_by_log_stream_name_prefix(
            log_group_name=self.LOG_GROUP_NAME,
            log_stream_name_prefix=self.LOG_STREAM_NAME,
            start_time=now - timedelta(minutes=5),
            end_time=now,
            filter_pattern=pattern,
            limit=10,
        )
        assert response == events

        boto_client.filter_log_events.assert_called_once_with(
            logGroupName=self.LOG_GROUP_NAME,
            logStreamNamePrefix=self.LOG_STREAM_NAME,
            startTime=int((now - timedelta(minutes=5)).timestamp() * 1000),
            endTime=int(now.timestamp() * 1000),
            filterPattern=pattern,
            limit=10,
            interleaved=True,
        )
