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
from unittest.mock import call
from unittest.mock import Mock

from cdh_core.aws_clients.cloudwatch_log_writer import CloudwatchLogWriter
from cdh_core_dev_tools.testing.builder import Builder


class TestCloudwatchLogWriter:
    def test_cloudwatch_log_writer_is_singleton(self) -> None:
        log_client = Mock()
        clw_client_1 = CloudwatchLogWriter(log_client, "foo", "bar")
        clw_client_2 = CloudwatchLogWriter(log_client, "bar", "foo")
        clw_client_3 = CloudwatchLogWriter(log_client, "foo", "bar")

        assert clw_client_1 == clw_client_3
        assert clw_client_1 != clw_client_2

    # pylint: disable=protected-access # is required for mocking
    def test_log_stream_is_created_once(self) -> None:
        log_client = Mock()
        log_client.create_log_stream = Mock()
        log_stream_name = Builder.build_random_string()
        log_group_name = Builder.build_random_string()
        clw_client = CloudwatchLogWriter(log_client, log_group_name, log_stream_name)

        clw_client.write_log(["log entry 1"])
        clw_client.write_log(["log entry 2"])
        clw_client.write_log(["log entry 3"])

        clw_client._logs_client.create_log_stream.assert_called_once_with(  # type: ignore
            log_group_name, log_stream_name
        )

    def test_cloudwatch_log_writer_uses_correct_next_sequence_tokens(self) -> None:
        log_client = Mock()
        next_sequence_token = Builder.build_random_string()
        log_client.write_log = Mock(return_value=next_sequence_token)
        log_stream_name = Builder.build_random_string()
        log_group_name = Builder.build_random_string()
        clw_client = CloudwatchLogWriter(log_client, log_group_name, log_stream_name)
        log_message_1 = [Builder.build_random_string()]
        clw_client.write_log(log_message_1)
        assert clw_client._next_sequence_token == next_sequence_token

        log_message_2 = [Builder.build_random_string()]
        clw_client.write_log(log_message_2)
        assert log_client.write_log.call_args_list == [
            call(
                log_group_name=log_group_name,
                log_stream_name=log_stream_name,
                messages=log_message_1,
                next_sequence_token="",
            ),
            call(
                log_group_name=log_group_name,
                log_stream_name=log_stream_name,
                messages=log_message_2,
                next_sequence_token=next_sequence_token,
            ),
        ]
