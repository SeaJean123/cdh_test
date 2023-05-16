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
# pylint: disable=E1101
# pylint: disable=unused-argument
import datetime
import time
from http import HTTPStatus
from typing import Any
from typing import Dict
from typing import Generator
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import notify_teams
import pytest
from mypy_boto3_dynamodb import DynamoDBClient
from notify_teams.notify_teams import DEFAULT_FRIENDLY_NAME_FOR_UNKNOWN_ACCOUNTS
from notify_teams.notify_teams import handler
from notify_teams.notify_teams import LOG
from notify_teams.notify_teams import resolve_account_friendly_name
from notify_teams.notify_teams import SilenceInfo
from notify_teams.notify_teams import trim_message
from requests_mock.mocker import Mocker

from cdh_core.enums.aws_test import build_region
from cdh_core.log.log_safe import GenericLambdaException
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.fixtures import mock_dynamodb  # pylint: disable=unused-import


@pytest.mark.usefixtures("mock_dynamodb")
class TestNotifyTeams:
    REGION = build_region()
    SILENCED_ALERTS_TABLE_NAME = Builder.build_random_string()
    WEBHOOK_URL = Builder.build_random_url()

    @pytest.fixture(autouse=True)
    def setup_environment_variables(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("AWS_REGION", self.REGION.value)
        monkeypatch.setenv("SILENCED_ALERTS_TABLE", self.SILENCED_ALERTS_TABLE_NAME)
        monkeypatch.setenv("WEBHOOK_URL", self.WEBHOOK_URL)
        monkeypatch.setenv("ENABLED", str(True))

    @pytest.fixture()
    def moto_dynamodb(self, setup_environment_variables: Any) -> Generator[DynamoDBClient, None, None]:
        dynamodb = boto3.client("dynamodb", region_name=self.REGION.value)
        dynamodb.create_table(
            AttributeDefinitions=[
                {"AttributeName": "hash", "AttributeType": "S"},
            ],
            TableName=self.SILENCED_ALERTS_TABLE_NAME,
            KeySchema=[
                {"AttributeName": "hash", "KeyType": "HASH"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.update_time_to_live(
            TableName=self.SILENCED_ALERTS_TABLE_NAME, TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"}
        )
        dynamodb.put_item(
            TableName=self.SILENCED_ALERTS_TABLE_NAME,
            Item={
                "hash": {"S": Builder.build_random_string()},
                "comment": {"S": "should be ignored"},
                "ttl": {"N": str(int(time.time()) + 3600)},
            },
        )
        yield dynamodb

    @staticmethod
    def create_notification_logs_ok(event_hash: str = "foo") -> Dict[str, Any]:
        return {
            "Records": [
                {
                    "messageId": "67782013-a78a-4a24-ba9b-8045d2cd2bf7",
                    "receiptHandle": "AQEBJEqcHWWlCLdPCta8yBTYXaLdqA==",
                    "body": '{\n  "Type" : "Notification",\n  "MessageId" : "4b17aaed-6db1-5fdb-97fd-fc51312dddb3",\n  '
                    '"TopicArn" : "arn:aws:sns:eu-central-1:123456789012:cdh-core-alerts",\n  '
                    '"Subject" : "Log Alarm for \\"ERROR cdh-accounts-db-sync\\"",\n'
                    f'  "Message" : "{{\\"severity\\": \\"ERROR\\", \\"hash\\": \\"{event_hash}\\",'
                    ' \\"description\\": \\"my error text\\", \\"time_stamp\\": 1581348999, '
                    '\\"account_id\\": \\"123456789012\\", \\"region\\": \\"eu-central-1\\", '
                    '\\"uri\\": \\"https://dummy\\"}",\n  "Timestamp" : "2020-02-11T07:40:12.300Z",\n'
                    ' "SignatureVersion" : "1",\n  "Signature" : "pTg==",\n '
                    ' "UnsubscribeURL" : "https://dummy"\n}',
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1581406812353",
                        "SenderId": "AIDAISMY7JYY5F7RTT6AO",
                        "ApproximateFirstReceiveTimestamp": "1581406812430",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "747db9053eedc29475f381a9e7aacb30",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:eu-central-1:123456789012:cdh-notify-teams",
                    "awsRegion": "eu-central-1",
                }
            ]
        }

    @staticmethod
    def create_notification_logs_unsupported_event() -> Dict[str, Any]:
        return {
            "Records": [
                {
                    "messageId": "67782013-a78a-4a24-ba9b-8045d2cd2bf7",
                    "receiptHandle": "AQEBJEqcHWWlCLdPCta8yBTYXaLdqA==",
                    "body": '{\n  "Type" : "Notification",\n  "MessageId" : "4b17aaed-6db1-5fdb-97fd-fc51312dddb3",\n  '
                    '"TopicArn" : "arn:aws:sns:us-east-1:123456789012:cdh-core-alerts",\n  '
                    '"Subject" : "Log Alarm for \\"ERROR cdh-accounts-db-sync\\"",\n'
                    '  "Message_WRONG_NAME" : "{\\"severity\\": \\"ERROR\\", \\"hash\\": \\"ERROR \\",'
                    ' \\"description\\": \\"my error text\\", \\"time_stamp\\": 1581348999, '
                    '\\"account_id\\": \\"123456789012\\", \\"region\\": \\"us-east-1\\", '
                    '\\"uri\\": \\"https://dummy\\"}",\n  "Timestamp" : "2020-02-11T07:40:12.300Z",\n'
                    ' "SignatureVersion" : "1",\n  "Signature" : "pTg==",\n '
                    ' "UnsubscribeURL" : "https://dummy"\n}',
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1581406812353",
                        "SenderId": "AIDAISMY7JYY5F7RTT6AO",
                        "ApproximateFirstReceiveTimestamp": "1581406812430",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "747db9053eedc29475f381a9e7aacb30",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:cdh-notify-teams",
                    "awsRegion": "us-east-1",
                }
            ]
        }

    @staticmethod
    def create_notification_metric_ok() -> Dict[str, Any]:
        return {
            "Records": [
                {
                    "messageId": "9008c0c9-10f2-40df-8d20-336521ae6cc0",
                    "receiptHandle": "AQEBN+yXjUkIcOKIlCg==",
                    "body": '{\n  "Type" : "Notification",\n  "MessageId" : "6f076d52-c71e-5e8b-8552-3bef17ccbd0f",\n'
                    ' "TopicArn" : "arn:aws:sns:us-west-1:123456789012:cdh-core-alerts",\n'
                    ' "Subject" : "ALARM: \\"cdh-accounts-db-sync-invocation-errors\\" in EU (Ireland)",\n'
                    ' "Message" : "{\\"AlarmName\\":\\"cdh-accounts-db-sync-invocation-errors\\",'
                    '\\"AlarmDescription\\":\\"The lambda function cdh-accounts-db-sync has invocation errors.'
                    '\\",\\"AWSAccountId\\":\\"123456789012\\",\\"NewStateValue\\":\\"ALARM\\",'
                    '\\"NewStateReason\\":\\"testing alarm\\",\\"StateChangeTime\\":'
                    '\\"2020-02-12T10:08:18.763+0000\\",\\"Region\\":\\"EU (Ireland)\\",\\"OldStateValue\\":'
                    '\\"OK\\",\\"Trigger\\":{\\"MetricName\\":\\"Errors\\",\\"Namespace\\":\\"AWS/Lambda\\",'
                    '\\"StatisticType\\":\\"Statistic\\",\\"Statistic\\":\\"SUM\\",\\"Unit\\":null,'
                    '\\"Dimensions\\":[{\\"value\\":\\"cdh-accounts-db-sync\\",\\"name\\":'
                    '\\"FunctionName\\"}],\\"Period\\":300,\\"EvaluationPeriods\\":1,\\"ComparisonOperator\\":'
                    '\\"GreaterThanThreshold\\",\\"Threshold\\":0.0,\\"TreatMissingData\\":'
                    '\\"- TreatMissingData:                    notBreaching\\",\\"EvaluateLowSampleCountPercentile'
                    '\\":\\"\\"}}",\n  "Timestamp" : "2020-02-12T10:08:18.817Z",\n  "SignatureVersion" : "1"'
                    ',\n  "Signature" : "mzjgnPw==",\n  "SigningCertURL" : "https://dummy-uri",\n '
                    '"UnsubscribeURL" : "https://dummy-uri"\n}',
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1581502098855",
                        "SenderId": "AIDAISMY7JYY5F7RTT6AO",
                        "ApproximateFirstReceiveTimestamp": "1581502098860",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "df68a2074e560af7f0bd50bc0924967f",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-west-1:123456789012:cdh-notify-teams",
                    "awsRegion": "us-west-1",
                }
            ]
        }

    @staticmethod
    def create_notification_metric_unsupported_event() -> Dict[str, Any]:
        return {
            "Records": [
                {
                    "messageId": "9008c0c9-10f2-40df-8d20-336521ae6cc0",
                    "receiptHandle": "AQEBN+yXjUkIcOKIlCg==",
                    "body": '{\n  "Type" : "Notification",\n  "MessageId" : "6f076d52-c71e-5e8b-8552-3bef17ccbd0f",\n'
                    ' "TopicArn" : "arn:aws:sns:us-west-1:123456789012:cdh-core-alerts",\n'
                    ' "Subject" : "ALARM: \\"cdh-accounts-db-sync-invocation-errors\\" in EU ('
                    'Ireland)",\n'
                    ' "Message" : "{\\"AlarmName\\":\\"cdh-accounts-db-sync-invocation-errors\\",'
                    '\\"AlarmDescription\\":\\"The lambda function cdh-accounts-db-sync has invocation '
                    "errors."
                    '\\",\\"AWSAccountId\\":\\"123456789012\\",\\"NewStateValue_NOT_EXISTS\\":\\"ALARM\\",'
                    '\\"NewStateReason\\":\\"testing alarm\\",\\"StateChangeTime\\":'
                    '\\"2020-02-12T10:08:18.763+0000\\",\\"Region\\":\\"EU (Ireland)\\",\\"OldStateValue\\":'
                    '\\"OK\\",\\"Trigger\\":{\\"MetricName\\":\\"Errors\\",'
                    '\\"Namespace\\":\\"AWS/Lambda\\",'
                    '\\"StatisticType\\":\\"Statistic\\",\\"Statistic\\":\\"SUM\\",\\"Unit\\":null,'
                    '\\"Dimensions\\":[{\\"value\\":\\"cdh-accounts-db-sync\\",\\"name\\":'
                    '\\"FunctionName\\"}],\\"Period\\":300,\\"EvaluationPeriods\\":1,'
                    '\\"ComparisonOperator\\":'
                    '\\"GreaterThanThreshold\\",\\"Threshold\\":0.0,\\"TreatMissingData\\":'
                    '\\"- TreatMissingData:                    notBreaching\\",\\"EvaluateLowSampleCountPercentile'
                    '\\":\\"\\"}}",\n  "Timestamp" : "2020-02-12T10:08:18.817Z",\n  "SignatureVersion" : '
                    '"1"'
                    ',\n  "Signature" : "mzjgnPw==",\n  "SigningCertURL" : "https://dummy-uri",\n '
                    '"UnsubscribeURL" : "https://dummy-uri"\n}',
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": "1581502098855",
                        "SenderId": "AIDAISMY7JYY5F7RTT6AO",
                        "ApproximateFirstReceiveTimestamp": "1581502098860",
                    },
                    "messageAttributes": {},
                    "md5OfBody": "df68a2074e560af7f0bd50bc0924967f",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-west-1:123456789012:cdh-notify-teams",
                    "awsRegion": "us-west-1",
                }
            ]
        }

    @patch("notify_teams.notify_teams.send_message_to_teams")
    def test_handler_on_notification_logs_successful(
        self, send_message_to_teams: Mock, moto_dynamodb: DynamoDBClient
    ) -> None:
        LOG.info = Mock()  # type: ignore # noqa: D106
        LOG.error = Mock()  # type: ignore
        LOG.critical = Mock()  # type: ignore

        handler(self.create_notification_logs_ok(), "dummy")

        LOG.error.assert_not_called()
        LOG.critical.assert_not_called()
        send_message_to_teams.assert_called_once()

    @patch("notify_teams.notify_teams.send_message_to_teams")
    def test_handler_on_notification_logs_silence_info(
        self, send_message_to_teams: Mock, moto_dynamodb: DynamoDBClient
    ) -> None:
        LOG.info = Mock()  # type: ignore # noqa: D106
        LOG.error = Mock()  # type: ignore
        LOG.critical = Mock()  # type: ignore
        event_hash = Builder.build_random_string()
        comment = Builder.build_random_string()
        valid_until = datetime.datetime.now().replace(microsecond=0) + datetime.timedelta(days=14)
        moto_dynamodb.put_item(
            TableName=self.SILENCED_ALERTS_TABLE_NAME,
            Item={
                "hash": {"S": event_hash},
                "comment": {"S": comment},
                "ttl": {"N": str(int(valid_until.timestamp()))},
            },
        )

        handler(self.create_notification_logs_ok(event_hash), "dummy")

        LOG.error.assert_not_called()
        LOG.critical.assert_not_called()
        send_message_to_teams.assert_called_once()
        assert send_message_to_teams.call_args[0][1] == SilenceInfo(
            hash=event_hash, comment=comment, valid_until=valid_until
        )

    @patch("notify_teams.notify_teams.send_message_to_teams")
    def test_handler_on_notification_logs_unsupported_event(self, send_message_to_teams: Mock) -> None:
        LOG.info = Mock()  # type: ignore
        LOG.error = Mock()  # type: ignore
        LOG.critical = Mock()  # type: ignore

        with pytest.raises(GenericLambdaException):
            handler(self.create_notification_logs_unsupported_event(), "dummy")
        LOG.error.assert_called()
        LOG.critical.assert_not_called()
        send_message_to_teams.assert_not_called()

    @patch("notify_teams.notify_teams.send_message_to_teams")
    def test_handler_on_notification_metric_successful(self, send_message_to_teams: Mock) -> None:
        LOG.info = Mock()  # type: ignore
        LOG.error = Mock()  # type: ignore
        LOG.critical = Mock()  # type: ignore

        handler(self.create_notification_metric_ok(), "dummy")

        LOG.error.assert_not_called()
        LOG.critical.assert_not_called()
        send_message_to_teams.assert_called_once()

    @patch("notify_teams.notify_teams.send_message_to_teams")
    def test_handler_on_notification_metric_unsupported_event(self, send_message_to_teams: Mock) -> None:
        LOG.info = Mock()  # type: ignore
        LOG.error = Mock()  # type: ignore
        LOG.critical = Mock()  # type: ignore

        with pytest.raises(GenericLambdaException):
            handler(self.create_notification_metric_unsupported_event(), "dummy")

        LOG.error.assert_called()
        LOG.critical.assert_not_called()
        send_message_to_teams.assert_not_called()

    MESSAGE_PREFIX = "The following message was too long and was cut of: \r\r"

    @pytest.mark.parametrize(
        "msg,max_msg_len,expected,error_log_calls,desc",
        [
            ("abc", 10, "abc", 0, "msg should not be cut of"),
            ("abc", 2, MESSAGE_PREFIX + "ab", 1, "msg should be cut of"),
            ("你", 2, MESSAGE_PREFIX + "你", 1, "msg should be cut of"),  # 你 is 3 byte long
            ("你你", 2, MESSAGE_PREFIX + "你", 1, "msg should be cut of"),  # 你 is 3 byte long
        ],
    )
    def test_trim_message(self, msg: str, max_msg_len: int, expected: str, error_log_calls: int, desc: str) -> None:
        LOG.error = Mock()  # type: ignore
        assert trim_message(msg, max_msg_len) == expected, desc
        assert LOG.error.call_count == error_log_calls

    @pytest.mark.parametrize(
        "account_id_str,expected_friendly_name",
        [
            ("987654321098", "cdh-prod-security-global-987654321098"),
            ("111122223333", "cdh-prod-resources-global-prod-111122223333"),
            ("3", DEFAULT_FRIENDLY_NAME_FOR_UNKNOWN_ACCOUNTS),
        ],
    )
    def test_resolve_account_friendly_name(self, account_id_str: str, expected_friendly_name: str) -> None:
        assert resolve_account_friendly_name(account_id=account_id_str) == expected_friendly_name

    def test_send_to_teams_success(self, requests_mock: Mocker) -> None:
        message = Builder.build_random_string()
        requests_mock.request(method="POST", url=self.WEBHOOK_URL, status_code=HTTPStatus.OK)

        notify_teams.notify_teams.send_message_to_teams(message)

        assert len(requests_mock.request_history) == 1
        assert requests_mock.request_history[0].url in {self.WEBHOOK_URL, self.WEBHOOK_URL + "/"}
        assert requests_mock.request_history[0].method == "POST"
        assert requests_mock.request_history[0].body == f'{{"text": "{message}"}}'.encode()  # noqa: B028
        assert requests_mock.request_history[0].headers.get("Content-Type") == "application/json"

    @pytest.mark.parametrize("status_code", [HTTPStatus.BAD_REQUEST, HTTPStatus.INTERNAL_SERVER_ERROR])
    def test_send_to_teams_failure(self, requests_mock: Mocker, status_code: HTTPStatus) -> None:
        message = Builder.build_random_string()
        requests_mock.request(method="POST", url=self.WEBHOOK_URL, status_code=status_code)

        with pytest.raises(Exception) as exc_info:
            notify_teams.notify_teams.send_message_to_teams(message)
        assert f"failed with the following error: {status_code.value}" in str(exc_info.value)

    def test_send_to_teams_disabled(self, requests_mock: Mocker, monkeypatch: Any) -> None:
        monkeypatch.setenv("ENABLED", Builder.build_random_string())
        LOG.info = Mock()  # type: ignore

        notify_teams.notify_teams.send_message_to_teams(Builder.build_random_string())

        assert len(requests_mock.request_history) == 0
        LOG.info.assert_called()
        assert any("Not forwarding the message to Teams" in call.args[0] for call in LOG.info.call_args_list)

    def test_send_to_teams_disabled_env_var_not_set(self, requests_mock: Mocker, monkeypatch: Any) -> None:
        monkeypatch.delenv("ENABLED")
        LOG.info = Mock()  # type: ignore

        notify_teams.notify_teams.send_message_to_teams(Builder.build_random_string())

        assert len(requests_mock.request_history) == 0
        LOG.info.assert_called()
        assert any("Not forwarding the message to Teams" in call.args[0] for call in LOG.info.call_args_list)

    def test_send_to_teams_silenced(self, requests_mock: Mocker) -> None:
        silence_info = SilenceInfo(
            hash=Builder.build_random_string(),
            comment=Builder.build_random_string(),
            valid_until=datetime.datetime.now().replace(microsecond=0) + datetime.timedelta(days=14),
        )
        LOG.info = Mock()  # type: ignore

        notify_teams.notify_teams.send_message_to_teams(Builder.build_random_string(), silence_info)

        assert len(requests_mock.request_history) == 0
        LOG.info.assert_called()
        assert any(
            "Not forwarding the message to Teams" in call.args[0] and str(silence_info) in call.args[0]
            for call in LOG.info.call_args_list
        )
