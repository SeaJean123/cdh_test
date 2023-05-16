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
from datetime import datetime
from datetime import timedelta
from unittest.mock import Mock

from freezegun import freeze_time

from cdh_core.aws_clients.cloudwatch_client import CloudwatchClient
from cdh_core.aws_clients.cloudwatch_client import ExpressionDefinition
from cdh_core.aws_clients.cloudwatch_client import MetricDefinition
from cdh_core.aws_clients.cloudwatch_client import MetricResult
from cdh_core.enums.aws_clients import CloudwatchStatisticType
from cdh_core.enums.aws_clients import CloudwatchUnit

NAMESPACE = "AWS/Soccer"
METRIC_ID = "numberOfFouls"
METRIC_NAME = "NumberOfFouls"
DIMENSIONS = {"CardColor": "Red", "Player": "Sepp"}


EXPRESSION_ID = "expressionId"
EXPRESSION_VALUE = "expression"


class TestCloudwatchClient:
    def test_last_value_of_metrics(self) -> None:
        cloudwatch = Mock()
        cloudwatch.get_metric_statistics.return_value = {}
        now = datetime.now()
        time_span_in_seconds = 1234
        period_in_seconds = 5678
        client = CloudwatchClient(cloudwatch)

        cloudwatch.get_metric_data.return_value = {
            "MetricDataResults": [
                {"Id": METRIC_ID, "Label": METRIC_ID, "Values": [5.0], "Timestamps": [now]},
                {"Id": EXPRESSION_ID, "Label": EXPRESSION_ID, "Values": [], "Timestamps": []},
            ]
        }

        with freeze_time(now):
            result = client.last_value_of_metrics(
                requested_metrics=[
                    MetricDefinition(
                        metric_id=METRIC_ID,
                        namespace=NAMESPACE,
                        name=METRIC_NAME,
                        dimensions=DIMENSIONS,
                        statistic_type=CloudwatchStatisticType.MAXIMUM,
                    ),
                    ExpressionDefinition(expression=EXPRESSION_VALUE, metric_id=EXPRESSION_ID),
                ],
                time_span_in_seconds=time_span_in_seconds,
                period_in_seconds=period_in_seconds,
            )

            assert result == [
                MetricResult(metric_id=METRIC_ID, label=METRIC_ID, values=[5.0], timestamps=[now]),
                MetricResult(metric_id=EXPRESSION_ID, label=EXPRESSION_ID, values=[], timestamps=[]),
            ]

        cloudwatch.get_metric_data.assert_called_once_with(
            MetricDataQueries=[
                {
                    "Id": METRIC_ID,
                    "Label": METRIC_ID,
                    "ReturnData": True,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": NAMESPACE,
                            "MetricName": METRIC_NAME,
                            "Dimensions": [{"Name": "CardColor", "Value": "Red"}, {"Name": "Player", "Value": "Sepp"}],
                        },
                        "Period": period_in_seconds,
                        "Stat": CloudwatchStatisticType.MAXIMUM.value,
                    },
                },
                {
                    "Id": EXPRESSION_ID,
                    "Label": EXPRESSION_ID,
                    "ReturnData": True,
                    "Expression": EXPRESSION_VALUE,
                },
            ],
            StartTime=now - timedelta(seconds=time_span_in_seconds),
            EndTime=now,
            ScanBy="TimestampDescending",
        )

    def test_last_value_of_metric(self) -> None:
        cloudwatch = Mock()
        cloudwatch.get_metric_statistics.return_value = {}
        now = datetime.now()
        time_span_in_seconds = 1234
        client = CloudwatchClient(cloudwatch)

        with freeze_time(now):
            client.last_value_of_metric(
                namespace=NAMESPACE,
                metric_name=METRIC_NAME,
                dimension=DIMENSIONS,
                time_span_in_seconds=time_span_in_seconds,
                statistic_type=CloudwatchStatisticType.MAXIMUM,
            )

        cloudwatch.get_metric_statistics.assert_called_once_with(
            Namespace=NAMESPACE,
            MetricName=METRIC_NAME,
            Dimensions=[{"Name": "CardColor", "Value": "Red"}, {"Name": "Player", "Value": "Sepp"}],
            StartTime=now - timedelta(seconds=time_span_in_seconds),
            EndTime=now,
            Period=time_span_in_seconds,
            Statistics=["Maximum"],
        )

    def test_last_value_of_metric_strips_metadata(self) -> None:
        cloudwatch = Mock()
        cloudwatch.get_metric_statistics.return_value = {"Datapoints": ["foo"], "ResponseMetadata": "bar"}
        client = CloudwatchClient(cloudwatch)

        assert client.last_value_of_metric(
            namespace=NAMESPACE,
            metric_name=METRIC_NAME,
            dimension=DIMENSIONS,
            time_span_in_seconds=1234,
            statistic_type=CloudwatchStatisticType.MAXIMUM,
        ) == {"Datapoints": ["foo"]}

    def test_put_metric_data(self) -> None:
        cloudwatch = Mock()
        cloudwatch.get_metric_statistics.return_value = {}
        now = datetime.now()
        client = CloudwatchClient(cloudwatch)

        with freeze_time(now):
            client.put_metric_data(
                name_space=NAMESPACE,
                metric_name=METRIC_NAME,
                dimensions=DIMENSIONS,
                unit=CloudwatchUnit.COUNT,
                value=5.0,
            )

        cloudwatch.put_metric_data.assert_called_once_with(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "Dimensions": [{"Name": "CardColor", "Value": "Red"}, {"Name": "Player", "Value": "Sepp"}],
                    "MetricName": METRIC_NAME,
                    "Timestamp": now,
                    "Value": 5.0,
                    "Unit": CloudwatchUnit.COUNT.value,
                }
            ],
        )
