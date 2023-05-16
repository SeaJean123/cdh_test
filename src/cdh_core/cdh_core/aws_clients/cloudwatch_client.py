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
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from cdh_core.aws_clients.boto_retry_decorator import create_boto_retry_decorator
from cdh_core.enums.aws_clients import CloudwatchStatisticType
from cdh_core.enums.aws_clients import CloudwatchUnit

if TYPE_CHECKING:
    from mypy_boto3_cloudwatch import CloudWatchClient
    from mypy_boto3_cloudwatch.literals import StandardUnitType
    from mypy_boto3_cloudwatch.literals import StatisticType
    from mypy_boto3_cloudwatch.type_defs import GetMetricDataOutputTypeDef
    from mypy_boto3_cloudwatch.type_defs import GetMetricStatisticsOutputTypeDef
    from mypy_boto3_cloudwatch.type_defs import MetricDataQueryTypeDef
else:
    CloudWatchClient = object
    StandardUnitType = object
    StatisticType = object
    GetMetricDataOutputTypeDef = object
    MetricDataQueryTypeDef = object
    GetMetricStatisticsOutputTypeDef = Dict[str, Any]


@dataclass(frozen=True)
class MetricDefinition:
    """Metric stats definition for metric retrival."""

    metric_id: str
    namespace: str
    name: str
    dimensions: Dict[str, str]
    statistic_type: CloudwatchStatisticType
    return_data: bool = True
    label: Optional[str] = None


@dataclass(frozen=True)
class ExpressionDefinition:
    """Expression definition for metric retrival."""

    metric_id: str
    expression: str
    return_data: bool = True
    label: Optional[str] = None


@dataclass(frozen=True)
class MetricResult:
    """Metric result definition for metric retrival."""

    metric_id: str
    label: str
    values: List[float]
    timestamps: List[datetime]


class CloudwatchClient:
    """Abstracts the boto3 cloudwatch client."""

    def __init__(self, boto3_cloudwatch_client: CloudWatchClient):
        self._client = boto3_cloudwatch_client
        self._sleep = time.sleep

    retry = create_boto_retry_decorator("_sleep")

    @retry(num_attempts=20, wait_between_attempts=1, retryable_error_codes=["ServiceUnavailable"])
    def last_value_of_metrics(
        self,
        time_span_in_seconds: int,
        period_in_seconds: int,
        requested_metrics: Collection[Union[MetricDefinition, ExpressionDefinition]],
    ) -> List[MetricResult]:
        """Get the last value of a metric."""
        now = datetime.now()
        queries: List[MetricDataQueryTypeDef] = []
        for definition in requested_metrics:
            if isinstance(definition, MetricDefinition):
                queries.append(
                    {
                        "Id": definition.metric_id,
                        "Label": definition.label or definition.metric_id,
                        "ReturnData": definition.return_data,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": definition.namespace,
                                "MetricName": definition.name,
                                "Dimensions": [{"Name": k, "Value": v} for k, v in definition.dimensions.items()],
                            },
                            "Period": period_in_seconds,
                            "Stat": cast(StatisticType, definition.statistic_type.value),
                        },
                    }
                )
            elif isinstance(definition, ExpressionDefinition):
                queries.append(
                    {
                        "Id": definition.metric_id,
                        "Label": definition.label or definition.metric_id,
                        "ReturnData": definition.return_data,
                        "Expression": definition.expression,
                    }
                )
            else:
                raise ValueError("Type of definition unknown")

        result = self._client.get_metric_data(
            MetricDataQueries=queries,
            StartTime=now - timedelta(seconds=time_span_in_seconds),
            EndTime=now,
            ScanBy="TimestampDescending",
        )
        return [
            MetricResult(
                metric_id=item["Id"], label=item["Label"], values=item["Values"], timestamps=item["Timestamps"]
            )
            for item in result["MetricDataResults"]
        ]

    def last_value_of_metric(  # pylint: disable=too-many-arguments
        self,
        namespace: str,
        metric_name: str,
        dimension: Dict[str, str],
        time_span_in_seconds: int,
        statistic_type: CloudwatchStatisticType,
    ) -> Dict[str, Any]:
        """Get the last value of a metric."""
        now = datetime.now()
        result: GetMetricStatisticsOutputTypeDef = self._client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=[{"Name": key, "Value": value} for key, value in dimension.items()],
            StartTime=now - timedelta(seconds=time_span_in_seconds),
            EndTime=now,
            Period=time_span_in_seconds,
            Statistics=[cast(StatisticType, statistic_type.value)],
        )
        return {k: v for k, v in result.items() if k != "ResponseMetadata"}

    @retry(num_attempts=20, wait_between_attempts=1, retryable_error_codes=["ServiceUnavailable"])
    def last_value_of_metrics_by_search(
        self, search_pattern: str, statistic_type: CloudwatchStatisticType, time_span_in_seconds: int
    ) -> GetMetricDataOutputTypeDef:
        """Get the last value of a metric with a search pattern."""
        expression = f"SEARCH(' {search_pattern} ', \"{statistic_type.value}\", {time_span_in_seconds})"  # noqa: B028
        now = datetime.now()
        return self._client.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "search_metrics",
                    "Expression": expression,
                    "ReturnData": True,
                }
            ],
            StartTime=now - timedelta(seconds=time_span_in_seconds),
            EndTime=now,
            ScanBy="TimestampDescending",
        )

    def put_metric_data(  # pylint: disable=too-many-arguments
        self, name_space: str, metric_name: str, dimensions: Dict[str, str], unit: CloudwatchUnit, value: float
    ) -> None:
        """Write the given metric data."""
        self._client.put_metric_data(
            Namespace=name_space,
            MetricData=[
                {
                    "Dimensions": [{"Name": key, "Value": value} for key, value in dimensions.items()],
                    "MetricName": metric_name,
                    "Timestamp": datetime.now(),
                    "Value": value,
                    "Unit": cast(StandardUnitType, unit.value),
                }
            ],
        )
