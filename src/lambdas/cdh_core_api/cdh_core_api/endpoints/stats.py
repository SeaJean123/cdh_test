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
from dataclasses import dataclass
from http import HTTPStatus
from logging import getLogger
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union

from botocore.exceptions import ClientError
from cdh_core_api.api.openapi_spec.openapi import OpenApiSchema
from cdh_core_api.api.openapi_spec.openapi import OpenApiTypes
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.services.utils import fetch_resource
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.aws_clients.cloudwatch_client import CloudwatchClient
from cdh_core.aws_clients.cloudwatch_client import ExpressionDefinition
from cdh_core.aws_clients.cloudwatch_client import MetricDefinition
from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.entities.accounts import Account
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_clients import CloudwatchStatisticType
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.exceptions.http import TooManyRequestsError

LOG = getLogger(__name__)


S3_STATS_SCHEMA = OpenApiSchema(
    "S3ResourceStats",
    {
        "bucketSizeBytes": OpenApiTypes.optional_float_with_description(
            description="Estimated maximum size of all objects in s3 bucket"
        ),
        "numberOfMessagesPublished": OpenApiTypes.optional_float_with_description(
            description="Count of messages published to sns queue of s3 bucket"
        ),
        "numberOfObjects": OpenApiTypes.optional_float_with_description(
            description="Number of objects stored in s3 bucket"
        ),
        "publishSize": OpenApiTypes.optional_float_with_description(
            description="Payload size of messages published to sns queue of s3 bucket"
        ),
        "allRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of HTTP-REST operations on s3 bucket"
        ),
        "getRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of Get operations on s3 bucket"
        ),
        "headRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of Head operations on s3 bucket"
        ),
        "listRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of List operations on s3 bucket"
        ),
        "putRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of Put operations on s3 bucket"
        ),
        "deleteRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of Delete operations on s3 bucket"
        ),
        "postRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of Post operations on s3 bucket"
        ),
        "selectRequests": OpenApiTypes.optional_float_with_description(
            description="Total count of Select operations on s3 bucket"
        ),
        "bytesDownloaded": OpenApiTypes.optional_float_with_description(
            description="Number of bytes downloaded from s3 bucket"
        ),
        "bytesUploaded": OpenApiTypes.optional_float_with_description(
            description="Number of bytes uploaded to s3 bucket"
        ),
        "4xxErrors": OpenApiTypes.optional_float_with_description(description="Number of 4xx HTTP-Errors on s3 bucket"),
        "5xxErrors": OpenApiTypes.optional_float_with_description(description="Number of 5xx HTTP-Errors on s3 bucket"),
        "firstByteLatency": OpenApiTypes.optional_float_with_description(
            description="Time in ms at which the first byte of s3-object is transferred"
        ),
        "totalRequestLatency": OpenApiTypes.optional_float_with_description(
            description="Time in ms at which the last byte of s3-object is transferred"
        ),
        "selectBytesScanned": OpenApiTypes.optional_float_with_description(
            description="Number of bytes read on s3 bucket by using s3 select"
        ),
        "selectBytesReturned": OpenApiTypes.optional_float_with_description(
            description="Number of bytes returned from s3 bucket by using s3 select"
        ),
    },
)


@dataclass(frozen=True)
class StatsPath:
    """Represents the path parameters for the GET /{hub}/resources/s3/{datasetId}/{stage}/{region}/stats endpoint."""

    hub: Hub
    stage: Stage
    region: Region
    datasetId: DatasetId  # pylint: disable=invalid-name


@coreapi.route("/{hub}/resources/s3/{datasetId}/{stage}/{region}/stats", ["GET"])
@openapi.response(HTTPStatus.OK, S3_STATS_SCHEMA)
def get_stats_of_s3(
    path: StatsPath,
    aws: AwsClientFactory,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> JsonResponse:
    """Return cloudwatch statistics for visible S3 resources aggregated over the last 24 hours."""
    s3_bucket = cast(
        S3Resource,
        fetch_resource(
            hub=path.hub,
            resource_type=ResourceType.s3,
            dataset_id=path.datasetId,
            stage=path.stage,
            region=path.region,
            visible_data_loader=visible_data_loader,
        ),
    )
    bucket_name = s3_bucket.name
    topic_name = s3_bucket.sns_topic_arn.identifier
    cloudwatch = aws.cloudwatch_client(s3_bucket.resource_account_id, AccountPurpose("resources"), s3_bucket.region)
    try:
        response = gather_metrics(cloudwatch, bucket_name, topic_name)
        return JsonResponse(body=response)
    except ClientError as err:
        if err.response["Error"]["Code"] == "Throttling":
            LOG.warning("The request was throttled by CloudWatch. Please try again later. %s", err)
            raise TooManyRequestsError(err) from err
        raise


def gather_metrics(
    cloudwatch: CloudwatchClient,
    bucket_name: str,
    topic_name: str,
    time_span_in_seconds: int = 3 * 24 * 3600,
    period_in_seconds: int = 24 * 3600,
) -> Dict[str, Optional[int]]:
    """Return some cloudwatch metrics for the given S3 bucket and sns topic."""
    metrics: List[Union[MetricDefinition, ExpressionDefinition]] = []

    bucket_metrics: Sequence[Union[MetricDefinition, ExpressionDefinition]] = [
        MetricDefinition(
            metric_id="numberOfObjects",
            namespace="AWS/S3",
            name="NumberOfObjects",
            dimensions={"StorageType": "AllStorageTypes", "BucketName": bucket_name},
            statistic_type=CloudwatchStatisticType.MAXIMUM,
        ),
        ExpressionDefinition(
            metric_id="allStorageVolume",
            expression=(
                """SEARCH('{AWS/S3, BucketName, StorageType} """  # noqa: B028
                f"""MetricName=BucketSizeBytes AND BucketName="{bucket_name}"', """
                f"""{CloudwatchStatisticType.MAXIMUM.value!r}, {time_span_in_seconds})"""
            ),
            return_data=False,
        ),
        ExpressionDefinition(metric_id="bucketSizeBytes", expression="SUM(allStorageVolume)"),
    ]
    metrics.extend(bucket_metrics)

    request_metrics: Sequence[Union[MetricDefinition, ExpressionDefinition]] = (
        [
            MetricDefinition(
                metric_id=_lower_first_char(name),
                namespace="AWS/S3",
                name=name,
                dimensions={"FilterId": "EntireBucket", "BucketName": bucket_name},
                statistic_type=CloudwatchStatisticType.MAXIMUM,
            )
            for name in ("FirstByteLatency", "TotalRequestLatency")
        ]
        + [
            MetricDefinition(
                metric_id=_lower_first_char(name),
                namespace="AWS/S3",
                name=name,
                dimensions={"FilterId": "EntireBucket", "BucketName": bucket_name},
                statistic_type=CloudwatchStatisticType.SUM,
            )
            for name in (
                "AllRequests",
                "GetRequests",
                "HeadRequests",
                "ListRequests",
                "PutRequests",
                "DeleteRequests",
                "PostRequests",
                "SelectRequests",
                "BytesDownloaded",
                "BytesUploaded",
                "SelectBytesScanned",
                "SelectBytesReturned",
            )
        ]
        + [
            MetricDefinition(
                metric_id="x_4xxErrors",
                label="4xxErrors",
                namespace="AWS/S3",
                name="4xxErrors",
                dimensions={"FilterId": "EntireBucket", "BucketName": bucket_name},
                statistic_type=CloudwatchStatisticType.SUM,
            ),
            MetricDefinition(
                metric_id="x_5xxErrors",
                label="5xxErrors",
                namespace="AWS/S3",
                name="5xxErrors",
                dimensions={"FilterId": "EntireBucket", "BucketName": bucket_name},
                statistic_type=CloudwatchStatisticType.SUM,
            ),
        ]
    )
    metrics.extend(request_metrics)

    sns_metrics: Sequence[Union[MetricDefinition, ExpressionDefinition]] = [
        MetricDefinition(
            metric_id="numberOfMessagesPublished",
            namespace="AWS/SNS",
            name="NumberOfMessagesPublished",
            dimensions={"TopicName": topic_name},
            statistic_type=CloudwatchStatisticType.SUM,
        ),
        MetricDefinition(
            metric_id="publishSize",
            namespace="AWS/SNS",
            name="PublishSize",
            dimensions={"TopicName": topic_name},
            statistic_type=CloudwatchStatisticType.AVERAGE,
        ),
    ]
    metrics.extend(sns_metrics)

    results = cloudwatch.last_value_of_metrics(
        time_span_in_seconds=time_span_in_seconds, period_in_seconds=period_in_seconds, requested_metrics=metrics
    )
    response: Dict[str, Optional[int]] = {k: None for k in S3_STATS_SCHEMA.properties.keys()}
    for item in results:
        if item.values and item.label in S3_STATS_SCHEMA.properties:
            response[item.label] = int(item.values[0])
    return response


def _lower_first_char(text: str) -> str:
    return text[:1].lower() + text[1:] if text else ""
