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
from enum import Enum


class PolicyDocumentType(Enum):
    """Types of policy documents."""

    MANAGED = "managed"
    GLUE = "glue"
    BUCKET = "bucket"
    KMS = "kms"
    SNS = "sns"

    def get_max_policy_length(self) -> int:
        """AWS defined policy length limits.

        Extracted of service specific AWS documentation.
        """
        limits = {
            PolicyDocumentType.MANAGED: 6144,
            PolicyDocumentType.GLUE: 10240,
            PolicyDocumentType.BUCKET: 20480,
            PolicyDocumentType.KMS: 32768,
            PolicyDocumentType.SNS: 30720,
        }
        return limits[self]


class CloudwatchUnit(Enum):
    """Unit for metric.

    Refer to https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html.
    """

    SECONDS = "Seconds"
    MICROSECONDS = "Microseconds"
    MILLISECONDS = "Milliseconds"
    BYTES = "Bytes"
    KILOBYTES = "Kilobytes"
    MEGABYTES = "Megabytes"
    GIGABYTES = "Gigabytes"
    TERABYTES = "Terabytes"
    BITS = "Bits"
    KILOBITS = "Kilobits"
    MEGABITS = "Megabits"
    GIGABITS = "Gigabits"
    TERABITS = "Terabits"
    PERCENT = "Percent"
    COUNT = "Count"
    BYTES_PER_SECOND = "Bytes/Second"
    KILOBYTES_PER_SECOND = "Kilobytes/Second"
    MEGABYTES_PER_SECOND = "Megabytes/Second"
    GIGABYTES_PER_SECOND = "Gigabytes/Second"
    TERABYTES_PER_SECOND = "Terabytes/Second"
    BITS_PER_SECOND = "Bits/Second"
    KILOBITS_PER_SECOND = "Kilobits/Second"
    MEGABITS_PER_SECOND = "Megabits/Second"
    GIGABITS_PER_SECOND = "Gigabits/Second"
    TERABITS_PER_SECOND = "Terabits/Second"
    COUNT_PER_SECOND = "Count/Second"
    NONE = "None"


class CloudwatchStatisticType(Enum):
    """Types of aggregations over specified periods of time in AWS CloudWatch.

    Refer to https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Statistic.
    """

    AVERAGE = "Average"
    MAXIMUM = "Maximum"
    MINIMUM = "Minimum"
    SAMPLE_COUNT = "SampleCount"
    SUM = "Sum"
