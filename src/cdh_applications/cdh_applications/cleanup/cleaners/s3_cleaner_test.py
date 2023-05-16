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
# pylint: disable=protected-access
from io import BytesIO
from logging import getLogger
from typing import List
from unittest.mock import Mock

import boto3
import pytest

from cdh_applications.cleanup.cleaners.s3_cleaner import S3Cleaner
from cdh_applications.cleanup.cleanup_utils_test import PREFIX
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core_dev_tools.testing.builder import Builder


@pytest.mark.usefixtures("mock_s3")
class TestS3Cleaner:
    @pytest.fixture()
    def region(self) -> str:
        regions: List[str] = [region.value for region in list(Region)]
        return Builder.get_random_element(
            regions,
            exclude={"us-east-1"},
        )

    def test_dont_delete_bucket_if_clean_filter_returns_false(self, region: str) -> None:
        bucket_name = f"{PREFIX}cdh-{build_business_object().value}-test"
        clean_filter = Mock(return_value=False)
        s3_cleaner = S3Cleaner(region=region, prefix=PREFIX, clean_filter=clean_filter, credentials={}, log=getLogger())
        s3_cleaner._resource.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}  # type: ignore
        )

        s3_cleaner.clean()

        clean_filter.assert_called_once_with("bucket", bucket_name, getLogger())
        assert [bucket["Name"] for bucket in boto3.client("s3", region_name=region).list_buckets()["Buckets"]] == [
            bucket_name
        ]

    def test_only_delete_valid_buckets(self, region: str) -> None:
        valid_bucket_name = f"{PREFIX}cdh-{build_business_object().value}-test"
        invalid_bucket_name = f"{PREFIX}cdh-test"
        clean_filter = Mock(return_value=True)
        s3_cleaner = S3Cleaner(region=region, prefix=PREFIX, clean_filter=clean_filter, credentials={}, log=getLogger())
        for bucket_name in [valid_bucket_name, invalid_bucket_name]:
            s3_cleaner._resource.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}  # type: ignore
            )

        s3_cleaner.clean()

        clean_filter.assert_called_once_with("bucket", valid_bucket_name, getLogger())
        assert [bucket["Name"] for bucket in boto3.client("s3", region_name=region).list_buckets()["Buckets"]] == [
            invalid_bucket_name
        ]

    def test_delete_nonempty_buckets(self, region: str) -> None:
        bucket_name = f"{PREFIX}cdh-{build_business_object().value}-test"
        clean_filter = Mock(return_value=True)
        s3_cleaner = S3Cleaner(region=region, prefix=PREFIX, clean_filter=clean_filter, credentials={}, log=getLogger())
        s3_cleaner._resource.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}  # type: ignore
        )
        s3_cleaner._resource.Bucket(bucket_name).put_object(Key="test", Body=BytesIO(b"test"))

        s3_cleaner.clean()

        clean_filter.assert_called_once_with("bucket", bucket_name, getLogger())
        assert [bucket["Name"] for bucket in boto3.client("s3", region_name=region).list_buckets()["Buckets"]] == []
