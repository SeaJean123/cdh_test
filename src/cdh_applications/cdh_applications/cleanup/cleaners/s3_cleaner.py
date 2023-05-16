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
# pylint: disable=duplicate-code
from logging import Logger
from typing import Any
from typing import Callable
from typing import Dict

import boto3
from botocore.exceptions import ClientError

from cdh_applications.cleanup.cleanup_utils import has_resource_prefix
from cdh_applications.cleanup.generic_cleaner import GenericCleaner


class S3Cleaner(GenericCleaner):
    """Cleaner class for the functional tests for the AWS S3 service."""

    def __init__(  # pylint: disable=too-many-arguments,super-init-not-called
        self,
        region: str,
        prefix: str,
        clean_filter: Callable[[str, str, Any], bool],
        credentials: Dict[str, Any],
        log: Logger,
    ) -> None:
        self._prefix = prefix
        self._should_clean = clean_filter
        self._resource = boto3.resource("s3", region_name=region, **credentials)
        self.logger = log

    def clean(self) -> None:
        """Start the cdh_cleanup of the AWS S3 service."""
        self.logger.info("Looking for S3 buckets...")
        for bucket in self._resource.buckets.all():
            if self._filter_bucket(bucket.name) and self._should_clean("bucket", bucket.name, self.logger):
                self._empty_and_delete_bucket(bucket)

    def _filter_bucket(self, name: str) -> bool:
        return has_resource_prefix(name, self._prefix) and not name.startswith(self._prefix + "cdh-core")

    def _empty_and_delete_bucket(self, bucket: Any) -> None:
        # clean up s3 buckets does not yet work for versioned buckets
        try:
            bucket.objects.all().delete()
            bucket.delete()
        except ClientError as error:
            self.logger.warning(error)
