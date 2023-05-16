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
from typing import Generator
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from cdh_applications.cleanup.cleanup_utils import has_prefix
from cdh_applications.cleanup.generic_cleaner import GenericCleaner
from cdh_core.aws_clients.boto_retry_decorator import create_boto_retry_decorator
from cdh_core.aws_clients.glue_resource_policy import GlueResourcePolicy
from cdh_core.entities.accounts import BaseAccount
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws import Partition


class GlueCleaner(GenericCleaner):
    """Cleaner class for the functional tests for the AWS Glue service."""

    retry = create_boto_retry_decorator()

    def __init__(  # pylint: disable=too-many-arguments,super-init-not-called
        self,
        region: str,
        prefix: str,
        clean_filter: Callable[[str, str, Any], bool],
        credentials: Dict[str, Any],
        partition: Partition,
        account: BaseAccount,
        log: Logger,
    ):
        self._region = region
        self._prefix = prefix
        self._should_clean = clean_filter
        self._client = boto3.client("glue", region_name=region, **credentials)
        self._partition = partition
        self._account_id = account.id
        self._s3_resource = boto3.resource("s3", region_name=region, **credentials)
        self._cache_bucket_name = self._get_cache_bucket_name()
        self.logger = log

    def clean(self) -> None:
        """Start the cdh_cleanup of the AWS Glue service."""
        self._clean_resource_policy()
        self._clean_databases()
        self._clean_cache_bucket()
        self._clean_crawlers()

    def _clean_cache_bucket(self) -> None:
        if self._cache_bucket_name:
            self.logger.info(f"Looking for cached dbs in {self._cache_bucket_name}")
            if has_prefix(self._cache_bucket_name, self._prefix) and self._should_clean(
                "database_cache", self._cache_bucket_name, self.logger
            ):
                bucket = self._s3_resource.Bucket(self._cache_bucket_name)
                bucket.object_versions.all().delete()
                bucket.objects.all().delete()
        else:
            self.logger.info("No cache bucket found!")

    def _clean_crawlers(self) -> None:
        self.logger.info(f"Looking for crawlers to reset in {self._region}...")
        paginator = self._client.get_paginator("get_crawlers")
        for page in paginator.paginate():
            for entry in page["Crawlers"]:
                crawler = entry["Name"]
                if has_prefix(crawler, self._prefix) and self._should_clean("crawler", crawler, self.logger):
                    self._reset_crawler_sources(crawler)

    def _clean_databases(self) -> None:
        self.logger.info(f"Looking for Glue databases and tables in {self._region}...")
        for database_name in self._list_all_databases():
            if has_prefix(database_name, self._prefix) and self._should_clean("database", database_name, self.logger):
                self._delete_database(database_name)

    @retry(num_attempts=3, wait_between_attempts=1, retryable_error_codes=["ConditionCheckFailureException"])
    def _clean_resource_policy(self) -> None:
        self.logger.info(f"Looking for protected Glue databases in the resource policy in region {self._region}...")
        if resource_policy := self._get_glue_resource_policy():
            arns_to_remove = {
                resource_arn
                for resource_arn in resource_policy.protected_resources
                if (name := self._get_database_name(resource_arn))
                and has_prefix(name, self._prefix)
                and self._should_clean("database-protection", name, self.logger)
            }
            if arns_to_remove:
                self._update_glue_resource_policy(
                    glue_resource_policy=resource_policy.remove_resource_protection(
                        principal=f"arn:{self._partition.value}:iam::{self._account_id}:root",
                        resources_to_remove=arns_to_remove,
                    ),
                )

    def _get_cache_bucket_name(self) -> Optional[str]:
        filtered_buckets = self._s3_resource.buckets.filter(
            Prefix=f"{self._prefix}cdh-core-glue-sync-cache-"  # type: ignore
        )
        for bucket in filtered_buckets:
            return bucket.name
        return None

    def _get_database_name(self, resource_arn: Arn) -> Optional[str]:
        expected_prefix = "database/"
        if resource_arn.identifier.startswith(expected_prefix):
            return resource_arn.identifier[len(expected_prefix) :]
        self.logger.warning(f"Protected resource {resource_arn} is not a database, cannot clean it up")
        return None

    def _list_all_databases(self) -> Generator[str, None, None]:
        paginator = self._client.get_paginator("get_databases")
        for page in paginator.paginate():
            yield from [entry["Name"] for entry in page["DatabaseList"]]

    def _update_glue_resource_policy(self, glue_resource_policy: GlueResourcePolicy) -> None:
        if glue_resource_policy:
            self._client.put_resource_policy(
                PolicyInJson=glue_resource_policy.to_boto(),
                PolicyHashCondition=glue_resource_policy.policy_hash,  # type: ignore
            )
        else:
            self._client.delete_resource_policy(PolicyHashCondition=glue_resource_policy.policy_hash)  # type: ignore

    def _get_glue_resource_policy(self) -> Optional[GlueResourcePolicy]:
        try:
            response = self._client.get_resource_policy()
        except self._client.exceptions.EntityNotFoundException:
            return None
        return GlueResourcePolicy.from_boto(response)

    def _delete_database(self, database_name: str) -> None:
        try:
            # If the resource policy is not cleaned fast enough, we want to retry to still clean up the database
            self._delete_database_with_retry(database_name=database_name)
        except ClientError as error:
            self.logger.warning(error)

    @retry(num_attempts=10, wait_between_attempts=0.5, retryable_error_codes=["AccessDeniedException"])
    def _delete_database_with_retry(self, database_name: str) -> None:
        # According to the docs, this will also delete tables "asynchronously in a timely manner".
        self._client.delete_database(Name=database_name)

    def _reset_crawler_sources(self, crawler_name: str) -> None:
        try:
            self._client.update_crawler(
                Name=crawler_name, Targets={"S3Targets": [{"Path": "reset-by-crawler", "Exclusions": ["*"]}]}
            )
        except ClientError as error:
            self.logger.warning(error)
