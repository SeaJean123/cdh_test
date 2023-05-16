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
from typing import Iterator

import boto3

from cdh_applications.cleanup.cleanup_utils import has_prefix
from cdh_applications.cleanup.generic_cleaner import GenericCleaner


class DynamoCleaner(GenericCleaner):
    """Cleaner class for the functional tests for the AWS DynamoDB service."""

    def __init__(  # pylint: disable=too-many-arguments,super-init-not-called
        self,
        region: str,
        prefix: str,
        clean_filter: Callable[[str, str, Any], bool],
        credentials: Dict[str, Any],
        log: Logger,
    ) -> None:
        self._region = region
        self._prefix = prefix
        self._should_clean = clean_filter
        self._client = boto3.client("dynamodb", region_name=region, **credentials)
        self.logger = log

    def clean(self) -> None:
        """Start the cdh_cleanup of the AWS DynamoDB service."""
        self.logger.info(f"Looking for Dynamo Tables to truncate in {self._region}...")

        for table_name in self._iterate_over_tables():
            if has_prefix(table_name, self._prefix) and self._should_clean(
                "contents of table", table_name, self.logger
            ):
                self._clean_table(table_name)

    def _iterate_over_tables(self) -> Iterator[str]:
        paginator = self._client.get_paginator("list_tables")
        page_iterator = paginator.paginate()

        for page in page_iterator:
            yield from page["TableNames"]

    def _clean_table(self, table_name: str) -> None:
        cleaned_items = 0
        paginator = self._client.get_paginator("scan")

        response_iterator = paginator.paginate(TableName=table_name, Select="ALL_ATTRIBUTES")

        key_schema = self._client.describe_table(TableName=table_name)["Table"]["KeySchema"]
        primary_key_attributes = [item["AttributeName"] for item in key_schema]

        for page in response_iterator:
            for item in page["Items"]:
                item_identifier = {key: value for key, value in item.items() if key in primary_key_attributes}
                self._client.delete_item(TableName=table_name, Key=item_identifier)
                cleaned_items += 1
        self.logger.info(
            f"Finished cleaning table {table_name} in {self._region}. {cleaned_items} were found and deleted."
        )
