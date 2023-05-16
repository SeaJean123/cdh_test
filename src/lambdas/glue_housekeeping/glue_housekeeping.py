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
import json
import os
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import boto3

from cdh_core.aws_clients.glue_client import GlueClient
from cdh_core.aws_clients.glue_client import GlueTableNotFound
from cdh_core.aws_clients.sqs_client import SqsClient
from cdh_core.log.log_safe import log_safe
from cdh_core.log.logger import configure_logging

LOG = getLogger(__name__)


class TableVersionsCleaner:
    """Handles the incoming event for table version cleanup."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        queue_url: str,
        max_table_versions: int,
        prefix: Optional[str] = None,
        glue_client: Optional[GlueClient] = None,
        sqs_client: Optional[SqsClient] = None,
    ):
        self._queue_url = queue_url
        self._max_table_versions = max_table_versions
        self._prefix = prefix
        self._glue_client = glue_client or GlueClient(boto3.client("glue"))
        self._sqs_client = sqs_client or SqsClient(boto3.client("sqs"))

    def _process_scheduled_event(self) -> None:
        LOG.info("Handling scheduled event")
        databases = self._glue_client.get_all_database_names()
        if not self._prefix:
            messages = [{"database": database} for database in databases]
        else:
            messages = [{"database": database} for database in databases if database.startswith(self._prefix)]
        self._sqs_client.send_messages(queue_url=self._queue_url, messages=messages)

    def _cleanup_database(self, database_name: str) -> None:
        tables = self._glue_client.get_tables(database=database_name)
        for table in tables:
            try:
                version_ids = self._glue_client.get_table_version_ids(database=database_name, table=table.name)
                if len(version_ids) > self._max_table_versions:
                    version_ids.sort(reverse=True, key=int)
                    self._glue_client.delete_table_versions(
                        database=database_name, table=table.name, version_ids=version_ids[self._max_table_versions :]
                    )
            except GlueTableNotFound:
                LOG.warning(f"Table {table} was not found. It may have been deleted in the last few minutes.")

    def _process_sqs_message(self, records: List[Dict[str, Any]]) -> None:
        for record in records:
            database_name = json.loads(record.get("body", "{}")).get("database")
            if not database_name:
                LOG.error("No database could be parsed from this record: %s", record)
            else:
                LOG.info("Handling database %s", database_name)
                self._cleanup_database(database_name)

    def handle_event(self, event: Dict[str, Any]) -> None:
        """Handle the incoming event for the lambda function."""
        LOG.debug("Handling event %s", event)
        if event.get("detail-type") == "Scheduled Event":
            self._process_scheduled_event()
        elif "Records" in event:
            self._process_sqs_message(event["Records"])
        else:
            raise ValueError("Input event not understood.")


@log_safe()
def handler(event: Dict[str, Any], context: Any) -> None:  # pylint: disable=unused-argument
    """Create a cleaner object from the environment variables and call its handler."""
    configure_logging(__name__)
    table_versions_cleaner = TableVersionsCleaner(
        queue_url=os.environ["QUEUE_URL"],
        max_table_versions=int(os.environ["MAX_TABLE_VERSIONS"]),
        prefix=os.environ["RESOURCE_NAME_PREFIX"],
    )
    table_versions_cleaner.handle_event(event)
