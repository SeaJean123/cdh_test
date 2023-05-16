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
import logging
from typing import Dict
from typing import List
from unittest.mock import Mock

import pytest
from _pytest.logging import LogCaptureFixture
from glue_housekeeping.glue_housekeeping import TableVersionsCleaner

from cdh_core.aws_clients.glue_client import GlueTable
from cdh_core.aws_clients.glue_client import GlueTableNotFound
from cdh_core_dev_tools.testing.builder import Builder

MAX_TABLE_VERSIONS = 5


class TestTableVersionsCleaner:
    def setup_method(self) -> None:
        self.queue_url = Builder.build_random_string()
        self.glue_client = Mock()
        self.sqs_client = Mock()

        self.table_versions_cleaner = TableVersionsCleaner(
            queue_url=self.queue_url,
            max_table_versions=MAX_TABLE_VERSIONS,
            glue_client=self.glue_client,
            sqs_client=self.sqs_client,
        )

        self.database_name = Builder.build_random_string()
        self.table_name = "table"
        self.table = self._build_glue_table(self.table_name)

    def _build_sqs_event(self, database_names: List[str]) -> Dict[str, List[Dict[str, str]]]:
        return {"Records": [{"body": json.dumps({"database": database_name})} for database_name in database_names]}

    def _build_glue_table(self, table_name: str) -> GlueTable:
        return GlueTable(
            name=table_name, database_name=Builder.build_random_string(), location=Builder.build_random_string()
        )

    def test_handle_scheduled_event(self) -> None:
        databases = ["database", "another_database"]
        self.glue_client.get_all_database_names.return_value = databases
        expected_messages = [{"database": "database"}, {"database": "another_database"}]

        self.table_versions_cleaner.handle_event({"detail-type": "Scheduled Event"})

        self.sqs_client.send_messages.assert_called_once_with(queue_url=self.queue_url, messages=expected_messages)

    def test_handle_scheduled_event_prefixed(self) -> None:
        prefix = Builder.build_random_string()
        table_versions_cleaner = TableVersionsCleaner(
            queue_url=self.queue_url,
            max_table_versions=MAX_TABLE_VERSIONS,
            prefix=prefix,
            glue_client=self.glue_client,
            sqs_client=self.sqs_client,
        )
        databases = [f"{prefix}database", "database"]
        self.glue_client.get_all_database_names.return_value = databases
        expected_messages = [{"database": f"{prefix}database"}]

        table_versions_cleaner.handle_event({"detail-type": "Scheduled Event"})

        self.sqs_client.send_messages.assert_called_once_with(queue_url=self.queue_url, messages=expected_messages)

    def test_handle_database_event_calls_get_tables(self) -> None:
        records = self._build_sqs_event(database_names=[self.database_name])
        self.glue_client.get_tables.return_value = []

        self.table_versions_cleaner.handle_event(records)

        self.glue_client.get_tables.assert_called_once_with(database=self.database_name)

    def test_handle_database_event_calls_get_table_versions(self) -> None:
        records = self._build_sqs_event(database_names=[self.database_name])
        self.glue_client.get_tables.return_value = [self.table]
        self.glue_client.get_table_version_ids.return_value = []

        self.table_versions_cleaner.handle_event(records)

        self.glue_client.get_table_version_ids.assert_called_once_with(
            database=self.database_name, table=self.table_name
        )

    def test_handle_database_event_deletes(self) -> None:
        version_ids = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        version_ids_to_delete = ["4", "3", "2", "1", "0"]
        records = self._build_sqs_event(database_names=[self.database_name])
        self.glue_client.get_tables.return_value = [self.table]
        self.glue_client.get_table_version_ids.return_value = version_ids

        self.table_versions_cleaner.handle_event(records)

        self.glue_client.delete_table_versions.assert_called_once_with(
            database=self.database_name, table=self.table_name, version_ids=version_ids_to_delete
        )

    def test_handle_database_event_does_not_delete(self) -> None:
        version_ids = ["128466", "96854", "857", "58921"]
        records = self._build_sqs_event(database_names=[self.database_name])
        self.glue_client.get_tables.return_value = [self.table]
        self.glue_client.get_table_version_ids.return_value = version_ids

        self.table_versions_cleaner.handle_event(records)

        self.glue_client.delete_table_versions.assert_not_called()

    def test_handle_database_event_catches_table_not_found(self, caplog: LogCaptureFixture) -> None:
        records = self._build_sqs_event(database_names=[self.database_name])
        self.glue_client.get_tables.return_value = [self.table]
        self.glue_client.get_table_version_ids.side_effect = GlueTableNotFound(self.table_name)

        with caplog.at_level(logging.WARNING):
            self.table_versions_cleaner.handle_event(records)
            assert f"Table {self.table} was not found." in caplog.text

    def test_unknown_event_raises(self) -> None:
        with pytest.raises(ValueError):
            self.table_versions_cleaner.handle_event({})

    def test_bad_record_no_body(self, caplog: LogCaptureFixture) -> None:
        with caplog.at_level(logging.ERROR):
            self.table_versions_cleaner.handle_event({"Records": [{}]})
            assert "No database could be parsed from this record" in caplog.text
            self.glue_client.get_tables.assert_not_called()

    def test_bad_record_no_database_name(self, caplog: LogCaptureFixture) -> None:
        with caplog.at_level(logging.ERROR):
            self.table_versions_cleaner.handle_event({"Records": [{"body": "{}"}]})
            assert "No database could be parsed from this record" in caplog.text
            self.glue_client.get_tables.assert_not_called()
