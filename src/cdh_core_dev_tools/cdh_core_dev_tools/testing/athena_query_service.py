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
from typing import Any
from typing import Dict
from typing import Optional

import boto3
from waiting import wait


class AthenaQueryService:
    """Class to query Athena."""

    query_execution_id: Optional[str] = None

    def __init__(self, athena_workgroup: str, region: str, credentials: Dict[str, Any]):
        self.athena_workgroup = athena_workgroup
        self.client = boto3.client("athena", region_name=region, **credentials)

    def run_athena_query(
        self, database_name: str, table_name: str, timeout_seconds: int = 300
    ) -> Optional[Dict[str, Any]]:
        """Run an Athena query and return its result."""
        return wait(  # type: ignore
            lambda: self._get_query_results(database_name=database_name, table_name=table_name),
            timeout_seconds=timeout_seconds,
            sleep_seconds=5,
            waiting_for="Athena query result",
        )

    def _get_query_results(self, database_name: str, table_name: str) -> Optional[Dict[str, Any]]:
        if not self.query_execution_id:
            self._start_query_execution(database_name=database_name, table_name=table_name)
        state = self._get_query_execution_state()
        if state == "SUCCEEDED":
            return self.client.get_query_results(QueryExecutionId=self.query_execution_id, MaxResults=2)  # type: ignore
        if state in ["FAILED", "CANCELLED"]:
            self._start_query_execution(database_name=database_name, table_name=table_name)
        return None

    def _start_query_execution(self, database_name: str, table_name: str) -> None:
        self.query_execution_id = self.client.start_query_execution(
            QueryString=f'SELECT * FROM "{database_name}"."{table_name}" limit 10;',  # noqa: B028
            QueryExecutionContext={"Database": database_name},
            WorkGroup=self.athena_workgroup,
        )["QueryExecutionId"]

    def _get_query_execution_state(self) -> Optional[str]:
        # Possible states (https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatus.html):
        # QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
        if not self.query_execution_id:
            return None

        return self.client.get_query_execution(QueryExecutionId=self.query_execution_id)["QueryExecution"]["Status"][
            "State"
        ]
