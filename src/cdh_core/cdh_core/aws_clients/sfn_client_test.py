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
from typing import Any
from typing import Dict
from typing import List
from unittest.mock import call
from unittest.mock import patch

import boto3
import pytest

from cdh_core.aws_clients.sfn_client import ExecutionAlreadyExists
from cdh_core.aws_clients.sfn_client import ExecutionDoesNotExist
from cdh_core.aws_clients.sfn_client import ExecutionInformation
from cdh_core.aws_clients.sfn_client import SfnClient
from cdh_core.aws_clients.sfn_client import StateMachineExecutionStatus
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.builder import Builder


OUTPUT_JSON = '{"foo": "bar"}'

RUNNING_EXECUTION_HISTORY_RESPONSE = {
    "events": [
        {
            "type": "LambdaFunctionStarted",
        },
        {
            "type": "LambdaFunctionScheduled",
        },
        {
            "type": "TaskStateEntered",
            "stateEnteredEventDetails": {"name": "RunningState"},
        },
        {
            "type": "TaskStateExited",
            "stateExitedEventDetails": {"name": "StateBeforeRunningState", "output": OUTPUT_JSON},
        },
    ],
}

FAILED_EXECUTION_HISTORY_RESPONSE = {
    "events": [
        {
            "type": "ExecutionFailed",
            "executionFailedEventDetails": {
                "error": "HttpStatusCodeNotInExpectedCodes",
                "cause": '{"errorMessage": "some-error-message"}',
            },
        },
        {
            "type": "LambdaFunctionFailed",
            "lambdaFunctionFailedEventDetails": {
                "error": "HttpStatusCodeNotInExpectedCodes",
                "cause": '{"errorMessage": "some-error-message"}',
            },
        },
        {
            "type": "LambdaFunctionStarted",
        },
        {
            "type": "LambdaFunctionScheduled",
        },
        {
            "type": "TaskStateEntered",
            "stateEnteredEventDetails": {"name": "StateThatFails"},
        },
        {
            "type": "TaskStateExited",
            "stateExitedEventDetails": {"name": "StateBeforeStateThatFails", "output": OUTPUT_JSON},
        },
    ],
}

ABORTED_EXECUTION_HISTORY_RESPONSE = {
    "events": [
        {"type": "ExecutionAborted"},
        {
            "type": "LambdaFunctionStarted",
        },
        {
            "type": "LambdaFunctionScheduled",
        },
        {
            "type": "TaskStateEntered",
            "stateEnteredEventDetails": {"name": "StateThatIsAborted"},
        },
        {
            "type": "TaskStateExited",
            "stateExitedEventDetails": {"name": "StateBeforeStateThatIsAborted", "output": OUTPUT_JSON},
        },
    ],
}

SUCCEEDED_EXECUTION_HISTORY_RESPONSE = {
    "events": [
        {"type": "ExecutionSucceeded"},
        {"type": "TaskStateExited", "stateExitedEventDetails": {"name": "FinalState", "output": OUTPUT_JSON}},
    ],
}

INSUFFICIENT_EXECUTION_HISTORY_RESPONSE: Dict[str, List[Any]] = {"events": []}

PARALLEL_EXECUTION_HISTORY_RESPONSE = {
    "events": [
        {"type": "ExecutionSucceeded"},
        {
            "type": "TaskStateExited",
            "stateExitedEventDetails": {"name": "FinalState", "output": json.dumps([json.loads(OUTPUT_JSON)])},
        },
    ],
}

PARALLEL_EXECUTION_MULTIPLE_STATES_HISTORY_RESPONSE = {
    "events": [
        {"type": "ExecutionSucceeded"},
        {
            "type": "TaskStateExited",
            "stateExitedEventDetails": {
                "name": "FinalState",
                "output": json.dumps([json.loads(OUTPUT_JSON), json.loads(OUTPUT_JSON)]),
            },
        },
    ],
}


class TestSfnClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_stepfunctions: Any) -> None:  # pylint: disable=unused-argument
        self.boto_client = boto3.client("stepfunctions", region_name=build_region().value)
        self.client = SfnClient(self.boto_client)

        self.state_machine_name = Builder.build_random_string()
        create_state_machine_response = self.boto_client.create_state_machine(
            name=self.state_machine_name,
            definition=Builder.build_random_string(),
            roleArn=str(build_role_arn(partition=build_partition())),
        )
        self.state_machine_arn = Arn(create_state_machine_response["stateMachineArn"])

        self.execution_name = Builder.build_random_string()
        start_execution_response = self.boto_client.start_execution(
            stateMachineArn=str(self.state_machine_arn), name=self.execution_name, input="{}"
        )
        self.execution_arn = Arn(start_execution_response["executionArn"])
        self.executions = self.boto_client.list_executions(stateMachineArn=str(self.state_machine_arn))["executions"]

    def test_get_execution_arn(self) -> None:
        state_machine_name = Builder.build_random_string()
        execution_name = Builder.build_random_string()
        state_machine_arn = build_arn(service="states", resource=f"stateMachine:{state_machine_name}")

        execution_arn = SfnClient.get_execution_arn(state_machine_arn=state_machine_arn, execution_name=execution_name)

        expected_execution_arn = build_arn(
            service="states",
            resource=f"execution:{state_machine_name}:{execution_name}",
            account_id=state_machine_arn.account_id,
            region=Region(state_machine_arn.region),
            partition=state_machine_arn.partition,
        )
        assert execution_arn == expected_execution_arn

    def test_get_non_existent_execution_state(self) -> None:
        with pytest.raises(ExecutionDoesNotExist):
            self.client.get_execution_information(
                state_machine_arn=self.state_machine_arn,
                execution_name=Builder.build_random_string(),
            )

    def test_get_execution_pagination(self) -> None:
        with patch.object(self.boto_client, "list_executions") as mocked_list_executions:
            mocked_list_executions.side_effect = [{"executions": [], "nextToken": 42}, {"executions": self.executions}]
            with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
                mocked_get_execution_history.side_effect = [
                    {"events": [], "nextToken": 24},
                    RUNNING_EXECUTION_HISTORY_RESPONSE,
                ]

                response_state = self.client.get_execution_information(
                    state_machine_arn=self.state_machine_arn,
                    execution_name=self.execution_name,
                )

                assert response_state.execution_status is StateMachineExecutionStatus.RUNNING
                mocked_list_executions.assert_has_calls(
                    [
                        call(stateMachineArn=str(self.state_machine_arn)),
                        call(stateMachineArn=str(self.state_machine_arn), nextToken=42),
                    ]
                )
                mocked_get_execution_history.has_calls(
                    [
                        call(executionArn=str(self.execution_arn)),
                        call(executionArn=str(self.execution_arn), nextToken=24),
                    ]
                )

    def test_get_execution_information_running(self) -> None:
        with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
            mocked_get_execution_history.return_value = RUNNING_EXECUTION_HISTORY_RESPONSE

            response_state = self.client.get_execution_information(
                state_machine_arn=self.state_machine_arn,
                execution_name=self.execution_name,
            )

            assert response_state == ExecutionInformation(
                execution_status=StateMachineExecutionStatus.RUNNING,
                current_state="RunningState",
                last_state_output=json.loads(OUTPUT_JSON),
            )
            mocked_get_execution_history.assert_called_once_with(
                executionArn=str(self.execution_arn), reverseOrder=True
            )

    def test_get_execution_information_aborted(self) -> None:
        self.boto_client.stop_execution(executionArn=str(self.execution_arn))
        with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
            mocked_get_execution_history.return_value = ABORTED_EXECUTION_HISTORY_RESPONSE

            response_state = self.client.get_execution_information(
                state_machine_arn=self.state_machine_arn,
                execution_name=self.execution_name,
            )

            assert response_state == ExecutionInformation(
                execution_status=StateMachineExecutionStatus.ABORTED,
                current_state="StateThatIsAborted",
                last_state_output=json.loads(OUTPUT_JSON),
            )

    def test_get_execution_information_failed(self) -> None:
        with patch.object(self.boto_client, "list_executions") as mocked_list_executions:
            mocked_list_executions.return_value = {"executions": [{**self.executions[0], "status": "FAILED"}]}
            with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
                mocked_get_execution_history.return_value = FAILED_EXECUTION_HISTORY_RESPONSE

                response_state = self.client.get_execution_information(
                    state_machine_arn=self.state_machine_arn,
                    execution_name=self.execution_name,
                )

                assert response_state == ExecutionInformation(
                    execution_status=StateMachineExecutionStatus.FAILED,
                    current_state="StateThatFails",
                    last_state_output=json.loads(OUTPUT_JSON),
                )

    def test_get_execution_information_succeeded(self) -> None:
        with patch.object(self.boto_client, "list_executions") as mocked_list_executions:
            mocked_list_executions.return_value = {"executions": [{**self.executions[0], "status": "SUCCEEDED"}]}
            with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
                mocked_get_execution_history.return_value = SUCCEEDED_EXECUTION_HISTORY_RESPONSE

                response_state = self.client.get_execution_information(
                    state_machine_arn=self.state_machine_arn,
                    execution_name=self.execution_name,
                )

                assert response_state == ExecutionInformation(
                    execution_status=StateMachineExecutionStatus.SUCCEEDED, last_state_output=json.loads(OUTPUT_JSON)
                )

    def test_get_execution_information_empty_events(self) -> None:
        with patch.object(self.boto_client, "list_executions") as mocked_list_executions:
            mocked_list_executions.return_value = {"executions": [{**self.executions[0], "status": "RUNNING"}]}
            with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
                mocked_get_execution_history.return_value = INSUFFICIENT_EXECUTION_HISTORY_RESPONSE

                response_state = self.client.get_execution_information(
                    state_machine_arn=self.state_machine_arn,
                    execution_name=self.execution_name,
                )

                assert response_state == ExecutionInformation(
                    execution_status=StateMachineExecutionStatus.RUNNING,
                    current_state=None,
                    last_state_output=None,
                )

    def test_get_execution_information_parallel_state_single_output(self) -> None:
        with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
            mocked_get_execution_history.return_value = PARALLEL_EXECUTION_HISTORY_RESPONSE

            response_state = self.client.get_execution_information(
                state_machine_arn=self.state_machine_arn,
                execution_name=self.execution_name,
            )

            assert response_state == ExecutionInformation(
                execution_status=StateMachineExecutionStatus.RUNNING,
                last_state_output=json.loads(OUTPUT_JSON),
            )
            mocked_get_execution_history.assert_called_once_with(
                executionArn=str(self.execution_arn), reverseOrder=True
            )

    def test_get_execution_information_parallel_state_multiple_outputs(self) -> None:
        with patch.object(self.boto_client, "get_execution_history") as mocked_get_execution_history:
            mocked_get_execution_history.return_value = PARALLEL_EXECUTION_MULTIPLE_STATES_HISTORY_RESPONSE

            with pytest.raises(ValueError):
                self.client.get_execution_information(
                    state_machine_arn=self.state_machine_arn,
                    execution_name=self.execution_name,
                )

    def test_start_execution(self) -> None:
        other_execution_name = Builder.build_random_string()
        other_execution_arn = self.client.start_execution(
            state_machine_arn=self.state_machine_arn, execution_input="{}", execution_name=other_execution_name
        )

        executions = self.boto_client.list_executions(stateMachineArn=str(self.state_machine_arn))["executions"]
        assert len(executions) == 2
        assert {execution["name"] for execution in executions} == {self.execution_name, other_execution_name}
        assert {execution["executionArn"] for execution in executions} == {
            str(self.execution_arn),
            str(other_execution_arn),
        }

    def test_start_execution_already_exists(self) -> None:
        other_input = {"foo": Builder.build_random_string()}

        with pytest.raises(ExecutionAlreadyExists):
            self.client.start_execution(
                state_machine_arn=self.state_machine_arn,
                execution_input=str(other_input),
                execution_name=self.execution_name,
            )
