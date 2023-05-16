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
from dataclasses import dataclass
from enum import Enum
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.entities.arn import Arn
from cdh_core.iterables import unwrap_singleton

if TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
    from mypy_boto3_stepfunctions.type_defs import HistoryEventTypeDef
else:
    SFNClient = object
    HistoryEventTypeDef = Dict[str, Any]


LOG = getLogger(__name__)

# Taken from https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
MAX_ALLOWED_HISTORY_RESULTS = 1000


class StateMachineExecutionStatus(Enum):
    """The status of a state machine execution."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    ABORTED = "ABORTED"

    @property
    def has_current_state(self) -> bool:
        """Return True if the execution status is associated with an uncompleted state.

        This can be a state that is currently being executed or a state that failed during execution.
        StateMachineExecutionStatus.TIMED_OUT is omitted here, since it occurs after 1 year and the behaviour could
        not be tested.
        """
        return self in [
            StateMachineExecutionStatus.RUNNING,
            StateMachineExecutionStatus.FAILED,
            StateMachineExecutionStatus.ABORTED,
        ]

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        return self.value.title()  # pylint: disable=no-member


@dataclass(frozen=True)
class StateMachineExecution(DataClassJsonCDHMixin):
    """Contains all relevant information associated with a state machine execution."""

    name: str
    arn: Arn
    state_machine_arn: Arn
    status: StateMachineExecutionStatus


@dataclass(frozen=True)
class ExecutionInformation(DataClassJsonCDHMixin):
    """Contains the current state/progress of a state machine execution."""

    execution_status: StateMachineExecutionStatus
    current_state: Optional[str] = None
    last_state_output: Optional[Dict[str, Any]] = None


class SfnClient:
    """Abstracts the boto3 stepfunctions client."""

    def __init__(self, boto_sfn_client: SFNClient):
        self._client = boto_sfn_client

    @staticmethod
    def get_execution_arn(state_machine_arn: Arn, execution_name: str) -> Arn:
        """Retrieve the execution arn from the execution name and state machine arn."""
        execution_arn = f"{str(state_machine_arn).replace('stateMachine', 'execution')}:{execution_name}"
        return Arn(execution_arn)

    def get_execution_information(self, state_machine_arn: Arn, execution_name: str) -> ExecutionInformation:
        """Retrieve the execution information of a stepfunctions execution."""
        execution = self._get_execution(state_machine_arn=state_machine_arn, execution_name=execution_name)

        execution_history = self._get_execution_history(
            state_machine_arn=state_machine_arn, execution_name=execution_name
        )
        current_state = (
            self._get_current_state_from_history(execution_history=execution_history)
            if execution.status.has_current_state
            else None
        )
        last_state_output = self._get_last_state_output_from_history(execution_history=execution_history)

        return ExecutionInformation(
            execution_status=execution.status, current_state=current_state, last_state_output=last_state_output
        )

    def start_execution(self, state_machine_arn: Arn, execution_input: str, execution_name: str) -> Arn:
        """Start a new stepfunctions execution."""
        try:
            response = self._client.start_execution(
                stateMachineArn=str(state_machine_arn),
                name=execution_name,
                input=execution_input,
            )
            return Arn(response["executionArn"])
        except self._client.exceptions.ExecutionAlreadyExists as error:
            raise ExecutionAlreadyExists(state_machine_arn=state_machine_arn, execution_name=execution_name) from error

    def _get_execution(self, state_machine_arn: Arn, execution_name: str) -> StateMachineExecution:
        paginator = self._client.get_paginator("list_executions")
        for page in paginator.paginate(stateMachineArn=str(state_machine_arn)):
            for execution in page["executions"]:
                if execution["name"] == execution_name:
                    return StateMachineExecution(
                        name=execution_name,
                        arn=Arn(execution["executionArn"]),
                        state_machine_arn=state_machine_arn,
                        status=StateMachineExecutionStatus(execution["status"]),
                    )
        raise ExecutionDoesNotExist(state_machine_arn=state_machine_arn, execution_name=execution_name)

    def _get_execution_history(
        self,
        state_machine_arn: Arn,
        execution_name: str,
        max_results: int = MAX_ALLOWED_HISTORY_RESULTS,
        reverse_order: bool = True,
    ) -> List[HistoryEventTypeDef]:
        execution_arn = self.get_execution_arn(state_machine_arn=state_machine_arn, execution_name=execution_name)
        try:
            events = []
            paginator = self._client.get_paginator("get_execution_history")
            for page in paginator.paginate(
                executionArn=str(execution_arn), reverseOrder=reverse_order, PaginationConfig={"MaxItems": max_results}
            ):
                events.extend(page["events"])
            return events
        except self._client.exceptions.ExecutionDoesNotExist as error:
            raise ExecutionDoesNotExist(state_machine_arn=state_machine_arn, execution_name=execution_name) from error

    @staticmethod
    def _get_current_state_from_history(execution_history: List[HistoryEventTypeDef]) -> Optional[str]:
        for event in execution_history:
            if "StateEntered" in event["type"]:
                return event["stateEnteredEventDetails"]["name"]
        LOG.error("Could not retrieve the current state. Most probably the execution history is truncated.")
        return None

    @staticmethod
    def _get_last_state_output_from_history(execution_history: List[HistoryEventTypeDef]) -> Optional[Dict[str, Any]]:
        for event in execution_history:
            if "StateExited" in event["type"]:
                output: Union[Dict[str, Any], List[Dict[str, Any]]] = json.loads(
                    event["stateExitedEventDetails"]["output"]
                )
                if isinstance(output, List):
                    output = unwrap_singleton(output)
                return output
        return None


class ExecutionDoesNotExist(Exception):
    """Signals that the expected stepfunctions execution does not exist."""

    def __init__(self, state_machine_arn: Arn, execution_name: str) -> None:
        super().__init__(f"State machine {state_machine_arn!r} has no execution named {execution_name!r}")


class ExecutionAlreadyExists(Exception):
    """Signals that a stepfunctions execution with the same name already exists."""

    def __init__(self, state_machine_arn: Arn, execution_name: str) -> None:
        super().__init__(f"State machine {state_machine_arn!r} already has an execution named {execution_name!r}")
