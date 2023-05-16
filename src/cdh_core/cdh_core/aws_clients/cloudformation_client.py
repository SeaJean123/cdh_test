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
from enum import Enum
from enum import unique
from logging import getLogger
from typing import Any
from typing import Dict
from typing import Optional
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from waiting import TimeoutExpired
from waiting import wait

if TYPE_CHECKING:
    from mypy_boto3_cloudformation import CloudFormationClient
else:
    CloudFormationClient = object


LOG = getLogger(__name__)


@unique
class _StackStatus(Enum):
    CREATE_COMPLETE = "CREATE_COMPLETE"
    CREATE_IN_PROGRESS = "CREATE_IN_PROGRESS"
    CREATE_FAILED = "CREATE_FAILED"
    DELETE_COMPLETE = "DELETE_COMPLETE"
    DELETE_FAILED = "DELETE_FAILED"
    DELETE_IN_PROGRESS = "DELETE_IN_PROGRESS"
    REVIEW_IN_PROGRESS = "REVIEW_IN_PROGRESS"
    ROLLBACK_COMPLETE = "ROLLBACK_COMPLETE"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"
    ROLLBACK_IN_PROGRESS = "ROLLBACK_IN_PROGRESS"
    UPDATE_COMPLETE = "UPDATE_COMPLETE"
    UPDATE_COMPLETE_CLEANUP_IN_PROGRESS = "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"
    UPDATE_FAILED = "UPDATE_FAILED"
    UPDATE_IN_PROGRESS = "UPDATE_IN_PROGRESS"
    UPDATE_ROLLBACK_COMPLETE = "UPDATE_ROLLBACK_COMPLETE"
    UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS = "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS"
    UPDATE_ROLLBACK_FAILED = "UPDATE_ROLLBACK_FAILED"
    UPDATE_ROLLBACK_IN_PROGRESS = "UPDATE_ROLLBACK_IN_PROGRESS"
    IMPORT_IN_PROGRESS = "IMPORT_IN_PROGRESS"
    IMPORT_COMPLETE = "IMPORT_COMPLETE"
    IMPORT_ROLLBACK_IN_PROGRESS = "IMPORT_ROLLBACK_IN_PROGRESS"
    IMPORT_ROLLBACK_FAILED = "IMPORT_ROLLBACK_FAILED"
    IMPORT_ROLLBACK_COMPLETE = "IMPORT_ROLLBACK_COMPLETE"

    def is_final(self) -> bool:
        return self in [
            _StackStatus.CREATE_COMPLETE,
            _StackStatus.CREATE_FAILED,
            _StackStatus.DELETE_COMPLETE,
            _StackStatus.DELETE_FAILED,
            _StackStatus.ROLLBACK_COMPLETE,
            _StackStatus.ROLLBACK_FAILED,
            _StackStatus.UPDATE_COMPLETE,
            _StackStatus.UPDATE_FAILED,
            _StackStatus.UPDATE_ROLLBACK_COMPLETE,
            _StackStatus.UPDATE_ROLLBACK_FAILED,
            _StackStatus.IMPORT_COMPLETE,
            _StackStatus.IMPORT_ROLLBACK_FAILED,
            _StackStatus.IMPORT_ROLLBACK_COMPLETE,
        ]


class CloudformationClient:
    """Abstracts the boto3 cloudformation client."""

    def __init__(self, boto3_cloudformation_client: CloudFormationClient):
        self._client = boto3_cloudformation_client

    def create_stack(
        self,
        stack_name: str,
        template: Dict[str, Any],
        parameters: Dict[str, Any],
    ) -> None:
        """Create a new stack from the given template."""
        summary = self._client.get_template_summary(TemplateBody=json.dumps(template))
        try:
            self._client.create_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(template),
                Capabilities=summary.get("Capabilities", []),
                Parameters=[
                    {
                        "ParameterKey": param_key,
                        "ParameterValue": param_value,
                    }
                    for param_key, param_value in parameters.items()
                ],
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "AlreadyExistsException":
                raise StackAlreadyExists(stack_name) from error
            raise StackCreationFailed(stack_name, str(error), cleanup_successful=None) from error
        try:
            self._wait_for_stack_status(
                stack_name=stack_name,
                expected_status=_StackStatus.CREATE_COMPLETE,
            )
        except StackStatusError as exception:
            # clean up by removing dangling stack if in state CREATE_FAILED
            clean = False
            try:
                if exception.actual_status == _StackStatus.CREATE_FAILED:
                    self.delete_stack(stack_name=stack_name)
                    clean = True
            finally:
                raise StackCreationFailed(
                    stack_name=stack_name, error=str(exception), cleanup_successful=clean
                ) from exception

    def update_stack(
        self,
        stack_name: str,
        template: Dict[str, Any],
        parameters: Dict[str, Any],
    ) -> None:
        """Update the existing stack with the given template."""
        summary = self._client.get_template_summary(TemplateBody=json.dumps(template))
        try:
            self._client.update_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(template),
                Capabilities=summary.get("Capabilities", []),
                Parameters=[
                    {
                        "ParameterKey": param_key,
                        "ParameterValue": param_value,
                    }
                    for param_key, param_value in parameters.items()
                ],
            )
        except ClientError as error:
            error_details = error.response["Error"]
            if (
                error_details["Code"] == "ValidationError"
                and error_details["Message"] == "No updates are to be performed."
            ):
                LOG.debug("Everything up to date already, nothing to do.")
                return
            raise
        self._wait_for_stack_status(
            stack_name=stack_name,
            expected_status=_StackStatus.UPDATE_COMPLETE,
        )

    def delete_stack(self, stack_name: str) -> None:
        """Delete the stack with the given name."""
        self._client.delete_stack(StackName=stack_name)
        try:
            self._wait_for_stack_status(stack_name=stack_name, expected_status=_StackStatus.DELETE_COMPLETE)
        except StackDoesNotExist:
            return

    def stack_exists(self, stack_name: str) -> bool:
        """Test if the stack name is used already."""
        try:
            self._get_stack_status(stack_name=stack_name)
            return True
        except StackDoesNotExist:
            return False

    def _wait_for_stack_status(self, stack_name: str, expected_status: _StackStatus) -> None:
        if not expected_status.is_final():
            raise ValueError(f"Cannot wait for non-final status {expected_status.value}")
        final_status = None
        try:
            final_status = wait(
                lambda: status  # pylint: disable=undefined-variable
                if (status := self._get_stack_status(stack_name)).is_final()
                else None,
                timeout_seconds=600,
                sleep_seconds=5,
            )
        except TimeoutExpired:
            LOG.warning(
                f"Timeout expired while waiting for stack {stack_name!r} to reach status {expected_status.value!r}"
            )
        final_status = final_status or self._get_stack_status(stack_name)
        if final_status is not expected_status:
            raise StackStatusError(stack_name=stack_name, current_status=final_status, expected_status=expected_status)

    def _get_stack_status(self, stack_name: str) -> _StackStatus:
        try:
            response = self._client.describe_stacks(StackName=stack_name)
        except ClientError as error:
            error_details = error.response["Error"]
            if (
                error_details["Code"] == "ValidationError"
                and error_details["Message"] == f"Stack with id {stack_name} does not exist"
            ):
                raise StackDoesNotExist(stack_name) from error
            raise
        return _StackStatus(response["Stacks"][0]["StackStatus"])


class StackCreationFailed(Exception):
    """Signals that a generic error occurred during the creation of the stack."""

    def __init__(self, stack_name: str, error: str, cleanup_successful: Optional[bool]):
        super().__init__(
            self,
            f"Failed to create Cloudformation stack {stack_name!r}: {error}. "
            f"{self._get_cleanup_message(cleanup_successful)}",
        )

    @classmethod
    def _get_cleanup_message(cls, cleanup_successful: Optional[bool]) -> str:
        if cleanup_successful is None:
            message = ""
        elif cleanup_successful:
            message = "Deleted dangling stack, no inconsistent state left behind"
        else:
            message = "Cleanup failed, may have left an inconsistent state"
        return message


class StackAlreadyExists(StackCreationFailed):
    """Signals that the stack has been created already."""

    def __init__(self, stack_name: str):
        super().__init__(stack_name=stack_name, error="Stack already exists", cleanup_successful=None)


class StackDoesNotExist(Exception):
    """Signals that the expected stack name does not exist."""

    def __init__(self, stack_name: str):
        super().__init__(self, f"Cloudformation stack {stack_name!r} does not exist")


class StackStatusError(Exception):
    """Signals that the stack status is not as expected."""

    def __init__(self, stack_name: str, current_status: _StackStatus, expected_status: _StackStatus):
        super().__init__(
            self,
            f"Cloudformation stack {stack_name!r} has status {current_status.value!r}, "
            f"expected {expected_status.value!r}.",
        )
        self.actual_status = current_status
