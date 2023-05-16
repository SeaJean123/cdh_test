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
from typing import Any
from typing import Dict
from typing import List
from typing import TYPE_CHECKING

from cdh_core.aws_clients.utils import repeat_continuation_call
from cdh_core.entities.arn import Arn

if TYPE_CHECKING:
    from mypy_boto3_events.client import EventBridgeClient
    from mypy_boto3_events.type_defs import TargetTypeDef
else:
    EventBridgeClient = object
    TargetTypeDef = Dict[str, str]


@dataclass(frozen=True)
class LambdaTarget:
    """LambdaTarget represents the target a events rule gets applied to."""

    target_id: str
    lambda_arn: Arn
    input: Dict[str, str]

    def to_dict(self) -> TargetTypeDef:
        """Return the target as dict."""
        return {"Id": self.target_id, "Arn": str(self.lambda_arn), "Input": json.dumps(self.input)}


class EventsClient:
    """Abstracts the boto3 event bridge client."""

    def __init__(self, boto_events_client: EventBridgeClient):
        self._client = boto_events_client

    @staticmethod
    def get_rule_name_from_arn(arn: Arn) -> str:
        """Return the rule name from an ARN."""
        rule_prefix = "rule/"
        if not arn.identifier.startswith(rule_prefix):
            raise ValueError(f"Invalid arn for a CloudWatch event rule: {arn}")
        return arn.identifier[len(rule_prefix) :]

    def create_or_update_periodic_rule(self, name: str, frequency_in_minutes: int) -> Arn:
        """Create or update a periodic rule."""
        response = self._client.put_rule(
            Name=name,
            ScheduleExpression=f"rate({frequency_in_minutes} minutes)",
            State="ENABLED",
        )
        return Arn(response["RuleArn"])

    def add_target(self, rule_arn: Arn, target: LambdaTarget) -> None:
        """Add another target to rule, based on the ARN."""
        try:
            self._client.put_targets(
                Rule=self.get_rule_name_from_arn(rule_arn),
                Targets=[target.to_dict()],
            )
        except self._client.exceptions.ResourceNotFoundException as error:
            raise RuleNotFound(self.get_rule_name_from_arn(rule_arn)) from error

    def delete_rule(self, rule_arn: Arn) -> None:
        """Delete a rule based on an ARN."""
        rule_name = self.get_rule_name_from_arn(rule_arn)
        targets = self.list_targets(rule_name=rule_name)
        if targets:
            self._client.remove_targets(Rule=rule_name, Ids=[target["Id"] for target in targets])
        self._client.delete_rule(Name=rule_name)

    def list_targets(self, rule_name: str) -> List[Dict[str, Any]]:
        """List targets of a rule."""
        try:
            return list(repeat_continuation_call(self._client.list_targets_by_rule, "Targets", Rule=rule_name))
        except self._client.exceptions.ResourceNotFoundException as error:
            raise RuleNotFound(rule_name) from error


class RuleNotFound(Exception):
    """Signals a rule cannot be found."""

    def __init__(self, rule_name: str):
        super().__init__(f"Rule {rule_name!r} not found")
