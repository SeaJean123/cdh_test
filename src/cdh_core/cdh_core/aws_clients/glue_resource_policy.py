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
from typing import Optional
from typing import Set
from typing import TYPE_CHECKING

from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.entities.arn import Arn

if TYPE_CHECKING:
    from mypy_boto3_glue.type_defs import GetResourcePolicyResponseTypeDef
else:
    GetResourcePolicyResponseTypeDef = Dict[str, Any]

PROTECT_RESOURCE_LINKS_SID = "ResourceLinkDeleteDenyManagedByCDH"


class GlueResourcePolicy:
    """Represents a GlueResourcePolicy in AWS managing access to Glue objects such as databases."""

    def __init__(self, document: PolicyDocument, policy_hash: Optional[str] = None) -> None:
        self.policy_hash = policy_hash
        self._document = document

    @classmethod
    def from_boto(cls, get_resource_policy_response: GetResourcePolicyResponseTypeDef) -> "GlueResourcePolicy":
        """Create a GlueResourcePolicy from a `get_resource_policy` call."""
        policy_document_raw = json.loads(get_resource_policy_response["PolicyInJson"])
        policy_document = PolicyDocument.create_glue_resource_policy(policy_document_raw["Statement"])
        return GlueResourcePolicy(policy_document, policy_hash=get_resource_policy_response["PolicyHash"])

    def to_boto(self) -> str:
        """Convert to a format accepted by boto3."""
        if self:
            return self._document.encode()
        raise ValueError("Glue resource policy empty")

    @property
    def protected_resources(self) -> Set[Arn]:
        """Gather the resources protected by the designated Resource Link Protection Statement."""
        try:
            statement = self._document.get_policy_statement_by_sid(PROTECT_RESOURCE_LINKS_SID)
        except KeyError:
            return set()
        return self._get_protected_resources(statement)

    @staticmethod
    def _get_protected_resources(statement: Dict[str, Any]) -> Set[Arn]:
        resource = statement["Resource"]
        if isinstance(resource, str):
            return {Arn(resource)}
        return {Arn(entry) for entry in resource}

    def __bool__(self) -> bool:
        """Return true iff the policy document has at least one statement."""
        return self._document.has_statements()

    def remove_resource_protection(self, principal: str, resources_to_remove: Set[Arn]) -> "GlueResourcePolicy":
        """Remove protection for the given resources."""
        if not resources_to_remove:
            return self
        try:
            statement = self._document.get_policy_statement_by_sid(PROTECT_RESOURCE_LINKS_SID)
        except KeyError:
            return self
        current_protected_resources = self._get_protected_resources(statement)
        if len(resources_to_remove & current_protected_resources) == 0:
            return self
        new_protected_resources = current_protected_resources - resources_to_remove
        if new_protected_resources:
            statement = self.create_resource_link_protect_policy_statement(
                principal=principal, resources=new_protected_resources
            )
            document = self._document.add_or_update_statement(statement)
        else:
            document = self._document.delete_statement_if_present(PROTECT_RESOURCE_LINKS_SID)
        return GlueResourcePolicy(document=document, policy_hash=self.policy_hash)

    def add_resource_protection(self, principal: str, resources_to_add: Set[Arn]) -> "GlueResourcePolicy":
        """Add protection for the given resources."""
        if not resources_to_add:
            return self
        try:
            statement = self._document.get_policy_statement_by_sid(PROTECT_RESOURCE_LINKS_SID)
        except KeyError:
            resources_to_protect = resources_to_add
        else:
            current_protected_resources = self._get_protected_resources(statement)
            if resources_to_add <= current_protected_resources:
                return self
            resources_to_protect = current_protected_resources | resources_to_add
        statement = self.create_resource_link_protect_policy_statement(
            principal=principal, resources=resources_to_protect
        )
        return GlueResourcePolicy(
            document=self._document.add_or_update_statement(statement),
            policy_hash=self.policy_hash,
        )

    @classmethod
    def create_resource_link_protect_policy_statement(cls, principal: str, resources: Set[Arn]) -> Dict[str, Any]:
        """Create a Glue resource policy statement to protect a database resource link."""
        return {
            "Sid": PROTECT_RESOURCE_LINKS_SID,
            "Effect": "Deny",
            "Action": ["glue:DeleteDatabase"],
            "Principal": {"AWS": [principal]},
            "Resource": sorted(str(res) for res in resources),
        }
