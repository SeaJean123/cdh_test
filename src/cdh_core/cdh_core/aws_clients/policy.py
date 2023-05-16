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
from __future__ import annotations

import json
from collections import defaultdict
from copy import deepcopy
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.enums.aws_clients import PolicyDocumentType
from cdh_core.iterables import chunks_of_bounded_weight

LOG = getLogger(__name__)

_Statement = Dict[str, Any]


class PolicyDocument:
    """AWS IAM policy document."""

    REQUIRED_KEYS_IN_RESOURCE_POLICY = {"Sid", "Effect", "Principal", "Action", "Resource"}
    OPTIONAL_KEYS_IN_RESOURCE_POLICY = {"Condition"}
    PRINCIPAL_TYPES_IN_RESOURCE_POLICY = {"AWS", "Federated"}

    version: str
    statements: List[_Statement]

    def __init__(
        self,
        version: str,
        statements: List[_Statement],
        policy_document_type: Optional[PolicyDocumentType] = None,
    ):
        self.version = version
        self.statements = statements
        self._type = policy_document_type
        if self._type is not None:
            self._ensure_size_does_not_exceed(self._type.get_max_policy_length())
        if self._type is PolicyDocumentType.SNS:
            self.validate_resource_policy_statements()

    @property
    def _document(self) -> Dict[str, Any]:
        return {"Version": self.version, "Statement": self.statements}

    @classmethod
    def create_managed_policy(cls, statements: List[_Statement]) -> PolicyDocument:
        """Create a managed policy."""
        return cls._create_policy_document(statements, PolicyDocumentType.MANAGED)

    @classmethod
    def create_bucket_policy(cls, statements: List[_Statement]) -> PolicyDocument:
        """Create a bucket policy."""
        return cls._create_policy_document(statements, PolicyDocumentType.BUCKET)

    @classmethod
    def create_key_policy(cls, statements: List[_Statement]) -> PolicyDocument:
        """Create a KMS key policy."""
        return cls._create_policy_document(statements, PolicyDocumentType.KMS)

    @classmethod
    def create_sns_policy(cls, statements: List[_Statement]) -> PolicyDocument:
        """Create a SNS policy."""
        return cls._create_policy_document(statements, PolicyDocumentType.SNS)

    @classmethod
    def create_glue_resource_policy(cls, statements: List[_Statement]) -> PolicyDocument:
        """Create a Glue data catalog resource policy."""
        return cls._create_policy_document(statements, PolicyDocumentType.GLUE)

    @classmethod
    def _create_policy_document(
        cls, statements: List[_Statement], policy_document_type: Optional[PolicyDocumentType] = None
    ) -> PolicyDocument:
        return PolicyDocument(version="2012-10-17", statements=statements, policy_document_type=policy_document_type)

    def __eq__(self, other: object) -> bool:
        """Compare the contests of policies."""
        if isinstance(other, PolicyDocument):
            return self.version == other.version and self.statements == other.statements
        return NotImplemented

    def __repr__(self) -> str:
        """Return compacted representation of policy."""
        return f"PolicyDocument({self.encode()})"

    def as_dict(self) -> Dict[str, Any]:
        """Convert policy to dict."""
        return self._document

    def encode(self) -> str:
        """Encode the given policy to a JSON string as compact as possible."""
        return PolicyDocument.encode_policy_document(self._document)

    def has_statements(self) -> bool:
        """Check if the policy document has any statements."""
        return len(self.statements) > 0

    def has_statement_with_sid(self, sid: str) -> bool:
        """Test statement matching sid exists."""
        return any(statement.get("Sid") == sid for statement in self.statements)

    def get_policy_statement_by_sid(self, sid: str) -> _Statement:
        """Return statement matching sid."""
        for statement in self.statements:
            if statement.get("Sid") == sid:
                return statement
        raise KeyError(f"Statement with Sid {sid!r} not found")

    def add_or_update_statement(self, statement: _Statement) -> PolicyDocument:
        """Add or update a statement."""
        statements = list(
            filter(lambda item: "Sid" not in item or item["Sid"] != statement.get("Sid"), self.statements)
        )
        statements.append(statement)
        return self._create_policy_document(deepcopy(statements), policy_document_type=self._type)

    def delete_statement_if_present(self, sid: str) -> PolicyDocument:
        """Delete statement matching sid."""
        statements = list(filter(lambda item: "Sid" not in item or item["Sid"] != sid, self.statements))
        return self._create_policy_document(statements, policy_document_type=self._type)

    def _ensure_size_does_not_exceed(self, limit_bytes: int) -> None:
        policy_size = len(self.encode())
        if policy_size > limit_bytes:
            raise PolicySizeExceeded()
        # Even though the size check passes here, putting the policy via boto sometimes causes a policy size limit
        # exceeded error
        if policy_size > 0.95 * limit_bytes:
            LOG.warning(f"Close to policy size limit: {policy_size}/{limit_bytes}")

    def validate_resource_policy_statements(self) -> None:
        """Validate resource policy statements."""
        for statement in self.statements:
            keys = set(statement.keys())
            if not self.REQUIRED_KEYS_IN_RESOURCE_POLICY <= keys:
                raise InvalidPolicyStatement("missing required key", statement)
            if not keys <= self.REQUIRED_KEYS_IN_RESOURCE_POLICY | self.OPTIONAL_KEYS_IN_RESOURCE_POLICY:
                raise InvalidPolicyStatement("unsupported keys", statement)
            if statement["Effect"] not in ["Allow", "Deny"]:
                raise InvalidPolicyStatement("invalid effect", statement)
            if not statement["Principal"].keys() <= self.PRINCIPAL_TYPES_IN_RESOURCE_POLICY:
                raise InvalidPolicyStatement("invalid principal", statement)

    @classmethod
    def split_statements_to_managed_policies(cls, statements: List[_Statement]) -> List[PolicyDocument]:
        """Split a list of statements into well-sized policy documents."""

        def _get_char_count_available_for_managed_policy_statements() -> int:
            max_length = PolicyDocumentType.MANAGED.get_max_policy_length()
            empty_policy = PolicyDocument.create_managed_policy([])
            return max_length - len(empty_policy.encode())

        chunks = chunks_of_bounded_weight(
            items=statements,
            get_weight=lambda statement: len(PolicyDocument.encode_policy_document(statement))
            + 1,  # +1 for the adjacent comma
            max_weight=_get_char_count_available_for_managed_policy_statements(),
        )
        policies = [PolicyDocument.create_managed_policy(chunk) for chunk in chunks]
        if (
            len(policies)
            > ConfigFileLoader.get_config().aws_service.iam.configured_limits.max_managed_policies_per_role
        ):
            raise PolicyCountExceeded()
        return policies

    @classmethod
    def encode_policy_document(cls, document: Dict[str, Any]) -> str:
        """
        Encode the given policy to a JSON string as compact as possible.

        If you want to check the size of your policy against AWS limits, you must use this encoding.
        """
        return json.dumps(document, separators=(",", ":"))

    def get_principals_with_action(self, action: str) -> Dict[str, List[str]]:
        """Return a list of actions for each principal listed in the policy."""
        principals: Dict[str, List[str]] = defaultdict(list)
        for statement in self.statements:
            if action in self._ensure_non_empty_list(statement["Action"]):
                for principal_type, principals_of_type in statement["Principal"].items():
                    principals[principal_type].extend(self._ensure_non_empty_list(principals_of_type))
        return principals

    @staticmethod
    def _ensure_non_empty_list(item_or_list: Union[str, List[str]]) -> List[str]:
        return [item_or_list] if isinstance(item_or_list, str) else item_or_list


class PolicySizeExceeded(Exception):
    """Signals that policy size is to big."""


class PolicyCountExceeded(Exception):
    """Signals the number of statements do not fit in maximum allowed number of managed policies."""


class InvalidPolicyStatement(Exception):
    """Signals validation of statements failed."""

    def __init__(self, reason: str, statement: Dict[str, Any]):
        super().__init__(f"Invalid statement in resource policy ({reason}): {statement}")
