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
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from logging import getLogger
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from botocore.exceptions import WaiterError

from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws_clients import PolicyDocumentType
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_iam import IAMClient
    from mypy_boto3_iam.type_defs import WaiterConfigTypeDef, PolicyVersionTypeDef
else:
    IAMClient = object
    WaiterConfigTypeDef = Dict[str, int]
    PolicyVersionTypeDef = Dict[str, Any]

LOG = getLogger(__name__)

# https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_managed-versioning.html
MAX_VERSIONS_PER_POLICY = 5


@dataclass(frozen=True)
class Role:
    """Represents an AWS IAM role."""

    name: str
    arn: Arn
    description: str = ""


@dataclass(frozen=True)
class AttachedPolicy:
    """Represents an AWS IAM policy attached to a role."""

    name: str
    arn: Arn
    document: PolicyDocument
    version: str


class ManageRolePolicyOperationType(Enum):
    """The types of operation possible for ManageRolePolicyOperation."""

    CREATE = "create"
    UPDATE = "update"


@dataclass(frozen=True)
class ManageRolePolicyOperation:
    """Represents an operation on an AttachedPolicy."""

    arn: Arn
    type: ManageRolePolicyOperationType
    previous_version: str = ""


class IamClient:
    """Abstracts the boto3 IAM client."""

    default_timedelta = timedelta(hours=1)

    def __init__(self, boto3_iam_client: IAMClient, account_id: AccountId, partition: Partition):
        self._client = boto3_iam_client
        self.account_id = account_id
        self.partition = partition
        self._waiter_config: WaiterConfigTypeDef = {"Delay": 1, "MaxAttempts": 60}

    def get_role(self, name: str) -> Role:
        """Get a role by its name."""
        try:
            result = self._client.get_role(RoleName=name)
        except self._client.exceptions.NoSuchEntityException as err:
            raise RoleNotFound(name) from err

        return Role(
            name=result["Role"]["RoleName"],
            arn=Arn(result["Role"]["Arn"]),
            description=result["Role"].get("Description", ""),
        )

    def get_attached_policy_arn(self, role_name: str, policy_name: str) -> Optional[Arn]:
        """Get the ARN with the given policy name attached to the given role."""
        result = self._client.list_attached_role_policies(RoleName=role_name)
        for policy in result["AttachedPolicies"]:
            if policy.get("PolicyName") == policy_name:
                return Arn(policy["PolicyArn"])
        return None

    def build_policy_arn(self, policy_name: str) -> Arn:
        """Build an ARN for the given policy name."""
        return Arn(f"arn:{self.partition.value}:iam::{self.account_id}:policy/{policy_name}")

    def get_default_policy_version_id(self, policy_arn: Arn) -> str:
        """Get the id of the default version of the IAM policy with the given ARN."""
        get_policy_result = self._client.get_policy(PolicyArn=str(policy_arn))
        return get_policy_result["Policy"]["DefaultVersionId"]

    def get_attached_policies(self, role_name: str) -> List[AttachedPolicy]:
        """Get a list of all the IAM policies attached to the given role."""
        result = self._client.list_attached_role_policies(RoleName=role_name)
        return [self._get_attached_policy(Arn(policy["PolicyArn"])) for policy in result["AttachedPolicies"]]

    def _get_attached_policy(self, policy_arn: Arn) -> AttachedPolicy:
        get_policy_result = self._client.get_policy(PolicyArn=str(policy_arn))
        version = get_policy_result["Policy"]["DefaultVersionId"]
        get_version_result_document = self._client.get_policy_version(PolicyArn=str(policy_arn), VersionId=version)[
            "PolicyVersion"
        ]["Document"]

        return AttachedPolicy(
            name=get_policy_result["Policy"]["PolicyName"],
            arn=policy_arn,
            document=PolicyDocument(
                version=get_version_result_document["Version"],  # type: ignore
                statements=get_version_result_document["Statement"],  # type: ignore
                policy_document_type=PolicyDocumentType.MANAGED,
            ),
            version=version,
        )

    def get_attached_policy(self, role_name: str, policy_name: str) -> AttachedPolicy:
        """Get the policy with the given name attached to the given role."""
        if policy_arn := self.get_attached_policy_arn(role_name, policy_name):
            return self._get_attached_policy(policy_arn)
        raise PolicyNotFound(role_name, policy_name)

    def create_role(
        self,
        role_name: str,
        assume_role_policy: PolicyDocument,
        *,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        max_session_duration: timedelta = default_timedelta,
    ) -> Role:
        """Create a role with the given name and assume policy.

        An empty description has the same effect as removing the Description parameter.
        """
        with self.create_role_transaction(
            role_name=role_name,
            assume_role_policy=assume_role_policy,
            description=description,
            tags=tags,
            max_session_duration=max_session_duration,
        ) as role:
            return role

    @contextmanager
    def create_role_transaction(
        self,
        role_name: str,
        assume_role_policy: PolicyDocument,
        *,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        max_session_duration: timedelta = default_timedelta,
    ) -> Iterator[Role]:
        """Create a role with the given name and assume policy, in a transactional manner.

        If an exception is raised, the change is rolled back.
        An empty description has the same effect as removing the Description parameter.
        """
        LOG.info(f"Creating role {role_name} in {self.account_id} in partition {self.partition}")
        try:
            response = self._client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=assume_role_policy.encode(),
                Description=description,
                Tags=[{"Key": key, "Value": value} for key, value in (tags or {}).items()],
                MaxSessionDuration=int(max_session_duration.total_seconds()),
            )
        except self._client.exceptions.EntityAlreadyExistsException as err:
            raise RoleAlreadyExists(role_name) from err
        except ClientError:
            LOG.error(f"Creating role {role_name} in account {self.account_id} in {self.partition} failed")
            raise

        self._wait_for_role(role_name)

        try:
            yield Role(
                name=response["Role"]["RoleName"],
                arn=Arn(response["Role"]["Arn"]),
                description=response["Role"].get("Description", ""),
            )
        except Exception:
            self._rollback_create_role(role_name)
            raise

    def _rollback_create_role(self, role_name: str) -> None:
        LOG.warning(f"Rolling back creation of role {role_name} in {self.account_id} in partition {self.partition}")
        try:
            self._client.delete_role(RoleName=role_name)
        except ClientError:
            LOG.exception(
                f"Could not roll back creation of role {role_name} in {self.account_id} in partition {self.partition}"
            )

    def _wait_for_role(self, role_name: str) -> None:
        waiter = self._client.get_waiter("role_exists")
        try:
            waiter.wait(RoleName=role_name, WaiterConfig=self._waiter_config)
        except WaiterError:
            LOG.error(f"Could not create role {role_name} in {self.account_id} in partition {self.partition} in time")
            raise

    def manage_role_policy(
        self,
        role_name: str,
        policy_name: str,
        document: PolicyDocument,
        description: str = "",
    ) -> ManageRolePolicyOperation:
        """Create a new policy or update an existion one, and attach it to the given role.

        Policy names must be unique to prevent unwanted overwriting.
        An empty description has the same effect as removing the Description parameter.
        """
        manage_role_policy_operation = self._create_or_update_policy(
            role_name=role_name, policy_name=policy_name, document=document, description=description
        )
        self._attach_policy_to_role(
            role_name=role_name, policy_name=policy_name, manage_role_policy_operation=manage_role_policy_operation
        )
        return manage_role_policy_operation

    def _create_or_update_policy(
        self,
        role_name: str,
        policy_name: str,
        document: PolicyDocument,
        description: str,
    ) -> ManageRolePolicyOperation:
        try:
            policy_arn = Arn(
                self._client.create_policy(
                    PolicyName=policy_name,
                    PolicyDocument=document.encode(),
                    Description=description,
                )["Policy"]["Arn"]
            )
            previous_policy_version = ""
            LOG.info(f"Created policy {policy_arn}")
            policy_operation = ManageRolePolicyOperationType.CREATE
            self._wait_for_policy(policy_arn, role_name)
        except self._client.exceptions.EntityAlreadyExistsException:
            policy_operation = ManageRolePolicyOperationType.UPDATE
            policy_arn = self.build_policy_arn(policy_name)
            previous_policy_version = self.get_default_policy_version_id(policy_arn)
            try:
                self.update_policy(policy_arn, document)
            except ClientError as err:
                raise ManageRolePolicyException(
                    role_name=role_name,
                    policy_name=policy_name,
                    manage_role_policy=ManageRolePolicyOperation(
                        arn=policy_arn, type=policy_operation, previous_version=previous_policy_version
                    ),
                ) from err
        return ManageRolePolicyOperation(
            arn=policy_arn, type=policy_operation, previous_version=previous_policy_version
        )

    def _attach_policy_to_role(
        self, role_name: str, policy_name: str, manage_role_policy_operation: ManageRolePolicyOperation
    ) -> None:
        try:
            self._client.attach_role_policy(RoleName=role_name, PolicyArn=str(manage_role_policy_operation.arn))
            LOG.info(f"Attached policy {manage_role_policy_operation.arn} to {role_name}")
        except ClientError as err:
            if manage_role_policy_operation.type == ManageRolePolicyOperationType.CREATE:
                self._rollback_create_policy(manage_role_policy_operation.arn, role_name)
            # rollback for "update" is managed in ManagePolicyAction.rollback()
            raise ManageRolePolicyException(
                role_name=role_name,
                policy_name=policy_name,
                manage_role_policy=manage_role_policy_operation,
            ) from err

    def _rollback_create_policy(self, policy_arn: Arn, role_name: str) -> None:
        LOG.warning(f"Rolling back creation of policy {policy_arn} for {role_name}")
        try:
            self._client.delete_policy(PolicyArn=str(policy_arn))
        except ClientError:
            LOG.exception(f"Could not roll back creation of policy {policy_arn} for {role_name}")

    def _wait_for_policy(self, policy_arn: Arn, role_name: str) -> None:
        waiter = self._client.get_waiter("policy_exists")
        try:
            waiter.wait(PolicyArn=str(policy_arn), WaiterConfig=self._waiter_config)
        except WaiterError:
            LOG.error(f"Could not create policy {policy_arn} for {role_name} in time")
            raise

    def update_policy(self, policy_arn: Arn, document: PolicyDocument) -> None:
        """Create a new policy version, deleting the oldest one if the maximum number of versions is reached."""
        LOG.info(f"Updating policy {policy_arn}")
        self._delete_oldest_policy_version_if_limit_reached(policy_arn)
        self._client.create_policy_version(
            PolicyArn=str(policy_arn),
            PolicyDocument=document.encode(),
            SetAsDefault=True,
        )

    def _list_policy_versions(self, policy_arn: Arn) -> List[PolicyVersionTypeDef]:
        result: List[PolicyVersionTypeDef] = []
        paginator = self._client.get_paginator("list_policy_versions")
        response_iterator = paginator.paginate(PolicyArn=str(policy_arn))
        for page in response_iterator:
            result += page["Versions"]
        return result

    @staticmethod
    def _get_non_default_policy_versions_sorted(
        policy_versions: List[PolicyVersionTypeDef],
    ) -> List[PolicyVersionTypeDef]:
        non_default_versions = [version for version in policy_versions if not version["IsDefaultVersion"]]
        non_default_versions.sort(key=lambda version: version["CreateDate"])
        return non_default_versions

    def _delete_oldest_policy_version_if_limit_reached(self, policy_arn: Arn) -> None:
        policy_versions = self._list_policy_versions(policy_arn)
        if len(policy_versions) >= MAX_VERSIONS_PER_POLICY:
            non_default_versions = self._get_non_default_policy_versions_sorted(policy_versions)
            version_to_delete = non_default_versions[0]
            LOG.info(f"Deleting oldest version {version_to_delete['VersionId']} of policy {policy_arn}")
            self._client.delete_policy_version(PolicyArn=str(policy_arn), VersionId=version_to_delete["VersionId"])

    def set_policy_version(self, policy_arn: Arn, version: str) -> None:
        """Set the version with the given ID as default for the given policy."""
        self._client.set_default_policy_version(PolicyArn=str(policy_arn), VersionId=version)

    def detach_and_delete_policy(self, role_name: str, policy_arn: Arn) -> None:
        """Detach the given policy from the given role, then delete the policy."""
        LOG.info(f"Detach policy {policy_arn} from {role_name}")
        self._client.detach_role_policy(RoleName=role_name, PolicyArn=str(policy_arn))
        try:
            for policy_version in self._get_non_default_policy_versions_sorted(self._list_policy_versions(policy_arn)):
                LOG.info(f"Delete version {policy_version['VersionId']} of policy {policy_arn}")
                self._client.delete_policy_version(PolicyArn=str(policy_arn), VersionId=policy_version["VersionId"])
            LOG.info(f"Delete policy {policy_arn}")
            self._client.delete_policy(PolicyArn=str(policy_arn))
        except ClientError:
            LOG.exception(f"Could not delete policy {policy_arn} after detaching it")

    def _delete_role(self, role_name: str) -> None:
        for policy_name in self._client.list_role_policies(RoleName=role_name)["PolicyNames"]:
            self._client.delete_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
            )
        self._client.delete_role(RoleName=role_name)

    def delete_policies_and_role(self, role_name: str) -> None:
        """Delete the given role and all policies attached to it."""
        try:
            policies = self.get_attached_policies(role_name)
            for policy in policies:
                self.detach_and_delete_policy(role_name, policy.arn)
            self._delete_role(role_name)
        except self._client.exceptions.NoSuchEntityException as err:
            raise RoleNotFound(role_name) from err
        except ClientError:
            LOG.error(f"Deleting role {role_name} in account {self.account_id} in {self.partition} failed")
            raise


class RoleNotFound(Exception):
    """Signals the requested role does not exist."""

    def __init__(self, role_name: str):
        super().__init__(f"Role {role_name!r} not found")


class RoleAlreadyExists(Exception):
    """Signals a role with the same name already exists."""

    def __init__(self, role_name: str):
        super().__init__(f"Cannot create role {role_name!r} because a role with the same name already exists")


class PolicyNotFound(Exception):
    """Signals the requested policy does not exist or is not attached to the requested role."""

    def __init__(self, role_name: str, policy_name: str):
        super().__init__(f"Role {role_name} has no attached policy {policy_name!r}")


class ManageRolePolicyException(Exception):
    """Signals the requested ManageRolePolicyOperation cannot not be performed."""

    def __init__(self, role_name: str, policy_name: str, manage_role_policy: ManageRolePolicyOperation):
        super().__init__(f"Error creating/updating policy {policy_name} of role {role_name}: {manage_role_policy}")
        self.manage_role_policy = manage_role_policy
