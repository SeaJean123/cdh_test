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
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import TYPE_CHECKING

from cdh_core.aws_clients.utils import FailedToDeleteResourcesStillAssociating
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn import build_arn_string
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_ram import RAMClient as BotoRAMClient
    from mypy_boto3_ram.type_defs import ResourceShareAssociationTypeDef
else:
    BotoRAMClient = object
    ResourceShareAssociationTypeDef = Dict[str, Any]


LOG = getLogger(__name__)

GLUE_WRITE_PERMISSION_RESOURCES = [
    "permission/AWSRAMPermissionGlueDatabaseReadWrite",
    "permission/AWSRAMPermissionGlueDatabaseReadWriteForTable",
    "permission/AWSRAMPermissionGlueDatabaseReadWriteForCatalog",
]


class RamClient:
    """Abstracts the boto3 RAM client."""

    def __init__(self, boto3_ram_client: BotoRAMClient):
        self._client = boto3_ram_client

    def create_glue_resource_share_with_write_permissions(
        self,
        database: GlueDatabase,
        target_account_id: AccountId,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """Give read-write permissions for the provided glue resource to a target account."""
        try:
            self._get_resource_share(database.name)
        except ResourceShareNotFound:
            pass
        else:
            raise ConflictingResourceShare(database.name)
        try:
            self._client.create_resource_share(
                name=database.name,
                resourceArns=[str(arn) for arn in self._get_glue_resource_arns(database)],
                principals=[target_account_id],
                allowExternalPrincipals=True,
                permissionArns=[str(arn) for arn in self._get_glue_write_permission_arns(database)],
                tags=[{"key": key, "value": value} for key, value in tags.items()] if tags else [],
            )
        except self._client.exceptions.OperationNotPermittedException as error:
            raise GlueDatabaseInWrongAccount(database.name, database.account_id) from error
        except self._client.exceptions.MalformedArnException as error:
            raise GlueDatabaseInWrongRegion(database.name, database.region) from error

    def set_glue_write_permissions(
        self,
        database: GlueDatabase,
        target_account_id: AccountId,
    ) -> None:
        """Re-set the read-write permissions for the provided glue resource to a target account.

        This requires an existing resource share for the given glue resource.
        """
        resource_share = self._get_resource_share(database.name)

        self._client.associate_resource_share(
            resourceShareArn=str(resource_share),
            resourceArns=[str(arn) for arn in self._get_glue_resource_arns(database)],
            principals=[target_account_id],
        )

    def revoke_glue_share_if_necessary(self, database: GlueDatabase) -> None:
        """Remove the resource share for the target glue resource."""
        try:
            arn_to_delete = self._get_resource_share(database.name)
        except ResourceShareNotFound:
            LOG.info(f"Resource share {database.name} not present, nothing to delete.")
            return
        try:
            response = self._client.delete_resource_share(resourceShareArn=str(arn_to_delete))
        except self._client.exceptions.InvalidStateTransitionException as error:
            raise FailedToDeleteResourceShareResourcesStillAssociating(database.name) from error
        if not response.get("returnValue", False):
            raise FailedToDeleteResourceShare(database.name, database.account_id)

    def is_ready_to_write(self, name: str) -> bool:
        """Indicate whether the provided glue resource is ready for write access by an external account."""
        return self._is_everything_associated(self._get_resource_share(name), expected_number_of_associations=4)

    def is_share_modification_in_progress(self, name: str) -> bool:
        """Indicate whether a share modification process for the provided glue resource is in progress."""
        return self._is_modification_in_progress(self._get_resource_share(name))

    def _get_resource_share(self, name: str) -> Arn:
        share = [
            active_share
            for active_share in self._client.get_resource_shares(resourceOwner="SELF", name=name)["resourceShares"]
            if active_share["status"] not in {"DELETING", "DELETED"}
        ]
        try:
            arn = Arn(next(iter(share))["resourceShareArn"])
        except StopIteration as error:
            raise ResourceShareNotFound(name) from error
        return arn

    def _is_everything_associated(
        self, resource_share_arn: Arn, expected_number_of_associations: Optional[int] = None
    ) -> bool:
        associations = self._get_resource_share_associations(resource_share_arn)
        if expected_number_of_associations is not None and expected_number_of_associations != len(associations):
            return False
        return all(resource_share_association["status"] == "ASSOCIATED" for resource_share_association in associations)

    def _is_modification_in_progress(self, resource_share_arn: Arn) -> bool:
        associations = self._get_resource_share_associations(resource_share_arn)
        return any(
            resource_share_association["status"] in ["ASSOCIATING", "DISASSOCIATING"]
            for resource_share_association in associations
        )

    def _get_resource_share_associations(self, resource_share_arn: Arn) -> List[ResourceShareAssociationTypeDef]:
        result: List[ResourceShareAssociationTypeDef] = []
        paginator = self._client.get_paginator("get_resource_share_associations")
        association_types: List[Literal["PRINCIPAL", "RESOURCE"]] = ["PRINCIPAL", "RESOURCE"]
        for association_type in association_types:
            response_iterator = paginator.paginate(
                associationType=association_type, resourceShareArns=[str(resource_share_arn)]
            )
            for page in response_iterator:
                result += page["resourceShareAssociations"]

        if failed_associations := [association for association in result if association["status"] == "FAILED"]:
            LOG.warning(
                f"The following resource share associations for {resource_share_arn} are in status 'FAILED': "
                f"{failed_associations}"
            )

        return result

    @staticmethod
    def _get_glue_resource_arns(database: GlueDatabase) -> List[Arn]:
        return [database.arn, database.tables_arn, database.catalog_arn]

    @staticmethod
    def _get_glue_write_permission_arns(database: GlueDatabase) -> List[Arn]:
        return [
            Arn(
                build_arn_string(
                    service="ram",
                    region=None,
                    account=AccountId("aws"),
                    resource=resource,
                    partition=database.region.partition,
                )
            )
            for resource in GLUE_WRITE_PERMISSION_RESOURCES
        ]


class ResourceShareNotFound(Exception):
    """Signals the requested resource share does not exist."""

    def __init__(self, name: str):
        super().__init__(f"Resource share {name} was not found")


class ConflictingResourceShare(Exception):
    """Signals there already is a resource share with the given name."""

    def __init__(self, name: str):
        super().__init__(f"A resource shares with {name=} already exists")


class GlueDatabaseInWrongAccount(Exception):
    """Signals the glue database passed is not in the same account as the RAM client."""

    def __init__(self, database_name: str, owning_account: AccountId):
        super().__init__(
            f"Cannot share database {database_name}, because it resides in another account: {owning_account}"
        )


class GlueDatabaseInWrongRegion(Exception):
    """Signals the glue database passed is not in the same region as the RAM client."""

    def __init__(self, database_name: str, database_region: Region):
        super().__init__(
            f"Cannot share database {database_name}, because it resides in another region: {database_region}"
        )


class FailedToDeleteResourceShare(Exception):
    """Signals the resource share could not be deleted."""

    def __init__(self, name: str, account: AccountId):
        super().__init__(f"Failed to delete resource share {name} in account {account}. Please do it manually")


class FailedToDeleteResourceShareResourcesStillAssociating(FailedToDeleteResourcesStillAssociating):
    """Signals the resource share could not be deleted."""

    def __init__(self, name: str):
        super().__init__(
            f"Could not delete resource share {name} because resources were still associating."
            "Please try again in a couple of minutes"
        )
