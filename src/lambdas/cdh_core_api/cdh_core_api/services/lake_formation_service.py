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
from functools import lru_cache
from logging import getLogger

from cdh_core_api.config import Config
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.lakeformation_client import LakeFormationClient
from cdh_core.aws_clients.utils import FailedToDeleteResourcesStillAssociating
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)


class LakeFormationService:
    """Grants and revokes Lake Formation permissions."""

    def __init__(self, aws: AwsClientFactory, config: Config):
        self._aws = aws
        self._config = config

    def setup_lake_formation_governance(
        self, glue_resource: GenericGlueSyncResource, s3_resource: GenericS3Resource
    ) -> None:
        """Perform all necessary steps to administer a glue database and s3 bucket via lake formation."""
        lf_client = self._get_lake_formation_client(s3_resource.resource_account_id, s3_resource.region)
        lf_client.register_resource(
            resource_arn=s3_resource.arn,
            role_arn=self._config.get_lake_formation_registration_role_arn(
                s3_resource.resource_account_id, s3_resource.region
            ),
        )
        lf_client.revoke_iam_allowed_principals_permissions_for_database(glue_resource.glue_database)

    def setup_provider_access(self, glue_resource: GenericGlueSyncResource, s3_resource: GenericS3Resource) -> None:
        """Give the glue resource owner account write access to the glue database and s3 bucket."""
        lf_client = self._get_lake_formation_client(s3_resource.resource_account_id, s3_resource.region)
        lf_client.grant_write_access_for_s3_resource(principal=glue_resource.owner_account_id, s3_resource=s3_resource)
        lf_client.grant_write_access_for_database(
            principal=glue_resource.owner_account_id, database=glue_resource.glue_database
        )

    def teardown_lake_formation_governance(self, s3_resource: GenericS3Resource) -> None:
        """Perform all necessary steps to remove s3 bucket administration via lake formation."""
        lf_client = self._get_lake_formation_client(s3_resource.resource_account_id, s3_resource.region)
        lf_client.deregister_resource(s3_resource.arn)

    def teardown_provider_access(self, glue_resource: GenericGlueSyncResource, s3_resource: GenericS3Resource) -> None:
        """Remove the glue resource owner account's access to the registered data location of the s3 bucket."""
        lf_client = self._get_lake_formation_client(s3_resource.resource_account_id, s3_resource.region)
        lf_client.revoke_write_access_for_s3_resource(principal=glue_resource.owner_account_id, s3_resource=s3_resource)
        lf_client.revoke_write_access_for_database(
            principal=glue_resource.owner_account_id, database=glue_resource.glue_database
        )

    def grant_read_access(self, target_account_id: AccountId, source_database: GlueDatabase) -> None:
        """Grant permissions to the database for target account."""
        lf_client = self._get_lake_formation_client(
            resource_account_id=source_database.account_id, region=source_database.region
        )
        lf_client.grant_read_access_for_database(principal=target_account_id, database=source_database, grantable=True)

    def revoke_read_access(self, target_account_id: AccountId, source_database: GlueDatabase) -> None:
        """Revoke permissions to the database for the target account."""
        lf_client = self._get_lake_formation_client(
            resource_account_id=source_database.account_id, region=source_database.region
        )
        try:
            lf_client.revoke_read_access_for_database(
                principal=target_account_id, database=source_database, grantable=True
            )
        except FailedToDeleteResourcesStillAssociating as err:
            raise ConflictingReadAccessModificationInProgress(source_database.name) from err

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def _get_lake_formation_client(self, resource_account_id: AccountId, region: Region) -> LakeFormationClient:
        return self._aws.lake_formation_client(
            account_id=resource_account_id,
            account_purpose=AccountPurpose("resources"),
            region=region,
        )


class ConflictingReadAccessModificationInProgress(Exception):
    """Signals a read access modification failed due to a conflicting operation."""

    def __init__(self, database_name: str):
        self.database_name = database_name
        super().__init__(self, f"Conflicting read access modification in progress for database {database_name}")
