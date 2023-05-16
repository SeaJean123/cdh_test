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
from unittest.mock import Mock

import pytest
from cdh_core_api.config_test import build_config
from cdh_core_api.services.lake_formation_service import ConflictingReadAccessModificationInProgress
from cdh_core_api.services.lake_formation_service import LakeFormationService

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.lakeformation_client import LakeFormationClient
from cdh_core.aws_clients.utils import FailedToDeleteResourcesStillAssociating
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.arn import Arn
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core_dev_tools.testing.builder import Builder


class TestLakeFormationService:
    def setup_method(self) -> None:
        self.prefix = Builder.build_random_string()
        self.config = build_config(prefix=self.prefix)
        self.aws = Mock(AwsClientFactory)
        self.lf_client = Mock(LakeFormationClient)
        self.aws.lake_formation_client.return_value = self.lf_client
        self.lake_formation_service = LakeFormationService(aws=self.aws, config=self.config)

        self.target_account = build_account()
        self.glue_resource = build_glue_sync_resource()
        self.s3_resource = build_s3_resource()

    def test_setup_lake_formation_governance(self) -> None:
        self.lake_formation_service.setup_lake_formation_governance(self.glue_resource, self.s3_resource)

        self.lf_client.register_resource.assert_called_once_with(
            resource_arn=self.s3_resource.arn,
            role_arn=Arn(
                f"arn:{self.s3_resource.region.partition.value}:iam::{self.s3_resource.resource_account_id}:role/"
                f"{self.prefix}cdh-lakeformation-registration"
            ),
        )
        self.lf_client.revoke_iam_allowed_principals_permissions_for_database.assert_called_once_with(
            self.glue_resource.glue_database
        )

    def test_setup_provider_access(self) -> None:
        self.lake_formation_service.setup_provider_access(self.glue_resource, self.s3_resource)

        self.lf_client.grant_write_access_for_s3_resource.assert_called_once_with(
            principal=self.glue_resource.owner_account_id, s3_resource=self.s3_resource
        )
        self.lf_client.grant_write_access_for_database.assert_called_once_with(
            principal=self.glue_resource.owner_account_id, database=self.glue_resource.glue_database
        )

    def test_teardown_lake_formation_governance(self) -> None:
        self.lake_formation_service.teardown_lake_formation_governance(self.s3_resource)

        self.lf_client.deregister_resource.assert_called_once_with(self.s3_resource.arn)

    def test_teardown_provider_access(self) -> None:
        self.lake_formation_service.teardown_provider_access(self.glue_resource, self.s3_resource)

        self.lf_client.revoke_write_access_for_s3_resource.assert_called_once_with(
            principal=self.glue_resource.owner_account_id, s3_resource=self.s3_resource
        )
        self.lf_client.revoke_write_access_for_database.assert_called_once_with(
            principal=self.glue_resource.owner_account_id, database=self.glue_resource.glue_database
        )

    def test_grant_access_to_target_account(self) -> None:
        self.lake_formation_service.grant_read_access(self.target_account.id, self.glue_resource.glue_database)

        self.lf_client.grant_read_access_for_database.assert_called_once_with(
            principal=self.target_account.id, database=self.glue_resource.glue_database, grantable=True
        )

    def test_revoke_access_to_target_account(self) -> None:
        self.lake_formation_service.revoke_read_access(self.target_account.id, self.glue_resource.glue_database)

        self.lf_client.revoke_read_access_for_database.assert_called_once_with(
            principal=self.target_account.id, database=self.glue_resource.glue_database, grantable=True
        )

    def test_revoke_access_to_target_account_failed_to_delete(self) -> None:
        self.lf_client.revoke_read_access_for_database.side_effect = FailedToDeleteResourcesStillAssociating()
        with pytest.raises(ConflictingReadAccessModificationInProgress):
            self.lake_formation_service.revoke_read_access(self.target_account.id, self.glue_resource.glue_database)
