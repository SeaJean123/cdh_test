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
from unittest.mock import patch

import pytest
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.services.resource_link import GlueEncryptionFailed
from cdh_core_api.services.resource_link import ResourceLink

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.glue_client import GlueClient
from cdh_core.aws_clients.glue_client import GlueDatabaseNotFound
from cdh_core.aws_clients.glue_client import GlueEncryptionException
from cdh_core.entities.glue_database_test import build_glue_database
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class TestResourceLink:
    def setup_method(self) -> None:
        self.accounts_table = Mock(AccountsTable)
        self.boto_glue_client = Mock()
        self.region = build_region()
        self.boto_glue_client.meta.region_name = self.region
        self.glue_client = Mock()
        self.aws = Mock(AwsClientFactory)
        self.aws.create_client.return_value = self.boto_glue_client
        self.metadata_role_assumer = Mock()
        self.resource_link: ResourceLink = ResourceLink(self.aws, self.accounts_table, self.metadata_role_assumer)
        self.target_account_id = build_account_id()
        self.glue_sync = build_glue_sync_resource(region=self.region)
        self.glue_database = build_glue_database(name=self.glue_sync.database_name, region=self.region)

    @patch.object(GlueClient, "is_database_a_resource_link")
    @pytest.mark.parametrize("sync_type", SyncType)
    def test_successful_has_glue_db_sync_type_glue_sync_if_db_exists(
        self, mocked_is_database_a_resource_link: Mock, sync_type: SyncType
    ) -> None:
        mocked_is_database_a_resource_link.return_value = sync_type in [SyncType.resource_link, SyncType.lake_formation]

        response = self.resource_link.has_glue_db_sync_type_glue_sync_if_db_exists(self.glue_database)

        expected_response = sync_type is SyncType.glue_sync
        assert response == expected_response

    @patch.object(GlueClient, "is_database_a_resource_link")
    def test_successful_get_glue_db_sync_type_if_db_does_not_exist(
        self, mocked_is_database_a_resource_link: Mock
    ) -> None:
        mocked_is_database_a_resource_link.side_effect = GlueDatabaseNotFound(Builder.build_random_string())

        response = self.resource_link.has_glue_db_sync_type_glue_sync_if_db_exists(self.glue_database)

        assert response is None

    @patch.object(ResourceLink, "_get_glue_client")
    def test_successful_create_resource_link(self, mocked_get_glue_client: Mock) -> None:
        mocked_get_glue_client.return_value = self.glue_client

        self.resource_link.create_resource_link(self.target_account_id, self.glue_database)
        mocked_get_glue_client.assert_called_once_with(self.target_account_id, self.glue_sync.region)
        self.glue_client.create_resource_link.assert_called_once_with(
            database_name=self.glue_database.name, source_account_id=self.glue_database.account_id
        )
        self.glue_client.add_deletion_protection.assert_called_once_with(
            self.glue_database.name, self.target_account_id
        )

    @patch.object(ResourceLink, "_get_glue_client")
    def test_successful_glue_db_exists(self, mocked_get_glue_client: Mock) -> None:
        db_exists = Builder.get_random_bool()
        mocked_get_glue_client.return_value = self.glue_client
        self.glue_client.database_exists.return_value = db_exists

        assert self.resource_link.glue_db_exists(self.glue_database) == db_exists

        mocked_get_glue_client.assert_called_once_with(self.glue_database.account_id, self.glue_sync.region)
        self.glue_client.database_exists.assert_called_once_with(self.glue_database.name)

    @patch.object(ResourceLink, "_get_glue_client")
    def test_successful_delete_resource_link(self, mocked_get_glue_client: Mock) -> None:
        mocked_get_glue_client.return_value = self.glue_client

        self.resource_link.delete_resource_link(self.target_account_id, self.glue_database)

        mocked_get_glue_client.assert_called_once_with(self.target_account_id, self.glue_sync.region)
        self.glue_client.delete_protected_database.assert_called_once_with(
            self.glue_database.name, self.target_account_id
        )

    @patch.object(GlueClient, "create_resource_link")
    def test_create_resource_link_fails_with_glue_encryption_exception(self, mocked_create_resource_link: Mock) -> None:
        mocked_create_resource_link.side_effect = GlueEncryptionException(Mock())

        with pytest.raises(GlueEncryptionFailed):
            self.resource_link.create_resource_link(self.target_account_id, self.glue_database)
        self.glue_client.add_deletion_protection.assert_not_called()
