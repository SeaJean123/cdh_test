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
from dataclasses import replace
from typing import Optional
from unittest.mock import Mock

import pytest
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.config_test import build_config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.utils import fetch_dataset
from cdh_core_api.services.utils import fetch_resource
from cdh_core_api.services.utils import find_permission
from cdh_core_api.services.utils import get_user

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.entities.resource_test import build_resource
from cdh_core.enums.hubs import Hub
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import NotFoundError
from cdh_core_dev_tools.testing.builder import Builder


class TestGetUser:
    def setup_method(self) -> None:
        self.user = Builder.build_random_string()
        self.authorization_api = Mock(AuthorizationApi)

    def test_not_using_auth_api(self) -> None:
        user = get_user(
            requester_identity=build_requester_identity(user=self.user),
            config=build_config(use_authorization=False),
            authorization_api=self.authorization_api,
        )

        assert user == self.user
        self.authorization_api.get_user_id.assert_not_called()

    def test_auth_api_returns_none(self) -> None:
        self.authorization_api.get_user_id.return_value = None

        user = get_user(
            requester_identity=build_requester_identity(user=self.user),
            config=build_config(use_authorization=True),
            authorization_api=self.authorization_api,
        )

        assert user == self.user

    def test_user_from_auth_api(self) -> None:
        self.authorization_api.get_user_id.return_value = self.user

        user = get_user(
            requester_identity=build_requester_identity(user=Builder.build_random_string()),
            config=build_config(use_authorization=True),
            authorization_api=self.authorization_api,
        )

        assert user == self.user


class TestFetchDataset:
    def setup_method(self) -> None:
        self.visible_data_loader = Mock()
        self.dataset = build_dataset()
        self.visible_data_loader.get_dataset.return_value = self.dataset

    def test_successful(self) -> None:
        assert self.dataset == fetch_dataset(
            hub=self.dataset.hub, dataset_id=self.dataset.id, visible_data_loader=self.visible_data_loader
        )

    def test_dataset_does_not_exist(self) -> None:
        self.visible_data_loader.get_dataset.side_effect = DatasetNotFound(self.dataset.id)
        with pytest.raises(NotFoundError):
            fetch_dataset(
                hub=self.dataset.hub, dataset_id=self.dataset.id, visible_data_loader=self.visible_data_loader
            )

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_wrong_hub(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        other_hub = Builder.get_random_element(Hub, exclude=[self.dataset.hub])
        with pytest.raises(NotFoundError):
            fetch_dataset(hub=other_hub, dataset_id=self.dataset.id, visible_data_loader=self.visible_data_loader)


class TestFetchResource:
    def setup_method(self) -> None:
        self.visible_data_loader = Mock()
        self.resource = build_resource()
        self.visible_data_loader.get_resource.return_value = self.resource

    def test_successful(self) -> None:
        assert self.resource == fetch_resource(
            hub=self.resource.hub,
            dataset_id=self.resource.dataset_id,
            stage=self.resource.stage,
            region=self.resource.region,
            resource_type=self.resource.type,
            visible_data_loader=self.visible_data_loader,
        )

    def test_resource_does_not_exist(self) -> None:
        self.visible_data_loader.get_resource.side_effect = ResourceNotFound(self.resource.dataset_id, "foo")
        with pytest.raises(NotFoundError):
            fetch_resource(
                hub=self.resource.hub,
                dataset_id=self.resource.dataset_id,
                stage=self.resource.stage,
                region=self.resource.region,
                resource_type=self.resource.type,
                visible_data_loader=self.visible_data_loader,
            )

    def test_wrong_hub(self) -> None:
        other_hub = Builder.get_random_element(Hub, exclude=[self.resource.hub])
        with pytest.raises(NotFoundError):
            fetch_resource(
                hub=other_hub,
                dataset_id=self.resource.dataset_id,
                stage=self.resource.stage,
                region=self.resource.region,
                resource_type=self.resource.type,
                visible_data_loader=self.visible_data_loader,
            )


class TestFindPermission:
    def setup_method(self) -> None:
        self.permission = build_dataset_account_permission()
        self.other_permission = build_dataset_account_permission()
        self.dataset = build_dataset(permissions=frozenset({self.permission, self.other_permission}))

    def _find_permission(self, dataset: Optional[Dataset] = None) -> DatasetAccountPermission:
        return find_permission(
            account_id=self.permission.account_id,
            dataset=dataset or self.dataset,
            region=self.permission.region,
            stage=self.permission.stage,
        )

    def test_existing_permission(self) -> None:
        assert self._find_permission() == self.permission

    def test_missing_permission(self) -> None:
        dataset = replace(self.dataset, permissions=frozenset())
        with pytest.raises(ConflictError):
            self._find_permission(dataset)
