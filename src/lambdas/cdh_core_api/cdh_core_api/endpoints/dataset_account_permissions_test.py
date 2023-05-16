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
from http import HTTPStatus
from typing import Optional
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from cdh_core_api.bodies.datasets_test import build_dataset_account_permission_body
from cdh_core_api.bodies.datasets_test import build_dataset_account_permission_post_body
from cdh_core_api.endpoints.dataset_account_permissions import get_permissions
from cdh_core_api.endpoints.dataset_account_permissions import grant_access
from cdh_core_api.endpoints.dataset_account_permissions import PermissionsPath
from cdh_core_api.endpoints.dataset_account_permissions import revoke_access
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.dataset_permissions_validator import DatasetPermissionsValidator
from cdh_core_api.services.dataset_permissions_validator_test import build_validated_dataset_access_permission
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.entities.dataset import DatasetAccountPermissionAction
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import ResponseDatasetPermissions
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.exceptions.http import ConflictError


def build_permission_path(hub: Optional[Hub] = None, dataset_id: Optional[DatasetId] = None) -> PermissionsPath:
    return PermissionsPath(hub=hub or build_hub(), datasetId=dataset_id or build_dataset_id())


class DatasetPermissionTest:
    def setup_method(self) -> None:
        self.path = build_permission_path()
        self.dataset_account_permission_body = build_dataset_account_permission_body()
        self.dataset_account_permission_post_body = build_dataset_account_permission_post_body()
        self.validated_permission = build_validated_dataset_access_permission()
        self.dataset_permissions_validator = Mock(DatasetPermissionsValidator)
        self.dataset_permissions_manager = Mock(DatasetPermissionsManager)


class TestGetDatasetPermissions:
    def test_get_dataset_permissions(self) -> None:
        permissions = frozenset([build_dataset_account_permission() for _ in range(5)])
        dataset = build_dataset(permissions=permissions)
        path = PermissionsPath(dataset.hub, dataset.id)
        visible_data_loader = Mock(VisibleDataLoader)

        with patch(
            "cdh_core_api.endpoints.dataset_account_permissions.fetch_dataset", return_value=dataset
        ) as mocked_fetch_dataset:
            response = get_permissions(path=path, visible_data_loader=visible_data_loader)

        mocked_fetch_dataset.assert_called_once_with(
            hub=dataset.hub, dataset_id=dataset.id, visible_data_loader=visible_data_loader
        )
        assert isinstance(response.body, ResponseDatasetPermissions)
        assert response.body.permissions == permissions


class TestGrantAccess(DatasetPermissionTest):
    def test_no_update_if_validation_fails(self) -> None:
        self.dataset_permissions_validator.validate_dataset_access_request.side_effect = ConflictError("")

        with pytest.raises(ConflictError):
            grant_access(
                body=self.dataset_account_permission_post_body,
                path=self.path,
                dataset_permissions_validator=self.dataset_permissions_validator,
                dataset_permissions_manager=self.dataset_permissions_manager,
            )

        self.dataset_permissions_manager.add_or_remove_permission_handle_errors.assert_not_called()

    def test_access_is_granted(self) -> None:
        self.dataset_permissions_validator.validate_dataset_access_request.return_value = self.validated_permission

        response = grant_access(
            body=self.dataset_account_permission_post_body,
            path=self.path,
            dataset_permissions_validator=self.dataset_permissions_validator,
            dataset_permissions_manager=self.dataset_permissions_manager,
        )

        assert response.status_code == HTTPStatus.CREATED
        assert isinstance(response.body, DataClassJsonCDHMixin)
        assert response.body == self.validated_permission.permission
        self.dataset_permissions_validator.validate_dataset_access_request.assert_called_once_with(
            hub=self.path.hub, dataset_id=self.path.datasetId, body=self.dataset_account_permission_post_body
        )
        self.dataset_permissions_manager.add_or_remove_permission_handle_errors.assert_called_once_with(
            self.validated_permission, DatasetAccountPermissionAction.add
        )


class TestRevokeAccess(DatasetPermissionTest):
    def test_access_is_revoked(self) -> None:
        self.dataset_permissions_validator.validate_revoke.return_value = self.validated_permission

        response = revoke_access(
            body=self.dataset_account_permission_body,
            path=self.path,
            dataset_permissions_validator=self.dataset_permissions_validator,
            dataset_permissions_manager=self.dataset_permissions_manager,
        )

        assert response.status_code == HTTPStatus.OK
        assert response.body == self.validated_permission.permission
        self.dataset_permissions_validator.validate_revoke.assert_called_once_with(
            hub=self.path.hub, dataset_id=self.path.datasetId, body=self.dataset_account_permission_body
        )
        self.dataset_permissions_manager.add_or_remove_permission_handle_errors.assert_called_once_with(
            self.validated_permission, DatasetAccountPermissionAction.remove
        )

    def test_no_changes_if_validation_fails(self) -> None:
        self.dataset_permissions_validator.validate_revoke.side_effect = ConflictError("")

        with pytest.raises(ConflictError):
            revoke_access(
                body=self.dataset_account_permission_body,
                path=self.path,
                dataset_permissions_validator=self.dataset_permissions_validator,
                dataset_permissions_manager=self.dataset_permissions_manager,
            )

        self.dataset_permissions_manager.add_or_remove_permission_handle_errors.assert_not_called()
