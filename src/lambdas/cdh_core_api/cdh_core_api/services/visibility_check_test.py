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
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.full_vision_check import FullVisionCheck
from cdh_core_api.services.visibility_check import VisibilityCheck

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.arn_test import build_sts_assumed_role_arn
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.hubs import Hub
from cdh_core_dev_tools.testing.builder import Builder


class TestVisibilityCheck:
    @pytest.fixture(autouse=True)
    def service_setup(self) -> None:
        self.authorization_api = Mock(AuthorizationApi)
        self.requester = build_sts_assumed_role_arn()
        self.full_vision_check = Mock(FullVisionCheck, return_value=False)
        self.config = build_config()
        self.visibility_check = VisibilityCheck(
            full_vision_check=self.full_vision_check,
            authorization_api=self.authorization_api,
            requester=self.requester,
            config=self.config,
        )
        self.extended = False


class TestAccountVisibility(TestVisibilityCheck):
    @pytest.mark.parametrize("batch", [False, True])
    def test_full_vision(self, batch: bool) -> None:
        self.full_vision_check.return_value = True
        assert self.visibility_check.get_account_visibility_check(batch=batch)(build_account())

        assert not self.authorization_api.method_calls

    def test_batch(self) -> None:
        visible_account = build_account()
        invisible_account = build_account()
        self.authorization_api.get_visible_account_ids.return_value = [visible_account.id]
        visibility_check = self.visibility_check.get_account_visibility_check(batch=True)

        assert visibility_check(visible_account)
        assert not visibility_check(invisible_account)
        self.authorization_api.get_visible_account_ids.assert_called_once()
        self.authorization_api.is_account_visible.assert_not_called()

    @pytest.mark.parametrize("is_visible", [False, True])
    def test_single(self, is_visible: bool) -> None:
        self.authorization_api.is_account_visible.return_value = is_visible
        visibility_check = self.visibility_check.get_account_visibility_check(batch=False)
        account = build_account()

        assert visibility_check(account) == is_visible
        self.authorization_api.is_account_visible.assert_called_once_with(account.id)


class TestDatasetVisibility(TestVisibilityCheck):
    @pytest.mark.parametrize("batch", [False, True])
    def test_full_vision(self, batch: bool) -> None:
        self.full_vision_check.return_value = True
        assert self.visibility_check.get_dataset_visibility_check(batch=batch)(build_dataset())

        assert not self.authorization_api.method_calls

    def test_batch(self) -> None:
        visible_dataset = build_dataset()
        invisible_dataset = build_dataset()
        self.authorization_api.get_visible_dataset_ids.return_value = {visible_dataset.id}
        visibility_check = self.visibility_check.get_dataset_visibility_check(batch=True)

        assert visibility_check(visible_dataset)
        assert not visibility_check(invisible_dataset)
        self.authorization_api.get_visible_dataset_ids.assert_called_once()
        self.authorization_api.is_dataset_visible.assert_not_called()

    @pytest.mark.parametrize("is_visible", [False, True])
    def test_single(self, is_visible: bool) -> None:
        self.authorization_api.is_dataset_visible.return_value = is_visible
        visibility_check = self.visibility_check.get_dataset_visibility_check(batch=False)
        dataset = build_dataset()

        assert visibility_check(dataset) == is_visible
        self.authorization_api.is_dataset_visible.assert_called_once_with(dataset.id)


class TestDatasetIdVisibility(TestVisibilityCheck):
    @pytest.mark.parametrize("batch", [False, True])
    def test_full_vision(self, batch: bool) -> None:
        self.full_vision_check.return_value = True
        assert self.visibility_check.get_dataset_id_visibility_check(batch=batch)(build_dataset_id())

        assert not self.authorization_api.method_calls

    def test_batch(self) -> None:
        visible_dataset_id = build_dataset_id()
        invisible_dataset_id = build_dataset_id()
        self.authorization_api.get_visible_dataset_ids.return_value = {visible_dataset_id}
        visibility_check = self.visibility_check.get_dataset_id_visibility_check(batch=True)

        assert visibility_check(visible_dataset_id)
        assert not visibility_check(invisible_dataset_id)
        self.authorization_api.get_visible_dataset_ids.assert_called_once()
        self.authorization_api.is_dataset_visible.assert_not_called()

    @pytest.mark.parametrize("is_visible", [False, True])
    def test_single(self, is_visible: bool) -> None:
        self.authorization_api.is_dataset_visible.return_value = is_visible
        visibility_check = self.visibility_check.get_dataset_id_visibility_check(batch=False)
        dataset_id = build_dataset_id()

        assert visibility_check(dataset_id) == is_visible
        self.authorization_api.is_dataset_visible.assert_called_once_with(dataset_id)


class TestResourceVisibility(TestVisibilityCheck):
    @pytest.mark.parametrize("batch", [False, True])
    def test_full_vision(self, batch: bool) -> None:
        self.full_vision_check.return_value = True
        assert self.visibility_check.get_resource_visibility_check(batch=batch)(build_s3_resource())

        assert not self.authorization_api.method_calls

    def test_batch(self) -> None:
        visible_dataset = build_dataset()
        visible_resources = [build_s3_resource(dataset=visible_dataset) for _ in range(3)]
        invisible_resources = [build_s3_resource() for _ in range(3)]
        self.authorization_api.get_visible_dataset_ids.return_value = {visible_dataset.id}
        visibility_check = self.visibility_check.get_resource_visibility_check(batch=True)

        assert all(visibility_check(resource) for resource in visible_resources)
        assert all(not visibility_check(resource) for resource in invisible_resources)
        self.authorization_api.get_visible_dataset_ids.assert_called_once()
        self.authorization_api.is_dataset_visible.assert_not_called()

    @pytest.mark.parametrize("is_visible", [False, True])
    def test_single(self, is_visible: bool) -> None:
        self.authorization_api.is_dataset_visible.return_value = is_visible
        visibility_check = self.visibility_check.get_resource_visibility_check(batch=False)
        resource = build_s3_resource()

        assert visibility_check(resource) == is_visible
        self.authorization_api.is_dataset_visible.assert_called_once_with(resource.dataset_id)


@pytest.mark.parametrize(
    "mock_config_file",
    [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS],
    indirect=True,
)
class TestHubVisibility:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        self.authorization_api = Mock(AuthorizationApi)
        self.requester = build_sts_assumed_role_arn()
        self.full_vision_check = Mock(FullVisionCheck, return_value=False)
        self.config = build_config()
        self.visibility_check = VisibilityCheck(
            full_vision_check=self.full_vision_check,
            authorization_api=self.authorization_api,
            requester=self.requester,
            config=self.config,
        )
        self.extended = True

    @pytest.mark.parametrize("batch", [False, True])
    def test_full_vision(self, batch: bool) -> None:
        self.full_vision_check.return_value = True
        assert self.visibility_check.get_hub_visibility_check(batch=batch)(Builder.get_random_element(self.config.hubs))

        assert not self.authorization_api.method_calls

    @pytest.mark.parametrize("batch", [False, True])
    def test_full_vision_bad_hub(self, batch: bool) -> None:
        self.full_vision_check.return_value = True
        hub = Builder.get_random_element(list(Hub), exclude=self.config.hubs)
        assert not self.visibility_check.get_hub_visibility_check(batch=batch)(hub)

        assert not self.authorization_api.method_calls

    def test_batch(self) -> None:
        hub, invisible_hub = Builder.choose_without_repetition(self.config.hubs, 2)
        non_existent_hub = Builder.get_random_element(list(Hub), exclude=self.config.hubs)
        self.authorization_api.get_visible_hubs.return_value = {hub, non_existent_hub}
        visibility_check = self.visibility_check.get_hub_visibility_check(batch=True)

        assert visibility_check(hub)
        assert not visibility_check(invisible_hub)
        assert not visibility_check(non_existent_hub)
        self.authorization_api.get_visible_hubs.assert_called_once()
        self.authorization_api.is_hub_visible.assert_not_called()

    @pytest.mark.parametrize("is_visible", [False, True])
    def test_single(self, is_visible: bool) -> None:
        self.authorization_api.is_hub_visible.return_value = is_visible
        visibility_check = self.visibility_check.get_hub_visibility_check(batch=False)
        hub = Builder.get_random_element(self.config.hubs)

        assert visibility_check(hub) == is_visible
        self.authorization_api.is_hub_visible.assert_called_once_with(hub)

    @pytest.mark.parametrize("is_visible", [False, True])
    def test_single_bad_hub(self, is_visible: bool) -> None:
        self.authorization_api.is_hub_visible.return_value = is_visible
        hub = Builder.get_random_element(list(Hub), exclude=self.config.hubs)
        visibility_check = self.visibility_check.get_hub_visibility_check(batch=False)

        assert not visibility_check(hub)
        self.authorization_api.is_hub_visible.assert_not_called()
