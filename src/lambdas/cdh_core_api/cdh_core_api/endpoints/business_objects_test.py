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
from unittest.mock import Mock

import pytest
from cdh_core_api.config import Config
from cdh_core_api.endpoints.business_objects import BusinessObjectPath
from cdh_core_api.endpoints.business_objects import get_business_object
from cdh_core_api.endpoints.business_objects import get_business_objects
from cdh_core_api.services.users_api import UsersApi
from cdh_core_api.services.visibility_check import VisibilityCheck
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from cdh_core_api.validation.common_paths import HubPath

from cdh_core.entities.hub_business_object import HubBusinessObject
from cdh_core.entities.hub_business_object import HubBusinessObjectList
from cdh_core.entities.hub_business_object_test import build_hub_business_object
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.hubs_test import build_hub
from cdh_core.exceptions.http import NotFoundError
from cdh_core_dev_tools.testing.utils import UnusableMock


class BusinessObjectTestCase:
    def setup_method(self) -> None:
        self.config = Mock(Config)
        self.config.using_authorization_api = True
        self.users_api = Mock(UsersApi)
        self.visibility_check = Mock(VisibilityCheck)
        self.is_hub_visible = self.visibility_check.get_hub_visibility_check.return_value
        self.is_hub_visible.return_value = True
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.hub = build_hub()


class TestGetBusinessObject(BusinessObjectTestCase):
    def setup_method(self) -> None:
        super().setup_method()
        self.hub_business_object = build_hub_business_object(hub=self.hub)
        self.business_object = self.hub_business_object.business_object
        self.path = BusinessObjectPath(self.hub, self.business_object)

    def test_get_existing_business_object(self) -> None:
        self.users_api.get_hub_business_object.return_value = self.hub_business_object

        response = get_business_object(
            config=self.config,
            users_api=self.users_api,
            path=self.path,
            visibility_check=self.visibility_check,
        )
        assert response.body == self.hub_business_object
        assert response.status_code == HTTPStatus.OK

    def test_get_unknown_business_object(self) -> None:
        self.users_api.get_hub_business_object.return_value = None

        response = get_business_object(
            config=self.config,
            users_api=self.users_api,
            path=self.path,
            visibility_check=self.visibility_check,
        )

        assert response.body == HubBusinessObject.get_default_hub_business_object(self.hub, self.business_object)
        assert response.status_code == HTTPStatus.OK

    def test_get_invisible_business_object(self) -> None:
        self.is_hub_visible.return_value = False

        with pytest.raises(NotFoundError):
            get_business_object(
                config=self.config,
                users_api=self.users_api,
                path=self.path,
                visibility_check=self.visibility_check,
            )

    def test_get_business_object_in_non_integrated_environment(self) -> None:
        config = Mock()
        config.using_authorization_api = False
        users_api = UnusableMock()

        response = get_business_object(
            config=config,
            users_api=users_api,
            path=self.path,
            visibility_check=self.visibility_check,
        )

        assert response.body == HubBusinessObject.get_default_hub_business_object(self.hub, self.business_object)
        assert response.status_code == HTTPStatus.OK


class TestGetBusinessObjects(BusinessObjectTestCase):
    def setup_method(self) -> None:
        super().setup_method()
        self.path = HubPath(self.hub)

    def test_get_all_business_objects(self) -> None:
        hub_business_objects = {
            business_object.value: build_hub_business_object(hub=self.hub, business_object=business_object)
            for business_object in BusinessObject
        }
        self.users_api.get_all_hub_business_objects.return_value = hub_business_objects

        response = get_business_objects(
            config=self.config,
            users_api=self.users_api,
            path=self.path,
            visibility_check=self.visibility_check,
        )

        assert response.status_code == HTTPStatus.OK
        assert response.body == HubBusinessObjectList(list(hub_business_objects.values()))

    def test_get_all_business_objects_unset(self) -> None:
        self.users_api.get_all_hub_business_objects.return_value = {}

        response = get_business_objects(
            config=self.config,
            users_api=self.users_api,
            path=self.path,
            visibility_check=self.visibility_check,
        )

        assert response.status_code == HTTPStatus.OK
        assert response.body == HubBusinessObjectList(
            [
                HubBusinessObject.get_default_hub_business_object(
                    hub=self.hub,
                    business_object=bo,
                )
                for bo in BusinessObject
            ]
        )

    def test_get_all_business_objects_invisible_hub(self) -> None:
        self.is_hub_visible.return_value = False

        with pytest.raises(NotFoundError):
            get_business_objects(
                config=self.config,
                users_api=self.users_api,
                visibility_check=self.visibility_check,
                path=self.path,
            )

    def test_get_all_business_objects_in_non_integrated_environment(self) -> None:
        config = Mock()
        config.using_authorization_api = False
        users_api = UnusableMock()

        response = get_business_objects(
            config=config,
            users_api=users_api,
            path=self.path,
            visibility_check=self.visibility_check,
        )

        assert response.status_code == HTTPStatus.OK
        assert response.body == HubBusinessObjectList(
            [
                HubBusinessObject.get_default_hub_business_object(
                    hub=self.hub,
                    business_object=bo,
                )
                for bo in BusinessObject
            ]
        )
