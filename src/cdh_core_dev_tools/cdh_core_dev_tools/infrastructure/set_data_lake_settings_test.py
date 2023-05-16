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
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from cdh_core_dev_tools.infrastructure.set_data_lake_settings import DataLakeSettings

if TYPE_CHECKING:
    from mypy_boto3_lakeformation.type_defs import DataLakeSettingsTypeDef
else:
    DataLakeSettingsTypeDef = object


class TestDataLakeSettings:
    def setup_method(self) -> None:
        self.data_lake_settings = DataLakeSettings(
            {
                "DataLakeAdmins": [{"DataLakePrincipalIdentifier": "some/admin"}],
                "Parameters": {"CROSS_ACCOUNT_VERSION": "3"},
            }
        )
        self.iam_client = Mock()
        self.iam_list_roles_paginator = Mock()
        self.iam_client.get_paginator.return_value = self.iam_list_roles_paginator
        self.iam_list_roles_paginator.paginate.return_value = [
            {"Roles": [{"RoleName": "admin"}, {"RoleName": "otherRole"}]},
            {"Roles": [{"RoleName": "new_admin"}]},
        ]

    def test_init(self) -> None:
        assert not self.data_lake_settings.changed

    def test_update_admins_with_new_admin(self) -> None:
        self.data_lake_settings.update_admins_if_needed({"some/new_admin"}, self.iam_client)

        assert self.data_lake_settings.settings["DataLakeAdmins"] == [
            {"DataLakePrincipalIdentifier": "some/admin"},
            {"DataLakePrincipalIdentifier": "some/new_admin"},
        ]
        assert self.data_lake_settings.changed

    def test_update_admins_with_new_not_existent_admin(self) -> None:
        self.iam_list_roles_paginator.paginate.return_value = [
            {"Roles": [{"RoleName": "admin"}, {"RoleName": "otherRole"}]},
        ]

        with pytest.raises(ValueError):
            self.data_lake_settings.update_admins_if_needed({"some/new_admin"}, self.iam_client)

    def test_update_admin_with_new_admin_current_admin_no_longer_exists(self) -> None:
        self.iam_list_roles_paginator.paginate.return_value = [
            {"Roles": [{"RoleName": "otherRole"}]},
            {"Roles": [{"RoleName": "new_admin"}]},
        ]

        self.data_lake_settings.update_admins_if_needed({"some/new_admin"}, self.iam_client)

        assert self.data_lake_settings.settings["DataLakeAdmins"] == [
            {"DataLakePrincipalIdentifier": "some/new_admin"},
        ]
        assert self.data_lake_settings.changed

    def test_update_admins_with_current_admin(self) -> None:
        self.data_lake_settings.update_admins_if_needed({"some/admin"}, self.iam_client)

        assert self.data_lake_settings.settings["DataLakeAdmins"] == [{"DataLakePrincipalIdentifier": "some/admin"}]
        assert not self.data_lake_settings.changed

    def test_update_admins_with_current_not_existent_admin(self) -> None:
        self.iam_list_roles_paginator.paginate.return_value = [{"Roles": [{"RoleName": "otherRole"}]}]

        with pytest.raises(ValueError):
            self.data_lake_settings.update_admins_if_needed({"some/admin"}, self.iam_client)

    @pytest.mark.parametrize("settings", [{"Parameters": {"CROSS_ACCOUNT_VERSION": "2"}}, {"Parameters": {}}, {}])
    def test_update_cross_account_version_changed(self, settings: DataLakeSettingsTypeDef) -> None:
        data_lake_settings = DataLakeSettings(settings)

        data_lake_settings.update_cross_account_version_if_needed()

        assert data_lake_settings.settings["Parameters"] == {"CROSS_ACCOUNT_VERSION": "3"}
        assert data_lake_settings.changed

    def test_update_cross_account_version_not_changed(self) -> None:
        self.data_lake_settings.update_cross_account_version_if_needed()

        assert self.data_lake_settings.settings["Parameters"] == {"CROSS_ACCOUNT_VERSION": "3"}
        assert not self.data_lake_settings.changed
