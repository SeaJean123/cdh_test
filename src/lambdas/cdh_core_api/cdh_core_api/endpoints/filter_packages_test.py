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
from cdh_core_api.catalog.filter_packages_table import FilterPackageNotFound
from cdh_core_api.endpoints import filter_packages
from cdh_core_api.endpoints.filter_packages import FilterPackageByIdPath
from cdh_core_api.endpoints.filter_packages import FilterPackagesPath
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.filter_package import FilterPackages
from cdh_core.entities.filter_package_test import build_filter_package
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import NotFoundError


class TestGetFilterPackage:
    def setup_method(self) -> None:
        self.filter_package = build_filter_package()
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.visible_data_loader.get_filter_package.return_value = self.filter_package

    def test_get_filter_package(self) -> None:
        response = filter_packages.get_filter_package(
            path=FilterPackageByIdPath(
                hub=self.filter_package.hub,
                datasetId=self.filter_package.dataset_id,
                stage=self.filter_package.stage,
                region=self.filter_package.region,
                packageId=self.filter_package.id,
            ),
            visible_data_loader=self.visible_data_loader,
        )
        assert response.body == self.filter_package
        assert response.status_code == 200
        self.visible_data_loader.get_filter_package.assert_called_once()

    def test_get_nonexisting_filter_package(self) -> None:
        self.visible_data_loader.get_filter_package.side_effect = FilterPackageNotFound(
            hash_key=f"{self.filter_package.dataset_id}_"
            f"{self.filter_package.stage.value}_"
            f"{self.filter_package.region.value}",
            package_id=self.filter_package.id,
        )
        with pytest.raises(NotFoundError):
            filter_packages.get_filter_package(
                path=FilterPackageByIdPath(
                    hub=self.filter_package.hub,
                    datasetId=self.filter_package.dataset_id,
                    stage=self.filter_package.stage,
                    region=self.filter_package.region,
                    packageId=self.filter_package.id,
                ),
                visible_data_loader=self.visible_data_loader,
            )


class TestGetAllFilterPackage:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.dataset_id = build_dataset(hub=self.hub).id
        self.stage = build_stage()
        self.region = build_region()
        self.filter_packages = [
            build_filter_package(
                hub=self.hub,
                dataset_id=self.dataset_id,
                stage=self.stage,
                region=self.region,
            )
            for _ in range(5)
        ]
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.visible_data_loader.get_filter_packages.return_value = self.filter_packages

    def test_get_filter_packages(self) -> None:
        response = filter_packages.get_filter_packages(
            path=FilterPackagesPath(
                hub=self.hub,
                datasetId=self.dataset_id,
                stage=self.stage,
                region=self.region,
            ),
            visible_data_loader=self.visible_data_loader,
        )
        assert response.body == FilterPackages(self.filter_packages)
        assert response.status_code == 200
        self.visible_data_loader.get_filter_packages.assert_called_once()
