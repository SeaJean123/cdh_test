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
import os

import pytest

from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.filter_package import PackageId
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from functional_tests.conftest import NonMutatingTestSetup


class TestFilterPackages:
    """Test Class for all filter packages endpoints."""

    def setup_method(self) -> None:
        environment = Environment(os.environ["ENVIRONMENT"])
        self.dataset_id = DatasetId("bi_cdh_functional_test_src")
        self.hub = Hub("global")
        self.region = Region("eu-west-1")
        self.stage = Stage.prod
        if environment == Environment("prod"):
            self.package_id = PackageId("3ea0183d-34ae-41ee-a367-6a0a4a014031")
        else:
            self.package_id = PackageId("4409bc3c-b74b-430a-95e8-d3a5ca77a7c2")

    def test_get_all_filter_packages(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources/glue-sync/{datasetId}/{stage}/filter-packages endpoint."""
        filter_packages = non_mutating_test_setup.core_api_client.get_filter_packages(
            hub=self.hub, dataset_id=self.dataset_id, stage=self.stage, region=self.region
        )

        assert len(filter_packages) > 0
        assert all(filter_package.hub is self.hub for filter_package in filter_packages)
        assert all(filter_package.dataset_id == self.dataset_id for filter_package in filter_packages)
        assert all(filter_package.stage is self.stage for filter_package in filter_packages)
        assert all(filter_package.region is self.region for filter_package in filter_packages)

    def test_get_filter_package(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources/glue-sync/{datasetId}/{stage}/filter-packages/{packageId} endpoint."""
        filter_package = non_mutating_test_setup.core_api_client.get_filter_package(
            hub=self.hub,
            dataset_id=self.dataset_id,
            stage=self.stage,
            region=self.region,
            package_id=self.package_id,
        )

        assert filter_package.hub is self.hub
        assert filter_package.dataset_id == self.dataset_id
        assert filter_package.stage is self.stage
        assert filter_package.region is self.region
        assert filter_package.id == self.package_id

    def test_get_filter_package_not_found(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources/glue-sync/{datasetId}/{stage}/filter-packages/{packageId} endpoint."""
        package_id = PackageId("non-existing-package-id")
        with pytest.raises(Exception) as excinfo:
            non_mutating_test_setup.core_api_client.get_filter_package(
                hub=self.hub,
                dataset_id=self.dataset_id,
                stage=self.stage,
                region=self.region,
                package_id=package_id,
            )
        assert "404" in str(excinfo.value)
        assert "not found" in str(excinfo.value)
