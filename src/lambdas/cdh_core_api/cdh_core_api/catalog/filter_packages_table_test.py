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
from random import sample
from typing import Any
from typing import Collection
from typing import Dict
from typing import Sequence

import pytest
from asserts import assert_count_equal
from cdh_core_api.catalog.base_test import get_nullable_attributes
from cdh_core_api.catalog.filter_packages_table import _FilterPackageModel
from cdh_core_api.catalog.filter_packages_table import _TableFilterAttribute
from cdh_core_api.catalog.filter_packages_table import FilterPackageNotFound
from cdh_core_api.catalog.filter_packages_table import FilterPackagesTable
from mypy_boto3_dynamodb.service_resource import Table

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.filter_package import FilterPackage
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.filter_package_test import build_filter_package
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core_dev_tools.testing.builder import Builder


class FilterPackagesTableTest:
    @pytest.fixture(autouse=True)
    def dynamo_setup(self, resource_name_prefix: str, mock_filter_packages_dynamo_table: Table) -> None:
        self.mock_filter_packages_dynamo_table = mock_filter_packages_dynamo_table
        self.filter_packages_table = FilterPackagesTable(resource_name_prefix)

    @staticmethod
    def build_dynamo_json(filter_package: FilterPackage) -> Dict[str, Any]:
        date_format = "%Y-%m-%dT%H:%M:%S.%f%z"
        return {
            "id": filter_package.id,
            "datasetid_stage_region": (
                f"{filter_package.dataset_id}_{filter_package.stage.value}_{filter_package.region.value}"
            ),
            "dataset_id": filter_package.dataset_id,
            "stage": filter_package.stage.value,
            "region": filter_package.region.value,
            "friendly_name": filter_package.friendly_name,
            "description": filter_package.description,
            "table_access": [
                {
                    "filter_id": table_filter.filter_id,
                    "package_id": table_filter.package_id,
                    "resource_account_id": table_filter.resource_account_id,
                    "database_name": table_filter.database_name,
                    "table_name": table_filter.table_name,
                    "full_access": table_filter.full_access,
                    "row_filter": table_filter.row_filter,
                    "included_columns": table_filter.included_columns,
                    "excluded_columns": table_filter.excluded_columns,
                    "creation_date": table_filter.creation_date.strftime(date_format),
                    "creator_user_id": table_filter.creator_user_id,
                    "update_date": table_filter.update_date.strftime(date_format),
                }
                for table_filter in filter_package.table_access
            ],
            "hub": filter_package.hub.value,
            "creation_date": filter_package.creation_date.strftime(date_format),
            "creator_user_id": filter_package.creator_user_id,
            "update_date": filter_package.update_date.strftime(date_format),
        }


class TestGet(FilterPackagesTableTest):
    @pytest.fixture()
    def expected_filter_package(self) -> FilterPackage:
        expected_package = build_filter_package()
        self.mock_filter_packages_dynamo_table.put_item(Item=self.build_dynamo_json(expected_package))
        return expected_package

    def test_get_for_existing_filter_package(self, expected_filter_package: FilterPackage) -> None:
        assert (
            self.filter_packages_table.get(
                dataset_id=expected_filter_package.dataset_id,
                stage=expected_filter_package.stage,
                region=expected_filter_package.region,
                package_id=expected_filter_package.id,
            )
            == expected_filter_package
        )

    def test_get_non_existing_dataset_id(self, expected_filter_package: FilterPackage) -> None:
        with pytest.raises(FilterPackageNotFound):
            self.filter_packages_table.get(
                dataset_id=DatasetId("other_dataset_id"),
                stage=expected_filter_package.stage,
                region=expected_filter_package.region,
                package_id=expected_filter_package.id,
            )

    def test_get_wrong_stage(self, expected_filter_package: FilterPackage) -> None:
        with pytest.raises(FilterPackageNotFound):
            self.filter_packages_table.get(
                dataset_id=expected_filter_package.dataset_id,
                stage=Builder.get_random_element(list(Stage), exclude={expected_filter_package.stage}),
                region=expected_filter_package.region,
                package_id=expected_filter_package.id,
            )

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_get_wrong_region(
        self, expected_filter_package: FilterPackage, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:
        with pytest.raises(FilterPackageNotFound):
            self.filter_packages_table.get(
                dataset_id=expected_filter_package.dataset_id,
                stage=expected_filter_package.stage,
                region=Builder.get_random_element(
                    to_choose_from=list(Region),
                    exclude={region for region in list(Region) if region.value == expected_filter_package.region.value},
                ),
                package_id=expected_filter_package.id,
            )

    def test_get_non_existing_package_id(self, expected_filter_package: FilterPackage) -> None:
        with pytest.raises(FilterPackageNotFound):
            self.filter_packages_table.get(
                dataset_id=expected_filter_package.dataset_id,
                stage=expected_filter_package.stage,
                region=expected_filter_package.region,
                package_id=PackageId("other-package-id"),
            )

    def test_get_all_nullable_fields_none(self) -> None:
        filter_package = build_filter_package()
        dynamo_json = self.build_dynamo_json(filter_package)
        for nullable_attribute in get_nullable_attributes(_FilterPackageModel):
            dynamo_json.pop(nullable_attribute, None)
        for table_filter in dynamo_json["table_access"]:
            for nullable_attribute in get_nullable_attributes(_TableFilterAttribute):  # type: ignore
                table_filter.pop(nullable_attribute, None)
        self.mock_filter_packages_dynamo_table.put_item(Item=dynamo_json)
        self.filter_packages_table.get(
            dataset_id=filter_package.dataset_id,
            stage=filter_package.stage,
            region=filter_package.region,
            package_id=filter_package.id,
        )  # no exception is raised


class TestList(FilterPackagesTableTest):
    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_list_filters(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        dataset_id, other_dataset_id = build_dataset_id(), build_dataset_id()
        stage, other_stage = sample(list(Stage), 2)
        region, other_region = sample(list(Region), 2)
        expected_packages = [
            build_filter_package(dataset_id=dataset_id, stage=stage, region=region),
        ]
        other_packages = [
            build_filter_package(dataset_id=other_dataset_id, stage=stage, region=region),
            build_filter_package(dataset_id=dataset_id, stage=other_stage, region=region),
            build_filter_package(dataset_id=dataset_id, stage=stage, region=other_region),
        ]
        self._fill_dynamo(expected_packages + other_packages)

        filter_packages = self.filter_packages_table.list(dataset_id, stage, region)

        assert len(filter_packages) == 1
        assert filter_packages[0] == expected_packages[0]

    def test_list_multiple(self) -> None:
        dataset_id = build_dataset_id()
        stage = build_stage()
        region = build_region()
        expected_packages = [build_filter_package(dataset_id=dataset_id, stage=stage, region=region) for _ in range(3)]
        other_packages = [build_filter_package() for _ in range(5)]
        self._fill_dynamo(expected_packages + other_packages)

        filter_packages = self.filter_packages_table.list(dataset_id, stage, region)

        assert_count_equal(filter_packages, expected_packages)

    def test_list_none(self) -> None:
        other_packages = [build_filter_package() for _ in range(5)]
        self._fill_dynamo(other_packages)

        filter_packages = self.filter_packages_table.list(build_dataset_id(), build_stage(), build_region())

        assert filter_packages == []

    def _fill_dynamo(self, filter_packages: Collection[FilterPackage]) -> None:
        packages_shuffled: Sequence[FilterPackage] = sample(list(filter_packages), len(filter_packages))
        with self.mock_filter_packages_dynamo_table.batch_writer() as batch:
            for filter_package in packages_shuffled:
                batch.put_item(self.build_dynamo_json(filter_package))
