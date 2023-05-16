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
from cdh_core.entities.dataset import DatasetId
from cdh_core.enums.hubs import Hub
from functional_tests.conftest import NonMutatingTestSetup


class TestDatasets:
    """Test Class for all Dataset endpoints."""

    def setup_method(self) -> None:
        self.hub = Hub("global")
        self.dataset_id = DatasetId("bi_cdh_functional_test_src")

    def test_get_all_datasets(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/datasets endpoint."""
        datasets = non_mutating_test_setup.core_api_client.get_datasets(self.hub)

        assert len(datasets) > 0
        assert all(dataset.hub is self.hub for dataset in datasets)

    def test_get_datasets_cross_hub(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the cross-hub GET /datasets endpoint."""
        datasets = non_mutating_test_setup.core_api_client.get_datasets_cross_hub([self.dataset_id])

        assert {dataset.id for dataset in datasets} == {self.dataset_id}
        assert all(len(dataset.engineers) > 0 for dataset in datasets)

    def test_get_dataset(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/datasets/{datasetId} endpoint."""
        dataset = non_mutating_test_setup.core_api_client.get_dataset(hub=self.hub, dataset_id=self.dataset_id)

        assert dataset.id == self.dataset_id
        assert len(dataset.engineers) > 0

    def test_get_dataset_permissions(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/datasets/{datasetId}/permissions endpoint."""
        permissions = non_mutating_test_setup.core_api_client.get_dataset_permissions(
            hub=self.hub, dataset_id=self.dataset_id
        )

        assert len(permissions) >= 0
