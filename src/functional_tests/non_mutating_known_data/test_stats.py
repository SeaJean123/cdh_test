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

from cdh_core.entities.dataset import DatasetId
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import Stage
from functional_tests.conftest import NonMutatingTestSetup


class TestS3ResourceStats:
    """Test Class for Stats endpoint."""

    def setup_method(self) -> None:
        self.hub = Hub("global")
        self.region = Region("eu-west-1")
        self.dataset_id = DatasetId("bi_cdh_functional_test_src")
        self.stage = Stage.prod

    def test_get_stats_of_s3_resource(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources/s3/{datasetId}/{stage}/{region}/stats endpoint."""
        response = non_mutating_test_setup.http_client.get(
            f"/{self.hub.value}/resources/s3/{self.dataset_id}/" f"{self.stage.value}/{self.region.value}/stats",
            expected_status_codes=[HTTPStatus.OK],
        )
        required_keys = {"bucketSizeBytes", "numberOfObjects"}
        assert required_keys <= response.keys()
