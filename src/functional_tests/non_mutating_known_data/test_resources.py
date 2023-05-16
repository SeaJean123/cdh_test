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

from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.dataset import DatasetId
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from functional_tests.conftest import NonMutatingTestSetup
from functional_tests.utils import get_main_test_account


class TestResources:
    """Test Class for all Resource endpoints."""

    def setup_method(self) -> None:
        partition = Partition(os.environ.get("AWS_PARTITION", "aws"))
        environment = Environment(os.environ["ENVIRONMENT"])
        self.hub = Hub("global")
        self.region = Region("eu-west-1")
        self.stage = Stage.prod
        if environment == Environment("prod"):
            self.s3_resource_bucket_name = "cdh-bi-cdh-functional-test-src-2of6"
        else:
            self.s3_resource_bucket_name = "cdh-bi-cdh-functional-test-src-485j"
        self.s3_resource_dataset_id = DatasetId("bi_cdh_functional_test_src")
        self.glue_resource_dataset_id = DatasetId("humres_cdh_functional_test_src")
        self.resource_account = AccountStore().query_account(
            environments=environment,
            partitions=partition,
            account_purposes=AccountPurpose("resources"),
            hubs=self.hub,
            stages=self.stage,
        )
        self.owner_account_id = get_main_test_account(partition, environment).id

    def test_get_all_resources(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources endpoint."""
        resources = non_mutating_test_setup.core_api_client.get_resources(hub=self.hub)

        assert len(resources) > 0
        assert all(resource.hub is self.hub for resource in resources)

    def test_get_resources_with_query_parameter_match_s3(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources endpoint with query parameters matching the fixed S3 resource."""
        resources = non_mutating_test_setup.core_api_client.get_resources(
            hub=self.hub,
            dataset_id=self.s3_resource_dataset_id,
            stage=self.stage,
            region=self.region,
            resource_account_id=self.resource_account.id,
        )

        assert len(resources) > 0
        assert all(resource.hub is self.hub for resource in resources)
        assert all(resource.dataset_id == self.s3_resource_dataset_id for resource in resources)
        assert all(resource.region is self.region for resource in resources)
        assert all(resource.stage is self.stage for resource in resources)

    def test_get_resources_with_query_parameter_match_glue_sync(
        self, non_mutating_test_setup: NonMutatingTestSetup
    ) -> None:
        """Test the GET /{hub}/resources endpoint with query parameters matching the fixed Glue Sync resource."""
        resources = non_mutating_test_setup.core_api_client.get_resources(
            hub=self.hub,
            dataset_id=self.glue_resource_dataset_id,
            stage=self.stage,
            region=self.region,
            resource_account_id=self.resource_account.id,
        )

        assert len(resources) > 0
        assert all(resource.hub is self.hub for resource in resources)
        assert all(resource.dataset_id == self.glue_resource_dataset_id for resource in resources)
        assert all(resource.region is self.region for resource in resources)
        assert all(resource.stage is self.stage for resource in resources)

    def test_get_resources_with_query_parameter_match_both(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources endpoint with query parameters matching both resources."""
        resources = non_mutating_test_setup.core_api_client.get_resources(
            hub=self.hub,
            region=self.region,
        )

        assert len(resources) >= 2
        assert {resource.type for resource in resources} == set(ResourceType)
        resource_dataset_ids = {resource.dataset_id for resource in resources}
        expected_dataset_ids = {self.s3_resource_dataset_id, self.glue_resource_dataset_id}
        assert expected_dataset_ids.issubset(resource_dataset_ids)

    def test_get_s3_resource(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources/{type}/{datasetId}/{stage}/{region} endpoint."""
        resource = non_mutating_test_setup.core_api_client.get_s3_resource(
            hub=self.hub, dataset_id=self.s3_resource_dataset_id, stage=self.stage, region=self.region
        )

        assert resource.hub is self.hub
        assert resource.dataset_id == self.s3_resource_dataset_id
        assert resource.region is self.region
        assert resource.stage is self.stage
        assert resource.owner_account_id == self.owner_account_id
        assert resource.resource_account_id == self.resource_account.id
        assert resource.type is ResourceType.s3

    def test_get_s3_resource_by_bucket_name(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /resources/s3?bucketName={bucket_name} endpoint."""
        resource = non_mutating_test_setup.core_api_client.get_s3_resource_by_bucket_name(self.s3_resource_bucket_name)

        assert resource.hub is self.hub
        assert resource.dataset_id == self.s3_resource_dataset_id
        assert resource.region is self.region
        assert resource.stage is self.stage
        assert resource.owner_account_id == self.owner_account_id
        assert resource.resource_account_id == self.resource_account.id
        assert resource.type is ResourceType.s3

    def test_get_glue_resource(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/resources/{type}/{datasetId}/{stage}/{region} endpoint."""
        resource = non_mutating_test_setup.core_api_client.get_glue_resource(
            hub=self.hub, dataset_id=self.glue_resource_dataset_id, stage=self.stage, region=self.region
        )

        assert resource.hub is self.hub
        assert resource.dataset_id == self.glue_resource_dataset_id
        assert resource.region is self.region
        assert resource.stage is self.stage
        assert resource.owner_account_id == self.owner_account_id
        assert resource.resource_account_id == self.resource_account.id
        assert resource.type is ResourceType.glue_sync
