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
import itertools
from dataclasses import replace
from random import choice
from random import randint
from random import sample
from typing import Any
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union

import pytest
from asserts import assert_count_equal
from cdh_core_api.catalog.base_test import get_nullable_attributes
from cdh_core_api.catalog.resource_table import GenericResourceModel
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.catalog.resource_table import ResourcesTable
from mypy_boto3_dynamodb.service_resource import Table

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import Resource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties_test import build_sync_type
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class ResourceTableTest:
    @pytest.fixture(autouse=True)
    def dynamo_setup(self, resource_name_prefix: str, mock_resources_dynamo_table: Table) -> None:
        self.mock_resources_dynamo_table = mock_resources_dynamo_table
        self.resources_table = ResourcesTable(resource_name_prefix)


class TestGet(ResourceTableTest):
    @pytest.fixture(params=ResourceType)
    def expected_resource(
        self,
        request: Any,
    ) -> Resource:
        dataset = build_dataset()
        stage = build_stage()
        region = build_region()
        if request.param is ResourceType.s3:
            expected_resource: Union[GlueSyncResource, S3Resource] = build_s3_resource(
                dataset=dataset, stage=stage, region=region
            )
        else:
            sync_type = build_sync_type()
            expected_resource = build_glue_sync_resource(
                dataset=dataset, stage=stage, region=region, sync_type=sync_type
            )
        self.mock_resources_dynamo_table.put_item(Item=build_dynamo_json(expected_resource))
        return expected_resource

    def test_get_for_existing_resource(
        self,
        expected_resource: Resource,
    ) -> None:
        assert (
            self.resources_table.get(
                resource_type=expected_resource.type,
                dataset_id=expected_resource.dataset_id,
                stage=expected_resource.stage,
                region=expected_resource.region,
            )
            == expected_resource
        )

    def test_get_for_wrong_resource_type(
        self,
        expected_resource: Resource,
    ) -> None:
        other_type = ResourceType.s3 if expected_resource.type is ResourceType.glue_sync else ResourceType.glue_sync
        with pytest.raises(ResourceNotFound):
            self.resources_table.get(
                resource_type=other_type,
                dataset_id=expected_resource.dataset_id,
                stage=expected_resource.stage,
                region=expected_resource.region,
            )

    def test_get_for_not_existing_dataset_id(self, expected_resource: Resource) -> None:
        with pytest.raises(ResourceNotFound):
            self.resources_table.get(
                resource_type=expected_resource.type,
                dataset_id="other_dataset_id",
                stage=expected_resource.stage,
                region=expected_resource.region,
            )

    def test_get_for_wrong_stage(self, expected_resource: Resource) -> None:  # pylint: disable=unused-argument
        with pytest.raises(ResourceNotFound):
            self.resources_table.get(
                resource_type=expected_resource.type,
                dataset_id=expected_resource.dataset_id,
                stage=Builder.get_random_element(list(Stage), exclude={expected_resource.stage}),
                region=expected_resource.region,
            )

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_get_for_wrong_region(
        self, expected_resource: Resource, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:  # pylint: disable=unused-argument
        with pytest.raises(ResourceNotFound):
            self.resources_table.get(
                resource_type=expected_resource.type,
                dataset_id=expected_resource.dataset_id,
                stage=expected_resource.stage,
                region=Builder.get_random_element(
                    to_choose_from=list(Region),
                    exclude={region for region in list(Region) if region.value == expected_resource.region.value},
                ),
            )

    def test_get_for_wrong_hub(self, expected_resource: Resource) -> None:  # pylint: disable=unused-argument
        dataset = build_dataset()
        dataset_another_hub = Dataset.build_id(
            dataset.business_object,
            dataset.name,
            dataset.layer,
            Builder.get_random_element(to_choose_from=list(Hub), exclude={expected_resource.hub}),
        )
        with pytest.raises(ResourceNotFound):
            self.resources_table.get(
                resource_type=expected_resource.type,
                dataset_id=dataset_another_hub,
                stage=expected_resource.stage,
                region=expected_resource.region,
            )

    def test_exists(self) -> None:  # pylint: disable=unused-argument
        stage = choice(list(Stage))
        resource = build_glue_sync_resource(stage=stage)
        assert self.resources_table.exists(ResourceType.glue_sync, resource.dataset_id, stage, resource.region) is False
        self.resources_table.create(resource)
        assert self.resources_table.exists(ResourceType.glue_sync, resource.dataset_id, stage, resource.region) is True

    def test_get_all_nullable_fields_none(self) -> None:
        dataset = build_dataset()
        stage = choice(list(Stage))
        region = choice(list(Region))
        glue_sync = build_glue_sync_resource(
            stage=stage,
            dataset=dataset,
            region=region,
        )
        dynamo_json = build_dynamo_json(glue_sync)
        nullable_attributes = get_nullable_attributes(GenericResourceModel)
        for nullable_attribute in nullable_attributes:
            if nullable_attribute != "glue_sync":
                dynamo_json.pop(nullable_attribute, None)
        self.mock_resources_dynamo_table.put_item(Item=dynamo_json)
        self.resources_table.get_glue_sync(
            dataset_id=dataset.id,
            stage=stage,
            region=region,
        )  # no exception is raised

        s3_resource = build_s3_resource(dataset=dataset, stage=stage, region=region)
        dynamo_json = build_dynamo_json(s3_resource)

        for nullable_attribute in nullable_attributes:
            if nullable_attribute != "s3":
                dynamo_json.pop(nullable_attribute, None)
        self.mock_resources_dynamo_table.put_item(Item=dynamo_json)
        self.resources_table.get_s3(
            dataset_id=dataset.id,
            stage=stage,
            region=region,
        )  # no exception is raised


class TestList(ResourceTableTest):
    hub = build_hub()

    def build_resource_set(self, hub: Optional[Hub] = None) -> List[Union[GlueSyncResource, S3Resource]]:
        dataset = build_dataset(hub=hub or self.hub)
        return [
            build_glue_sync_resource(dataset=dataset),
            build_s3_resource(dataset=dataset),
        ]

    def test_list_all(self) -> None:
        expected_resources = [build_resource() for _ in range(5)]
        self._fill_dynamo(resources=expected_resources)

        resources = self.resources_table.list()
        assert len(resources) == len(expected_resources)
        assert_count_equal(resources, expected_resources)

    def test_list_with_hubs(self) -> None:
        expected_resource_sets = [item for _ in range(3) for item in self.build_resource_set()]
        other_hub = Builder.get_random_element(list(Hub), exclude={self.hub})
        foreign_resource_sets = [item for _ in range(3) for item in self.build_resource_set(hub=other_hub)]
        self._fill_dynamo(resources=expected_resource_sets + foreign_resource_sets)

        resources = self.resources_table.list(hub=self.hub)
        assert_count_equal(resources, expected_resource_sets)

    def test_list_with_owner(self) -> None:
        resources = [build_resource() for _ in range(5)]
        expected_resources = [resources[0]]
        self._fill_dynamo(resources=resources)
        actual_resources = self.resources_table.list(owner=expected_resources[0].owner_account_id)

        assert len(actual_resources) == len(expected_resources)
        assert_count_equal(actual_resources, expected_resources)

    def test_list_across_hubs(self) -> None:
        expected_resource_sets = [item for _ in range(3) for item in self.build_resource_set()]
        possible_hubs = set(Hub).symmetric_difference({self.hub})
        foreign_resource_sets = [item for hub in possible_hubs for item in self.build_resource_set(hub=hub)]
        self._fill_dynamo(resources=expected_resource_sets + foreign_resource_sets)

        resources = self.resources_table.list()
        assert_count_equal(resources, expected_resource_sets + foreign_resource_sets)

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_list_filters(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        account_id1, account_id2 = build_account_id(), build_account_id()
        dataset_1, dataset_2, dataset_3 = build_dataset(), build_dataset(), build_dataset()
        region_1, region_2 = sample(list(Region), 2)
        resources = [
            build_s3_resource(
                dataset=dataset_1,
                region=region_1,
                stage=Stage.dev,
                resource_account_id=account_id1,
            ),
            build_s3_resource(
                dataset=dataset_1,
                region=region_2,
                stage=Stage.prod,
                resource_account_id=account_id2,
            ),
            build_s3_resource(
                dataset=dataset_2,
                region=region_1,
                stage=Stage.dev,
                resource_account_id=account_id1,
            ),
            build_s3_resource(
                dataset=dataset_3,
                region=region_1,
                stage=Stage.dev,
                resource_account_id=account_id2,
            ),
        ]
        self._fill_dynamo(resources=resources)

        assert len(self.resources_table.list(stage=Stage.dev)) == 3
        assert len(self.resources_table.list(resource_account=account_id1)) == 2
        assert len(self.resources_table.list(region=region_2)) == 1

        assert len(self.resources_table.list(dataset_id=dataset_3.id)) == 1
        assert len(self.resources_table.list(region=region_1, resource_account=account_id2)) == 1

    def test_list_s3(self) -> None:
        s3_resources = [build_s3_resource() for _ in range(randint(3, 5))]
        glue_sync_resources = [build_glue_sync_resource() for _ in range(randint(3, 5))]
        self._fill_dynamo(s3_resources + glue_sync_resources)  # type: ignore

        assert_count_equal(self.resources_table.list_s3(), s3_resources)

    def test_list_glue_sync(self) -> None:
        s3_resources = [build_s3_resource() for _ in range(randint(3, 5))]
        glue_sync_resources = [build_glue_sync_resource() for _ in range(randint(3, 5))]
        self._fill_dynamo(s3_resources + glue_sync_resources)  # type: ignore

        assert_count_equal(self.resources_table.list_glue_sync(), glue_sync_resources)

    def _fill_dynamo(self, resources: Collection[Resource]) -> None:
        resources_shuffled: Sequence[Resource] = sample(list(resources), len(resources))
        with self.mock_resources_dynamo_table.batch_writer() as batch:
            for resource in resources_shuffled:
                batch.put_item(build_dynamo_json(resource))


class TestGetResourceIterator(ResourceTableTest):
    def test_iterate_empty_dynamo(
        self,
    ) -> None:
        iterator = self.resources_table.get_resources_iterator()
        with pytest.raises(StopIteration):
            next(iterator)

    def test_iterate_all(self) -> None:
        expected_resources = [build_resource() for _ in range(5)]
        self._fill_dynamo(resources=expected_resources)

        iterator = self.resources_table.get_resources_iterator()

        assert_count_equal(list(iterator), expected_resources)
        assert iterator.last_evaluated_key is None

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_iterate_filter_by_region(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        region, other_region = Builder.choose_without_repetition(list(Region), 2)
        expected_resources = [build_resource(region=region) for _ in range(5)]
        other_resources = [build_resource(region=other_region) for _ in range(5)]
        self._fill_dynamo(resources=expected_resources + other_resources)

        iterator = self.resources_table.get_resources_iterator(region=region)
        resources = list(iterator)

        assert_count_equal(resources, expected_resources)
        assert iterator.last_evaluated_key is None

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_iterate_filter_by_owner(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        owner = build_account_id()
        other_owner = build_account_id()
        expected_resources = [build_resource(owner_account_id=owner) for _ in range(5)]
        other_resources = [build_resource(owner_account_id=other_owner) for _ in range(5)]
        self._fill_dynamo(resources=expected_resources + other_resources)

        iterator = self.resources_table.get_resources_iterator(owner=owner)
        resources = list(iterator)

        assert_count_equal(resources, expected_resources)
        assert iterator.last_evaluated_key is None

    def test_last_evaluated_mid_iteration_not_none(self) -> None:
        expected_resources = [build_resource() for _ in range(5)]
        self._fill_dynamo(resources=expected_resources)

        iterator = self.resources_table.get_resources_iterator()

        iteration_over = False
        for _ in iterator:
            assert not iteration_over
            iteration_over = iterator.last_evaluated_key is None

    def test_iterate_with_last_evaluated_key_resumes(self) -> None:
        expected_resources = [build_resource() for _ in range(10)]
        self._fill_dynamo(resources=expected_resources)
        cutoff = randint(1, len(expected_resources) - 1)
        first_iterator = self.resources_table.get_resources_iterator()
        for _ in range(cutoff):
            next(first_iterator)

        second_iterator = self.resources_table.get_resources_iterator(
            last_evaluated_key=first_iterator.last_evaluated_key
        )

        for first_item, second_item in itertools.zip_longest(first_iterator, second_iterator):
            assert first_item == second_item
            assert first_iterator.last_evaluated_key == second_iterator.last_evaluated_key

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_iterate_with_last_evaluated_key_and_filter(
        self, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:
        region, other_region = Builder.choose_without_repetition(list(Region), 2)
        expected_resources = [build_resource(region=region) for _ in range(5)]
        other_resources = [build_resource(region=other_region) for _ in range(5)]
        self._fill_dynamo(resources=expected_resources + other_resources)
        cutoff = randint(1, len(expected_resources) - 1)
        first_iterator = self.resources_table.get_resources_iterator(region=region)
        for _ in range(cutoff):
            next(first_iterator)

        second_iterator = self.resources_table.get_resources_iterator(
            last_evaluated_key=first_iterator.last_evaluated_key, region=region
        )

        for first_item, second_item in itertools.zip_longest(first_iterator, second_iterator):
            assert first_item == second_item
            assert first_iterator.last_evaluated_key == second_iterator.last_evaluated_key

    def _fill_dynamo(self, resources: Collection[Resource]) -> None:
        resources_shuffled: Sequence[Resource] = sample(list(resources), len(resources))
        with self.mock_resources_dynamo_table.batch_writer() as batch:
            for resource in resources_shuffled:
                batch.put_item(build_dynamo_json(resource))


class TestUpdateOwnerAccountId(ResourceTableTest):
    @pytest.mark.parametrize("resource_type", [ResourceType.glue_sync, ResourceType.s3])
    def test_update(self, resource_type: ResourceType) -> None:
        resource = build_resource(resource_type=resource_type)
        self.resources_table.create(resource)
        new_owner_account_id = build_account_id()

        updated_resource = self.resources_table.update_owner_account_id(
            resource=resource,
            new_owner_account_id=new_owner_account_id,
        )

        expected_resource = replace(
            resource,
            owner_account_id=new_owner_account_id,
        )
        assert updated_resource == expected_resource
        assert (
            self.resources_table.get(resource_type, resource.dataset_id, resource.stage, resource.region)
            == expected_resource
        )

    @pytest.mark.parametrize("resource_type", [ResourceType.glue_sync, ResourceType.s3])
    def test_update_resource_that_does_not_exist(self, resource_type: ResourceType) -> None:
        resource = build_resource(resource_type=resource_type)

        with pytest.raises(ResourceNotFound):
            self.resources_table.update_owner_account_id(
                resource=resource,
                new_owner_account_id=build_account_id(),
            )


class TestDelete(ResourceTableTest):
    def test_delete_existing(self) -> None:
        resource = build_resource(ResourceType.s3)
        self.resources_table.create(resource)
        self.resources_table.delete(resource.type, resource.dataset_id, resource.stage, resource.region)
        assert self.mock_resources_dynamo_table.scan()["Items"] == []

    def test_delete_nonexisting(self) -> None:  # pylint: disable=unused-argument
        resource = build_resource(ResourceType.s3)
        with pytest.raises(ResourceNotFound):
            self.resources_table.delete(resource.type, resource.dataset_id, resource.stage, resource.region)


def _get_dynamo_json_common_dict(resource: Resource) -> Dict[str, Any]:
    update_date = resource.update_date or resource.creation_date
    return {
        "arn": str(resource.arn),
        "dataset_id": resource.dataset_id,
        "hub": resource.hub.value,
        "creator_user_id": resource.creator_user_id,
        "creation_date": resource.creation_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "id": f"{resource.type.value}_{resource.stage.value}_{resource.region.value}",
        "region": resource.region.value,
        "resource_account_id": resource.resource_account_id,
        "stage": resource.stage.value,
        "type": resource.type.value,
        "update_date": update_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "owner_account_id": resource.owner_account_id,
    }


def build_dynamo_json(resource: Resource) -> Dict[str, Any]:
    common = _get_dynamo_json_common_dict(resource=resource)
    if isinstance(resource, S3Resource):
        s3_attributes: Dict[str, Any] = {
            "sns_topic_arn": str(resource.sns_topic_arn),
            "kms_key_arn": str(resource.kms_key_arn),
        }
        return {
            **common,
            "s3": s3_attributes,
        }
    if isinstance(resource, GlueSyncResource):
        glue_sync_dict = {
            "database_name": resource.dataset_id,
            "sync_type": resource.sync_type.value,
        }
        return {
            **common,
            "glue_sync": glue_sync_dict,
        }
    raise TypeError(f"resources is of invalid type {type(resource)}")
