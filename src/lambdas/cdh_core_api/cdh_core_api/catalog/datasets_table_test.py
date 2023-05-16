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
import inspect
import itertools
from dataclasses import replace
from datetime import datetime
from random import randint
from random import sample
from typing import Any
from typing import Collection
from typing import Dict
from typing import Sequence
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from asserts import assert_count_equal
from cdh_core_api.catalog.base_test import get_attributes_of_type
from cdh_core_api.catalog.base_test import get_nullable_attributes
from cdh_core_api.catalog.datasets_table import _DatasetModel
from cdh_core_api.catalog.datasets_table import DatasetAlreadyExists
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.datasets_table import DatasetUpdateInconsistent
from mypy_boto3_dynamodb.service_resource import Table
from pynamodb.attributes import UnicodeAttribute
from pynamodb.attributes import UnicodeSetAttribute

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermissionAction
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.dataset_test import build_dataset_lineage
from cdh_core.entities.dataset_test import build_external_link
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.dataset_properties_test import build_dataset_purpose
from cdh_core.enums.dataset_properties_test import build_ingest_frequency
from cdh_core.enums.dataset_properties_test import build_retention_period
from cdh_core.enums.dataset_properties_test import build_support_level
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


def test_get_existing(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    expected_dataset = build_dataset()
    mock_datasets_dynamo_table.put_item(Item=build_dynamo_json(expected_dataset))
    assert DatasetsTable(resource_name_prefix).get(expected_dataset.id) == expected_dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_get_non_existing(resource_name_prefix: str) -> None:
    with pytest.raises(DatasetNotFound):
        DatasetsTable(resource_name_prefix).get(Builder.build_random_string())


def test_get_all_nullable_fields_none(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    dataset = build_dataset()
    dynamo_json = build_dynamo_json(dataset)

    for attribute in get_nullable_attributes(_DatasetModel):
        dynamo_json.pop(attribute, None)
    mock_datasets_dynamo_table.put_item(Item=dynamo_json)

    DatasetsTable(resource_name_prefix).get(dataset.id)


def test_list(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    expected_datasets = [build_dataset(name=f"dataset{i}") for i in range(3)]
    with mock_datasets_dynamo_table.batch_writer() as batch:
        for dataset in expected_datasets:
            batch.put_item(build_dynamo_json(dataset))
    datasets = DatasetsTable(resource_name_prefix).list()
    assert {dataset.id for dataset in datasets} == {dataset.id for dataset in expected_datasets}


def test_list_with_hubs(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    expected_datasets = [build_dataset(name=f"dataset{i}") for i in range(3)]
    other_hub = Builder.get_random_element(
        to_choose_from=list(Hub), exclude={dataset.hub for dataset in expected_datasets}
    )
    foreign_hub_datasets = [build_dataset(name=f"dataset{i}", hub=other_hub) for i in range(2)]
    with mock_datasets_dynamo_table.batch_writer() as batch:
        for dataset in expected_datasets:
            batch.put_item(build_dynamo_json(dataset))
        for dataset in foreign_hub_datasets:
            batch.put_item(build_dynamo_json(dataset))
    datasets = DatasetsTable(resource_name_prefix).list(hub=Hub("global"))
    assert {dataset.id for dataset in datasets} == {dataset.id for dataset in expected_datasets}


def test_list_with_owner(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    datasets = [build_dataset(name=f"dataset{i}") for i in range(3)]
    expected_datasets = [datasets[0]]
    with mock_datasets_dynamo_table.batch_writer() as batch:
        for dataset in datasets:
            batch.put_item(build_dynamo_json(dataset))
    actual_datasets = DatasetsTable(resource_name_prefix).list(owner=expected_datasets[0].owner_account_id)
    assert {dataset.id for dataset in actual_datasets} == {dataset.id for dataset in expected_datasets}


def test_list_across_hubs(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    expected_datasets = [build_dataset(name=f"dataset{i}") for i in range(3)]
    foreign_hub_datasets = [
        build_dataset(
            name=f"dataset{i}",
            hub=Builder.get_random_element(
                to_choose_from=list(Hub), exclude={dataset.hub for dataset in expected_datasets}
            ),
        )
        for i in range(2)
    ]
    with mock_datasets_dynamo_table.batch_writer() as batch:
        for dataset in expected_datasets:
            batch.put_item(build_dynamo_json(dataset))
        for dataset in foreign_hub_datasets:
            batch.put_item(build_dynamo_json(dataset))
    datasets = DatasetsTable(resource_name_prefix).list()
    assert {dataset.id for dataset in datasets} == {dataset.id for dataset in expected_datasets + foreign_hub_datasets}


def test_create(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    dataset = build_dataset(lineage=build_dataset_lineage({build_dataset_id() for _ in range(3)}))
    DatasetsTable(resource_name_prefix).create(dataset)
    assert mock_datasets_dynamo_table.scan()["Items"] == [build_dynamo_json(dataset)]


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_create_exists_already(resource_name_prefix: str) -> None:
    table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    table.create(dataset)
    with pytest.raises(DatasetAlreadyExists):
        table.create(dataset)


def test_delete_existing(mock_datasets_dynamo_table: Table, resource_name_prefix: str) -> None:
    dataset1 = build_dataset(name="new1")
    dataset2 = build_dataset(name="new2")
    table = DatasetsTable(resource_name_prefix)
    table.create(dataset1)
    table.create(dataset2)
    assert len(mock_datasets_dynamo_table.scan()["Items"]) == 2
    table.delete(dataset1.id)
    assert len(mock_datasets_dynamo_table.scan()["Items"]) == 1


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_delete_nonexisting(resource_name_prefix: str) -> None:
    with pytest.raises(DatasetNotFound):
        DatasetsTable(resource_name_prefix).delete(build_dataset_id())


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_update_no_field(resource_name_prefix: str) -> None:
    dt_now = datetime.now()
    old_dataset = build_dataset()
    datasets_table = DatasetsTable(resource_name_prefix)
    datasets_table.create(old_dataset)

    updated_dataset = datasets_table.update(
        old_dataset,
        update_date=dt_now,
    )
    assert updated_dataset == old_dataset
    assert datasets_table.get(old_dataset.id) == old_dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_update_all_fields(resource_name_prefix: str) -> None:
    dt_now = datetime.now()
    old_dataset = build_dataset()
    datasets_table = DatasetsTable(resource_name_prefix)
    datasets_table.create(old_dataset)
    expected_dataset = replace(
        old_dataset,
        business_object=Builder.get_random_element(
            to_choose_from=set(BusinessObject), exclude={old_dataset.business_object}
        ),
        confidentiality=build_confidentiality(),
        contains_pii=not old_dataset.contains_pii,
        description=Builder.build_random_string(),
        documentation=Builder.build_random_string(),
        external_links=[build_external_link() for _ in range(3)],
        friendly_name=Builder.build_random_string(),
        hub_visibility={build_hub() for _ in range(3)},
        ingest_frequency=build_ingest_frequency(),
        labels={Builder.build_random_string()},
        lineage=build_dataset_lineage({build_dataset_id() for _ in range(3)}),
        purpose={build_dataset_purpose()},
        quality_score=randint(0, 100),
        retention_period=build_retention_period(),
        source_identifier=Builder.build_random_string(),
        status=Builder.get_random_element(to_choose_from=set(DatasetStatus), exclude={old_dataset.status}),
        support_group=Builder.build_random_string(),
        support_level=build_support_level(),
        tags={Builder.build_random_string(): Builder.build_random_string()},
        update_date=dt_now,
    )

    updated_dataset = datasets_table.update(
        old_dataset,
        business_object=expected_dataset.business_object,
        confidentiality=expected_dataset.confidentiality,
        contains_pii=expected_dataset.contains_pii,
        description=expected_dataset.description,
        documentation=expected_dataset.documentation,
        external_links=expected_dataset.external_links,
        friendly_name=expected_dataset.friendly_name,
        hub_visibility=expected_dataset.hub_visibility,
        ingest_frequency=expected_dataset.ingest_frequency,
        labels=expected_dataset.labels,
        lineage=expected_dataset.lineage,
        purpose=expected_dataset.purpose,
        quality_score=expected_dataset.quality_score,
        retention_period=expected_dataset.retention_period,
        source_identifier=expected_dataset.source_identifier,
        status=expected_dataset.status,
        support_group=expected_dataset.support_group,
        support_level=expected_dataset.support_level,
        tags=expected_dataset.tags,
        update_date=dt_now,
    )
    assert updated_dataset == expected_dataset
    assert datasets_table.get(old_dataset.id) == expected_dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
@pytest.mark.parametrize(
    "attribute",
    sorted(set(get_attributes_of_type(_DatasetModel, UnicodeAttribute)) & set(get_nullable_attributes(_DatasetModel))),
)
def test_update_optional_string_field_to_empty_string(resource_name_prefix: str, attribute: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    if attribute not in inspect.signature(datasets_table.update).parameters:
        # field cannot be updated
        return
    dt_now = datetime.now()
    old_dataset = build_dataset()

    datasets_table.create(old_dataset)

    updated_dataset = datasets_table.update(old_dataset, update_date=dt_now, **{attribute: ""})  # type: ignore

    expected_dataset = replace(
        old_dataset,
        update_date=dt_now,
        **{attribute: None},
    )
    assert updated_dataset == expected_dataset
    assert datasets_table.get(old_dataset.id) == expected_dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
@pytest.mark.parametrize("attribute", sorted(get_attributes_of_type(_DatasetModel, UnicodeSetAttribute)))
def test_update_set_attribute_to_empty_set(resource_name_prefix: str, attribute: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    if attribute not in inspect.signature(datasets_table.update).parameters:
        # field cannot be updated
        return
    dt_now = datetime.now()
    old_dataset = build_dataset()
    datasets_table.create(old_dataset)

    updated_dataset = datasets_table.update(
        dataset=old_dataset,
        update_date=dt_now,
        **{attribute: set()},  # type: ignore
    )

    expected_dataset = replace(
        old_dataset,
        update_date=dt_now,
        **{attribute: set()},
    )
    assert updated_dataset == expected_dataset
    assert datasets_table.get(old_dataset.id) == expected_dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_update_dataset_that_does_not_exist(resource_name_prefix: str) -> None:
    dt_now = datetime.now()
    dataset = build_dataset()
    datasets_table = DatasetsTable(resource_name_prefix)
    with pytest.raises(DatasetNotFound):
        datasets_table.update(
            dataset,
            tags={},
            description="some description",
            friendly_name="someone friendly",
            update_date=dt_now,
            contains_pii=False,
        )


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_add_dataset_permissions(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    datasets_table.create(dataset)

    new_permission = build_dataset_account_permission()
    expected_dataset = replace(dataset, permissions=frozenset([*dataset.permissions, new_permission]))

    with datasets_table.update_permissions_transaction(
        dataset_id=dataset.id,
        permission=new_permission,
        action=DatasetAccountPermissionAction.add,
    ) as new_dataset:
        assert new_dataset == expected_dataset
        assert datasets_table.get(dataset.id) == expected_dataset

    assert datasets_table.get(dataset.id) == expected_dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_remove_dataset_permissions(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    datasets_table.create(dataset)

    permission_to_remove = next(iter(dataset.permissions))
    expected_dataset = replace(
        dataset, permissions=frozenset({p for p in dataset.permissions if p != permission_to_remove})
    )

    with datasets_table.update_permissions_transaction(
        dataset_id=dataset.id,
        permission=permission_to_remove,
        action=DatasetAccountPermissionAction.remove,
    ) as new_dataset:
        assert new_dataset == expected_dataset
        assert datasets_table.get(dataset.id) == expected_dataset

    assert datasets_table.get(dataset.id) == expected_dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_update_permissions_of_nonexisting_dataset(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    with pytest.raises(DatasetNotFound):
        with datasets_table.update_permissions_transaction(
            dataset_id=build_dataset_id(),
            permission=build_dataset_account_permission(),
            action=DatasetAccountPermissionAction.add,
        ):
            raise AssertionError("Should not have reached this line")


# pylint: disable=protected-access,no-member
@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_update_permissions_inconsistent(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset(permissions=frozenset({build_dataset_account_permission()}))
    datasets_table.create(dataset)
    # patch pynamo to return a DatasetModel inconsistent with the table's content to simulate concurrent changes
    dataset_model = datasets_table._model.from_dataset(dataset)
    dataset_model.permissions = []
    datasets_table._model.get = Mock(return_value=dataset_model)  # type: ignore
    with pytest.raises(DatasetUpdateInconsistent):
        with datasets_table.update_permissions_transaction(
            dataset_id=dataset.id,
            permission=build_dataset_account_permission(),
            action=DatasetAccountPermissionAction.add,
        ):
            raise AssertionError("Should not have reached this line")


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
@pytest.mark.parametrize("action", DatasetAccountPermissionAction)
def test_update_permissions_rollback(resource_name_prefix: str, action: DatasetAccountPermissionAction) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    datasets_table.create(dataset)

    permission = (
        next(iter(dataset.permissions))
        if action is DatasetAccountPermissionAction.remove
        else build_dataset_account_permission()
    )
    exception = Exception("Something goes wrong")
    with assert_raises(exception):
        with datasets_table.update_permissions_transaction(
            dataset_id=dataset.id,
            permission=permission,
            action=action,
        ):
            raise exception

    assert datasets_table.get(dataset.id) == dataset


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
@pytest.mark.parametrize("action", DatasetAccountPermissionAction)
def test_update_permissions_rollback_does_not_affect_other_changes(
    resource_name_prefix: str, action: DatasetAccountPermissionAction
) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    datasets_table.create(dataset)

    permission = (
        next(iter(dataset.permissions))
        if action is DatasetAccountPermissionAction.remove
        else build_dataset_account_permission()
    )
    other_permission_added_in_the_meantime = build_dataset_account_permission()
    exception = Exception("Something goes wrong")
    with assert_raises(exception):
        with datasets_table.update_permissions_transaction(
            dataset_id=dataset.id,
            permission=permission,
            action=action,
        ):
            with datasets_table.update_permissions_transaction(
                dataset_id=dataset.id,
                permission=other_permission_added_in_the_meantime,
                action=DatasetAccountPermissionAction.add,
            ):
                # this concurrent change completes successfully and should not be reverted
                pass
            raise exception

    assert datasets_table.get(dataset.id) == replace(
        dataset, permissions=frozenset([*dataset.permissions, other_permission_added_in_the_meantime])
    )


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_rollback_permissions_action_raises_original_exception(resource_name_prefix: str) -> None:
    class CustomException(Exception):
        pass

    datasets_table = DatasetsTable(resource_name_prefix)
    with patch.object(DatasetsTable, "_update_permissions") as mocked_update_permissions:
        mocked_update_permissions.side_effect = CustomException

        with pytest.raises(CustomException):
            datasets_table.rollback_permissions_action(
                dataset_id=Builder.build_random_string(),
                permission=build_dataset_account_permission(),
                action_to_rollback=DatasetAccountPermissionAction.add,
            )


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_exists_true(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    datasets_table.create(dataset)

    assert datasets_table.exists(dataset.id)


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_exists_false(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)

    assert not datasets_table.exists(build_dataset_id())


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_batch_get_empty_list(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    datasets_table.create(dataset)

    assert datasets_table.batch_get([]) == []


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_batch_get_no_item_found(resource_name_prefix: str) -> None:
    datasets_table = DatasetsTable(resource_name_prefix)
    dataset = build_dataset()
    datasets_table.create(dataset)

    assert datasets_table.batch_get([build_dataset_id()]) == []


@pytest.mark.usefixtures("mock_datasets_dynamo_table")
def test_batch_get_multiple_datasets(resource_name_prefix: str) -> None:
    datasets = [build_dataset() for _ in range(3)]
    datasets_table = DatasetsTable(resource_name_prefix)
    for dataset in datasets:
        datasets_table.create(dataset)

    assert_count_equal(datasets_table.batch_get([dataset.id for dataset in datasets]), datasets)


class TestGetDatasetsIterator:
    @pytest.fixture(autouse=True)
    def dynamo_setup(self, resource_name_prefix: str, mock_datasets_dynamo_table: Table) -> None:
        self.mock_datasets_dynamo_table = mock_datasets_dynamo_table
        self.datasets_table = DatasetsTable(resource_name_prefix)

    def test_iterate_empty_dynamo(
        self,
    ) -> None:
        iterator = self.datasets_table.get_datasets_iterator()
        with pytest.raises(StopIteration):
            next(iterator)

    def test_iterate_all(self) -> None:
        expected_datasets = [build_dataset() for _ in range(5)]
        self._fill_dynamo(datasets=expected_datasets)

        iterator = self.datasets_table.get_datasets_iterator()

        assert_count_equal(list(iterator), expected_datasets)
        assert iterator.last_evaluated_key is None

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_iterate_filter_by_hub(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        hub, other_hub = Builder.choose_without_repetition(list(Hub), 2)
        expected_datasets = [build_dataset(hub=hub) for _ in range(5)]
        other_datasets = [build_dataset(hub=other_hub) for _ in range(5)]
        self._fill_dynamo(datasets=expected_datasets + other_datasets)

        iterator = self.datasets_table.get_datasets_iterator(hub=hub)
        datasets = list(iterator)

        assert_count_equal(datasets, expected_datasets)
        assert iterator.last_evaluated_key is None

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_iterate_filter_by_owner(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        owner = build_account_id()
        other_owner = build_account_id()
        expected_datasets = [build_dataset(owner_account_id=owner) for _ in range(5)]
        other_datasets = [build_dataset(owner_account_id=other_owner) for _ in range(5)]
        self._fill_dynamo(datasets=expected_datasets + other_datasets)

        iterator = self.datasets_table.get_datasets_iterator(owner=owner)
        datasets = list(iterator)

        assert_count_equal(datasets, expected_datasets)
        assert iterator.last_evaluated_key is None

    def test_last_evaluated_mid_iteration_not_none(self) -> None:
        expected_datasets = [build_dataset() for _ in range(5)]
        self._fill_dynamo(datasets=expected_datasets)

        iterator = self.datasets_table.get_datasets_iterator()

        iteration_over = False
        for _ in iterator:
            assert not iteration_over
            iteration_over = iterator.last_evaluated_key is None

    def test_iterate_with_last_evaluated_key_resumes(self) -> None:
        expected_datasets = [build_dataset() for _ in range(5)]
        self._fill_dynamo(datasets=expected_datasets)
        cutoff = randint(1, len(expected_datasets) - 1)
        first_iterator = self.datasets_table.get_datasets_iterator()
        for _ in range(cutoff):
            next(first_iterator)

        second_iterator = self.datasets_table.get_datasets_iterator(
            last_evaluated_key=first_iterator.last_evaluated_key
        )

        for first_item, second_item in itertools.zip_longest(first_iterator, second_iterator):
            assert first_item == second_item
            assert first_iterator.last_evaluated_key == second_iterator.last_evaluated_key

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_iterate_with_last_evaluated_key_and_filter(
        self, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:
        hub, other_hub = Builder.choose_without_repetition(list(Hub), 2)
        expected_datasets = [build_dataset(hub=hub) for _ in range(5)]
        other_datasets = [build_dataset(hub=other_hub) for _ in range(5)]
        self._fill_dynamo(datasets=expected_datasets + other_datasets)
        cutoff = randint(1, len(expected_datasets) - 1)
        first_iterator = self.datasets_table.get_datasets_iterator(hub=hub)
        for _ in range(cutoff):
            next(first_iterator)

        second_iterator = self.datasets_table.get_datasets_iterator(
            last_evaluated_key=first_iterator.last_evaluated_key, hub=hub
        )

        for first_item, second_item in itertools.zip_longest(first_iterator, second_iterator):
            assert first_item == second_item
            assert first_iterator.last_evaluated_key == second_iterator.last_evaluated_key

    def _fill_dynamo(self, datasets: Collection[Dataset]) -> None:
        datasets_shuffled: Sequence[Dataset] = sample(list(datasets), len(datasets))
        with self.mock_datasets_dynamo_table.batch_writer() as batch:
            for dataset in datasets_shuffled:
                batch.put_item(build_dynamo_json(dataset))


def build_dynamo_json(dataset: Dataset) -> Dict[str, Any]:
    result = {
        "id": dataset.id,
        "business_object": dataset.business_object.value,
        "hub": dataset.hub.value,
        "confidentiality": dataset.confidentiality.value,
        "contains_pii": dataset.contains_pii,
        "creator_user_id": dataset.creator_user_id,
        "creation_date": dataset.creation_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "description": dataset.description,
        "friendly_name": dataset.friendly_name,
        "labels": dataset.labels,
        "layer": dataset.layer.value,
        "lineage": {"upstream": dataset.lineage.upstream} if dataset.lineage.upstream else {},
        "name": dataset.name,
        "permissions": [
            {
                "account_id": p.account_id,
                "region": p.region.value,
                "stage": p.stage.value,
                "sync_type": p.sync_type.value,
            }
            for p in sorted(dataset.permissions, key=str)
        ],
        "preview_available": dataset.preview_available,
        "tags": dataset.tags,
        "owner_account_id": dataset.owner_account_id,
        "update_date": dataset.update_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "status": dataset.status.value if dataset.status else None,
        "external_links": [
            {
                "url": link.url,
                "name": link.name,
                "type": link.type.value,
            }
            for link in dataset.external_links
        ],
    }
    if dataset.documentation:
        result["documentation"] = dataset.documentation
    if dataset.ingest_frequency:
        result["ingest_frequency"] = dataset.ingest_frequency.value
    if dataset.retention_period:
        result["retention_period"] = dataset.retention_period.value
    if dataset.source_identifier:
        result["source_identifier"] = dataset.source_identifier
    if dataset.support_level:
        result["support_level"] = dataset.support_level.value
    if dataset.purpose:
        result["purpose"] = {purpose.value for purpose in dataset.purpose}
    if dataset.hub_visibility:
        result["hub_visibility"] = {hub.value for hub in dataset.hub_visibility}
    if dataset.support_group is not None:
        result["support_group"] = dataset.support_group
    if dataset.quality_score is not None:
        result["quality_score"] = dataset.quality_score
    return result
