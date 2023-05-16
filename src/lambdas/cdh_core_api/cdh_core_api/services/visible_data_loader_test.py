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
# pylint: disable=use-implicit-booleaness-not-comparison
import random
from contextlib import ExitStack
from random import randint
from typing import Union
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.base import DynamoItemIterator
from cdh_core_api.catalog.base_test import build_last_evaluated_key
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.filter_packages_table import FilterPackageNotFound
from cdh_core_api.catalog.filter_packages_table import FilterPackagesTable
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.services.visibility_check import VisibilityCheck
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.filter_package_test import build_filter_package
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.resource_test import build_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import InternalError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class VisibleDataLoaderTestCase:
    def setup_method(self) -> None:
        self.datasets_table = Mock(DatasetsTable)
        self.resources_table = Mock(ResourcesTable)
        self.accounts_table = Mock(AccountsTable)
        self.filter_packages_table = Mock(FilterPackagesTable)
        self.visibility_check = Mock(VisibilityCheck)
        self.visible_data_loader = VisibleDataLoader[Account, S3Resource, GlueSyncResource](
            datasets_table=self.datasets_table,
            resources_table=self.resources_table,
            accounts_table=self.accounts_table,
            filter_packages_table=self.filter_packages_table,
            visibility_check=self.visibility_check,
        )


class TestGetAccount(VisibleDataLoaderTestCase):
    def test_visible(self) -> None:
        account = build_account()
        self.accounts_table.get.return_value = account
        self.visibility_check.get_account_visibility_check.return_value.return_value = True

        assert self.visible_data_loader.get_account(account.id) == account

        self.visibility_check.get_account_visibility_check.assert_called_once_with(batch=False)
        self.visibility_check.get_account_visibility_check.return_value.assert_called_once_with(account)

    def test_invisible(self) -> None:
        self.visibility_check.get_account_visibility_check.return_value.return_value = False

        with pytest.raises(AccountNotFound):
            self.visible_data_loader.get_account(build_account_id())

        self.accounts_table.get.assert_called_once()


class TestGetAccounts(VisibleDataLoaderTestCase):
    def test_filter_visible_accounts(self) -> None:
        number_of_visible_accounts = 10
        number_of_pages = 3
        invisible_accounts = [build_account() for _ in range(number_of_visible_accounts)]
        visible_accounts = [build_account() for _ in range(number_of_visible_accounts * number_of_pages)]
        all_accounts = invisible_accounts + visible_accounts
        self.accounts_table.get_accounts_iterator.return_value = DynamoItemIterator(iter(all_accounts), lambda: None)
        self.visibility_check.get_account_visibility_check.return_value.side_effect = lambda account: account.id in [
            visible_account.id for visible_account in visible_accounts
        ]

        assert (
            self.visible_data_loader.get_accounts(number_of_visible_accounts, None)[0]
            == visible_accounts[:number_of_visible_accounts]
        )

        self.visibility_check.get_account_visibility_check.assert_called_once_with(batch=True)
        self.visibility_check.get_account_visibility_check.return_value.assert_has_calls(
            calls=[call(account) for account in visible_accounts[:number_of_visible_accounts]],
            any_order=True,
        )

    def test_stop_consuming_iterator_when_limit_reached(self) -> None:
        accounts = [build_account() for _ in range(10)]
        iterator = DynamoItemIterator(items=iter(accounts), get_last_evaluated_key=Mock())

        self.accounts_table.get_accounts_iterator.return_value = iterator
        self.visibility_check.get_account_visibility_check.return_value.side_effect = lambda _: True
        limit = randint(1, len(accounts) - 1)

        self.visible_data_loader.get_accounts(limit=limit, last_evaluated_key=build_last_evaluated_key())

        remaining_accounts = list(iterator)
        assert len(remaining_accounts) == len(accounts) - limit

    @pytest.mark.parametrize("reach_limit", [False, True])
    def test_last_evaluated_key_retrieved_after_iterator_consumed(self, reach_limit: bool) -> None:
        accounts = [build_account() for _ in range(10)]
        visible_accounts = sorted(random.sample(accounts, 5), key=accounts.index)
        raw_accounts_iter = iter(accounts)
        iterator = MagicMock()
        iterator.last_evaluated_key = build_last_evaluated_key()

        def get_next() -> Union[Account]:
            iterator.last_evaluated_key = build_last_evaluated_key()
            item = next(raw_accounts_iter)
            return item

        iterator.__iter__.return_value.__next__.side_effect = get_next

        self.accounts_table.get_accounts_iterator.return_value = iterator
        self.visibility_check.get_account_visibility_check.return_value.side_effect = (
            lambda account: account in visible_accounts
        )
        limit = len(visible_accounts) + (-1 if reach_limit else +1)

        _, last_evaluated_key = self.visible_data_loader.get_accounts(
            limit=limit, last_evaluated_key=build_last_evaluated_key()
        )
        assert last_evaluated_key == iterator.last_evaluated_key

    def test_filter_visible_accounts_reach_limit(self) -> None:
        accounts = [build_account() for _ in range(10)]
        visible_accounts = sorted(random.sample(accounts, 5), key=accounts.index)
        self.accounts_table.get_accounts_iterator.return_value = DynamoItemIterator(
            items=iter(accounts), get_last_evaluated_key=Mock()
        )
        self.visibility_check.get_account_visibility_check.return_value.side_effect = (
            lambda account: account in visible_accounts
        )
        limit = randint(1, len(visible_accounts) - 1)

        result, _ = self.visible_data_loader.get_accounts(limit=limit, last_evaluated_key=build_last_evaluated_key())
        assert result == visible_accounts[:limit]
        max_index_reached = accounts.index(result[-1])
        self.visibility_check.get_account_visibility_check.return_value.assert_has_calls(
            calls=[call(account) for account in accounts[: max_index_reached + 1]],
            any_order=True,
        )
        assert self.visibility_check.get_account_visibility_check.return_value.call_count == max_index_reached + 1

    def test_empty_account_iterator(self) -> None:
        expected_last_evaluated_key = build_last_evaluated_key()
        self.accounts_table.get_accounts_iterator.return_value = DynamoItemIterator(
            items=iter(()), get_last_evaluated_key=lambda: expected_last_evaluated_key
        )
        result, last_evaluated_key = self.visible_data_loader.get_accounts(
            limit=1, last_evaluated_key=build_last_evaluated_key()
        )

        assert result == []
        assert last_evaluated_key == expected_last_evaluated_key

    def test_apply_last_evaluated_key(self) -> None:
        self.accounts_table.get_accounts_iterator.return_value = DynamoItemIterator(
            items=iter(()), get_last_evaluated_key=build_last_evaluated_key
        )
        last_evaluated_key = build_last_evaluated_key()

        self.visible_data_loader.get_accounts(limit=randint(1, 10), last_evaluated_key=last_evaluated_key)

        assert self.accounts_table.get_accounts_iterator.call_args.kwargs["last_evaluated_key"] == last_evaluated_key


class TestGetDataset(VisibleDataLoaderTestCase):
    def test_visible(self) -> None:
        dataset = build_dataset()
        self.datasets_table.get.return_value = dataset
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = True
        self.visibility_check.get_dataset_visibility_check.return_value.return_value = True

        assert self.visible_data_loader.get_dataset(dataset.id) == dataset

        self.visibility_check.get_dataset_id_visibility_check.assert_called_once_with(batch=False)
        self.visibility_check.get_dataset_id_visibility_check.return_value.assert_called_once_with(dataset.id)

    def test_invisible(self) -> None:
        dataset = build_dataset()
        self.datasets_table.get.return_value = dataset
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = False

        with pytest.raises(DatasetNotFound):
            self.visible_data_loader.get_dataset(dataset.id)

        self.datasets_table.get.assert_not_called()


class TestGetDatasets(VisibleDataLoaderTestCase):
    def test_filter_visible_datasets_do_not_reach_limit(self) -> None:
        datasets = [build_dataset() for _ in range(10)]
        visible_datasets = sorted(random.sample(datasets, 5), key=datasets.index)

        self.datasets_table.get_datasets_iterator.return_value = DynamoItemIterator(
            items=iter(datasets), get_last_evaluated_key=Mock()
        )
        self.visibility_check.get_dataset_visibility_check.return_value.side_effect = (
            lambda dataset: dataset in visible_datasets
        )
        hub = build_hub()
        result, _ = self.visible_data_loader.get_datasets(
            hub, limit=len(visible_datasets) + 1, last_evaluated_key=build_last_evaluated_key()
        )
        assert result == visible_datasets

        self.visibility_check.get_dataset_visibility_check.assert_called_once_with(batch=True, hub=hub)
        self.visibility_check.get_dataset_visibility_check.return_value.assert_has_calls(
            calls=[call(dataset) for dataset in datasets],
            any_order=True,
        )

    def test_stop_consuming_iterator_when_limit_reached(self) -> None:
        datasets = [build_dataset() for _ in range(10)]
        iterator = DynamoItemIterator(items=iter(datasets), get_last_evaluated_key=Mock())

        self.datasets_table.get_datasets_iterator.return_value = iterator
        self.visibility_check.get_dataset_visibility_check.return_value.side_effect = lambda _: True
        limit = randint(1, len(datasets) - 1)

        self.visible_data_loader.get_datasets(build_hub(), limit=limit, last_evaluated_key=build_last_evaluated_key())

        remaining_datasets = list(iterator)
        assert len(remaining_datasets) == len(datasets) - limit

    @pytest.mark.parametrize("reach_limit", [False, True])
    def test_last_evaluated_key_retrieved_after_iterator_consumed(self, reach_limit: bool) -> None:
        datasets = [build_dataset() for _ in range(10)]
        visible_datasets = sorted(random.sample(datasets, 5), key=datasets.index)
        raw_datasets_iter = iter(datasets)
        iterator = MagicMock()
        iterator.last_evaluated_key = build_last_evaluated_key()

        def get_next() -> Dataset:
            iterator.last_evaluated_key = build_last_evaluated_key()
            item = next(raw_datasets_iter)
            return item

        iterator.__iter__.return_value.__next__.side_effect = get_next

        self.datasets_table.get_datasets_iterator.return_value = iterator
        self.visibility_check.get_dataset_visibility_check.return_value.side_effect = (
            lambda dataset: dataset in visible_datasets
        )
        limit = len(visible_datasets) + (-1 if reach_limit else +1)

        _, last_evaluated_key = self.visible_data_loader.get_datasets(
            build_hub(), limit=limit, last_evaluated_key=build_last_evaluated_key()
        )
        assert last_evaluated_key == iterator.last_evaluated_key

    def test_filter_visible_datasets_reach_limit(self) -> None:
        datasets = [build_dataset() for _ in range(10)]
        visible_datasets = sorted(random.sample(datasets, 5), key=datasets.index)
        self.datasets_table.get_datasets_iterator.return_value = DynamoItemIterator(
            items=iter(datasets), get_last_evaluated_key=Mock()
        )
        self.visibility_check.get_dataset_visibility_check.return_value.side_effect = (
            lambda dataset: dataset in visible_datasets
        )
        limit = randint(1, len(visible_datasets) - 1)

        result, _ = self.visible_data_loader.get_datasets(
            build_hub(), limit=limit, last_evaluated_key=build_last_evaluated_key()
        )
        assert result == visible_datasets[:limit]
        max_index_reached = datasets.index(result[-1])
        self.visibility_check.get_dataset_visibility_check.return_value.assert_has_calls(
            calls=[call(dataset) for dataset in datasets[: max_index_reached + 1]],
            any_order=True,
        )
        assert self.visibility_check.get_dataset_visibility_check.return_value.call_count == max_index_reached + 1

    def test_empty_dataset_iterator(self) -> None:
        expected_last_evaluated_key = build_last_evaluated_key()
        self.datasets_table.get_datasets_iterator.return_value = DynamoItemIterator(
            items=iter(()), get_last_evaluated_key=lambda: expected_last_evaluated_key
        )
        result, last_evaluated_key = self.visible_data_loader.get_datasets(
            build_hub(), limit=1, last_evaluated_key=build_last_evaluated_key()
        )

        assert result == []
        assert last_evaluated_key == expected_last_evaluated_key

    def test_apply_last_evaluated_key(self) -> None:
        self.datasets_table.get_datasets_iterator.return_value = DynamoItemIterator(
            items=iter(()), get_last_evaluated_key=build_last_evaluated_key
        )
        last_evaluated_key = build_last_evaluated_key()

        self.visible_data_loader.get_datasets(build_hub(), limit=randint(1, 10), last_evaluated_key=last_evaluated_key)

        assert self.datasets_table.get_datasets_iterator.call_args.kwargs["last_evaluated_key"] == last_evaluated_key


class TestGetDatasetsCrossHub(VisibleDataLoaderTestCase):
    def test_filter_visible_datasets(self) -> None:
        visible_dataset = build_dataset()
        invisible_dataset = build_dataset()
        catalog_response = Mock()
        self.datasets_table.batch_get.return_value = catalog_response
        self.visibility_check.get_dataset_id_visibility_check.return_value.side_effect = (
            lambda dataset_id: dataset_id == visible_dataset.id
        )

        assert (
            self.visible_data_loader.get_datasets_cross_hub([visible_dataset.id, invisible_dataset.id])
            == catalog_response
        )

        self.visibility_check.get_dataset_id_visibility_check.assert_called_once_with(
            batch=True, dataset_ids=[visible_dataset.id, invisible_dataset.id]
        )
        self.visibility_check.get_dataset_id_visibility_check.return_value.assert_has_calls(
            calls=[call(visible_dataset.id), call(invisible_dataset.id)],
            any_order=True,
        )
        self.datasets_table.batch_get.assert_called_once_with([visible_dataset.id])


class TestGetResource(VisibleDataLoaderTestCase):
    def test_visible(self) -> None:
        resource = build_resource()
        self.resources_table.get.return_value = resource
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = True

        assert (
            self.visible_data_loader.get_resource(
                resource_type=resource.type,
                dataset_id=resource.dataset_id,
                stage=resource.stage,
                region=resource.region,
            )
            == resource
        )

        self.visibility_check.get_dataset_id_visibility_check.assert_called_once_with(batch=False)
        self.visibility_check.get_dataset_id_visibility_check.return_value.assert_called_once_with(resource.dataset_id)

    def test_when_invisible(self) -> None:
        resource = build_resource()
        self.resources_table.get.return_value = resource
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = False

        with pytest.raises(ResourceNotFound):
            self.visible_data_loader.get_resource(
                resource_type=resource.type,
                dataset_id=resource.dataset_id,
                stage=resource.stage,
                region=resource.region,
            )

        self.resources_table.get.assert_not_called()


class TestGetResourceFromBucketName(VisibleDataLoaderTestCase):
    def test_get_dataset_id_from_bucket_name(self) -> None:
        dataset = build_dataset()
        s3_resource = build_s3_resource(dataset=dataset)
        bucket_name = s3_resource.name

        assert self.visible_data_loader.get_dataset_id_from_bucket_name(bucket_name) == dataset.id

    def find_s3_resource_in_dataset_by_bucket_name(self) -> None:
        bucket_name = Builder.build_random_string()
        dataset_id = Builder.build_random_string()
        s3_resource = build_s3_resource(arn=build_arn(service="s3", resource=bucket_name))
        self.resources_table.list_s3.return_value = [s3_resource]
        visibility_check_method = Mock(return_value=True)
        self.visibility_check.get_dataset_id_visibility_check.return_value = visibility_check_method

        resource = self.visible_data_loader.find_s3_resource_in_dataset_by_bucket_name(dataset_id, bucket_name)

        visibility_check_method.assert_called_once_with(dataset_id)
        self.resources_table.list_s3.assert_called_once_with(dataset_id)
        assert resource == s3_resource

    def test_find_s3_resource_in_dataset_by_bucket_name_not_visible(self) -> None:
        dataset_id = Builder.build_random_string()
        visibility_check_method = Mock(return_value=False)
        self.visibility_check.get_dataset_id_visibility_check.return_value = visibility_check_method

        resource = self.visible_data_loader.find_s3_resource_in_dataset_by_bucket_name(
            dataset_id, Builder.build_random_string()
        )

        visibility_check_method.assert_called_once_with(dataset_id)
        self.resources_table.list_s3.assert_not_called()
        assert resource is None

    def test_find_s3_resource_in_dataset_by_bucket_name_no_match(self) -> None:
        self.resources_table.list_s3.return_value = [build_s3_resource()]
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = True

        resource = self.visible_data_loader.find_s3_resource_in_dataset_by_bucket_name(
            Builder.build_random_string(), Builder.build_random_string()
        )

        assert resource is None

    def test_find_s3_resource_in_dataset_by_bucket_name_ambiguous(self) -> None:
        bucket_name = Builder.build_random_string()
        s3_resource = build_s3_resource(arn=build_arn(service="s3", resource=bucket_name))
        other_s3_resource = build_s3_resource(arn=build_arn(service="s3", resource=bucket_name))
        self.resources_table.list_s3.return_value = [s3_resource, other_s3_resource]
        self.visibility_check.get_dataset_id_visibility_check.return_value = Mock(return_value=True)

        with pytest.raises(InternalError):
            self.visible_data_loader.find_s3_resource_in_dataset_by_bucket_name(
                Builder.build_random_string(), bucket_name
            )

    def test_get_resource_from_bucket_name(self) -> None:
        bucket_name = Builder.build_random_string()
        dataset_id = Builder.build_random_string()
        s3_resource = build_s3_resource()

        with ExitStack() as stack:
            mock_get_dataset_id_from_bucket_name = stack.enter_context(
                patch.object(self.visible_data_loader, "get_dataset_id_from_bucket_name", return_value=dataset_id)
            )
            mock_find_s3_resource_in_dataset_by_bucket_name = stack.enter_context(
                patch.object(
                    self.visible_data_loader, "find_s3_resource_in_dataset_by_bucket_name", return_value=s3_resource
                )
            )

            resource = self.visible_data_loader.get_resource_from_bucket_name(bucket_name)

            mock_get_dataset_id_from_bucket_name.assert_called_once_with(bucket_name)
            mock_find_s3_resource_in_dataset_by_bucket_name.assert_called_once_with(dataset_id, bucket_name)
            assert resource is s3_resource

    def test_get_resource_from_bucket_name_cant_extract_dataset_id(self) -> None:
        bucket_name = Builder.build_random_string()

        with ExitStack() as stack:
            mock_get_dataset_id_from_bucket_name = stack.enter_context(
                patch.object(self.visible_data_loader, "get_dataset_id_from_bucket_name", return_value=None)
            )
            mock_find_s3_resource_in_dataset_by_bucket_name = stack.enter_context(
                patch.object(self.visible_data_loader, "find_s3_resource_in_dataset_by_bucket_name")
            )

            with pytest.raises(NotFoundError):
                self.visible_data_loader.get_resource_from_bucket_name(bucket_name)

            mock_get_dataset_id_from_bucket_name.assert_called_once_with(bucket_name)
            mock_find_s3_resource_in_dataset_by_bucket_name.assert_not_called()

    def test_get_resource_from_bucket_name_resource_not_found(self) -> None:
        bucket_name = Builder.build_random_string()
        dataset_id = Builder.build_random_string()

        with ExitStack() as stack:
            mock_get_dataset_id_from_bucket_name = stack.enter_context(
                patch.object(self.visible_data_loader, "get_dataset_id_from_bucket_name", return_value=dataset_id)
            )
            mock_find_s3_resource_in_dataset_by_bucket_name = stack.enter_context(
                patch.object(self.visible_data_loader, "find_s3_resource_in_dataset_by_bucket_name", return_value=None)
            )

            with pytest.raises(NotFoundError):
                self.visible_data_loader.get_resource_from_bucket_name(bucket_name)

            mock_get_dataset_id_from_bucket_name.assert_called_once_with(bucket_name)
            mock_find_s3_resource_in_dataset_by_bucket_name.assert_called_once_with(dataset_id, bucket_name)


class TestGetResources(VisibleDataLoaderTestCase):
    def test_filter_visible_resources_do_not_reach_limit(self) -> None:
        resources = [build_resource() for _ in range(10)]
        visible_resources = sorted(random.sample(resources, 5), key=resources.index)

        self.resources_table.get_resources_iterator.return_value = DynamoItemIterator(
            items=iter(resources), get_last_evaluated_key=Mock()
        )
        self.visibility_check.get_resource_visibility_check.return_value.side_effect = (
            lambda resource: resource in visible_resources
        )
        hub = build_hub()
        result, _ = self.visible_data_loader.get_resources(
            hub, limit=len(visible_resources) + 1, last_evaluated_key=build_last_evaluated_key()
        )
        assert result == visible_resources

        self.visibility_check.get_resource_visibility_check.assert_called_once_with(batch=True, hub=hub)
        self.visibility_check.get_resource_visibility_check.return_value.assert_has_calls(
            calls=[call(resource) for resource in resources],
            any_order=True,
        )

    def test_stop_consuming_iterator_when_limit_reached(self) -> None:
        resources = [build_resource() for _ in range(10)]
        iterator = DynamoItemIterator(items=iter(resources), get_last_evaluated_key=Mock())

        self.resources_table.get_resources_iterator.return_value = iterator
        self.visibility_check.get_resource_visibility_check.return_value.side_effect = lambda _: True
        limit = randint(1, len(resources) - 1)

        self.visible_data_loader.get_resources(build_hub(), limit=limit, last_evaluated_key=build_last_evaluated_key())

        remaining_resources = list(iterator)
        assert len(remaining_resources) == len(resources) - limit

    @pytest.mark.parametrize("reach_limit", [False, True])
    def test_last_evaluated_key_retrieved_after_iterator_consumed(self, reach_limit: bool) -> None:
        resources = [build_resource() for _ in range(10)]
        visible_resources = sorted(random.sample(resources, 5), key=resources.index)
        raw_resources_iter = iter(resources)
        iterator = MagicMock()
        iterator.last_evaluated_key = build_last_evaluated_key()

        def get_next() -> Union[S3Resource, GlueSyncResource]:
            iterator.last_evaluated_key = build_last_evaluated_key()
            item = next(raw_resources_iter)
            return item

        iterator.__iter__.return_value.__next__.side_effect = get_next

        self.resources_table.get_resources_iterator.return_value = iterator
        self.visibility_check.get_resource_visibility_check.return_value.side_effect = (
            lambda resource: resource in visible_resources
        )
        limit = len(visible_resources) + (-1 if reach_limit else +1)

        _, last_evaluated_key = self.visible_data_loader.get_resources(
            build_hub(), limit=limit, last_evaluated_key=build_last_evaluated_key()
        )
        assert last_evaluated_key == iterator.last_evaluated_key

    def test_filter_visible_resources_reach_limit(self) -> None:
        resources = [build_resource() for _ in range(10)]
        visible_resources = sorted(random.sample(resources, 5), key=resources.index)
        self.resources_table.get_resources_iterator.return_value = DynamoItemIterator(
            items=iter(resources), get_last_evaluated_key=Mock()
        )
        self.visibility_check.get_resource_visibility_check.return_value.side_effect = (
            lambda resource: resource in visible_resources
        )
        limit = randint(1, len(visible_resources) - 1)

        result, _ = self.visible_data_loader.get_resources(
            build_hub(), limit=limit, last_evaluated_key=build_last_evaluated_key()
        )
        assert result == visible_resources[:limit]
        max_index_reached = resources.index(result[-1])
        self.visibility_check.get_resource_visibility_check.return_value.assert_has_calls(
            calls=[call(resource) for resource in resources[: max_index_reached + 1]],
            any_order=True,
        )
        assert self.visibility_check.get_resource_visibility_check.return_value.call_count == max_index_reached + 1

    def test_empty_resource_iterator(self) -> None:
        expected_last_evaluated_key = build_last_evaluated_key()
        self.resources_table.get_resources_iterator.return_value = DynamoItemIterator(
            items=iter(()), get_last_evaluated_key=lambda: expected_last_evaluated_key
        )
        result, last_evaluated_key = self.visible_data_loader.get_resources(
            build_hub(), limit=1, last_evaluated_key=build_last_evaluated_key()
        )

        assert result == []
        assert last_evaluated_key == expected_last_evaluated_key

    def test_apply_last_evaluated_key(self) -> None:
        self.resources_table.get_resources_iterator.return_value = DynamoItemIterator(
            items=iter(()), get_last_evaluated_key=build_last_evaluated_key
        )
        last_evaluated_key = build_last_evaluated_key()

        self.visible_data_loader.get_resources(build_hub(), limit=randint(1, 10), last_evaluated_key=last_evaluated_key)

        assert self.resources_table.get_resources_iterator.call_args.kwargs["last_evaluated_key"] == last_evaluated_key


class TestGetHubs(VisibleDataLoaderTestCase):
    def test_get_visible_hubs(self) -> None:
        hubs = {build_hub() for _ in range(5)}
        self.visibility_check.get_hub_visibility_check.return_value.side_effect = lambda hub: hub in hubs

        assert set(self.visible_data_loader.get_hubs()) == hubs

        self.visibility_check.get_hub_visibility_check.assert_called_once_with(batch=True)
        self.visibility_check.get_hub_visibility_check.return_value.assert_has_calls(
            calls=[call(hub) for hub in Hub], any_order=True
        )


class TestGetFilterPackage(VisibleDataLoaderTestCase):
    def test_visible(self) -> None:
        filter_package = build_filter_package()
        self.filter_packages_table.get.return_value = filter_package
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = True

        assert (
            self.visible_data_loader.get_filter_package(
                dataset_id=filter_package.dataset_id,
                stage=filter_package.stage,
                region=filter_package.region,
                package_id=filter_package.id,
            )
            == filter_package
        )

        self.visibility_check.get_dataset_id_visibility_check.assert_called_once_with(batch=False)
        self.visibility_check.get_dataset_id_visibility_check.return_value.assert_called_once_with(
            filter_package.dataset_id
        )

    def test_when_invisible(self) -> None:
        filter_package = build_filter_package()
        self.filter_packages_table.get.return_value = filter_package
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = False

        with pytest.raises(FilterPackageNotFound):
            self.visible_data_loader.get_filter_package(
                dataset_id=filter_package.dataset_id,
                stage=filter_package.stage,
                region=filter_package.region,
                package_id=filter_package.id,
            )

        self.filter_packages_table.get.assert_not_called()


class TestGetFilterPackages(VisibleDataLoaderTestCase):
    def test_visible(self) -> None:
        hub = build_hub()
        dataset_id = build_dataset(hub=hub).id
        stage = build_stage()
        region = build_region()
        filter_packages = [
            build_filter_package(
                hub=hub,
                dataset_id=dataset_id,
                stage=stage,
                region=region,
            )
            for _ in range(5)
        ]
        self.filter_packages_table.list.return_value = filter_packages
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = True

        assert (
            self.visible_data_loader.get_filter_packages(dataset_id=dataset_id, stage=stage, region=region)
            == filter_packages
        )

        self.visibility_check.get_dataset_id_visibility_check.assert_called_once_with(batch=False)
        self.visibility_check.get_dataset_id_visibility_check.return_value.assert_called_once_with(dataset_id)

    def test_when_invisible(self) -> None:
        hub = build_hub()
        dataset_id = build_dataset(hub=hub).id
        stage = build_stage()
        region = build_region()
        filter_packages = [
            build_filter_package(
                hub=hub,
                dataset_id=dataset_id,
                stage=stage,
                region=region,
            )
            for _ in range(5)
        ]
        self.filter_packages_table.list.return_value = filter_packages
        self.visibility_check.get_dataset_id_visibility_check.return_value.return_value = False

        assert self.visible_data_loader.get_filter_packages(dataset_id=dataset_id, stage=stage, region=region) == []

        self.filter_packages_table.get.assert_not_called()
