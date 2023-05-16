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

from asserts import assert_count_equal
from cdh_core_api.config_test import build_config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.phone_book import PhoneBook
from cdh_core_api.services.response_dataset_builder import ResponseDatasetBuilder

from cdh_core.entities.dataset import ResponseDataset
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.dataset_participants_test import build_dataset_participants
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.request_test import build_requester_identity
from cdh_core_dev_tools.testing.builder import Builder


class TestBuildResponseDatasetsIncludingPermissions:
    def setup_method(self) -> None:
        self.config = build_config(use_authorization=True)
        self.datasets = [build_dataset() for _ in range(3)]
        self.phone_book = Mock(PhoneBook)
        self.phone_book.is_authorization_role.return_value = False
        self.dataset_participants = {dataset.id: build_dataset_participants() for dataset in self.datasets}
        self.authorization_api = Mock(AuthorizationApi)
        self.authorization_api.get_datasets_participants.return_value = self.dataset_participants
        self.requester_identity = build_requester_identity()
        self.response_dataset_builder = ResponseDatasetBuilder(
            config=self.config,
            authorization_api=self.authorization_api,
            phone_book=self.phone_book,
        )

    def test_use_participants_from_authorization(self) -> None:
        response_datasets = self.response_dataset_builder(
            datasets=self.datasets,
            requester_identity=self.requester_identity,
        )
        assert all(isinstance(response_dataset, ResponseDataset) for response_dataset in response_datasets)
        assert_count_equal(
            response_datasets,
            [ResponseDataset.from_dataset(dataset, self.dataset_participants[dataset.id]) for dataset in self.datasets],
        )

    def test_without_authorization_does_not_call_auth_api(self) -> None:
        config = build_config(use_authorization=False)
        response_dataset_builder = ResponseDatasetBuilder(
            config=config,
            authorization_api=self.authorization_api,
            phone_book=self.phone_book,
        )

        response_datasets = response_dataset_builder(
            datasets=self.datasets,
            requester_identity=self.requester_identity,
        )

        assert all(isinstance(response_dataset, ResponseDataset) for response_dataset in response_datasets)
        assert_count_equal(
            response_datasets,
            [ResponseDataset.from_dataset(dataset, DatasetParticipants([], [])) for dataset in self.datasets],
        )
        self.authorization_api.get_datasets_participants.assert_not_called()

    def test_empty_participants_if_requester_is_authorization(self) -> None:
        self.phone_book.is_authorization_role.return_value = True

        response_datasets = self.response_dataset_builder(
            datasets=self.datasets,
            requester_identity=self.requester_identity,
        )

        assert all(isinstance(response_dataset, ResponseDataset) for response_dataset in response_datasets)
        assert_count_equal(
            response_datasets,
            [ResponseDataset.from_dataset(dataset, DatasetParticipants([], [])) for dataset in self.datasets],
        )
        self.authorization_api.get_datasets_participants.assert_not_called()
        self.phone_book.is_authorization_role.assert_called_once_with(self.requester_identity.arn)

    def test_missing_participants_response(self) -> None:
        dataset_with_participants = Builder.get_random_element({dataset.id for dataset in self.datasets})
        participants_response = {dataset_with_participants: self.dataset_participants[dataset_with_participants]}
        self.authorization_api.get_datasets_participants.return_value = participants_response

        response_datasets = self.response_dataset_builder(
            datasets=self.datasets,
            requester_identity=self.requester_identity,
        )

        assert all(isinstance(response_dataset, ResponseDataset) for response_dataset in response_datasets)
        expected_participants = {
            dataset.id: DatasetParticipants([], []) for dataset in self.datasets
        } | participants_response
        assert_count_equal(
            response_datasets,
            [ResponseDataset.from_dataset(dataset, expected_participants[dataset.id]) for dataset in self.datasets],
        )
        self.authorization_api.get_datasets_participants.assert_called_once_with(
            dataset_ids=[dataset.id for dataset in self.datasets]
        )
