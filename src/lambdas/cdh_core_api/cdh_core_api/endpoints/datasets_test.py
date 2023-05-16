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
from datetime import datetime
from http import HTTPStatus
from random import randint
from typing import List
from typing import Optional
from unittest.mock import Mock

import cdh_core_api.endpoints
import pytest
from asserts import assert_count_equal
from cdh_core_api.bodies.datasets import UpdateDatasetBody
from cdh_core_api.bodies.datasets_test import build_new_dataset_body
from cdh_core_api.catalog.base_test import build_last_evaluated_key
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.config_test import build_config
from cdh_core_api.endpoints.datasets import DatasetCrossHubQueryParams
from cdh_core_api.endpoints.datasets import DatasetPath
from cdh_core_api.endpoints.datasets import DatasetsQuerySchema
from cdh_core_api.endpoints.datasets import get_all_datasets
from cdh_core_api.endpoints.datasets import get_dataset
from cdh_core_api.endpoints.datasets import get_datasets_cross_hub
from cdh_core_api.services.dataset_manager import DatasetManager
from cdh_core_api.services.dataset_participants_manager import DatasetParticipantsManager
from cdh_core_api.services.dataset_validator import DatasetValidator
from cdh_core_api.services.pagination_service import NextPageTokenContext
from cdh_core_api.services.pagination_service import PaginationService
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from cdh_core_api.validation.common_paths import HubPath

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.dataset import ResponseDataset
from cdh_core.entities.dataset import ResponseDatasets
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.dataset_participants_test import build_dataset_participant
from cdh_core.entities.dataset_participants_test import build_dataset_participants
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_response_dataset
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.exceptions.http import NotFoundError
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


class TestGetDataset:
    def setup_method(self) -> None:
        self.dataset = build_dataset()
        self.response_dataset = ResponseDataset.from_dataset(
            self.dataset, dataset_participants=build_dataset_participants()
        )
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.visible_data_loader.get_dataset.return_value = self.dataset
        self.response_dataset_builder = Mock()
        self.response_dataset_builder.return_value = [self.response_dataset]
        self.requester_identity = build_requester_identity()

    def test_get_dataset(self) -> None:
        response = get_dataset(
            requester_identity=self.requester_identity,
            path=DatasetPath(self.dataset.hub, self.dataset.id),
            visible_data_loader=self.visible_data_loader,
            response_dataset_builder=self.response_dataset_builder,
        )

        assert response.status_code is HTTPStatus.OK
        assert isinstance(response.body, ResponseDataset)
        assert response.body == self.response_dataset
        self.response_dataset_builder.assert_called_once_with(
            datasets=[self.dataset], requester_identity=self.requester_identity
        )

    def test_get_nonexisting_dataset(self) -> None:
        self.visible_data_loader.get_dataset.side_effect = DatasetNotFound(self.dataset.id)

        with pytest.raises(NotFoundError):
            get_dataset(
                requester_identity=self.requester_identity,
                path=DatasetPath(self.dataset.hub, self.dataset.id),
                visible_data_loader=self.visible_data_loader,
                response_dataset_builder=self.response_dataset_builder,
            )

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_get_dataset_wrong_hub_raises(
        self, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:
        other_hub = Builder.get_random_element(list(Hub), exclude={self.dataset.hub})

        with pytest.raises(NotFoundError):
            get_dataset(
                requester_identity=self.requester_identity,
                path=DatasetPath(other_hub, self.dataset.id),
                visible_data_loader=self.visible_data_loader,
                response_dataset_builder=self.response_dataset_builder,
            )


class TestGetDatasetsCrossHub:
    def setup_method(self) -> None:
        self.datasets = [build_dataset() for _ in range(3)]
        self.ids_to_query = [dataset.id for dataset in self.datasets]
        self.response_datasets = [
            ResponseDataset.from_dataset(dataset, dataset_participants=build_dataset_participants())
            for dataset in self.datasets
        ]
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.visible_data_loader.get_datasets_cross_hub.return_value = self.datasets
        self.response_dataset_builder = Mock()
        self.response_dataset_builder.return_value = self.response_datasets
        self.requester_identity = build_requester_identity()

    def test_get_datasets_cross_hub(self) -> None:
        response = get_datasets_cross_hub(
            requester_identity=self.requester_identity,
            query=DatasetCrossHubQueryParams(ids=self.ids_to_query),
            visible_data_loader=self.visible_data_loader,
            response_dataset_builder=self.response_dataset_builder,
        )

        assert response.status_code is HTTPStatus.OK
        assert isinstance(response.body, ResponseDatasets)
        assert_count_equal(response.body.datasets, self.response_datasets)
        self.visible_data_loader.get_datasets_cross_hub.assert_called_once_with(self.ids_to_query)
        self.response_dataset_builder.assert_called_once_with(
            datasets=self.datasets, requester_identity=self.requester_identity
        )


class TestGetAllDatasets:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.datasets = [build_dataset(hub=self.hub) for _ in range(5)]
        self.response_datasets = [build_response_dataset() for _ in range(5)]
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.pagination_service = Mock(PaginationService)
        self.pagination_service.decode_token.return_value = None
        self.response_dataset_builder = Mock()
        self.response_dataset_builder.return_value = self.response_datasets
        self.requester_identity = build_requester_identity()

    def test_all_datasets(self) -> None:
        self.visible_data_loader.get_datasets.return_value = (self.datasets, build_last_evaluated_key())

        response = get_all_datasets(
            path=HubPath(hub=self.hub),
            query=DatasetsQuerySchema(),
            visible_data_loader=self.visible_data_loader,
            config=build_config(),
            pagination_service=self.pagination_service,
            response_dataset_builder=self.response_dataset_builder,
            requester_identity=self.requester_identity,
        )

        assert response.body == ResponseDatasets(self.response_datasets)
        self.visible_data_loader.get_datasets.assert_called_once()
        self.response_dataset_builder.assert_called_once_with(
            datasets=self.datasets, requester_identity=self.requester_identity
        )

    def test_return_next_page_token(self) -> None:
        last_evaluated_key = build_last_evaluated_key()
        self.visible_data_loader.get_datasets.return_value = (self.datasets, last_evaluated_key)
        encrypted_token = Builder.build_random_string()
        self.pagination_service.issue_token.return_value = encrypted_token

        response = get_all_datasets(
            path=HubPath(hub=self.hub),
            query=DatasetsQuerySchema(),
            visible_data_loader=self.visible_data_loader,
            config=build_config(),
            pagination_service=self.pagination_service,
            response_dataset_builder=self.response_dataset_builder,
            requester_identity=self.requester_identity,
        )

        assert response.headers["nextPageToken"] == encrypted_token
        self.pagination_service.issue_token.assert_called_once_with(
            last_evaluated_key=last_evaluated_key,
            context=NextPageTokenContext.DATASETS,
        )

    def test_with_next_page_token_in_query(self) -> None:
        self.visible_data_loader.get_datasets.return_value = ([], None)
        next_page_token = Builder.build_random_string()
        last_evaluated_key = build_last_evaluated_key()
        self.pagination_service.decode_token.return_value = last_evaluated_key
        page_size = randint(1, 10)

        get_all_datasets(
            path=HubPath(hub=self.hub),
            query=DatasetsQuerySchema(nextPageToken=next_page_token),
            visible_data_loader=self.visible_data_loader,
            config=build_config(result_page_size=page_size),
            pagination_service=self.pagination_service,
            response_dataset_builder=self.response_dataset_builder,
            requester_identity=self.requester_identity,
        )

        self.visible_data_loader.get_datasets.assert_called_once_with(
            hub=self.hub, last_evaluated_key=last_evaluated_key, limit=page_size
        )
        self.pagination_service.decode_token.assert_called_once_with(
            next_page_token=next_page_token, context=NextPageTokenContext.DATASETS
        )


class TestCreateDataset:
    @pytest.fixture(autouse=True)
    def service_setup(self, time_travel: datetime) -> None:  # pylint: disable=unused-argument
        self.requester_identity = Mock(RequesterIdentity)
        self.response_dataset_builder = Mock()
        self.response_dataset = build_response_dataset()
        self.response_dataset_builder.return_value = [self.response_dataset]
        self.expected_dataset = build_dataset()
        self.sns_publisher = Mock(SnsPublisher)
        self.expected_engineers = [build_dataset_participant() for _ in range(3)]
        self.expected_stewards: List[DatasetParticipant] = []
        self.expected_dataset_participants = DatasetParticipants(
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
        )
        self.body = build_new_dataset_body(
            self.expected_dataset, engineers=list(self.expected_engineers), stewards=list(self.expected_stewards)
        )
        self.dataset_manager = Mock(DatasetManager)
        self.dataset_validator = Mock(DatasetValidator)
        self.dataset_validator.validate_new_dataset_body.return_value = self.expected_dataset
        self.dataset_participants_manager = Mock(DatasetParticipantsManager)
        self.dataset_participants_manager.validate_new_participants.return_value = self.expected_dataset_participants

    def test_create_success(self) -> None:
        response = cdh_core_api.endpoints.datasets.create_new_dataset(
            requester_identity=self.requester_identity,
            path=HubPath(hub=self.expected_dataset.hub),
            body=self.body,
            response_dataset_builder=self.response_dataset_builder,
            sns_publisher=self.sns_publisher,
            dataset_validator=self.dataset_validator,
            dataset_manager=self.dataset_manager,
            dataset_participants_manager=self.dataset_participants_manager,
        )

        assert response.status_code == 201
        assert isinstance(response.body, ResponseDataset)
        assert response.body == self.response_dataset
        self.response_dataset_builder.assert_called_once_with(
            datasets=[self.expected_dataset],
            requester_identity=self.requester_identity,
        )
        self.dataset_validator.validate_new_dataset_body.assert_called_once_with(
            body=self.body, hub=self.expected_dataset.hub
        )
        self.dataset_participants_manager.validate_new_participants.assert_called_once_with(
            layer=self.body.layer,
            engineers=self.body.engineers,
            stewards=self.body.stewards,
        )
        self.dataset_manager.create_dataset.assert_called_once_with(dataset=self.expected_dataset)
        self.dataset_participants_manager.create_dataset_participants.assert_called_once_with(
            dataset=self.expected_dataset,
            participants=self.expected_dataset_participants,
            requester_identity=self.requester_identity,
        )
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.CREATE,
            payload=self.expected_dataset,
            message_consistency=MessageConsistency.CONFIRMED,
        )

    @pytest.mark.parametrize("failed_validation", ["dataset", "participants"])
    def test_validation_fails(self, failed_validation: str) -> None:
        failed_callable = (
            self.dataset_validator.validate_new_dataset_body
            if failed_validation == "dataset"
            else self.dataset_participants_manager.validate_new_participants
        )
        exception = Exception(Builder.build_random_string())
        failed_callable.side_effect = exception

        with assert_raises(exception):
            cdh_core_api.endpoints.datasets.create_new_dataset(
                requester_identity=self.requester_identity,
                path=HubPath(hub=self.expected_dataset.hub),
                body=self.body,
                response_dataset_builder=self.response_dataset_builder,
                sns_publisher=self.sns_publisher,
                dataset_validator=self.dataset_validator,
                dataset_manager=self.dataset_manager,
                dataset_participants_manager=self.dataset_participants_manager,
            )

        self.dataset_manager.create_dataset.assert_not_called()
        self.dataset_participants_manager.create_dataset_participants.assert_not_called()
        self.sns_publisher.publish.assert_not_called()


class TestUpdateDataset:
    @pytest.fixture(autouse=True)
    def service_setup(self, time_travel: None) -> None:  # pylint: disable=unused-argument
        self.body = Mock(UpdateDatasetBody)
        self.sns_publisher = Mock(SnsPublisher)
        self.requester_identity = build_requester_identity()
        self.dataset_validator = Mock(DatasetValidator)
        self.dataset_manager = Mock(DatasetManager)
        self.dataset_participants_manager = Mock(DatasetParticipantsManager)

        self.old_dataset = build_dataset()
        self.path = DatasetPath(hub=self.old_dataset.hub, datasetId=self.old_dataset.id)
        self.dataset_validator.validate_update_dataset_body.return_value = self.old_dataset
        self.updated_dataset = build_dataset()
        self.dataset_manager.update_dataset.return_value = self.updated_dataset
        self.dataset_participants: Optional[DatasetParticipants] = build_dataset_participants()
        self.body.engineers = self.dataset_participants.engineers
        self.body.stewards = self.dataset_participants.stewards
        self.dataset_participants_manager.get_updated_participants.return_value = self.dataset_participants
        self.response_dataset_builder = Mock()
        self.response_dataset = build_response_dataset()
        self.response_dataset_builder.return_value = [self.response_dataset]

    def update_dataset(self) -> JsonResponse:
        return cdh_core_api.endpoints.datasets.update_dataset(
            path=self.path,
            body=self.body,
            response_dataset_builder=self.response_dataset_builder,
            sns_publisher=self.sns_publisher,
            requester_identity=self.requester_identity,
            dataset_validator=self.dataset_validator,
            dataset_manager=self.dataset_manager,
            dataset_participants_manager=self.dataset_participants_manager,
        )

    def _check_update_dataset_successful(self, response: JsonResponse) -> None:
        self.dataset_validator.validate_update_dataset_body.assert_called_once_with(
            dataset_id=self.old_dataset.id,
            body=self.body,
            hub=self.old_dataset.hub,
        )
        self.dataset_participants_manager.get_updated_participants.assert_called_once_with(
            old_dataset=self.old_dataset,
            body_engineers=self.body.engineers,
            body_stewards=self.body.stewards,
        )
        self.dataset_manager.update_dataset.assert_called_once_with(
            old_dataset=self.old_dataset,
            body=self.body,
        )
        self.dataset_participants_manager.update_dataset_participants.assert_called_once_with(
            dataset=self.updated_dataset,
            participants=self.dataset_participants,
            requester_identity=self.requester_identity,
        )
        assert response.status_code == 200
        assert isinstance(response.body, ResponseDataset)
        assert response.body == self.response_dataset
        self.response_dataset_builder.assert_called_once_with(
            datasets=[self.updated_dataset],
            requester_identity=self.requester_identity,
        )
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=self.updated_dataset,
            message_consistency=MessageConsistency.CONFIRMED,
        )

    def test_update_successful(self) -> None:
        response = self.update_dataset()

        self._check_update_dataset_successful(response)

    def test_update_no_participants_successful(self) -> None:
        self.dataset_participants = None
        self.dataset_participants_manager.get_updated_participants.return_value = self.dataset_participants

        response = self.update_dataset()

        self._check_update_dataset_successful(response)

    @pytest.mark.parametrize("failed_validation", ["dataset", "participants"])
    def test_validation_fails(self, failed_validation: str) -> None:
        failed_callable = (
            self.dataset_validator.validate_update_dataset_body
            if failed_validation == "dataset"
            else self.dataset_participants_manager.get_updated_participants
        )
        exception = Exception(Builder.build_random_string())
        failed_callable.side_effect = exception

        with assert_raises(exception):
            self.update_dataset()

        self.dataset_manager.update_dataset.assert_not_called()
        self.dataset_participants_manager.update_dataset_participants.assert_not_called()
        self.sns_publisher.publish.assert_not_called()


class TestDeleteDataset:
    @pytest.fixture(autouse=True)
    def service_setup(self, time_travel: None) -> None:  # pylint: disable=unused-argument
        self.hub = build_hub()
        self.dataset = build_dataset(hub=self.hub)
        self.path = DatasetPath(hub=self.hub, datasetId=self.dataset.id)
        self.sns_publisher = Mock(SnsPublisher)
        self.dataset_validator = Mock(DatasetValidator)
        self.dataset_manager = Mock(DatasetManager)
        self.dataset_participants_manager = Mock(DatasetParticipantsManager)

    def test_delete_dataset(self) -> None:
        self.dataset_validator.validate_deletion.return_value = self.dataset

        response = cdh_core_api.endpoints.datasets.delete_dataset(
            path=self.path,
            sns_publisher=self.sns_publisher,
            dataset_validator=self.dataset_validator,
            dataset_manager=self.dataset_manager,
            dataset_participants_manager=self.dataset_participants_manager,
        )

        assert response.status_code is HTTPStatus.NO_CONTENT
        assert response.body is None
        self.dataset_validator.validate_deletion.assert_called_once_with(hub=self.hub, dataset_id=self.dataset.id)
        self.dataset_manager.delete_dataset.assert_called_once_with(self.dataset, self.sns_publisher)
        self.dataset_participants_manager.delete_dataset_participants.assert_called_once_with(self.dataset)
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.DELETE,
            payload=self.dataset,
            message_consistency=MessageConsistency.CONFIRMED,
        )

    def test_validation_fails(self) -> None:
        exception = Exception(Builder.build_random_string())
        self.dataset_validator.validate_deletion.side_effect = exception

        with assert_raises(exception):
            cdh_core_api.endpoints.datasets.delete_dataset(
                path=self.path,
                sns_publisher=self.sns_publisher,
                dataset_validator=self.dataset_validator,
                dataset_manager=self.dataset_manager,
                dataset_participants_manager=self.dataset_participants_manager,
            )

        self.dataset_manager.update_dataset.assert_not_called()
        self.dataset_participants_manager.update_dataset_participants.assert_not_called()
        self.sns_publisher.publish.assert_not_called()
