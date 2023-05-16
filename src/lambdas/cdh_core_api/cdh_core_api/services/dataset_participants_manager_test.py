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
from dataclasses import replace
from typing import cast
from typing import List
from typing import Optional
from unittest.mock import call
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.datasets import DatasetParticipantBodyPart
from cdh_core_api.config_test import build_config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.dataset_participants_manager import DatasetParticipantsManager
from cdh_core_api.services.dataset_participants_manager import DEFAULT_ENGINEER
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.users_api import UsersApi
from marshmallow import ValidationError

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipantId
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.dataset_participants_test import build_dataset_participant
from cdh_core.entities.dataset_participants_test import build_dataset_participant_id
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.enums.dataset_properties import Layer
from cdh_core_dev_tools.testing.builder import Builder


class TestDatasetParticipantsManager:
    @pytest.fixture(autouse=True)
    def service_setup(self) -> None:
        self.authorization_api = Mock(AuthorizationApi)
        self.config = build_config()
        self.sns_publisher = Mock(SnsPublisher)
        self.users_api = Mock(UsersApi)
        self.dataset_participants_manager = DatasetParticipantsManager(
            authorization_api=self.authorization_api,
            config=self.config,
            sns_publisher=self.sns_publisher,
            users_api=self.users_api,
        )
        self.layer = Builder.get_random_element(to_choose_from=list(Layer), exclude={Layer.sem})
        self.dataset = build_dataset(layer=self.layer)
        self.requester_identity = build_requester_identity()
        self.expected_engineers = [build_dataset_participant() for _ in range(3)]
        self.expected_stewards: List[DatasetParticipant] = []
        self.expected_dataset_participants = DatasetParticipants(
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
        )
        self.engineers = cast(List[DatasetParticipantBodyPart], self.expected_engineers)
        self.stewards = cast(List[DatasetParticipantBodyPart], self.expected_stewards)
        self.authorization_api.get_datasets_participants.return_value = {
            self.dataset.id: self.expected_dataset_participants
        }

    def _validate_successful(self) -> None:
        participants = self.dataset_participants_manager.validate_new_participants(
            layer=self.layer,
            engineers=self.engineers,
            stewards=self.stewards,
        )

        assert participants == self.expected_dataset_participants

    def test_new_participants_valid(self) -> None:
        self._validate_successful()

    def test_uppercase_participants_valid(self) -> None:
        self.layer = Layer.sem
        dataset_id = Dataset.build_id(self.dataset.business_object, self.dataset.name, self.layer, self.dataset.hub)
        self.dataset = replace(self.dataset, layer=self.layer, id=dataset_id)
        self.expected_stewards = [build_dataset_participant() for _ in range(2)]
        self.expected_dataset_participants = DatasetParticipants(
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
        )
        self.stewards = cast(List[DatasetParticipantBodyPart], self.expected_stewards)
        self.engineers = [
            replace(responsible, id=cast(DatasetParticipantId, responsible.id.upper()))
            for responsible in self.engineers
        ]
        self.stewards = [
            replace(responsible, id=cast(DatasetParticipantId, responsible.id.upper())) for responsible in self.stewards
        ]

        self._validate_successful()

    def test_participants_with_stewards_valid(self) -> None:
        self.layer = Layer.sem
        dataset_id = Dataset.build_id(self.dataset.business_object, self.dataset.name, self.layer, self.dataset.hub)
        self.dataset = replace(self.dataset, layer=self.layer, id=dataset_id)
        self.expected_stewards = [build_dataset_participant() for _ in range(2)]
        self.expected_dataset_participants = DatasetParticipants(
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
        )
        self.stewards = cast(List[DatasetParticipantBodyPart], self.expected_stewards)

        self._validate_successful()

    @pytest.mark.parametrize(
        "engineers, stewards, layer",
        [
            (None, None, Layer.raw),
            ([], [], Layer.src),
            (None, [build_dataset_participant()], Layer.sem),
            ([], [build_dataset_participant()], Layer.sem),
            ([build_dataset_participant()], [], Layer.sem),
            ([build_dataset_participant()], [build_dataset_participant()], Layer.pre),
            (
                [DatasetParticipant(build_dataset_participant_id("same-id"), "some")],
                [DatasetParticipant(build_dataset_participant_id("same-id"), "other")],
                Layer.sem,
            ),
        ],
    )
    def test_standard_cases_new_participants_invalid(
        self,
        engineers: Optional[List[DatasetParticipant]],
        stewards: Optional[List[DatasetParticipant]],
        layer: Layer,
    ) -> None:
        with pytest.raises(ValidationError):
            self.dataset_participants_manager.validate_new_participants(
                layer=layer,
                engineers=cast(List[DatasetParticipantBodyPart], engineers),
                stewards=cast(List[DatasetParticipantBodyPart], stewards),
            )

    def test_only_engineers_provided_valid(self) -> None:
        participants = self.dataset_participants_manager.validate_new_participants(
            layer=self.layer,
            engineers=self.engineers,
            stewards=None,
        )
        assert participants == self.expected_dataset_participants

    def test_create_dataset_participants_successful(self) -> None:
        self.dataset_participants_manager.create_dataset_participants(
            dataset=self.dataset,
            participants=self.expected_dataset_participants,
            requester_identity=self.requester_identity,
        )

        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.CREATE,
            payload=self.dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )
        self.authorization_api.is_dataset_visible.assert_called_once_with(dataset_id=self.dataset.id)
        self.users_api.put_dataset_participants.assert_called_once_with(
            dataset_id=self.dataset.id,
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
            requester_identity=self.requester_identity,
        )
        self.authorization_api.get_datasets_participants.assert_called_once_with(dataset_ids=[self.dataset.id])

    def test_call_to_authorization_are_retried(self) -> None:
        self.authorization_api.is_dataset_visible.side_effect = (False, False, True)
        self.authorization_api.get_datasets_participants.side_effect = (
            {},
            {},
            {self.dataset.id: self.expected_dataset_participants},
        )

        self.dataset_participants_manager.create_dataset_participants(
            dataset=self.dataset,
            participants=self.expected_dataset_participants,
            requester_identity=self.requester_identity,
        )

        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.CREATE,
            payload=self.dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )
        self.authorization_api.is_dataset_visible.assert_has_calls([call(dataset_id=self.dataset.id) for _ in range(3)])
        self.users_api.put_dataset_participants.assert_called_once_with(
            dataset_id=self.dataset.id,
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
            requester_identity=self.requester_identity,
        )
        self.authorization_api.get_datasets_participants.assert_has_calls(
            [call(dataset_ids=[self.dataset.id]) for _ in range(3)]
        )

    def test_create_dataset_participants_no_auth_successful(self) -> None:
        self.config = build_config(use_authorization=False)
        self.dataset_participants_manager = DatasetParticipantsManager(
            authorization_api=self.authorization_api,
            config=self.config,
            sns_publisher=self.sns_publisher,
            users_api=self.users_api,
        )

        self.dataset_participants_manager.create_dataset_participants(
            dataset=self.dataset,
            participants=self.expected_dataset_participants,
            requester_identity=self.requester_identity,
        )

        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.CREATE,
            payload=self.dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )
        self.authorization_api.is_dataset_visible.assert_not_called()
        self.users_api.put_dataset_participants.assert_not_called()
        self.authorization_api.get_datasets_participants.assert_not_called()

    def test_delete_dataset_participants_successful(self) -> None:
        self.dataset_participants_manager.delete_dataset_participants(self.dataset)

        self.users_api.delete_dataset_participants.assert_called_once_with(self.dataset.id)

    def test_delete_dataset_participants_no_auth_successful(self) -> None:
        self.config = build_config(use_authorization=False)
        self.dataset_participants_manager = DatasetParticipantsManager(
            authorization_api=self.authorization_api,
            config=self.config,
            sns_publisher=self.sns_publisher,
            users_api=self.users_api,
        )

        self.dataset_participants_manager.delete_dataset_participants(self.dataset)

        self.users_api.delete_dataset_participants.assert_not_called()

    def test_get_updated_participants_no_update(self) -> None:
        participants = self.dataset_participants_manager.get_updated_participants(
            old_dataset=self.dataset, body_engineers=None, body_stewards=None
        )

        assert participants is None
        self.authorization_api.get_datasets_participants.assert_not_called()

    def test_get_updated_participants_same_participants(self) -> None:
        participants = self.dataset_participants_manager.get_updated_participants(
            old_dataset=self.dataset, body_engineers=self.engineers, body_stewards=self.stewards
        )

        assert participants == self.expected_dataset_participants
        self.authorization_api.get_datasets_participants.assert_called_once_with(dataset_ids=[self.dataset.id])

    def test_get_updated_participants_stewards_without_authorization_uses_default_engineers(self) -> None:
        self.dataset_participants_manager.using_auth = False
        self.dataset = build_dataset(layer=Layer.sem)
        self.expected_stewards = [build_dataset_participant() for _ in range(2)]
        self.expected_dataset_participants = DatasetParticipants(
            engineers=[DEFAULT_ENGINEER],
            stewards=self.expected_stewards,
        )
        self.stewards = cast(List[DatasetParticipantBodyPart], self.expected_stewards)

        participants = self.dataset_participants_manager.get_updated_participants(
            old_dataset=self.dataset, body_engineers=None, body_stewards=self.stewards
        )

        assert participants == self.expected_dataset_participants
        self.authorization_api.get_datasets_participants.assert_not_called()

    def test_get_updated_participants_only_engineers_provided_but_overlapping_existing_steward(self) -> None:
        self.authorization_api.get_datasets_participants.return_value = {
            self.dataset.id: DatasetParticipants(
                engineers=[build_dataset_participant()], stewards=self.expected_engineers
            )
        }
        with pytest.raises(ValidationError) as exc_info:
            self.dataset_participants_manager.get_updated_participants(
                old_dataset=self.dataset,
                body_engineers=self.engineers,
                body_stewards=None,
            )
        assert "Please provide participants with different ids for engineers and stewards." in str(exc_info.value)

    def test_get_updated_participants_switch_engineers_and_stewards(self) -> None:
        self.dataset = build_dataset(layer=Layer.sem)
        self.expected_stewards = [build_dataset_participant() for _ in range(2)]
        self.expected_dataset_participants = DatasetParticipants(
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
        )
        self.stewards = cast(List[DatasetParticipantBodyPart], self.expected_stewards)
        self.authorization_api.get_datasets_participants.return_value = {
            self.dataset.id: DatasetParticipants(engineers=self.expected_stewards, stewards=self.expected_engineers)
        }
        participants = self.dataset_participants_manager.get_updated_participants(
            old_dataset=self.dataset,
            body_engineers=self.engineers,
            body_stewards=self.stewards,
        )
        assert participants == self.expected_dataset_participants

    def test_get_updated_participants_remove_stewards_in_layer_sem_fails(self) -> None:
        self.dataset = build_dataset(layer=Layer.sem)
        self.expected_stewards = [build_dataset_participant() for _ in range(2)]
        self.expected_dataset_participants = DatasetParticipants(
            engineers=self.expected_engineers,
            stewards=self.expected_stewards,
        )
        self.authorization_api.get_datasets_participants.return_value = {
            self.dataset.id: DatasetParticipants(engineers=self.expected_stewards, stewards=self.expected_engineers)
        }
        with pytest.raises(ValidationError) as exc_info:
            self.dataset_participants_manager.get_updated_participants(
                old_dataset=self.dataset,
                body_engineers=None,
                body_stewards=[],
            )
        assert "Please provide at least one steward for datasets in layer sem." in str(exc_info.value)

    def test_update_dataset_participants(self) -> None:
        self.dataset_participants_manager.update_dataset_participants(
            dataset=self.dataset,
            participants=self.expected_dataset_participants,
            requester_identity=self.requester_identity,
        )

        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=self.dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )
        self.users_api.put_dataset_participants.assert_called_once_with(
            dataset_id=self.dataset.id,
            engineers=self.expected_dataset_participants.engineers,
            stewards=self.expected_dataset_participants.stewards,
            requester_identity=self.requester_identity,
        )
        self.authorization_api.get_datasets_participants.assert_called_once_with(dataset_ids=[self.dataset.id])

    def test_update_dataset_participants_no_auth(self) -> None:
        self.config = build_config(use_authorization=False)
        self.dataset_participants_manager = DatasetParticipantsManager(
            authorization_api=self.authorization_api,
            config=self.config,
            sns_publisher=self.sns_publisher,
            users_api=self.users_api,
        )

        self.dataset_participants_manager.update_dataset_participants(
            dataset=self.dataset,
            participants=self.expected_dataset_participants,
            requester_identity=self.requester_identity,
        )

        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=self.dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )
        self.users_api.put_dataset_participants.assert_not_called()
        self.authorization_api.get_datasets_participants.assert_not_called()

    def test_update_no_dataset_participants(self) -> None:
        self.dataset_participants_manager.update_dataset_participants(
            dataset=self.dataset, participants=None, requester_identity=self.requester_identity
        )

        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=self.dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )
        self.users_api.put_dataset_participants.assert_not_called()
        self.authorization_api.get_datasets_participants.assert_not_called()
