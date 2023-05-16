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
from typing import List
from typing import Optional

from cdh_core_api.bodies.datasets import DatasetParticipantBodyPart
from cdh_core_api.config import Config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import MessageConsistency
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.users_api import UsersApi
from marshmallow import ValidationError
from waiting import TimeoutExpired
from waiting import wait

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipantId
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.request import RequesterIdentity
from cdh_core.enums.dataset_properties import Layer
from cdh_core.exceptions.http import InternalError


class DatasetParticipantsManager:
    """Handles dataset participants."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        authorization_api: AuthorizationApi,
        config: Config,
        sns_publisher: SnsPublisher,
        users_api: UsersApi,
    ):
        self.authorization_api = authorization_api
        self.using_auth = config.using_authorization_api
        self.sns_publisher = sns_publisher
        self.users_api = users_api

    def validate_new_participants(
        self,
        layer: Layer,
        engineers: List[DatasetParticipantBodyPart],
        stewards: Optional[List[DatasetParticipantBodyPart]],
    ) -> DatasetParticipants:
        """Check that the participants for a new dataset are valid."""
        return self._validate_participants(layer=layer, requested_engineers=engineers, requested_stewards=stewards)

    def _validate_participants(  # pylint: disable=too-many-arguments
        self,
        layer: Layer,
        requested_engineers: Optional[List[DatasetParticipantBodyPart]],
        requested_stewards: Optional[List[DatasetParticipantBodyPart]],
        current_engineers: Optional[List[DatasetParticipant]] = None,
        current_stewards: Optional[List[DatasetParticipant]] = None,
    ) -> DatasetParticipants:
        engineers = (
            self._make_participants_lowercase(requested_engineers) if requested_engineers else current_engineers or []
        )
        stewards = (
            self._make_participants_lowercase(requested_stewards) if requested_stewards else current_stewards or []
        )
        if not engineers:
            raise ValidationError("Must provide engineers.")
        if layer is Layer.sem and requested_stewards == []:
            raise ValidationError(f"Please provide at least one steward for datasets in layer {Layer.sem.value}.")
        if layer is not Layer.sem and requested_stewards:
            raise ValidationError(f"Cannot specify stewards for layer {layer.value}.")
        participants = DatasetParticipants(engineers, stewards)
        self._check_engineers_stewards_non_overlapping(participants)
        return participants

    @staticmethod
    def _make_participants_lowercase(participants: List[DatasetParticipantBodyPart]) -> List[DatasetParticipant]:
        return [
            DatasetParticipant(DatasetParticipantId(participant.id.lower()), participant.idp.lower())
            for participant in participants
        ]

    @staticmethod
    def _check_engineers_stewards_non_overlapping(participants: DatasetParticipants) -> None:
        steward_ids = {steward.id for steward in participants.stewards}
        engineer_ids = {engineer.id for engineer in participants.engineers}
        if steward_ids.intersection(engineer_ids):
            raise ValidationError("Please provide participants with different ids for engineers and stewards.")

    def create_dataset_participants(
        self, dataset: Dataset, participants: DatasetParticipants, requester_identity: RequesterIdentity
    ) -> None:
        """Add the dataset participants for a new dataset to the auth API."""
        self._post_dataset_to_auth(dataset)
        self._update_dataset_participants(dataset, participants, requester_identity)

    def _post_dataset_to_auth(self, dataset: Dataset) -> None:
        self.sns_publisher.publish(
            entity_type=EntityType.DATASET,
            operation=Operation.CREATE,
            payload=dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )

        self._poll_until_dataset_exists_in_authorization(
            dataset=dataset,
        )

    def _poll_until_dataset_exists_in_authorization(self, dataset: Dataset) -> None:
        if not self.using_auth:
            return
        try:
            wait(
                lambda: self.authorization_api.is_dataset_visible(dataset_id=dataset.id),
                sleep_seconds=0.2,
                timeout_seconds=5,
            )
        except TimeoutExpired as error:
            raise InternalError("Dataset created, but timed out waiting for permissions") from error

    def _update_dataset_participants(
        self, dataset: Dataset, participants: DatasetParticipants, requester_identity: RequesterIdentity
    ) -> None:
        if not self.using_auth:
            return
        self.users_api.put_dataset_participants(
            dataset_id=dataset.id,
            engineers=participants.engineers,
            stewards=participants.stewards,
            requester_identity=requester_identity,
        )
        self._poll_until_dataset_participants_exist_in_authorization(
            dataset=dataset,
            expected_participants=participants,
        )

    def _poll_until_dataset_participants_exist_in_authorization(
        self,
        dataset: Dataset,
        expected_participants: DatasetParticipants,
    ) -> None:
        if not self.using_auth:
            return

        def _participants_as_expected(participants: Optional[DatasetParticipants]) -> bool:
            return (
                participants is not None
                and set(participants.engineers) == set(expected_participants.engineers)
                and set(participants.stewards) == set(expected_participants.stewards)
            )

        try:
            wait(
                lambda: _participants_as_expected(
                    self.authorization_api.get_datasets_participants(dataset_ids=[dataset.id]).get(dataset.id)
                ),
                sleep_seconds=0.2,
                timeout_seconds=5,
            )
        except TimeoutExpired as error:
            raise InternalError("Dataset created/updated, but timed out waiting for participants") from error

    def delete_dataset_participants(self, dataset: Dataset) -> None:
        """Delete the participant roles of the given dataset from the auth API."""
        if self.using_auth:
            self.users_api.delete_dataset_participants(dataset.id)

    def get_updated_participants(
        self,
        old_dataset: Dataset,
        body_engineers: Optional[List[DatasetParticipantBodyPart]],
        body_stewards: Optional[List[DatasetParticipantBodyPart]],
    ) -> Optional[DatasetParticipants]:
        """Compute the updated list of participants for the given dataset and the given body entries."""
        if body_engineers is not None or body_stewards is not None:
            if self.using_auth:
                current_participants = self.authorization_api.get_datasets_participants(dataset_ids=[old_dataset.id])[
                    old_dataset.id
                ]
                current_engineers = list(current_participants.engineers)
                current_stewards = list(current_participants.stewards)
            else:
                current_engineers = [DEFAULT_ENGINEER]
                current_stewards = []

            return self._validate_participants(
                layer=old_dataset.layer,
                requested_engineers=body_engineers,
                requested_stewards=body_stewards,
                current_engineers=current_engineers,
                current_stewards=current_stewards,
            )
        return None

    def update_dataset_participants(
        self, dataset: Dataset, participants: Optional[DatasetParticipants], requester_identity: RequesterIdentity
    ) -> None:
        """Update the participants in the auth API for the given dataset to the given participants."""
        self._update_dataset_in_authorization_api(dataset)
        if participants is not None:
            self._update_dataset_participants(dataset, participants, requester_identity)

    def _update_dataset_in_authorization_api(self, updated_dataset: Dataset) -> None:
        self.sns_publisher.publish(
            entity_type=EntityType.DATASET,
            operation=Operation.UPDATE,
            payload=updated_dataset,
            message_consistency=MessageConsistency.PRELIMINARY,
        )


DEFAULT_ENGINEER = DatasetParticipant(DatasetParticipantId("updated-prefix-responsible"), "updated-prefix-idp")
