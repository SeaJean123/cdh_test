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
from typing import Optional

from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipantId
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core_dev_tools.testing.builder import Builder


def build_dataset_participant_id(participant_id: Optional[str] = None) -> DatasetParticipantId:
    return DatasetParticipantId(participant_id or Builder.build_random_string())


def build_dataset_participant() -> DatasetParticipant:
    return DatasetParticipant(id=build_dataset_participant_id(), idp=Builder.build_random_string())


def build_dataset_participants() -> DatasetParticipants:
    return DatasetParticipants(engineers=[build_dataset_participant()], stewards=[build_dataset_participant()])
