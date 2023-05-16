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
from logging import getLogger
from typing import Dict
from typing import List

from cdh_core_api.config import Config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.phone_book import PhoneBook

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import ResponseDataset
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.request import RequesterIdentity

LOG = getLogger(__name__)


class ResponseDatasetBuilder:
    """Builds the `ResponseDataset`s corresponding to given `Dataset`s."""

    def __init__(self, config: Config, authorization_api: AuthorizationApi, phone_book: PhoneBook) -> None:
        self._config = config
        self._authorization_api = authorization_api
        self._phone_book = phone_book

    def __call__(
        self,
        datasets: List[Dataset],
        requester_identity: RequesterIdentity,
    ) -> List[ResponseDataset]:
        """
        Build the `ResponseDataset`s for a given list of `Dataset`s.

        Includes the respective participants provided by the Authorization Api.
        """
        datasets_participants = self._get_participants(datasets, requester_identity)
        return [
            ResponseDataset.from_dataset(
                dataset=dataset,
                dataset_participants=(
                    datasets_participants.get(dataset.id, DatasetParticipants(engineers=[], stewards=[]))
                ),
            )
            for dataset in datasets
        ]

    def _get_participants(
        self, datasets: List[Dataset], requester_identity: RequesterIdentity
    ) -> Dict[DatasetId, DatasetParticipants]:
        requester_arn = requester_identity.arn
        if not self._config.authorization_api_params.active or self._phone_book.is_authorization_role(requester_arn):
            return {}
        datasets_participants = self._authorization_api.get_datasets_participants(
            dataset_ids=[dataset.id for dataset in datasets]
        )
        if len(datasets_participants) != len(datasets):
            missing_dataset_ids = [dataset.id for dataset in datasets if dataset.id not in datasets_participants]
            jwt_user_id = requester_identity.jwt_user_id
            LOG.warning(
                f"The auth api did not send dataset participants for {missing_dataset_ids} for {jwt_user_id=} and "
                f"{requester_arn=}"
            )
        return datasets_participants
