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
from typing import Callable
from typing import Iterable
from typing import Optional

from cdh_core_api.config import Config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.full_vision_check import FullVisionCheck

from cdh_core.entities.accounts import Account
from cdh_core.entities.arn import Arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.resource import Resource
from cdh_core.enums.hubs import Hub


class VisibilityCheck:
    """
    Determines if the requester can see a given entity (hub, dataset, account or resource).

    Uses the FullVisionCheck to handle special roles and delegates to the AuthorizationApi otherwise.
    """

    def __init__(
        self,
        full_vision_check: FullVisionCheck,
        authorization_api: AuthorizationApi,
        requester: Arn,
        config: Config,
    ):
        self._config = config
        self._auth = authorization_api
        self._requester = requester
        self._full_vision_check = full_vision_check
        self._full_vision: Optional[bool] = None

    def _has_full_vision(self) -> bool:
        if self._full_vision is None:
            self._full_vision = self._full_vision_check(self._requester)
        return self._full_vision

    def get_account_visibility_check(self, batch: bool) -> Callable[[Account], bool]:
        """Return a method that decides whether an account is visible.

        Set the flag 'batch' to True if the method is to be used for multiple checks to reduce network traffic.
        """
        if self._has_full_vision():
            return lambda _: True
        if batch:
            visible_account_ids = self._auth.get_visible_account_ids()
            return lambda account: account.id in visible_account_ids

        return lambda account: self._auth.is_account_visible(account.id)

    def get_dataset_visibility_check(self, batch: bool, hub: Optional[Hub] = None) -> Callable[[Dataset], bool]:
        """Return a method that decides whether a dataset is visible.

        Set the flag 'batch' to True if the method is to be used for multiple checks to reduce network traffic.
        """
        dataset_id_visibility_check = self.get_dataset_id_visibility_check(batch, hub)
        return lambda dataset: dataset_id_visibility_check(dataset.id)

    def get_dataset_id_visibility_check(
        self, batch: bool, hub: Optional[Hub] = None, dataset_ids: Optional[Iterable[DatasetId]] = None
    ) -> Callable[[DatasetId], bool]:
        """Return a method that decides whether a dataset with a given ID is visible.

        Set the flag 'batch' to True if the method is to be used for multiple checks to reduce network traffic.
        """
        if self._has_full_vision():
            return lambda _: True
        if batch:
            visible_dataset_ids = self._auth.get_visible_dataset_ids(hub, dataset_ids)
            return lambda dataset_id: dataset_id in visible_dataset_ids
        return self._auth.is_dataset_visible

    def get_resource_visibility_check(self, batch: bool, hub: Optional[Hub] = None) -> Callable[[Resource], bool]:
        """Return a method that decides whether a resource is visible.

        Set the flag 'batch' to True if the method is to be used for multiple checks to reduce network traffic.
        """
        dataset_id_visibility_check = self.get_dataset_id_visibility_check(batch, hub)
        return lambda resource: dataset_id_visibility_check(resource.dataset_id)

    def get_hub_visibility_check(self, batch: bool) -> Callable[[Hub], bool]:
        """Return a method that decides whether a hub is visible.

        Set the flag 'batch' to True if the method is to be used for multiple checks to reduce network traffic.
        """

        def hub_exists(hub: Hub) -> bool:
            return hub in self._config.hubs

        if self._has_full_vision():
            return hub_exists
        if batch:
            visible_hubs = self._auth.get_visible_hubs()
            return lambda hub: hub_exists(hub) and hub in visible_hubs
        return lambda hub: hub_exists(hub) and self._auth.is_hub_visible(hub)
