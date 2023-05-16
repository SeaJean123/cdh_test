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
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.hubs import Hub


@dataclass(frozen=True)
class HubBusinessObject(DataClassJsonCDHMixin):
    """Dataclass containing information of the business object for a specific hub.

    Business object metadata like friendly name and list of responsibles can be customized on a per-hub basis.
    """

    hub: Hub
    business_object: BusinessObject
    friendly_name: str
    responsibles: List[str]

    @staticmethod
    def get_default_hub_business_object(hub: Hub, business_object: BusinessObject) -> HubBusinessObject:
        """Return the default hub_business_object.

        The friendly_name and responsibles fields are filled with default values.
        """
        return HubBusinessObject(
            hub=hub, business_object=business_object, friendly_name=business_object.friendly_name, responsibles=[]
        )


@dataclass(frozen=True)
class HubBusinessObjectList(DataClassJsonCDHMixin):
    """Dataclass containing a list of hub business objects."""

    business_objects: List[HubBusinessObject]
