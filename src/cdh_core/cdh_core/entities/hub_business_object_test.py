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

from cdh_core.entities.hub_business_object import HubBusinessObject
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core_dev_tools.testing.builder import Builder


def build_hub_business_object(
    hub: Optional[Hub] = None,
    business_object: Optional[BusinessObject] = None,
    responsibles: Optional[List[str]] = None,
) -> HubBusinessObject:
    return HubBusinessObject(
        hub=hub or build_hub(),
        business_object=business_object or build_business_object(),
        friendly_name=Builder.build_random_string(20),
        responsibles=responsibles if responsibles is not None else ["someone@example.com", "someone_else@example.de"],
    )


class TestHubBusinessObject:
    def test_get_default_hub_business_object(self) -> None:
        hub = build_hub()
        business_object = build_business_object()
        expected_hub_business_object = HubBusinessObject(
            hub=hub, business_object=business_object, friendly_name=business_object.friendly_name, responsibles=[]
        )
        assert (
            HubBusinessObject.get_default_hub_business_object(hub=hub, business_object=business_object)
            == expected_hub_business_object
        )
