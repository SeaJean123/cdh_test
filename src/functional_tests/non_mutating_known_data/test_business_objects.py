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
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.hubs_test import build_hub
from functional_tests.conftest import NonMutatingTestSetup


class TestBusinessObjects:
    """Test Class for all BusinessObject endpoints."""

    def test_get_all_business_objects(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/businessObjects endpoint."""
        hub = build_hub()

        hub_business_objects = non_mutating_test_setup.core_api_client.get_hub_business_objects(hub)

        assert all(hub_bo.hub is hub for hub_bo in hub_business_objects)

    def test_get_business_object(self, non_mutating_test_setup: NonMutatingTestSetup) -> None:
        """Test the GET /{hub}/businessObjects/{businessObject} endpoint."""
        hub = build_hub()
        business_object = build_business_object()

        hub_bo = non_mutating_test_setup.core_api_client.get_hub_business_object(hub, business_object)

        assert hub_bo.hub is hub
        assert hub_bo.business_object is business_object
