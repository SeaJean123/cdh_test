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
# pylint: disable=protected-access
from logging import getLogger
from unittest.mock import Mock

import pytest

from cdh_applications.cleanup.cleaners.ram_cleaner import RamCleaner
from cdh_applications.cleanup.cleanup_utils_test import PREFIX
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.builder import Builder


@pytest.mark.usefixtures("mock_ram")
class TestRamCleaner:
    @pytest.fixture()
    def region(self) -> str:
        return build_region().value  # type: ignore

    def test_dont_delete_resource_share_if_clean_filter_returns_false(self, region: str) -> None:
        resource_share_name = PREFIX + Builder.build_random_string()
        clean_filter = Mock(return_value=False)
        ram_cleaner = RamCleaner(
            region=region,
            prefix=PREFIX,
            clean_filter=clean_filter,
            log=getLogger(),
            credentials={},
        )
        ram_cleaner._client.create_resource_share(name=resource_share_name)

        ram_cleaner.clean()

        clean_filter.assert_called_once_with("resource share", resource_share_name, getLogger())
        assert (
            len(
                ram_cleaner._client.get_resource_shares(resourceOwner="SELF", name=resource_share_name)[
                    "resourceShares"
                ]
            )
            == 1
        )

    def test_delete_resource_share_with_prefix(self, region: str) -> None:
        resource_share_name = PREFIX + Builder.build_random_string()
        clean_filter = Mock(return_value=True)
        ram_cleaner = RamCleaner(
            region=region,
            prefix=PREFIX,
            clean_filter=clean_filter,
            log=getLogger(),
            credentials={},
        )
        ram_cleaner._client.create_resource_share(name=resource_share_name)
        assert (
            len(
                ram_cleaner._client.get_resource_shares(resourceOwner="SELF", name=resource_share_name)[
                    "resourceShares"
                ]
            )
            == 1
        )

        ram_cleaner.clean()

        clean_filter.assert_called_once_with("resource share", resource_share_name, getLogger())
        assert (
            ram_cleaner._client.get_resource_shares(resourceOwner="SELF", name=resource_share_name)["resourceShares"][
                0
            ]["status"]
            == "DELETED"
        )
