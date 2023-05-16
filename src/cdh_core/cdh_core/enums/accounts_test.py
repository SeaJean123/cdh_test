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
from random import choice

import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import SIMPLE_CONFIG_FILE
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core_dev_tools.testing.builder import Builder


def build_account_purpose() -> AccountPurpose:
    return choice(list(AccountPurpose))


def build_account_type() -> AccountType:
    return choice(list(AccountType))


def build_affiliation() -> Affiliation:
    return choice(list(Affiliation))


class TestAccountType:
    def test_friendly_name(self) -> None:
        assert AccountType.usecase.friendly_name == "Usecase"


class TestAccountPurpose:
    # pylint: disable=unused-argument
    @pytest.mark.parametrize(
        "mock_config_file",
        [
            replace(
                SIMPLE_CONFIG_FILE,
                account=ConfigFile.Account(
                    purpose=ConfigFile.Account.Purpose(
                        instances={
                            Builder.build_random_string(): ConfigFile.Account.Purpose.Entry(
                                value="a",
                                deployed_by_cdh_core=True,
                                hub_specific=True,
                                can_be_owner=True,
                            ),
                            Builder.build_random_string(): ConfigFile.Account.Purpose.Entry(
                                value="b",
                                deployed_by_cdh_core=False,
                                hub_specific=False,
                                can_be_owner=False,
                            ),
                        }
                    ),
                    instances_per_purpose=SIMPLE_CONFIG_FILE.account.instances_per_purpose,
                ),
            )
        ],
        indirect=True,
    )
    def test_purpose_properties(self, mock_config_file: ConfigFile) -> None:
        assert AccountPurpose("a").deployed_by_cdh_core
        assert not AccountPurpose("b").deployed_by_cdh_core
        assert AccountPurpose("a").hub_specific
        assert not AccountPurpose("b").hub_specific
        assert AccountPurpose("a").can_be_owner
        assert not AccountPurpose("b").can_be_owner

    @pytest.mark.usefixtures("mock_config_file")
    def test_purpose_properties_default_assert_values_are_present(self, mock_config_file: ConfigFile) -> None:
        assert any(account_purpose.deployed_by_cdh_core for account_purpose in AccountPurpose)
        assert any(account_purpose.hub_specific for account_purpose in AccountPurpose)
        assert any(account_purpose.can_be_owner for account_purpose in AccountPurpose)
