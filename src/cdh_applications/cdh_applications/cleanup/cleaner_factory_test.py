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
from typing import List
from typing import Set
from typing import Tuple
from typing import Type
from unittest.mock import Mock

import pytest
from asserts import assert_count_equal

from cdh_applications.cleanup.cleaner_factory import CleanerFactory
from cdh_applications.cleanup.cleaners.dynamo_cleaner import DynamoCleaner
from cdh_applications.cleanup.cleaners.glue_cleaner import GlueCleaner
from cdh_applications.cleanup.cleaners.lake_formation_cleaner import LakeFormationCleaner
from cdh_applications.cleanup.cleaners.ram_cleaner import RamCleaner
from cdh_applications.cleanup.cleaners.s3_cleaner import S3Cleaner
from cdh_applications.cleanup.cleaners.sns_cleaner import SnsCleaner
from cdh_applications.cleanup.cleanup_utils_test import PREFIX
from cdh_applications.cleanup.generic_cleaner import GenericCleaner
from cdh_core.entities.accounts_test import build_base_account
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_partition


@pytest.mark.usefixtures("mock_dynamodb")
@pytest.mark.usefixtures("mock_glue")
@pytest.mark.usefixtures("mock_ram")
@pytest.mark.usefixtures("mock_s3")
@pytest.mark.usefixtures("mock_sns")
class TestCleanerFactory:
    @pytest.fixture()
    def partition(self) -> Partition:
        return build_partition()

    @pytest.fixture()
    def regions(self, partition: Partition) -> List[str]:
        return [region.value for region in list(Region) if region.partition == partition]

    @pytest.mark.parametrize(
        "account_purpose_params",
        [
            ("api", {DynamoCleaner}),
            ("security", {}),
            (
                "resources",
                {
                    GlueCleaner,
                    LakeFormationCleaner,
                    S3Cleaner,
                    SnsCleaner,
                    RamCleaner,
                },
            ),
            (
                "test",
                {
                    GlueCleaner,
                    S3Cleaner,
                    SnsCleaner,
                },
            ),
        ],
    )
    def test_create_cleaners(
        self, partition: Partition, regions: List[str], account_purpose_params: Tuple[str, Set[Type[GenericCleaner]]]
    ) -> None:
        account_purpose, expected_cleaner_types = account_purpose_params
        account = build_base_account(purpose=AccountPurpose(account_purpose))

        cleaner_factory = CleanerFactory(
            account=account,
            prefix=PREFIX,
            clean_filter=Mock(return_value=True),
            regions=regions,
            partition=partition,
            log=getLogger(),
            credentials={},
        )

        assert_count_equal({type(cleaner) for cleaner in cleaner_factory.get_cleaners()}, expected_cleaner_types)
        for cleaner_type in expected_cleaner_types:
            self.check_regions_for_cleaner(regions, partition, cleaner_factory.get_cleaners(), cleaner_type)

    def check_regions_for_cleaner(
        self,
        regions: List[str],
        partition: Partition,
        cleaners: List[GenericCleaner],
        cleaner_type: Type[GenericCleaner],
    ) -> None:
        if cleaner_type in CleanerFactory.SERVICES_ONLY_IN_PREFERRED_PARTITION_REGION:
            regions = [Region.preferred(partition=partition).value]
        if cleaner_type is S3Cleaner:
            assert_count_equal(
                set(regions),
                {
                    cleaner._resource.meta.client.meta.region_name  # type: ignore  # pylint: disable=W0212
                    for cleaner in cleaners
                    if isinstance(cleaner, cleaner_type)
                },
            )
        elif cleaner_type is LakeFormationCleaner:
            assert_count_equal(
                set(regions),
                {
                    cleaner._lakeformation.meta.region_name  # type: ignore
                    for cleaner in cleaners
                    if isinstance(cleaner, cleaner_type)
                },
            )
            assert_count_equal(
                set(regions),
                {
                    cleaner._glue.meta.region_name  # type: ignore
                    for cleaner in cleaners
                    if isinstance(cleaner, cleaner_type)
                },
            )
        else:
            assert_count_equal(
                set(regions),
                {
                    cleaner._client.meta.region_name  # type: ignore
                    for cleaner in cleaners
                    if isinstance(cleaner, cleaner_type)
                },
            )
