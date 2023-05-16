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
# pylint: disable=duplicate-code
from logging import Logger
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Set
from typing import Type

from cdh_applications.cleanup.cleaners.dynamo_cleaner import DynamoCleaner
from cdh_applications.cleanup.cleaners.glue_cleaner import GlueCleaner
from cdh_applications.cleanup.cleaners.lake_formation_cleaner import LakeFormationCleaner
from cdh_applications.cleanup.cleaners.ram_cleaner import RamCleaner
from cdh_applications.cleanup.cleaners.s3_cleaner import S3Cleaner
from cdh_applications.cleanup.cleaners.sns_cleaner import SnsCleaner
from cdh_applications.cleanup.generic_cleaner import GenericCleaner
from cdh_core.entities.accounts import BaseAccount
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region


class CleanerFactory:
    """Cleaner Factory to create all individual cleaners."""

    SERVICES_ONLY_IN_PREFERRED_PARTITION_REGION: Set[Type[GenericCleaner]] = {S3Cleaner}

    def __init__(  # pylint: disable=too-many-arguments
        self,
        account: BaseAccount,
        prefix: str,
        clean_filter: Callable[[str, str, Any], bool],
        regions: List[str],
        partition: Partition,
        log: Logger,
        credentials: Dict[str, Any],
    ) -> None:
        cleaner_classes_for_account: List[Type[GenericCleaner]] = self._get_cleaners_for_account(account)
        self._cleaners: List[GenericCleaner] = []
        for cleaner_class in cleaner_classes_for_account:
            self._cleaners.extend(
                self._create_service_cleaners(
                    cleaner_class,
                    account=account,
                    prefix=prefix,
                    clean_filter=clean_filter,
                    regions=regions,
                    partition=partition,
                    log=log,
                    credentials=credentials,
                )
            )

    def _create_service_cleaners(  # pylint: disable=too-many-arguments
        self,
        cleaner_class: Type[GenericCleaner],
        account: BaseAccount,
        clean_filter: Callable[[str, str, Any], bool],
        credentials: Dict[str, Any],
        log: Logger,
        partition: Partition,
        prefix: str,
        regions: List[str],
    ) -> List[GenericCleaner]:
        if cleaner_class in self.SERVICES_ONLY_IN_PREFERRED_PARTITION_REGION:
            regions = [Region.preferred(partition).value]

        common_parameters = {"clean_filter": clean_filter, "credentials": credentials, "log": log, "prefix": prefix}

        if issubclass(cleaner_class, GlueCleaner) or issubclass(cleaner_class, LakeFormationCleaner):
            return [
                cleaner_class(account=account, region=region, partition=partition, **common_parameters)  # type: ignore
                for region in regions
            ]

        return [cleaner_class(region=region, **common_parameters) for region in regions]  # type: ignore

    @staticmethod
    def _get_cleaners_for_account(account: BaseAccount) -> List[Type[GenericCleaner]]:
        services: List[Type[GenericCleaner]] = []
        if account.purpose is AccountPurpose("api"):
            services = [DynamoCleaner]
        elif account.purpose is AccountPurpose("resources"):
            services = [
                LakeFormationCleaner,
                GlueCleaner,
                S3Cleaner,
                SnsCleaner,
                RamCleaner,
            ]
        elif account.purpose is AccountPurpose("test"):
            services = [
                GlueCleaner,
                S3Cleaner,
                SnsCleaner,
            ]
        return services

    def get_cleaners(self) -> List[GenericCleaner]:
        """Get a list of all cleaners."""
        return self._cleaners
