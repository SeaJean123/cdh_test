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
from typing import Generator

import boto3
from botocore.exceptions import ClientError

from cdh_applications.cleanup.cleanup_utils import has_prefix
from cdh_applications.cleanup.generic_cleaner import GenericCleaner
from cdh_core.aws_clients.utils import repeat_continuation_call


class RamCleaner(GenericCleaner):
    """Cleaner class for the functional tests for the AWS RAM service."""

    def __init__(  # pylint: disable=too-many-arguments,super-init-not-called
        self,
        region: str,
        prefix: str,
        clean_filter: Callable[[str, str, Any], bool],
        credentials: Dict[str, Any],
        log: Logger,
    ):
        self._region = region
        self._prefix = prefix
        self._should_clean = clean_filter
        self._client = boto3.client("ram", region_name=region, **credentials)
        self.logger = log

    def clean(self) -> None:
        """Start the cdh_cleanup of the AWS RAM service."""
        self.logger.info(f"Looking for resource shares in {self._region}...")
        for resource_share in self._list_all_resource_shares():
            if resource_share["status"] == "DELETED":
                continue
            name = resource_share["name"]
            arn = resource_share["resourceShareArn"]
            if has_prefix(name, self._prefix) and self._should_clean("resource share", name, self.logger):
                self._delete_resource_share(arn)

    def _list_all_resource_shares(self) -> Generator[Any, None, None]:
        yield from repeat_continuation_call(
            self._client.get_resource_shares,
            "resourceShares",
            resourceOwner="SELF",
        )

    def _delete_resource_share(self, arn: str) -> None:
        try:
            self._client.delete_resource_share(resourceShareArn=arn)
        except ClientError as error:
            self.logger.warning(error)
