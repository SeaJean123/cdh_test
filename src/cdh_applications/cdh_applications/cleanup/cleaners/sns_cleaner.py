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

from cdh_applications.cleanup.cleanup_utils import has_resource_prefix
from cdh_applications.cleanup.generic_cleaner import GenericCleaner
from cdh_core.aws_clients.utils import repeat_continuation_call


class SnsCleaner(GenericCleaner):
    """Cleaner class for the functional tests for the AWS SNS service."""

    def __init__(  # pylint: disable=too-many-arguments,super-init-not-called
        self,
        region: str,
        prefix: str,
        clean_filter: Callable[[str, str, Any], bool],
        credentials: Dict[str, Any],
        log: Logger,
    ) -> None:
        self._region = region
        self._prefix = prefix
        self._should_clean = clean_filter
        self._client = boto3.client("sns", region_name=region, **credentials)
        self.logger = log

    def clean(self) -> None:
        """Start the cdh_cleanup of the AWS SNS service."""
        self.logger.info(f"Looking for SNS topics in {self._region}...")
        for topic in self._list_all_topics():
            name = topic["TopicArn"].split(":")[-1]
            if self._filter_topic(name) and self._should_clean("topic", name, self.logger):
                self._delete_topic(topic["TopicArn"])

    def _list_all_topics(self) -> Generator[Any, None, None]:
        yield from repeat_continuation_call(self._client.list_topics, "Topics")

    def _filter_topic(self, name: str) -> bool:
        return has_resource_prefix(name, self._prefix) and not name.startswith(self._prefix + "cdh-core")

    def _delete_topic(self, arn: str) -> None:
        try:
            self._client.delete_topic(TopicArn=arn)
        except ClientError as error:
            self.logger.warning(error)
