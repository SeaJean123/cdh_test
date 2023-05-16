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

from cdh_applications.cleanup.cleaners.sns_cleaner import SnsCleaner
from cdh_applications.cleanup.cleanup_utils_test import PREFIX
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties_test import build_business_object


@pytest.mark.usefixtures("mock_sns")
class TestSnsCleaner:
    @pytest.fixture()
    def region(self) -> str:
        return build_region().value  # type: ignore

    def test_dont_delete_topic_if_clean_filter_returns_false(self, region: str) -> None:
        topic_name = f"{PREFIX}cdh-{build_business_object().value}-test"
        clean_filter = Mock(return_value=False)
        sns_cleaner = SnsCleaner(
            region=region, prefix=PREFIX, clean_filter=clean_filter, credentials={}, log=getLogger()
        )
        sns_cleaner._client.create_topic(Name=topic_name)

        sns_cleaner.clean()

        clean_filter.assert_called_once_with("topic", topic_name, getLogger())
        assert [topic_arn["TopicArn"].split(":")[-1] for topic_arn in sns_cleaner._client.list_topics()["Topics"]] == [
            topic_name
        ]

    def test_only_delete_valid_topics(self, region: str) -> None:
        valid_topic_name = f"{PREFIX}cdh-{build_business_object().value}-test"
        invalid_topic_name = f"{PREFIX}cdh-test"
        topic_names = [valid_topic_name, invalid_topic_name]
        clean_filter = Mock(return_value=True)
        sns_cleaner = SnsCleaner(
            region=region, prefix=PREFIX, clean_filter=clean_filter, credentials={}, log=getLogger()
        )
        for topic_name in topic_names:
            sns_cleaner._client.create_topic(Name=topic_name)

        sns_cleaner.clean()

        clean_filter.assert_called_once_with("topic", valid_topic_name, getLogger())
        assert sorted(
            [topic_arn["TopicArn"].split(":")[-1] for topic_arn in sns_cleaner._client.list_topics()["Topics"]]
        ) == [invalid_topic_name]
