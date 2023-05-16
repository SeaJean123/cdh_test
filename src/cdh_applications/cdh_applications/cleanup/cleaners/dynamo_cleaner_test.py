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
from mypy_boto3_dynamodb.client import DynamoDBClient

from cdh_applications.cleanup.cleaners.dynamo_cleaner import DynamoCleaner
from cdh_applications.cleanup.cleanup_utils_test import PREFIX
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.builder import Builder


@pytest.mark.usefixtures("mock_dynamodb")
class TestDynamoCleaner:
    DEFAULT_NUMBER_OF_ITEMS = 5

    @pytest.fixture()
    def region(self) -> str:
        return build_region().value  # type: ignore

    def _create_table(
        self,
        client: DynamoDBClient,
        table_name: str,
        hash_key_name: str,
        number_of_items: int = DEFAULT_NUMBER_OF_ITEMS,
    ) -> None:
        attribute_def = [{"AttributeName": hash_key_name, "AttributeType": "S"}]
        key_schema = [{"AttributeName": hash_key_name, "KeyType": "HASH"}]
        client.create_table(
            AttributeDefinitions=attribute_def,  # type: ignore
            TableName=table_name,
            KeySchema=key_schema,  # type: ignore
            ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
        )
        for i in range(number_of_items):
            dynamo_item = {hash_key_name: {"S": str(9999 - i)}}
            client.put_item(TableName=table_name, Item=dynamo_item)

    def test_dont_truncate_table_if_clean_filter_returns_false(self, region: str) -> None:
        table_name = f"{PREFIX}-{Builder.build_random_string()}"
        clean_filter = Mock(return_value=False)
        dynamo_cleaner = DynamoCleaner(
            region=region, prefix=PREFIX, clean_filter=clean_filter, log=getLogger(), credentials={}
        )
        self._create_table(
            client=dynamo_cleaner._client, table_name=table_name, hash_key_name=Builder.build_random_string()
        )

        dynamo_cleaner.clean()

        clean_filter.assert_called_once_with("contents of table", table_name, getLogger())
        response = dynamo_cleaner._client.scan(TableName=table_name)
        assert response["Count"] == self.DEFAULT_NUMBER_OF_ITEMS

    def test_only_truncate_matching_prefix_tables(self, region: str) -> None:
        table_name_with_prefix = f"{PREFIX}-{Builder.build_random_string()}"
        table_name_without_prefix = Builder.build_random_string()
        table_names = [table_name_with_prefix, table_name_without_prefix]
        clean_filter = Mock(return_value=True)
        dynamo_cleaner = DynamoCleaner(
            region=region, prefix=PREFIX, clean_filter=clean_filter, log=getLogger(), credentials={}
        )
        for table_name in table_names:
            self._create_table(
                client=dynamo_cleaner._client, table_name=table_name, hash_key_name=Builder.build_random_string()
            )

        dynamo_cleaner.clean()

        clean_filter.assert_called_once_with("contents of table", table_name_with_prefix, getLogger())
        with_prefix_response = dynamo_cleaner._client.scan(TableName=table_name_with_prefix)
        assert with_prefix_response["Count"] == 0
        without_prefix_response = dynamo_cleaner._client.scan(TableName=table_name_without_prefix)
        assert without_prefix_response["Count"] == self.DEFAULT_NUMBER_OF_ITEMS

    def test_truncating_large_table(self, region: str) -> None:
        table_name = f"{PREFIX}-{Builder.build_random_string()}"
        mocked_number_of_items = 3000
        clean_filter = Mock(return_value=True)
        dynamo_cleaner = DynamoCleaner(
            region=region, prefix=PREFIX, clean_filter=clean_filter, log=getLogger(), credentials={}
        )
        self._create_table(
            client=dynamo_cleaner._client,
            table_name=table_name,
            hash_key_name=Builder.build_random_string(),
            number_of_items=mocked_number_of_items,
        )

        assert dynamo_cleaner._client.scan(TableName=table_name)["Count"] == mocked_number_of_items

        dynamo_cleaner.clean()

        clean_filter.assert_called_once_with("contents of table", table_name, getLogger())
        assert dynamo_cleaner._client.scan(TableName=table_name)["Count"] == 0
