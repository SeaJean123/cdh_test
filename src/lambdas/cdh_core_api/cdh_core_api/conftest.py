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
# pylint: disable=unused-import
# pylint: disable=redefined-outer-name
from typing import Any

import boto3
import pytest
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.filter_packages_table import FilterPackagesTable
from cdh_core_api.catalog.resource_table import ResourcesTable
from mypy_boto3_dynamodb.service_resource import Table

from cdh_core.conftest import mock_config_file
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.fixtures import fixture_resource_name_prefix
from cdh_core_dev_tools.testing.fixtures import mock_dynamodb
from cdh_core_dev_tools.testing.fixtures import mock_kms
from cdh_core_dev_tools.testing.fixtures import mock_sns
from cdh_core_dev_tools.testing.fixtures import mock_xray
from cdh_core_dev_tools.testing.fixtures import time_travel

_DYNAMO_DB_REGION = Region.preferred(Partition.default()).value


@pytest.fixture()
def mock_datasets_dynamo_table(  # pylint: disable=unused-argument
    mock_dynamodb: None, resource_name_prefix: str  # noqa: F811
) -> Table:
    """Mock a dataset dynamo table with moto."""
    table_name = resource_name_prefix + "cdh-datasets"
    return boto3.resource("dynamodb", region_name=_DYNAMO_DB_REGION).create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )


@pytest.fixture()
def mock_locks_dynamo_table(
    mock_dynamodb: None, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> Table:
    """Mock a locks dynamo table with moto."""
    table_name = resource_name_prefix + "cdh-locks"
    return boto3.resource("dynamodb", region_name=_DYNAMO_DB_REGION).create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "lock_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "lock_id", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )


@pytest.fixture()
def mock_accounts_dynamo_table(
    mock_dynamodb: None, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> Table:
    """Mock an accounts dynamo table with moto."""
    table_name = resource_name_prefix + "cdh-accounts"
    return boto3.resource("dynamodb", region_name=_DYNAMO_DB_REGION).create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "account_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "account_id", "AttributeType": "S"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )


@pytest.fixture()
def mock_resources_dynamo_table(
    mock_dynamodb: None, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> Table:
    """Mock a resources dynamo table with moto."""
    table_name = resource_name_prefix + "cdh-resources"
    return boto3.resource("dynamodb", region_name=_DYNAMO_DB_REGION).create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "dataset_id", "KeyType": "HASH"}, {"AttributeName": "id", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "dataset_id", "AttributeType": "S"},
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )


@pytest.fixture()
def mock_filter_packages_dynamo_table(
    mock_dynamodb: None, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> Table:
    """Mock a filter packages dynamo table with moto."""
    table_name = resource_name_prefix + "cdh-filter-packages"
    return boto3.resource("dynamodb", region_name=_DYNAMO_DB_REGION).create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "datasetid_stage_region", "KeyType": "HASH"},
            {"AttributeName": "id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "datasetid_stage_region", "AttributeType": "S"},
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    )


@pytest.fixture()
def accounts_table(
    mock_accounts_dynamo_table: Any, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> AccountsTable:
    """Mock the AccountsTable with moto."""
    return AccountsTable(resource_name_prefix)


@pytest.fixture()
def datasets_table(
    mock_datasets_dynamo_table: Any, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> DatasetsTable:
    """Mock the DatasetsTable with moto."""
    return DatasetsTable(resource_name_prefix)


@pytest.fixture()
def resources_table(
    mock_resources_dynamo_table: Any, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> ResourcesTable:
    """Mock the ResourcesTable with moto."""
    return ResourcesTable(resource_name_prefix)


@pytest.fixture()
def filter_packages_table(
    mock_filter_packages_dynamo_table: Any, resource_name_prefix: str  # pylint: disable=unused-argument  # noqa: F811
) -> FilterPackagesTable:
    """Mock the ResourcesTable with moto."""
    return FilterPackagesTable(resource_name_prefix)
