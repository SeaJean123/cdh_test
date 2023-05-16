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
# pylint: disable=redefined-outer-name
import json
import os
import random
from logging import getLogger
from typing import Any
from typing import cast
from typing import Collection
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Union
from urllib.parse import urlparse

import boto3
import pytest
from aws_requests_auth.aws_auth import AWSRequestsAuth
from botocore.exceptions import ClientError
from waiting import wait

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.clients.http_client import HttpClient
from cdh_core.clients.http_client import HttpStatusCodeNotInExpectedCodes
from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.entities.accounts import HubAccount
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants_test import build_dataset_participant_id
from cdh_core.entities.resource import GlueSyncResourcePayload
from cdh_core.entities.resource import S3ResourcePayload
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.dataset_properties_test import build_confidentiality_not_secret
from cdh_core.enums.dataset_properties_test import build_layer
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core_dev_tools.testing.athena_query_service import AthenaQueryService
from cdh_core_dev_tools.testing.builder import Builder
from functional_tests.assume_role import assume_role
from functional_tests.conftest import FUNCTIONAL_TESTS_ROLE
from functional_tests.conftest import FUNCTIONAL_TESTS_VIEWER_ROLE
from functional_tests.utils import add_random_suffix
from functional_tests.utils import get_current_test_account

LOG = getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

DATA_EXPLORER_ROLE = "cdh-data-explorer"
ResourceList = List[Union[GlueSyncResourcePayload, S3ResourcePayload]]


class MutatingBasicTestConfig:
    """Test config for the mutating basic tests."""

    def __init__(self) -> None:
        try:
            self.base_url = os.environ["BASE_URL"]
            self.environment = Environment(os.environ["ENVIRONMENT"])
            self.resource_name_prefix = os.environ["RESOURCE_NAME_PREFIX"]
        except KeyError as missing_key:
            raise OSError(f"Environment variable {missing_key} has to be set.") from missing_key

        assert self.environment is Environment("dev")
        assert self.resource_name_prefix
        self.partition = Partition(os.environ.get("AWS_PARTITION", "aws"))

        self.test_role = FUNCTIONAL_TESTS_ROLE
        admin_role_name = ConfigFileLoader().get_config().account.admin_role_name
        self.admin_roles = ["CDHDevOps", admin_role_name, self.test_role]
        self.roles = [
            AccountRole(name=role, path="/", type=AccountRoleType.WRITE, friendly_name=role)
            for role in self.admin_roles
        ]
        self.athena_workgroup = self.resource_name_prefix + "cdh-functional-tests"

        self.core_api_account = next(
            iter(
                AccountStore().query_accounts(
                    account_purposes=AccountPurpose("api"), partitions=self.partition, environments=self.environment
                )
            )
        )

        self.test_accounts: Collection[HubAccount] = cast(
            Collection[HubAccount],
            AccountStore().query_accounts(
                account_purposes=AccountPurpose("test"),
                environments=self.environment,
                partitions=self.partition,
            ),
        )

    def __repr__(self) -> str:
        """Return the class as a dict, when print() is called."""
        return str(self.__dict__)


class MutatingBasicTestSetup:
    """Test setup for the mutating basic tests."""

    def __init__(self, mutating_basic_test_config: MutatingBasicTestConfig) -> None:
        self.test_provider_account = get_current_test_account(
            partition=mutating_basic_test_config.partition, environment=mutating_basic_test_config.environment
        )
        self.test_consumer_account = cast(
            HubAccount,
            Builder.get_random_element(
                to_choose_from=AccountStore().query_accounts(
                    account_purposes=AccountPurpose("test"),
                    partitions=mutating_basic_test_config.partition,
                    environments=mutating_basic_test_config.environment,
                ),
                exclude=[self.test_provider_account],
            ),
        )

        self.hub = random.choice(list(Hub.get_hubs(environment=mutating_basic_test_config.environment)))
        self.regions = self.hub.regions
        self.region = random.choice(list(self.regions))
        self.resource_account = random.choice(
            [
                *AccountStore().query_resource_accounts(
                    hubs=self.hub, environments=mutating_basic_test_config.environment, only_default=True
                )
            ]
        )
        self.stage = self.resource_account.stage
        self.business_object = build_business_object()
        self.layer = build_layer()

        self.provider_credentials = assume_role(
            prefix=mutating_basic_test_config.resource_name_prefix,
            account_id=self.test_provider_account.id,
            role=mutating_basic_test_config.test_role,
        )
        LOG.info(
            f"BASE_URL={mutating_basic_test_config.base_url} "
            f"RESOURCE_NAME_PREFIX={mutating_basic_test_config.resource_name_prefix} "
            f"TEST_PROVIDER={self.test_provider_account.id}"
        )
        region = Region.preferred(mutating_basic_test_config.partition)
        self.http_client = HttpClient(
            base_url=mutating_basic_test_config.base_url,
            credentials=(
                AWSRequestsAuth(
                    aws_access_key=self.provider_credentials["aws_access_key_id"],
                    aws_secret_access_key=self.provider_credentials["aws_secret_access_key"],
                    aws_token=self.provider_credentials["aws_session_token"],
                    aws_host=urlparse(mutating_basic_test_config.base_url).netloc,
                    aws_region=region.value,
                    aws_service="execute-api",
                )
            ),
        )
        self.core_api_client = CoreApiClient(http_client=self.http_client)

        self.api_account_credentials = assume_role(
            prefix=mutating_basic_test_config.resource_name_prefix,
            account_id=mutating_basic_test_config.core_api_account.id,
            role=FUNCTIONAL_TESTS_VIEWER_ROLE,
            credentials=self.provider_credentials,
        )


class BusinessTestSetup:
    """Test setup for the business cases tests."""

    def __init__(
        self, mutating_basic_test_setup: MutatingBasicTestSetup, mutating_basic_test_config: MutatingBasicTestConfig
    ) -> None:
        self.mutating_basic_test_setup = mutating_basic_test_setup
        self.mutating_basic_test_config = mutating_basic_test_config
        self.consumer_credentials = assume_role(
            prefix=mutating_basic_test_config.resource_name_prefix,
            account_id=mutating_basic_test_setup.test_consumer_account.id,
            role=mutating_basic_test_config.test_role,
            credentials=mutating_basic_test_setup.provider_credentials,
        )
        create_dataset_response = mutating_basic_test_setup.core_api_client.create_dataset(
            business_object=mutating_basic_test_setup.business_object,
            name=add_random_suffix("test"),
            layer=mutating_basic_test_setup.layer,
            hub=mutating_basic_test_setup.hub,
            confidentiality=self._get_confidentiality(),
            engineers=[DatasetParticipant(id=build_dataset_participant_id("someone@example.com"), idp="example")],
            description="Created by functional business test",
        )
        self.dataset_id = create_dataset_response.id
        self.database_name = (
            f"{mutating_basic_test_config.resource_name_prefix}{self.dataset_id}_"
            f"{mutating_basic_test_setup.stage.value}"
        )
        self.glue_client_provider: Any = boto3.client(
            "glue", region_name=mutating_basic_test_setup.region.value, **mutating_basic_test_setup.provider_credentials
        )
        self.glue_client_consumer: Any = boto3.client(
            "glue", region_name=mutating_basic_test_setup.region.value, **self.consumer_credentials
        )
        self.s3_prefix = "test_data"
        self.s3_key = f"{self.s3_prefix}/some_key_{random.randint(1, 999999)}"
        self.s3_data = f"businessCase sample data for key {self.s3_key}"
        self.provider_s3_client = boto3.client(
            "s3", region_name=mutating_basic_test_setup.region.value, **mutating_basic_test_setup.provider_credentials
        )
        self.consumer_s3_client = boto3.client(
            "s3", region_name=mutating_basic_test_setup.region.value, **self.consumer_credentials
        )
        LOG.info(f"DATASET_ID={self.dataset_id}")
        self.bucket = mutating_basic_test_setup.core_api_client.create_s3_resource(
            dataset_id=self.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            hub=mutating_basic_test_setup.hub,
            seconds_between_retries=10,
        )

        self.resource_account_credentials = assume_role(
            prefix=self.mutating_basic_test_config.resource_name_prefix,
            account_id=self.mutating_basic_test_setup.resource_account.id,
            role=FUNCTIONAL_TESTS_VIEWER_ROLE,
            credentials=self.mutating_basic_test_setup.provider_credentials,
        )
        self.data_explorer_credentials = assume_role(
            prefix=mutating_basic_test_config.resource_name_prefix,
            account_id=self.mutating_basic_test_setup.resource_account.id,
            role=DATA_EXPLORER_ROLE,
            credentials=self.resource_account_credentials,
        )
        self.data_explorer_workgroup = mutating_basic_test_config.resource_name_prefix + "cdh-data-explorer"
        self.explorer_s3_client = boto3.client(
            "s3", region_name=mutating_basic_test_setup.region.value, **self.data_explorer_credentials
        )

    def _get_confidentiality(self) -> Confidentiality:
        return build_confidentiality()

    def setup_provider(self, provider_sync_type: SyncType) -> None:
        """Create a glue-sync resource for the provider and add a table to it."""
        self._setup_glue_sync(provider_sync_type)
        wait(
            lambda: self._setup_table() is None,  # type:ignore
            expected_exceptions=self.glue_client_provider.exceptions.AccessDeniedException,
            timeout_seconds=60,
            sleep_seconds=5,
            waiting_for="association of resources after resource share creation",
        )

    def _setup_table(self, table_name: Optional[str] = None, s3_prefix: Optional[str] = None) -> None:
        self.glue_client_provider.create_table(
            DatabaseName=self.database_name,
            TableInput={
                "Name": table_name or self.database_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "columnA", "Type": "string"}],
                    "Location": f"s3://{self.bucket.name}/{s3_prefix or self.s3_prefix}/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "NumberOfBuckets": 0,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                        "Parameters": {"separatorChar": ";"},
                    },
                    "SortColumns": [{"Column": "columnA", "SortOrder": 1}],
                },
            },
        )
        # It looks like sometimes tables are not yet provisioned and following steps fail.
        # If we still have "AccessDeniedException when reading from source DatabaseConfig" errors despite waiting,
        # we probably have to add retries at sync_base.AccessDeniedException(initial_sync=true)
        self.assert_glue_table_exists(self.glue_client_provider, table_name)

    def _setup_glue_sync(self, provider_sync_type: SyncType) -> None:
        self.mutating_basic_test_setup.core_api_client.create_glue_sync(
            dataset_id=self.dataset_id,
            stage=self.mutating_basic_test_setup.stage,
            region=self.mutating_basic_test_setup.region,
            hub=self.mutating_basic_test_setup.hub,
            sync_type=provider_sync_type,
        )

    def assert_glue_table_exists(self, glue_client: Any, table_name: Optional[str] = None) -> None:
        """Assert that the table exists in the test glue database and the given glue client."""
        assert glue_client.get_table(DatabaseName=self.database_name, Name=table_name or self.database_name) is not None

    def assert_glue_database_deleted(self, glue_client: Any) -> None:
        """Assert that the glue database set up in the test does not exist for the given glue client."""
        try:
            glue_client.get_database(Name=self.database_name)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code", "UnknownError") != "EntityNotFoundException":
                raise

    def check_athena_query_by_consumer(self, table_name: Optional[str] = None, s3_data: Optional[str] = None) -> None:
        """Check Athena query from consumer account."""
        self.check_athena_query(
            athena_workgroup=self.mutating_basic_test_config.athena_workgroup,
            credentials=self.consumer_credentials,
            table_name=table_name,
            s3_data=s3_data,
        )

    def check_athena_query_by_data_explorer(self) -> None:
        """Check Athena query from data explorer role."""
        self.check_athena_query(
            athena_workgroup=self.data_explorer_workgroup,
            credentials=self.data_explorer_credentials,
        )

    def check_athena_query(
        self,
        athena_workgroup: str,
        credentials: Dict[str, Any],
        table_name: Optional[str] = None,
        s3_data: Optional[str] = None,
    ) -> None:
        """Check that Athena can be queried and the response matches the ingested data."""
        query_service = AthenaQueryService(
            athena_workgroup=athena_workgroup,
            region=self.mutating_basic_test_setup.region.value,
            credentials=credentials,
        )
        athena_query_response = query_service.run_athena_query(
            database_name=self.database_name,
            table_name=table_name or self.database_name,
        )
        assert athena_query_response
        assert athena_query_response["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"] == (s3_data or self.s3_data)

    def write_s3_data_from_provider(self, s3_key: Optional[str] = None, s3_data: Optional[str] = None) -> None:
        """Ingest S3 data from the provider account."""
        self.provider_s3_client.put_object(
            Bucket=self.bucket.name,
            Key=s3_key or self.s3_key,
            Body=(s3_data or self.s3_data).encode("UTF-8"),
            ContentType="text/plain",
            ACL="bucket-owner-full-control",
        )

    def setup_attribute_extractor_subscriber_queue(self) -> Any:
        """Set up queue that subscribes to the attribute extractor lambda."""
        partition = self.mutating_basic_test_config.partition
        sqs_queue_name = add_random_suffix(
            f"{self.mutating_basic_test_config.resource_name_prefix}functional-testing-attribute-extractor-queue"
        )
        sqs_res = boto3.resource(
            "sqs",
            region_name=self.mutating_basic_test_setup.region.value,
            **self.mutating_basic_test_setup.provider_credentials,
        )
        source_arn = (
            f"arn:{partition.value}:sns:*:{self.mutating_basic_test_setup.resource_account.id}:"
            f"{self.mutating_basic_test_config.resource_name_prefix}cdh-*"
        )
        policy_dict = {
            "Version": "2012-10-17",
            "Id": "sqspolicy",
            "Statement": [
                {
                    "Sid": "AllowSNS",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "sqs:SendMessage",
                    "Resource": "*",
                    "Condition": {"ArnEquals": {"aws:SourceArn": source_arn}},
                }
            ],
        }
        queue = sqs_res.create_queue(QueueName=sqs_queue_name, Attributes={"Policy": json.dumps(policy_dict)})
        queue.load()
        sqs_queue_arn = queue.attributes["QueueArn"].replace(":sqs.", ":")

        sns_topic = self.bucket.attributes.sns_topic_arn
        sns_client = boto3.client(
            "sns",
            region_name=self.mutating_basic_test_setup.region.value,
            **self.mutating_basic_test_setup.provider_credentials,
        )
        sns_client.subscribe(TopicArn=str(sns_topic), Protocol="sqs", Endpoint=sqs_queue_arn)
        return queue

    def assert_correct_message_from_attribute_extractor_sent(self, queue: Any) -> None:
        """Assert that th correct message is sent from the attribute extractor lambda."""
        message = self._get_message_by_key(key=self.s3_key, sqs_queue=queue)
        self._validate_message(message=message, bucket_name=self.bucket.name, key=self.s3_key)
        self._validate_no_other_message(message_id=message.message_id, queue=queue)

    @staticmethod
    def _validate_no_other_message(message_id: str, queue: Any) -> None:
        for message in queue.receive_messages(
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            VisibilityTimeout=15,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=1,
        ):
            if message:
                assert message.message_id == message_id

    @staticmethod
    def _validate_message(message: Any, bucket_name: str, key: str) -> None:
        msg_json = json.loads(message.body)
        assert msg_json["MessageAttributes"]["s3.bucket.name"]["Value"] == bucket_name
        expected_subject = "New Object: " + key
        expected_subject = (expected_subject[:95] + "...") if len(expected_subject) > 98 else expected_subject
        expected_subject = expected_subject.encode("ascii", errors="ignore").decode()
        assert msg_json["Subject"] == expected_subject
        message.delete()

    @staticmethod
    def _get_message_by_key(key: str, sqs_queue: Any) -> Any:
        for message in sqs_queue.receive_messages(
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            VisibilityTimeout=15,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
        ):
            msg_json = json.loads(message.body)
            msg_body = json.loads(msg_json["Message"])
            key_in_message = msg_body["Records"][0]["s3"]["object"]["key"]
            if key_in_message == key:
                return message
            raise Exception(
                f"Message with body: {message.body} and key: {key_in_message} "
                + f"found in sqs that did not match: {key}"
            )
        raise Exception(f"No message in sqs found with key: {key}")


class DataExplorerTestSetup(BusinessTestSetup):
    """Test setup for the data explorer test, where a preview must be available for the dataset."""

    def _get_confidentiality(self) -> Confidentiality:
        return build_confidentiality_not_secret()


class TestDatasetDefinition:
    """Dataset definition for tests."""

    __test__ = False

    def __init__(self, mutating_basic_test_setup: MutatingBasicTestSetup) -> None:
        self.dataset_name = add_random_suffix("test")
        self.dataset_id = Dataset.build_id(
            business_object=mutating_basic_test_setup.business_object,
            name=self.dataset_name,
            layer=mutating_basic_test_setup.layer,
            hub=mutating_basic_test_setup.hub,
        )


class TestDataset:
    """Test dataset."""

    __test__ = False

    def __init__(
        self, mutating_basic_test_setup: MutatingBasicTestSetup, mutating_basic_test_config: MutatingBasicTestConfig
    ) -> None:
        self.dataset_name = add_random_suffix("test")
        api_response = mutating_basic_test_setup.core_api_client.create_dataset(
            business_object=mutating_basic_test_setup.business_object,
            name=self.dataset_name,
            layer=mutating_basic_test_setup.layer,
            hub=mutating_basic_test_setup.hub,
            confidentiality=build_confidentiality(),
            description="Created by functional test",
            engineers=[DatasetParticipant(id=build_dataset_participant_id("someone@example.com"), idp="example")],
            preview_available=random.choice([None, True, False]),
        )
        self.dataset_id = api_response.id
        self.owner_id = api_response.owner_account_id
        self.database_name = mutating_basic_test_config.resource_name_prefix + self.dataset_id


@pytest.fixture(scope="module")
def mutating_basic_test_config() -> MutatingBasicTestConfig:
    """Get the fixture for the mutating basic test config."""
    return MutatingBasicTestConfig()


@pytest.fixture(scope="module")
def mutating_basic_test_setup(mutating_basic_test_config: MutatingBasicTestConfig) -> MutatingBasicTestSetup:
    """Get the fixture for the mutating basic test setup."""
    return MutatingBasicTestSetup(mutating_basic_test_config)


@pytest.fixture(scope="function")
def business_test_setup(
    mutating_basic_test_setup: MutatingBasicTestSetup, mutating_basic_test_config: MutatingBasicTestConfig
) -> BusinessTestSetup:
    """Get the fixture for the business cases test setup."""
    LOG.info(f"Using TestSetup: {mutating_basic_test_setup}")
    return BusinessTestSetup(mutating_basic_test_setup, mutating_basic_test_config)


@pytest.fixture(scope="function")
def data_explorer_test_setup(
    mutating_basic_test_setup: MutatingBasicTestSetup, mutating_basic_test_config: MutatingBasicTestConfig
) -> DataExplorerTestSetup:
    """Get the fixture for the data explorer test setup."""
    LOG.info(f"Using TestSetup: {mutating_basic_test_setup}")
    return DataExplorerTestSetup(mutating_basic_test_setup, mutating_basic_test_config)


@pytest.fixture(scope="function")
def test_dataset_definition(mutating_basic_test_setup: MutatingBasicTestSetup) -> TestDatasetDefinition:
    """Get the fixture for the test dataset definition."""
    return TestDatasetDefinition(mutating_basic_test_setup)


@pytest.fixture(scope="function")
def test_dataset(
    mutating_basic_test_setup: MutatingBasicTestSetup, mutating_basic_test_config: MutatingBasicTestConfig
) -> TestDataset:
    """Get the fixture for the test dataset."""
    return TestDataset(mutating_basic_test_setup, mutating_basic_test_config)


@pytest.fixture(scope="function")
def resources_to_clean_up(
    mutating_basic_test_setup: MutatingBasicTestSetup,
) -> Generator[ResourceList, None, None]:
    """Get the fixture for the resources to delete after test execution."""
    resources_to_clean_up: ResourceList = []
    yield resources_to_clean_up
    for resource in reversed(resources_to_clean_up):
        try:
            wait(
                lambda r=resource: mutating_basic_test_setup.core_api_client.delete_resource(  # type: ignore
                    hub=r.hub,
                    dataset_id=r.dataset_id,
                    resource_type=r.type,
                    stage=r.stage,
                    region=r.region,
                    fail_if_not_found=False,
                )
                is None,
                expected_exceptions=HttpStatusCodeNotInExpectedCodes,
                timeout_seconds=60,
                sleep_seconds=5,
                waiting_for="association of resources after resource share creation",
            )
        except Exception as err:  # pylint: disable=broad-except
            LOG.warning(f"Failed to remove resource {resource} during teardown: {err}")
