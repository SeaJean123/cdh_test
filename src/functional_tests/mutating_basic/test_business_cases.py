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
import os
from logging import getLogger
from typing import Any
from typing import Dict

import boto3
import pytest
from botocore.exceptions import ClientError
from waiting import wait

from cdh_core.clients.http_client import HttpStatusCodeNotInExpectedCodes
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.arn import Arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.environment import Environment
from cdh_core.primitives.account_id import AccountId
from functional_tests.mutating_basic.conftest import BusinessTestSetup
from functional_tests.mutating_basic.conftest import DataExplorerTestSetup
from functional_tests.mutating_basic.conftest import MutatingBasicTestSetup
from functional_tests.utils import add_random_suffix

# Business tests in this file run on prefixed deployments and may modify the data on those deployments.

LOG = getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


class TestDataAccess:
    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_coreapi_data_access(
        self,
        business_test_setup: BusinessTestSetup,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        sync_type: SyncType,
    ) -> None:
        """
        Test that a consumer can access Glue and S3 data from a provider.

        This test covers following business case:
        1. provider: Creation of glue-sync resource using Core-API
           (to sync glue-database/table to resource account and permitted consumer accounts)
        2. provider: Grant dataset access permission to a "consumer" account using the Core-Api.
           This automatically creates a resource-link to the glue db/table in the consumer account.
           (hint: we are using another provider-account to save costs)
        4. provider: Upload data to the provider's s3 bucket and validate that the
           s3-attribute-extractor lambda is running and event is pushed into SNS/SQS
        5. consumer: Query latest uploaded data (of step 4) using athena (in consumer account)
        6. provider: Revoke dataset access permission to a "consumer" account using Core-Api.
        7. consumer: Check glue databases is deleted (in consumer account).
        """
        business_test_setup.setup_provider(sync_type)

        # grant consumer access to the dataset
        mutating_basic_test_setup.core_api_client.grant_dataset_permission(
            dataset_id=business_test_setup.dataset_id,
            account_id=mutating_basic_test_setup.test_consumer_account.id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            hub=mutating_basic_test_setup.hub,
        )

        # upload data into bucket and verify attribute-extractor is working
        queue = business_test_setup.setup_attribute_extractor_subscriber_queue()
        business_test_setup.write_s3_data_from_provider()
        business_test_setup.assert_correct_message_from_attribute_extractor_sent(queue)

        # verify glue-sync including permissions are working so that consumer can use athena to collect data
        business_test_setup.assert_glue_table_exists(business_test_setup.glue_client_consumer)
        business_test_setup.check_athena_query_by_consumer()

        # revoke permission on dataset
        wait(
            lambda: mutating_basic_test_setup.core_api_client.revoke_dataset_permission(
                dataset_id=business_test_setup.dataset_id,
                account_id=mutating_basic_test_setup.test_consumer_account.id,
                stage=mutating_basic_test_setup.stage,
                region=mutating_basic_test_setup.region,
                hub=mutating_basic_test_setup.hub,
            )  # type: ignore
            is None,
            expected_exceptions=HttpStatusCodeNotInExpectedCodes,
            timeout_seconds=60,
            sleep_seconds=5,
            waiting_for="association of resources after resource share creation",
        )

        # verify glue-sync database is deleted in consumer account
        business_test_setup.assert_glue_database_deleted(business_test_setup.glue_client_consumer)


class TestS3WriteRestrictions:
    def test_bucket_write_restrictions(
        self,
        business_test_setup: BusinessTestSetup,
        mutating_basic_test_setup: MutatingBasicTestSetup,
    ) -> None:
        """
        Test that writing to CDH bucket is only possible when the following conditions are met.

        1. ACL 'bucket-owner-full-control' can be set (legacy way)
        2. Only the provider account has write access
        3. Cannot specify an encryption method other than 'aws:kms'
        4. Cannot specify a KMS key different from the one designated by the S3 resource
        Ensures that the bucket's designated KMS key is used by default.
        """
        bucket = business_test_setup.bucket

        assert not self._put_object_raises_access_denied(
            s3_client=business_test_setup.provider_s3_client,
            bucket_name=bucket.name,
            description="upload to S3 the legacy way",
            ACL="bucket-owner-full-control",
        )
        assert not self._put_object_raises_access_denied(
            s3_client=business_test_setup.provider_s3_client,
            bucket_name=bucket.name,
            description="upload to S3 the proper way",
        )
        assert self._put_object_raises_access_denied(
            s3_client=business_test_setup.consumer_s3_client,
            bucket_name=bucket.name,
            description="upload by consumer account",
        )
        assert self._put_object_raises_access_denied(
            s3_client=business_test_setup.provider_s3_client,
            bucket_name=bucket.name,
            description="upload to S3 with incorrect encryption method",
            ServerSideEncryption="AES256",
        )
        assert not self._put_object_raises_access_denied(
            s3_client=business_test_setup.provider_s3_client,
            bucket_name=bucket.name,
            description="upload to S3 with explicit bucket kms key",
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=str(bucket.attributes.kms_key_arn),
        )
        test_kms_key_arn = self._get_test_kms_key_arn(
            region=mutating_basic_test_setup.region,
            credentials=mutating_basic_test_setup.provider_credentials,
            user_account_id=mutating_basic_test_setup.test_provider_account.id,
        )
        assert self._put_object_raises_access_denied(
            s3_client=business_test_setup.provider_s3_client,
            bucket_name=bucket.name,
            description="upload to S3 with incorrect KMS key",
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=str(test_kms_key_arn),
        )

        assert business_test_setup.provider_s3_client.put_object(
            Bucket=bucket.name,
            Key="test-default-kms-encryption",
            Body="test-default-kms-encryption".encode("UTF-8"),
            ContentType="text/plain",
        )["SSEKMSKeyId"] == str(bucket.attributes.kms_key_arn)

    @staticmethod
    def _put_object_raises_access_denied(s3_client: Any, bucket_name: str, description: str, **s3_kwargs: Any) -> bool:
        try:
            key = add_random_suffix(description.replace(" ", "-"))
            s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=description.encode("UTF-8"),
                ContentType="text/plain",
                **s3_kwargs,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "AccessDenied":
                return True
        return False

    @staticmethod
    def _get_test_kms_key_arn(region: Region, credentials: Dict[str, Any], user_account_id: AccountId) -> Arn:
        kms_client = boto3.client("kms", region_name=region.value, **credentials)
        security_account = AccountStore().query_account(
            environments=frozenset(Environment),
            account_purposes=AccountPurpose("security"),
            partitions=Partition("aws"),
        )
        response = kms_client.describe_key(
            KeyId=f"arn:aws:kms:{region.value}:{security_account.id}:alias/cdh-internal-test-key-{user_account_id}"
        )
        return Arn(response["KeyMetadata"]["Arn"])


class TestGlueDeleteRestrictions:
    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_provider_cannot_delete_database(self, business_test_setup: BusinessTestSetup, sync_type: SyncType) -> None:
        business_test_setup.setup_provider(sync_type)

        assert not self._can_delete_glue_database(
            glue_client=business_test_setup.glue_client_provider, database_name=business_test_setup.database_name
        )

    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_consumer_cannot_delete_database(
        self,
        business_test_setup: BusinessTestSetup,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        sync_type: SyncType,
    ) -> None:
        business_test_setup.setup_provider(sync_type)

        mutating_basic_test_setup.core_api_client.grant_dataset_permission(
            dataset_id=business_test_setup.dataset_id,
            account_id=mutating_basic_test_setup.test_consumer_account.id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            hub=mutating_basic_test_setup.hub,
        )
        business_test_setup.assert_glue_table_exists(business_test_setup.glue_client_consumer)

        assert not self._can_delete_glue_database(
            glue_client=business_test_setup.glue_client_consumer, database_name=business_test_setup.database_name
        )

    @staticmethod
    def _can_delete_glue_database(glue_client: Any, database_name: str) -> bool:
        try:
            glue_client.delete_database(Name=database_name)
            return True
        except ClientError as error:
            if (
                error.response["Error"]["Code"] == "AccessDeniedException"
                and "explicit deny in a resource-based policy" in error.response["Error"]["Message"]
            ):
                return False
            raise Exception(
                f"Unexpected error occurred while checking whether Glue DB {database_name!r} could be deleted"
            ) from error


class TestDataExplorerAccess:
    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_data_explorer_access(
        self,
        data_explorer_test_setup: DataExplorerTestSetup,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        sync_type: SyncType,
    ) -> None:
        data_explorer_test_setup.setup_provider(sync_type)
        data_explorer_test_setup.write_s3_data_from_provider()

        initial_dataset = mutating_basic_test_setup.core_api_client.get_dataset(
            hub=mutating_basic_test_setup.hub, dataset_id=data_explorer_test_setup.dataset_id
        )
        assert initial_dataset.preview_available
        data_explorer_test_setup.check_athena_query_by_data_explorer()
        self._wait_for_correct_access(
            s3_client=data_explorer_test_setup.explorer_s3_client,
            bucket=data_explorer_test_setup.bucket.name,
            key=data_explorer_test_setup.s3_key,
            should_have_access=True,
        )

        mutating_basic_test_setup.core_api_client.update_dataset(
            hub=mutating_basic_test_setup.hub, dataset_id=data_explorer_test_setup.dataset_id, preview_available=False
        )
        self._wait_for_correct_access(
            s3_client=data_explorer_test_setup.explorer_s3_client,
            bucket=data_explorer_test_setup.bucket.name,
            key=data_explorer_test_setup.s3_key,
            should_have_access=False,
        )

        mutating_basic_test_setup.core_api_client.update_dataset(
            hub=mutating_basic_test_setup.hub, dataset_id=data_explorer_test_setup.dataset_id, preview_available=True
        )
        data_explorer_test_setup.check_athena_query_by_data_explorer()
        # Note: This final step was observed to be flaky, or at least take very long until access was available.
        # Additional logging output was added. Should this behaviour continue, it might be interesting to contact the
        # AWS support about this.
        self._wait_for_correct_access(
            s3_client=data_explorer_test_setup.explorer_s3_client,
            bucket=data_explorer_test_setup.bucket.name,
            key=data_explorer_test_setup.s3_key,
            should_have_access=True,
        )

    @staticmethod
    def _wait_for_correct_access(s3_client: Any, bucket: str, key: str, should_have_access: bool) -> None:
        def has_access() -> bool:
            try:
                response = s3_client.get_object(
                    Bucket=bucket,
                    Key=key,
                )
            except ClientError as err:
                if err.response["Error"]["Code"] == "AccessDenied":
                    LOG.info(f"Request ID {err.response['ResponseMetadata']['RequestId']} failed with access denied.")
                    return False
                raise
            LOG.info(f"Request ID {response['ResponseMetadata']['RequestId']} succeeded.")
            return True

        wait(
            lambda: has_access() == should_have_access,
            timeout_seconds=300,
            sleep_seconds=10,
        )
