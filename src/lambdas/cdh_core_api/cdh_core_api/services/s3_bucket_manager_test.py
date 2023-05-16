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
import json
import random
from typing import Any
from typing import Optional
from unittest.mock import ANY
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import PropertyMock

import pytest
from cdh_core_api.config_test import build_config
from cdh_core_api.services.s3_bucket_manager import DEFAULT_S3_TAGS
from cdh_core_api.services.s3_bucket_manager import LOG
from cdh_core_api.services.s3_bucket_manager import S3BucketManager
from cdh_core_api.services.s3_bucket_manager import S3ResourceSpecification

from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.s3_client import BucketAlreadyExists
from cdh_core.aws_clients.s3_client import BucketNotEmpty
from cdh_core.aws_clients.s3_client import BucketNotFound
from cdh_core.aws_clients.s3_client import S3Client
from cdh_core.aws_clients.sns_client import SnsTopic
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.arn import build_arn_string
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_kms_key_arn
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.environment import Environment
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core.primitives.constants import CREATED_BY_CORE_API_TAG
from cdh_core_dev_tools.testing.builder import Builder


class TestCreateBucket:
    def setup_method(self) -> None:
        self.resource_account = build_resource_account()
        self.hub = self.resource_account.hub
        self.stage = self.resource_account.stage
        self.config = build_config()
        self.dataset = build_dataset(hub=self.hub)
        self.owner_id = build_account_id()
        self.region = build_region()
        self.kms_key = KmsKey.parse_from_arn(build_kms_key_arn(region=self.region))
        self.spec = self.create_spec()
        self.aws = Mock(AwsClientFactory)
        self.s3_client = Mock(S3Client)
        self.account_bucket_count = random.randint(1, 10)
        self.s3_client.list_buckets.return_value = [Mock() for _ in range(self.account_bucket_count)]
        self.s3_client.bucket_exists.return_value = False
        self.s3_client.create_encrypted_bucket.side_effect = lambda name, *_, **__: build_arn(
            "s3", name, region=self.region
        )
        self.aws.s3_client.return_value = self.s3_client
        self.manager = S3BucketManager(self.config, self.aws)
        self.expected_bucket_name_without_random_part = f"{self.config.prefix}cdh-{self.dataset.id}-".replace("_", "-")
        self.logger = LOG

    def create_spec(self, stage: Optional[Stage] = None) -> S3ResourceSpecification:
        return S3ResourceSpecification(
            dataset=self.dataset,
            stage=stage or self.stage,
            region=self.region,
            resource_account_id=self.resource_account.id,
            owner_id=self.owner_id,
        )

    def test_create_bucket_successfully(self) -> None:
        bucket_arn = self.manager.create_bucket(spec=self.spec, kms_key=self.kms_key)

        bucket_name = bucket_arn.identifier
        assert bucket_name.startswith(self.expected_bucket_name_without_random_part)
        self.s3_client.create_encrypted_bucket.assert_called_once_with(
            name=bucket_name,
            region=self.region,
            kms_key_arn=self.kms_key.arn,
            tags=CREATED_BY_CORE_API_TAG,
        )
        self.s3_client.block_public_access.assert_called_once_with(bucket_name)
        self.s3_client.set_bucket_cors.assert_called_once_with(bucket_name)
        self.s3_client.set_lifecycle_configuration.assert_called_once_with(bucket_name)
        self.s3_client.enable_bucket_access_logging.assert_called_once_with(
            bucket=bucket_name, log_bucket=ANY, log_prefix=ANY
        )

    def test_create_bucket_with_access_logs(self) -> None:
        bucket_arn = self.manager.create_bucket(spec=self.spec, kms_key=self.kms_key)

        bucket_name = bucket_arn.identifier
        expected_log_bucket = (
            f"{self.config.prefix}cdh-core-s3-logging-{self.spec.resource_account_id}" f"-{self.spec.region.value}"
        )
        expected_log_prefix = f"{self.config.prefix}logs/{bucket_name}/"
        self.s3_client.enable_bucket_access_logging.assert_called_once_with(
            bucket=bucket_name, log_bucket=expected_log_bucket, log_prefix=expected_log_prefix
        )

    def test_append_random_suffix_until_available_name_found(self) -> None:
        retries = 3
        tested_bucket_names = []

        def create_bucket_side_effect() -> Any:
            count = 0

            def _inner(*args: Any, **kwargs: Any) -> Any:  # pylint: disable=unused-argument
                nonlocal count
                count += 1
                bucket_name = kwargs["name"]
                tested_bucket_names.append(bucket_name)
                if count == retries:
                    return build_arn(service="s3", resource=bucket_name)
                raise BucketAlreadyExists(bucket_name)

            return _inner

        self.s3_client.create_encrypted_bucket.side_effect = create_bucket_side_effect()

        self.manager.create_bucket(spec=self.spec, kms_key=self.kms_key)

        assert self.s3_client.create_encrypted_bucket.call_count == retries
        assert all(name.startswith(self.expected_bucket_name_without_random_part) for name in tested_bucket_names)
        assert len(set(tested_bucket_names)) == len(tested_bucket_names)  # make sure we actually use different names
        self.s3_client.create_encrypted_bucket.assert_called_with(
            name=tested_bucket_names[-1], region=ANY, kms_key_arn=ANY, tags=ANY
        )

    def test_replace_underscores_in_bucket_names(self) -> None:
        self.dataset = build_dataset(name="a_b_c_d_e")

        bucket_arn = self.manager.create_bucket(spec=self.spec, kms_key=self.kms_key)

        assert "_" not in bucket_arn.identifier

    def test_bucket_policy(self) -> None:
        owner_arn = build_arn_string("iam", None, self.owner_id, "root", partition=self.spec.region.partition)

        LOG.info = Mock()  # type: ignore

        bucket_arn = self.manager.create_bucket(spec=self.spec, kms_key=self.kms_key)

        assert LOG.info.call_args_list == [
            call(f"Successfully created bucket: {bucket_arn}"),
            call(
                json.dumps({"account_id": self.resource_account.id, "total_account_buckets": self.account_bucket_count})
            ),
        ]

        self.s3_client.set_bucket_policy.assert_called_once_with(bucket=bucket_arn.identifier, policy=ANY)
        policy: PolicyDocument = self.s3_client.set_bucket_policy.call_args.kwargs["policy"]
        # Instead of reimplementing the code that creates the policy here, we just test two examples
        # to make sure that the owner_arn and read_write_role_arn are correctly passed to that code.
        statement = policy.get_policy_statement_by_sid("AllowWriteForOwner")
        assert statement["Principal"] == {"AWS": owner_arn}
        statement = policy.get_policy_statement_by_sid("AllowPutObjectForOwner")
        assert statement["Principal"] == {"AWS": owner_arn}

    @pytest.mark.parametrize("extended_metrics_enabled", [True, False])
    @patch.object(Environment, "stages_with_extended_metrics", new_callable=PropertyMock)
    def test_enable_metrics_only_on_enabled_stages(
        self, stages_with_extended_metrics: Mock, extended_metrics_enabled: bool
    ) -> None:
        stage, other_stage = Builder.choose_without_repetition(list(Stage), 2)
        stages_with_extended_metrics.return_value = {stage} if extended_metrics_enabled else {other_stage}
        spec = self.create_spec(stage=stage)

        bucket_arn = self.manager.create_bucket(spec=spec, kms_key=self.kms_key)

        if extended_metrics_enabled:
            self.s3_client.enable_extended_metrics.assert_called_once_with(bucket_arn.identifier)
        else:
            self.s3_client.enable_extended_metrics.assert_not_called()


class TestDeleteBucket:
    def setup_method(self) -> None:
        self.region = build_region()
        self.account_id = build_account_id()
        self.aws = Mock(AwsClientFactory)
        self.s3_client = Mock(S3Client)
        self.aws.s3_client.return_value = self.s3_client
        self.s3_bucket_manager = S3BucketManager(config=build_config(), aws=self.aws)
        self.bucket_name = Builder.build_random_string()

    def test_delete_successful(self) -> None:
        self.s3_bucket_manager.delete_bucket(
            account_id=self.account_id, region=self.region, bucket_name=self.bucket_name
        )
        self.s3_client.delete_bucket.assert_called_once_with(bucket_name=self.bucket_name)

    def test_bucket_not_empty_error(self) -> None:
        self.s3_client.is_empty.return_value = True
        self.s3_client.delete_bucket.side_effect = BucketNotEmpty(self.bucket_name)
        with pytest.raises(BucketNotEmpty):
            self.s3_bucket_manager.delete_bucket(
                account_id=self.account_id, region=self.region, bucket_name=self.bucket_name
            )

    def test_check_if_bucket_empty(self) -> None:
        self.s3_client.is_empty.return_value = False
        with pytest.raises(BucketNotEmpty):
            self.s3_bucket_manager.delete_bucket(
                account_id=self.account_id, region=self.region, bucket_name=self.bucket_name
            )
        self.s3_client.delete_bucket.assert_not_called()

    def test_bucket_does_not_exist(self) -> None:
        self.s3_client.delete_bucket.side_effect = BucketNotFound(self.bucket_name)
        with pytest.raises(BucketNotFound):
            self.s3_bucket_manager.delete_bucket(
                account_id=self.account_id, region=self.region, bucket_name=self.bucket_name
            )


class TestUpdateBucketPolicyReadAccessStatementTransaction:
    def setup_method(self) -> None:
        self.bucket = build_s3_resource()
        self.aws = Mock(AwsClientFactory)
        self.s3_client = Mock(S3Client)
        self.s3_client.set_bucket_policy_transaction.return_value = MagicMock()
        self.old_policy = self.build_policy_document(sid="SomeFakeStatement")
        self.s3_client.get_bucket_policy.return_value = self.old_policy
        self.aws.s3_client.return_value = self.s3_client
        self.s3_bucket_manager = S3BucketManager(config=build_config(), aws=self.aws)

    @staticmethod
    def build_policy_document(sid: str) -> PolicyDocument:
        statement = {"Sid": sid, "Effect": "Allow", "Action": "*", "Resource": "*"}
        return PolicyDocument.create_managed_policy([statement])

    def test_updated_policy_contains_expected_sid(self) -> None:
        account_ids_with_read_access = frozenset((build_account_id() for _ in range(5)))
        with self.s3_bucket_manager.update_bucket_policy_read_access_statement_transaction(
            s3_resource=self.bucket, account_ids_with_read_access=account_ids_with_read_access
        ):
            self.s3_client.set_bucket_policy_transaction.assert_called_once_with(
                bucket_name=self.bucket.name,
                old_policy=self.old_policy,
                new_policy=ANY,
            )
            policy = self.s3_client.set_bucket_policy_transaction.call_args[1]["new_policy"]
            assert policy.has_statement_with_sid("GrantGetBucket")
            assert policy.has_statement_with_sid("SomeFakeStatement")
            assert policy.get_principals_with_action("s3:Get*") == {
                "AWS": [
                    build_arn_string(
                        service="iam",
                        region=None,
                        account=account_id,
                        resource="root",
                        partition=self.bucket.region.partition,
                    )
                    for account_id in account_ids_with_read_access
                ]
            }
            assert policy.get_principals_with_action("s3:List*") == {
                "AWS": [
                    build_arn_string(
                        service="iam",
                        region=None,
                        account=account_id,
                        resource="root",
                        partition=self.bucket.region.partition,
                    )
                    for account_id in account_ids_with_read_access
                ]
            }

    def test_updated_policy_does_not_contain_statement_if_no_account_ids_present(self) -> None:
        with self.s3_bucket_manager.update_bucket_policy_read_access_statement_transaction(
            s3_resource=self.bucket, account_ids_with_read_access=frozenset()
        ):
            self.s3_client.set_bucket_policy_transaction.assert_called_once_with(
                bucket_name=self.bucket.name,
                old_policy=self.old_policy,
                new_policy=ANY,
            )
            policy = self.s3_client.set_bucket_policy_transaction.call_args[1]["new_policy"]
            assert not policy.has_statement_with_sid("GrantGetBucket")
            assert policy.has_statement_with_sid("SomeFakeStatement")
            assert len(policy.get_principals_with_action("s3:List*")) == 0


def test_link_to_s3_attribute_extractor_lambda() -> None:
    config = build_config()
    region = build_region()
    resource_account_id = build_account_id()
    bucket_name = Builder.build_random_string()
    topic_arn = build_arn("sns", resource=bucket_name, account_id=resource_account_id, region=region)
    topic = SnsTopic(name=bucket_name, arn=topic_arn, region=region)

    aws = Mock(AwsClientFactory)
    s3_client = Mock(S3Client)
    aws.s3_client.return_value = s3_client
    s3_bucket_manager = S3BucketManager(config, aws)

    s3_bucket_manager.link_to_s3_attribute_extractor_lambda(bucket_name, topic)

    s3_client.set_bucket_tags.assert_called_once_with(
        bucket_name, tags={"snsTopicArn": str(topic.arn), **DEFAULT_S3_TAGS}
    )
    s3_client.send_creation_events_to_sns.assert_called_once_with(
        bucket_name, config.get_s3_attribute_extractor_topic_arn(resource_account_id, region)
    )
