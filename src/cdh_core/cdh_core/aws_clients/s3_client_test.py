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
# pylint: disable=unused-argument
from dataclasses import replace
from io import BytesIO
from typing import Any
from typing import Optional
from unittest.mock import Mock

import boto3
import pytest
from mypy_boto3_s3.type_defs import ServerSideEncryptionRuleTypeDef

from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.s3_client import BucketAlreadyExists
from cdh_core.aws_clients.s3_client import BucketNotEmpty
from cdh_core.aws_clients.s3_client import BucketNotFound
from cdh_core.aws_clients.s3_client import NoSuchBucketPolicy
from cdh_core.aws_clients.s3_client import S3Client
from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.aws import Region
from cdh_core_dev_tools.testing.assert_raises import assert_raises
from cdh_core_dev_tools.testing.builder import Builder


@pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
class TestS3Base:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile, mock_s3: Any) -> None:  # pylint: disable=unused-argument
        self._bucket_name = Builder.build_random_string()
        # LocationConstraint does NOT accept us-east-1
        self._region = Builder.get_random_element(list(Region), exclude={Region("us-east-1")})
        self._boto_s3_client = boto3.client("s3", region_name=self._region.value)
        self._s3_client = S3Client(self._boto_s3_client, sleep=Mock())
        self._kms_arn = build_arn("kms")


class TestS3Client(TestS3Base):
    def test_bucket_exists(self) -> None:
        assert self._s3_client.bucket_exists(self._bucket_name) is False
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        assert self._s3_client.bucket_exists(self._bucket_name) is True

    def test_set_tags(self) -> None:
        boto_s3_client = Mock()
        client = S3Client(boto_s3_client, sleep=Mock())
        tag = (Builder.build_random_string(), Builder.build_random_string())
        client.set_bucket_tags(self._bucket_name, {tag[0]: tag[1]})
        boto_s3_client.put_bucket_tagging.assert_called_once_with(
            Bucket=self._bucket_name, Tagging={"TagSet": [{"Key": tag[0], "Value": tag[1]}]}
        )

    def test_add_tags(self) -> None:
        boto_s3_client = Mock()
        client = S3Client(boto_s3_client, sleep=Mock())
        initial_tag = (Builder.build_random_string(), Builder.build_random_string())
        boto_s3_client.get_bucket_tagging.return_value = {"TagSet": [{"Key": initial_tag[0], "Value": initial_tag[1]}]}
        added_tag = (Builder.build_random_string(), Builder.build_random_string())

        client.add_bucket_tags(self._bucket_name, {added_tag[0]: added_tag[1]})

        expected_updated_tag_set = [
            {"Key": initial_tag[0], "Value": initial_tag[1]},
            {"Key": added_tag[0], "Value": added_tag[1]},
        ]
        boto_s3_client.put_bucket_tagging.assert_called_once_with(
            Bucket=self._bucket_name, Tagging={"TagSet": expected_updated_tag_set}
        )

    def test_update_existing_tag(self) -> None:
        boto_s3_client = Mock()
        client = S3Client(boto_s3_client, sleep=Mock())
        tag_key = Builder.build_random_string()
        initial_tag_value = Builder.build_random_string()
        boto_s3_client.get_bucket_tagging.return_value = {"TagSet": [{"Key": tag_key, "Value": initial_tag_value}]}
        updated_tag_value = Builder.build_random_string()

        client.add_bucket_tags(self._bucket_name, {tag_key: updated_tag_value})

        expected_updated_tag_set = [{"Key": tag_key, "Value": updated_tag_value}]
        boto_s3_client.put_bucket_tagging.assert_called_once_with(
            Bucket=self._bucket_name, Tagging={"TagSet": expected_updated_tag_set}
        )

    def test_remove_tags(self) -> None:
        boto_s3_client = Mock()
        client = S3Client(boto_s3_client, sleep=Mock())
        tag_to_remain = (Builder.build_random_string(), Builder.build_random_string())
        tag_to_remove = (Builder.build_random_string(), Builder.build_random_string())
        boto_s3_client.get_bucket_tagging.return_value = {
            "TagSet": [
                {"Key": tag_to_remain[0], "Value": tag_to_remain[1]},
                {"Key": tag_to_remove[0], "Value": tag_to_remove[1]},
            ]
        }

        client.remove_bucket_tags(self._bucket_name, {tag_to_remove[0]: tag_to_remove[1]})

        expected_updated_tag_set = [{"Key": tag_to_remain[0], "Value": tag_to_remain[1]}]
        boto_s3_client.put_bucket_tagging.assert_called_once_with(
            Bucket=self._bucket_name, Tagging={"TagSet": expected_updated_tag_set}
        )

    def test_remove_not_existent_tags(self) -> None:
        boto_s3_client = Mock()
        client = S3Client(boto_s3_client, sleep=Mock())
        tag_to_remain = (Builder.build_random_string(), Builder.build_random_string())
        boto_s3_client.get_bucket_tagging.return_value = {
            "TagSet": [{"Key": tag_to_remain[0], "Value": tag_to_remain[1]}]
        }

        client.remove_bucket_tags(self._bucket_name, {Builder.build_random_string(): Builder.build_random_string()})

        boto_s3_client.put_bucket_tagging.assert_not_called()

    def test_do_not_remove_tag_if_only_key_matches(self) -> None:
        boto_s3_client = Mock()
        client = S3Client(boto_s3_client, sleep=Mock())
        tag_to_remain = (Builder.build_random_string(), Builder.build_random_string())
        boto_s3_client.get_bucket_tagging.return_value = {
            "TagSet": [{"Key": tag_to_remain[0], "Value": tag_to_remain[1]}]
        }

        client.remove_bucket_tags(self._bucket_name, {tag_to_remain[0][0]: Builder.build_random_string()})

        boto_s3_client.put_bucket_tagging.assert_not_called()

    @pytest.mark.parametrize("enabled", [None, False, True])
    def test_is_bucket_key_enabled(self, enabled: Optional[bool]) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        encryption_rule: ServerSideEncryptionRuleTypeDef = {
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "aws:kms",
                "KMSMasterKeyID": str(self._kms_arn),
            }
        }
        if enabled is not None:
            encryption_rule["BucketKeyEnabled"] = enabled
        self._boto_s3_client.put_bucket_encryption(
            Bucket=self._bucket_name, ServerSideEncryptionConfiguration={"Rules": [encryption_rule]}
        )
        assert self._s3_client.is_bucket_key_enabled(bucket_name=self._bucket_name) == bool(enabled)


class TestCreate(TestS3Base):
    @pytest.mark.parametrize("include_kms_key", [True, False])
    def test_create_encrypted_bucket_successfully(self, include_kms_key: bool) -> None:
        tags = {"tagkey": "tagvalue"}

        self._s3_client.create_encrypted_bucket(
            self._bucket_name, self._region, self._kms_arn if include_kms_key else None, tags
        )

        response = self._boto_s3_client.get_bucket_tagging(Bucket=self._bucket_name)
        assert response["TagSet"] == [{"Key": "tagkey", "Value": "tagvalue"}]
        # Note: moto's get_bucket_encryption is always empty, so we cannot test that the bucket was encrypted.

    def test_create_existing_bucket_owned_by_us(self) -> None:
        boto_client = Mock()
        boto_client.create_bucket.side_effect = Builder.build_client_error("BucketAlreadyOwnedByYou")

        with assert_raises(BucketAlreadyExists(self._bucket_name)):
            S3Client(boto_client, sleep=Mock()).create_encrypted_bucket(self._bucket_name, self._region, self._kms_arn)

    def test_create_existing_bucket_owned_by_us_in_us_east(self) -> None:
        boto_client = Mock()
        boto_client.head_bucket.return_value = None

        with assert_raises(BucketAlreadyExists(self._bucket_name)):
            S3Client(boto_client, sleep=Mock()).create_encrypted_bucket(
                self._bucket_name, Region("us-east-1"), self._kms_arn
            )

    def test_create_existing_bucket_owned_by_other_account(self) -> None:
        boto_client = Mock()
        boto_client.create_bucket.side_effect = Builder.build_client_error("BucketAlreadyExists")

        with assert_raises(BucketAlreadyExists(self._bucket_name)):
            S3Client(boto_client, sleep=Mock()).create_encrypted_bucket(self._bucket_name, self._region, self._kms_arn)

    def test_create_existing_bucket_owned_by_other_account_in_us_east(self) -> None:
        boto_client = Mock()
        boto_client.head_bucket.side_effect = Builder.build_client_error("403")

        with assert_raises(BucketAlreadyExists(self._bucket_name)):
            S3Client(boto_client, sleep=Mock()).create_encrypted_bucket(
                self._bucket_name, Region("us-east-1"), self._kms_arn
            )

    def test_create_bucket_raises_error(self) -> None:
        boto_client = Mock()
        boto_client.create_bucket.side_effect = Builder.build_client_error(Builder.build_random_string())

        with assert_raises(boto_client.create_bucket.side_effect):
            S3Client(boto_client, sleep=Mock()).create_encrypted_bucket(self._bucket_name, self._region, self._kms_arn)

    def test_create_with_kms_key_use_key_to_encrypt(self) -> None:
        boto_client = Mock()

        S3Client(boto_client, sleep=Mock()).create_encrypted_bucket(self._bucket_name, self._region, self._kms_arn)

        assert "ServerSideEncryptionConfiguration" in boto_client.put_bucket_encryption.call_args.kwargs
        config_as_string = str(boto_client.put_bucket_encryption.call_args.kwargs["ServerSideEncryptionConfiguration"])
        assert "aws:kms" in config_as_string
        assert str(self._kms_arn) in config_as_string

    def test_create_without_kms_key_uses_default_encryption(self) -> None:
        boto_client = Mock()

        S3Client(boto_client, sleep=Mock()).create_encrypted_bucket(self._bucket_name, self._region)

        assert "ServerSideEncryptionConfiguration" in boto_client.put_bucket_encryption.call_args.kwargs
        config_as_string = str(boto_client.put_bucket_encryption.call_args.kwargs["ServerSideEncryptionConfiguration"])
        assert "AES256" in config_as_string
        assert str(self._kms_arn) not in config_as_string


class TestBlockPublicAccess(TestS3Base):
    def test_block_public_access(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )

        self._s3_client.block_public_access(self._bucket_name)

        result = self._boto_s3_client.get_public_access_block(Bucket=self._bucket_name)
        assert result["PublicAccessBlockConfiguration"] == {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }

    def test_block_public_access_on_non_existing_bucket(self) -> None:
        boto_client = Mock()
        boto_client.put_public_access_block.side_effect = Builder.build_client_error("NoSuchBucket")

        with assert_raises(BucketNotFound(self._bucket_name)):
            S3Client(boto_client, sleep=Mock()).block_public_access(self._bucket_name)

    def test_block_public_access_other_errors(self) -> None:
        boto_client = Mock()
        error = Builder.build_client_error("RoleNotFound")

        boto_client.put_public_access_block.side_effect = error
        with assert_raises(error):
            S3Client(boto_client, sleep=Mock()).block_public_access(self._bucket_name)


@pytest.mark.parametrize(
    "mock_config_file",
    [
        CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
        replace(
            CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
            aws_service=replace(
                CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.aws_service,
                s3=ConfigFile.AWSService.S3(allowed_origins=["*"]),
            ),
        ),
    ],
    indirect=True,
)
class TestBucketCors:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile, mock_s3: Any) -> None:  # pylint: disable=unused-argument
        self._bucket_name = Builder.build_random_string()
        # LocationConstraint does NOT accept us-east-1
        self._region = Builder.get_random_element(list(Region), exclude={Region("us-east-1")})
        self._boto_s3_client = boto3.client("s3", region_name=self._region.value)
        self._s3_client = S3Client(self._boto_s3_client, sleep=Mock())

    def test_set_bucket_cors(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        self._s3_client.set_bucket_cors(self._bucket_name)

        result = self._boto_s3_client.get_bucket_cors(Bucket=self._bucket_name)
        expected_result = self._s3_client._bucket_cores_header["CORSRules"]  # pylint: disable=protected-access
        # this is needed due to a moto bug - ExposeHeaders are missing in the result from moto
        del expected_result[0]["ExposeHeaders"]
        assert result["CORSRules"] == expected_result

    def test_set_bucket_cors_on_non_existing_bucket(self) -> None:
        boto_client = Mock()
        boto_client.put_bucket_cors.side_effect = Builder.build_client_error("NoSuchBucket")
        sleep = Mock()

        bucket_name = Builder.build_random_string()
        with assert_raises(BucketNotFound(bucket_name)):
            S3Client(boto_client, sleep).set_bucket_cors(bucket_name)


class TestBucketPolicy(TestS3Base):
    def build_policy_document(self, sid: str = "SampleSid") -> PolicyDocument:
        # Warning: this document must pass moto's validation
        return PolicyDocument.create_bucket_policy(
            [{"Sid": sid, "Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]
        )

    def test_set_bucket_policy(self) -> None:
        policy = self.build_policy_document()
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )

        self._s3_client.set_bucket_policy(self._bucket_name, policy)

        result = self._boto_s3_client.get_bucket_policy(Bucket=self._bucket_name)
        assert result["Policy"] == policy.encode()

    def test_set_bucket_policy_on_non_existing_bucket(self) -> None:
        boto_client = Mock()
        boto_client.put_bucket_policy.side_effect = Builder.build_client_error("NoSuchBucket")
        sleep = Mock()

        with assert_raises(BucketNotFound(self._bucket_name)):
            S3Client(boto_client, sleep).set_bucket_policy(self._bucket_name, self.build_policy_document())

    def test_set_bucket_policy_other_errors(self) -> None:
        boto_client = Mock()
        error = Builder.build_client_error("RoleNotFound")

        boto_client.put_bucket_policy.side_effect = error
        with assert_raises(error):
            S3Client(boto_client, sleep=Mock()).set_bucket_policy(self._bucket_name, self.build_policy_document())

    def test_get_bucket_policy_missing_bucket(self) -> None:
        boto_s3_client = Mock()
        boto_s3_client.get_bucket_policy.side_effect = Builder.build_client_error("NoSuchBucket")
        client = S3Client(boto_s3_client, sleep=Mock())
        with pytest.raises(BucketNotFound):
            client.get_bucket_policy("not-existing-bucket")

    def test_get_bucket_policy_no_policy(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        with pytest.raises(NoSuchBucketPolicy):
            self._s3_client.get_bucket_policy(self._bucket_name)

    def test_get_bucket_policy_successfully(self) -> None:
        s3_client = Mock(Spec=S3Client)
        policy = self.build_policy_document()
        s3_client.get_bucket_policy.return_value = policy
        response = s3_client.get_bucket_policy(self._bucket_name)
        assert response == policy

    def test_set_bucket_policy_transaction_empty_bucket_policy_successful(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        old_policy = self.build_policy_document("Old")
        new_policy = self.build_policy_document("New")

        with self._s3_client.set_bucket_policy_transaction(
            bucket_name=self._bucket_name, old_policy=old_policy, new_policy=new_policy
        ):
            assert self._s3_client.get_bucket_policy(self._bucket_name) == new_policy
        assert self._s3_client.get_bucket_policy(self._bucket_name) == new_policy

    def test_set_bucket_policy_transaction_empty_bucket_policy_rollback(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        new_policy = self.build_policy_document()
        error = Exception("my error")

        with pytest.raises(Exception) as exc_info:
            with self._s3_client.set_bucket_policy_transaction(
                bucket_name=self._bucket_name, old_policy=None, new_policy=new_policy
            ):
                raise error
        assert exc_info.value == error
        with pytest.raises(NoSuchBucketPolicy):
            self._s3_client.get_bucket_policy(self._bucket_name)

    def test_set_bucket_policy_transaction_successful(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        old_policy = self.build_policy_document()
        new_policy = self.build_policy_document("otherContent")
        self._boto_s3_client.put_bucket_policy(Bucket=self._bucket_name, Policy=old_policy.encode())

        with self._s3_client.set_bucket_policy_transaction(
            bucket_name=self._bucket_name, old_policy=old_policy, new_policy=new_policy
        ):
            assert self._s3_client.get_bucket_policy(self._bucket_name) == new_policy
        assert self._s3_client.get_bucket_policy(self._bucket_name) == new_policy

    def test_set_bucket_policy_transaction_rollback(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        old_policy = self.build_policy_document()
        new_policy = self.build_policy_document("otherContent")
        self._boto_s3_client.put_bucket_policy(Bucket=self._bucket_name, Policy=old_policy.encode())
        error = Exception("my error")

        with pytest.raises(Exception) as exc_info:
            with self._s3_client.set_bucket_policy_transaction(
                bucket_name=self._bucket_name, old_policy=old_policy, new_policy=new_policy
            ):
                assert self._s3_client.get_bucket_policy(self._bucket_name) == new_policy
                raise error
        assert exc_info.value == error
        assert self._s3_client.get_bucket_policy(self._bucket_name) == old_policy


class TestSetLifecycleConfiguration(TestS3Base):
    def test_set_lifecycle_configuration(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )

        self._s3_client.set_lifecycle_configuration(self._bucket_name)

        result = self._boto_s3_client.get_bucket_lifecycle_configuration(Bucket=self._bucket_name)
        assert result["Rules"] == [
            {
                "ID": f"{self._bucket_name}-lifecycle",
                "Transitions": [{"Days": 365, "StorageClass": "INTELLIGENT_TIERING"}],
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
            }
        ]

    def test_set_lifecycle_configuration_on_non_existing_bucket(self) -> None:
        boto_client = Mock()
        boto_client.put_bucket_lifecycle_configuration.side_effect = Builder.build_client_error("NoSuchBucket")

        with assert_raises(BucketNotFound(self._bucket_name)):
            S3Client(boto_client, sleep=Mock()).set_lifecycle_configuration(self._bucket_name)

    def test_set_lifecycle_configuration_other_errors(self) -> None:
        boto_client = Mock()
        error = Builder.build_client_error("RoleNotFound")

        boto_client.put_bucket_lifecycle_configuration.side_effect = error
        with assert_raises(error):
            S3Client(boto_client, sleep=Mock()).set_lifecycle_configuration(self._bucket_name)


class TestDelete(TestS3Base):
    def test_delete_bucket(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        assert self._s3_client.bucket_exists(self._bucket_name)
        self._s3_client.delete_bucket(self._bucket_name)
        assert not self._s3_client.bucket_exists(self._bucket_name)

    def test_delete_not_empty_bucket(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        self._boto_s3_client.put_object(Key="test file", Body=b"this is a testfile", Bucket=self._bucket_name)
        with pytest.raises(BucketNotEmpty):
            self._s3_client.delete_bucket(self._bucket_name)
        assert self._s3_client.bucket_exists(self._bucket_name)

    def test_delete_nonexistent_bucket(self) -> None:
        with pytest.raises(BucketNotFound):
            self._s3_client.delete_bucket("non-existing-bucket")


class TestIsEmpty(TestS3Base):
    def test_empty_bucket(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        assert self._s3_client.is_empty(self._bucket_name)

    def test_non_empty_bucket(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )

        self._boto_s3_client.upload_fileobj(
            Fileobj=BytesIO(b"Data data data"), Bucket=self._bucket_name, Key="data.txt"
        )

        assert not self._s3_client.is_empty(self._bucket_name)

    def test_non_existing_bucket(self) -> None:
        with pytest.raises(BucketNotFound):
            self._s3_client.is_empty(self._bucket_name)


class TestAccessLogs(TestS3Base):
    LOG_PREFIX = "mylogprefix"

    @pytest.fixture(autouse=True)
    def prepare_logbucket(self, service_setup: Any) -> None:
        self.log_bucket_name = "logbucket_" + Builder.build_random_string()
        self._boto_s3_client.create_bucket(
            Bucket=self.log_bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )
        # moto does not support granting access via bucket policy (as we actually do)
        self._boto_s3_client.put_bucket_acl(
            Bucket=self.log_bucket_name,
            GrantWrite="uri=http://acs.amazonaws.com/groups/s3/LogDelivery",
            GrantReadACP="uri=http://acs.amazonaws.com/groups/s3/LogDelivery",
        )

    def test_enable_bucket_access_logging(self) -> None:
        self._boto_s3_client.create_bucket(
            Bucket=self._bucket_name, CreateBucketConfiguration={"LocationConstraint": self._region.value}
        )

        self._s3_client.enable_bucket_access_logging(self._bucket_name, self.log_bucket_name, self.LOG_PREFIX)

        bucket_logging_config = self._boto_s3_client.get_bucket_logging(Bucket=self._bucket_name)
        assert bucket_logging_config["LoggingEnabled"] == {
            "TargetBucket": self.log_bucket_name,
            "TargetPrefix": self.LOG_PREFIX,
        }
