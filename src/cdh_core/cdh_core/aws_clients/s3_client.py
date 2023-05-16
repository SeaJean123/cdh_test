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
import time
from contextlib import contextmanager
from logging import getLogger
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from cdh_core.aws_clients.boto_retry_decorator import create_boto_retry_decorator
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.utils import get_error_code
from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_clients import PolicyDocumentType

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client as S3ClientType
    from mypy_boto3_s3.type_defs import BucketTypeDef
    from mypy_boto3_s3.type_defs import CORSConfigurationTypeDef
    from mypy_boto3_s3.type_defs import ServerSideEncryptionConfigurationTypeDef
else:
    S3ClientType = object
    BucketTypeDef = Dict[str, Any]
    CORSConfigurationTypeDef = Dict[str, Any]
    ServerSideEncryptionConfigurationTypeDef = Dict[str, Any]

LOG = getLogger(__name__)


class S3Client:
    """Abstracts the boto3 S3 client."""

    # we cannot use the waiters for bucket_exists since they (or repeatedly calling head-bucket) are broken in us-east-1
    # => we need to wait + retry manually
    retry = create_boto_retry_decorator("_sleep")

    def __init__(self, boto3_s3_client: S3ClientType, sleep: Callable[[float], None] = time.sleep):
        self._client = boto3_s3_client
        self._sleep = sleep
        self._bucket_cores_header: CORSConfigurationTypeDef = {
            "CORSRules": [
                {
                    "AllowedHeaders": [
                        "Authorization",
                        "X-Amz-ACL",
                        "X-Amz-Content-Sha256",
                        "X-Amz-Date",
                        "X-Amz-Security-Token",
                        "X-Amz-User-Agent",
                        "X-Amz-Copy-Source",
                        "X-Amz-Copy-Source-Range",
                        "Content-md5",
                        "Content-type",
                        "Content-Length",
                        "Content-Encoding",
                    ],
                    "AllowedMethods": ["GET", "POST", "PUT", "DELETE", "HEAD"],
                    "AllowedOrigins": ConfigFileLoader().get_config().aws_service.s3.allowed_origins,
                    "ExposeHeaders": ["ETag", "LastModified"],
                },
            ]
        }

    def create_encrypted_bucket(
        self,
        name: str,
        region: Region,
        kms_key_arn: Optional[Arn] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Arn:
        """Create an encrypted S3 bucket."""
        self._create_bucket(name, region)
        if tags:
            self.set_bucket_tags(name, tags)
        self._encrypt_bucket(name, kms_key_arn)
        return Arn(f"arn:{region.partition.value}:s3:::{name}")

    def bucket_exists(self, name: str) -> bool:
        """Return True if the S3 bucket exists."""
        try:
            self._client.head_bucket(Bucket=name)
            return True
        except ClientError as error:
            if get_error_code(error) == "404":
                return False
            if get_error_code(error) == "403":
                return True
            raise

    def _create_bucket(self, name: str, region: Region) -> None:
        try:
            if region.value == "us-east-1":
                if self.bucket_exists(name):
                    # in us-east-1, create_bucket does not raise an error if the bucket already exists :-(
                    raise BucketAlreadyExists(name)
                # LocationConstraint does NOT accept us-east-1 nor is an empty CreateBucketConfiguration allowed
                self._client.create_bucket(Bucket=name, ObjectOwnership="BucketOwnerEnforced")
            else:
                self._client.create_bucket(
                    Bucket=name,
                    CreateBucketConfiguration={"LocationConstraint": region.value},
                    ObjectOwnership="BucketOwnerEnforced",
                )
        except ClientError as error:
            if get_error_code(error) in ["BucketAlreadyExists", "BucketAlreadyOwnedByYou"]:
                raise BucketAlreadyExists(name) from error
            raise

    def list_buckets(self) -> List["BucketTypeDef"]:
        """List all buckets in an account."""
        return self._client.list_buckets()["Buckets"]

    @retry(num_attempts=20, wait_between_attempts=1, retryable_error_codes=["NoSuchBucket", "OperationAborted"])
    def set_bucket_tags(self, name: str, tags: Dict[str, str]) -> None:
        """Set the given tags at the S3 bucket."""
        self._client.put_bucket_tagging(
            Bucket=name, Tagging={"TagSet": [{"Key": key, "Value": value} for key, value in tags.items()]}
        )

    def add_bucket_tags(self, name: str, tags: Dict[str, str]) -> None:
        """Add the given tags to the S3 bucket."""
        current_tags = self._get_bucket_tags(name)
        self.set_bucket_tags(name=name, tags=current_tags | tags)

    def remove_bucket_tags(self, name: str, tags: Dict[str, str]) -> None:
        """Remove the given tags from the S3 bucket."""
        current_tags = self._get_bucket_tags(name)
        updated_tags = {key: value for key, value in current_tags.items() if (key, value) not in tags.items()}
        if current_tags != updated_tags:
            self.set_bucket_tags(name=name, tags=updated_tags)

    @retry(num_attempts=20, wait_between_attempts=1, retryable_error_codes=["NoSuchBucket", "OperationAborted"])
    def _get_bucket_tags(self, name: str) -> Dict[str, str]:
        response = self._client.get_bucket_tagging(Bucket=name)
        tags = {item["Key"]: item["Value"] for item in response["TagSet"]}
        return tags

    @retry(num_attempts=20, wait_between_attempts=1, retryable_error_codes=["NoSuchBucket"])
    def _encrypt_bucket(self, name: str, kms_key_arn: Optional[Arn]) -> None:
        encryption_config: ServerSideEncryptionConfigurationTypeDef = (
            {
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": str(kms_key_arn),
                        },
                        "BucketKeyEnabled": True,
                    }
                ],
            }
            if kms_key_arn is not None
            else {
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                        "BucketKeyEnabled": False,
                    }
                ]
            }
        )
        self._client.put_bucket_encryption(
            Bucket=name,
            ServerSideEncryptionConfiguration=encryption_config,
        )

    @retry(num_attempts=20, wait_between_attempts=1)
    def block_public_access(self, bucket: str) -> None:
        """Block all public access to the S3 bucket."""
        try:
            self._client.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
        except ClientError as error:
            if get_error_code(error) == "NoSuchBucket":
                raise BucketNotFound(bucket) from error
            LOG.warning(f"({get_error_code(error)}) Could block bucket access for bucket {bucket}")
            raise error

    def set_bucket_cors(self, bucket: str) -> None:
        """Set the CORS headers for the S3 bucket."""
        try:
            self._client.put_bucket_cors(Bucket=bucket, CORSConfiguration=self._bucket_cores_header)
        except ClientError as error:
            if get_error_code(error) == "NoSuchBucket":
                raise BucketNotFound(bucket) from error
            LOG.warning(f"({get_error_code(error)}) Could not set CORS rules for bucket {bucket}")
            raise error

    @retry(num_attempts=20, wait_between_attempts=1)
    def set_lifecycle_configuration(self, bucket: str) -> None:
        """Set the default lifecycle configuration for the S3 bucket."""
        try:
            self._client.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": f"{bucket}-lifecycle",
                            "Transitions": [{"Days": 365, "StorageClass": "INTELLIGENT_TIERING"}],
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                        }
                    ]
                },
            )
        except ClientError as error:
            if get_error_code(error) == "NoSuchBucket":
                raise BucketNotFound(bucket) from error
            LOG.warning(f"({get_error_code(error)}) Could not set lifecycle configuration for bucket {bucket}")
            raise error

    @retry(num_attempts=20, wait_between_attempts=1)
    def set_bucket_policy(self, bucket: str, policy: PolicyDocument) -> None:
        """Set a new S3 bucket policy."""
        try:
            self._client.put_bucket_policy(Bucket=bucket, Policy=policy.encode())
        except ClientError as error:
            if get_error_code(error) == "NoSuchBucket":
                raise BucketNotFound(bucket) from error
            LOG.warning(f"({get_error_code(error)}) Could not set bucket policy for bucket {bucket}: {policy}")
            raise error

    def get_bucket_policy(self, bucket: str) -> PolicyDocument:
        """Return the S3 bucket policy."""
        try:
            policy_document = json.loads(self._client.get_bucket_policy(Bucket=bucket)["Policy"])
            return PolicyDocument(
                version=policy_document["Version"],
                statements=policy_document["Statement"],
                policy_document_type=PolicyDocumentType.BUCKET,
            )
        except ClientError as error:
            if get_error_code(error) == "NoSuchBucket":
                raise BucketNotFound(bucket) from error
            if get_error_code(error) == "NoSuchBucketPolicy":
                raise NoSuchBucketPolicy(bucket) from error
            raise error

    @retry(num_attempts=20, wait_between_attempts=1)
    def enable_bucket_access_logging(self, bucket: str, log_bucket: str, log_prefix: str) -> None:
        """Configure bucket access logging."""
        try:
            self._client.put_bucket_logging(
                Bucket=bucket,
                BucketLoggingStatus={
                    "LoggingEnabled": {
                        "TargetBucket": log_bucket,
                        "TargetPrefix": log_prefix,
                    },
                },
            )
        except ClientError as error:
            LOG.warning(f"({get_error_code(error)}) Could not enable bucket logging for bucket {bucket}")
            raise error

    @contextmanager
    def set_bucket_policy_transaction(
        self, bucket_name: str, old_policy: Optional[PolicyDocument], new_policy: PolicyDocument
    ) -> Iterator[None]:
        """Try to set the new bucket policy, if it fails revert to the old policy."""
        LOG.info(f"Setting bucket policy for bucket {bucket_name}")
        self.set_bucket_policy(bucket_name, new_policy)
        try:
            yield
        except Exception:
            self._rollback_set_bucket_policy(bucket_name, old_policy)
            raise

    def _rollback_set_bucket_policy(self, bucket_name: str, bucket_policy_rollback: Optional[PolicyDocument]) -> None:
        LOG.warning("Rolling back update of bucket_policy for bucket %s", bucket_name)
        try:
            if bucket_policy_rollback:
                self.set_bucket_policy(bucket_name, bucket_policy_rollback)
            else:
                self._client.delete_bucket_policy(Bucket=bucket_name)
        except ClientError:
            LOG.exception("Could not roll back bucket_policy for bucket %s", bucket_name)

    @retry(num_attempts=20, wait_between_attempts=1, retryable_error_codes=["NoSuchBucket", "OperationAborted"])
    def send_creation_events_to_sns(self, bucket: str, topic_arn: Arn) -> None:
        """Enable S3 bucket creation events."""
        self._client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "TopicConfigurations": [
                    {
                        "TopicArn": str(topic_arn),
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ]
            },
        )

    def enable_extended_metrics(self, bucket_name: str) -> None:
        """Enable a more detailed metrics monitoring for the bucket."""
        try:
            self._client.put_bucket_metrics_configuration(
                Bucket=bucket_name,
                Id="EntireBucket",
                MetricsConfiguration={
                    "Id": "EntireBucket",
                },
            )
        except ClientError as error:
            if get_error_code(error) == "NoSuchBucket":
                raise BucketNotFound(bucket_name) from error
            raise error

    def delete_bucket(self, bucket_name: str) -> None:
        """Delete the given bucket."""
        try:
            self._client.delete_bucket(Bucket=bucket_name)
        except ClientError as error:
            if get_error_code(error) == "BucketNotEmpty":
                raise BucketNotEmpty(bucket_name) from error
            if get_error_code(error) == "NoSuchBucket":
                raise BucketNotFound(bucket_name) from error
            raise error

    def is_empty(self, bucket_name: str) -> bool:
        """Return True iff the S3 bucket does not contain objects."""
        try:
            return self._client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)["KeyCount"] == 0
        except self._client.exceptions.NoSuchBucket as error:
            raise BucketNotFound(bucket_name) from error

    def is_bucket_key_enabled(self, bucket_name: str) -> bool:
        """Return True iff a bucket key is used to encrypt stored objects."""
        try:
            encryption = self._client.get_bucket_encryption(Bucket=bucket_name)
        except self._client.exceptions.NoSuchBucket as error:
            raise BucketNotFound(bucket_name) from error
        rules = encryption["ServerSideEncryptionConfiguration"]["Rules"]
        if len(rules) != 1:
            raise RuntimeError(f"Invalid bucket encryption configuration for bucket {bucket_name}")
        return rules[0]["BucketKeyEnabled"]


class BucketNotFound(Exception):
    """Signals that the requested S3 bucket does not exist."""

    def __init__(self, bucket: str):
        super().__init__(f"Bucket {bucket} was not found")


class BucketAlreadyExists(Exception):
    """Signals that the S3 bucket to be created already exists."""

    def __init__(self, bucket: str):
        super().__init__(f"Bucket {bucket} already exists")


class NoSuchBucketPolicy(Exception):
    """Signals that the S3 bucket does not have a policy."""

    def __init__(self, bucket: str):
        super().__init__(f"Bucket {bucket} has no policy attached")


class BucketNotEmpty(Exception):
    """Signals that the S3 bucket contains objects."""

    def __init__(self, bucket: str):
        super().__init__(f"Bucket {bucket} is not empty")
