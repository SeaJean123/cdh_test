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
from contextlib import contextmanager
from dataclasses import dataclass
from logging import getLogger
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws import Partition
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient as BotoAthenaClient
else:
    BotoAthenaClient = object


LOG = getLogger(__name__)


@dataclass(frozen=True)
class AthenaWorkgroup:
    """Abstracts the athena workgroup."""

    name: str
    account_id: AccountId
    partition: Partition

    @property
    def arn(self) -> Arn:
        """Return the ARN for the workgroup."""
        return Arn(f"arn:{self.partition.value}:athena:*:{self.account_id}:workgroup/{self.name}")


class AthenaClient:
    """Abstracts the boto3 athena client."""

    def __init__(self, boto3_athena_client: BotoAthenaClient):
        self._client = boto3_athena_client

    @contextmanager
    def create_work_group_transaction(
        self, name: str, output_location: str, kms_key_arn: Arn, tags: Optional[Dict[str, str]] = None
    ) -> Iterator[None]:
        """Create a workgroup, if it fails try to rollback via deleting the group."""
        self._create_work_group(name=name, output_location=output_location, kms_key_arn=kms_key_arn, tags=tags or {})

        try:
            yield
        except Exception:
            self._rollback_work_group_creation(name)
            raise

    def _create_work_group(self, name: str, output_location: str, kms_key_arn: Arn, tags: Dict[str, str]) -> None:
        self._client.create_work_group(
            Name=name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": output_location,
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_KMS",
                        "KmsKey": str(kms_key_arn),
                    },
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
            },
            Description="Work group for IAM role " + name,
            Tags=[{"Key": key, "Value": value} for key, value in tags.items()],
        )

    def _rollback_work_group_creation(self, name: str) -> None:
        LOG.warning("Rolling back creation of work group %s", name)
        try:
            self.delete_workgroup(name=name)
        except ClientError:
            LOG.exception("Could not roll back creation of work group %s", name)

    def update_kms_key_for_workgroup(self, workgroup_name: str, kms_key: KmsKey) -> None:
        """Change the the KMS key for the given AWS athena workgroup."""
        try:
            self._client.update_work_group(
                WorkGroup=workgroup_name,
                Description="string",
                ConfigurationUpdates={
                    "ResultConfigurationUpdates": {
                        "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": str(kms_key.arn)},
                    },
                },
            )
        except ClientError:
            LOG.error(f"Could not update work group {workgroup_name} to use key {kms_key.arn}")
            raise

    def delete_workgroup(self, name: str) -> None:
        """Delete the given AWS athena workgroup."""
        self._client.delete_work_group(WorkGroup=name, RecursiveDeleteOption=True)
