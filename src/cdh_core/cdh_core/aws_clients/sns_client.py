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
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from logging import getLogger
from typing import Any
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.utils import get_error_code
from cdh_core.entities.arn import Arn
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_clients import PolicyDocumentType

LOG = getLogger(__name__)


if TYPE_CHECKING:
    from mypy_boto3_sns import SNSClient
    from mypy_boto3_sns.type_defs import MessageAttributeValueTypeDef
else:
    SNSClient = object
    MessageAttributeValueTypeDef = object


@dataclass(frozen=True)
class SnsTopic:
    """Dataclass for a AWS SNS topic. To this topic, notifications can be published."""

    name: str
    arn: Arn
    region: Region


class SnsClient:
    """Abstracts the boto3 SNS client."""

    def __init__(self, boto_sns_client: SNSClient):
        self._client = boto_sns_client

    def create_sns(self, name: str, tags: Optional[Dict[str, str]] = None, kms_key_arn: Optional[Arn] = None) -> Arn:
        """Create a new SNS topic with the given name/tags and returns its ARN."""
        tags = tags or {}
        return Arn(
            self._client.create_topic(
                Name=name,
                Tags=[{"Key": key, "Value": value} for key, value in tags.items()],
                Attributes={"KmsMasterKeyId": str(kms_key_arn)} if kms_key_arn else {},
            )["TopicArn"]
        )

    def get_sns_policy(self, sns_arn: Arn) -> PolicyDocument:
        """Return the PolicyDocument of a given SNS ARN. It contains the topic's access control policy."""
        policy_document = json.loads(self._client.get_topic_attributes(TopicArn=str(sns_arn))["Attributes"]["Policy"])
        return PolicyDocument(
            version=policy_document["Version"],
            statements=policy_document["Statement"],
            policy_document_type=PolicyDocumentType.SNS,
        )

    def set_sns_policy(self, sns_arn: Arn, sns_policy: PolicyDocument) -> None:
        """Set the policy of a given SNS ARN."""
        self._client.set_topic_attributes(
            TopicArn=str(sns_arn), AttributeName="Policy", AttributeValue=sns_policy.encode()
        )

    @contextmanager
    def set_sns_policy_transaction(self, sns_arn: Arn, sns_policy: PolicyDocument) -> Iterator[None]:
        """Set the policy of a given SNS ARN, in a transactional manner.

        If an exception is raised, recover the initial policy.
        """
        sns_policy_backup = self.get_sns_policy(sns_arn)
        LOG.info(f"Setting policy for sns topic {str(sns_arn)}")
        self.set_sns_policy(sns_arn, sns_policy)
        try:
            yield
        except Exception:
            self._rollback_set_sns_policy(sns_arn, sns_policy_backup)
            raise

    def _rollback_set_sns_policy(self, sns_arn: Arn, sns_policy_rollback: PolicyDocument) -> None:
        LOG.warning("Rolling back update of sns_policy for sns %s", sns_arn)
        try:
            self.set_sns_policy(sns_arn, sns_policy_rollback)
        except ClientError:
            LOG.exception("Could not roll back sns_policy for sns %s", sns_arn)

    def delete_sns_topic(self, sns_arn: Arn) -> None:
        """Delete a SNS topic specified by its ARN."""
        self._client.delete_topic(TopicArn=str(sns_arn))

    @staticmethod
    def is_fifo_topic(sns_arn: Arn) -> bool:
        """Return True if a given SNS ARN corresponds to a fifo topic."""
        return sns_arn.identifier.endswith(".fifo")

    def publish_message(  # pylint: disable=too-many-arguments
        self,
        sns_arn: Arn,
        message_subject: str,
        message_body: str,
        attributes: Dict[str, Any],
        message_group_id: str = "primary",
    ) -> None:
        """Send a message to a given SNS topic.

        attributes should contain the key-value pairs provided in the message_body.
        message_group_id applies only to fifo topics. Messages with the same group id are processed in a fifo manner.
        For non-fifo topics this tag must still be provided.
        """
        formatted_attributes: Dict[str, MessageAttributeValueTypeDef] = {
            str(key): {"DataType": "String", "StringValue": str(value)} for key, value in attributes.items() if value
        }
        parameters = {}
        if self.is_fifo_topic(sns_arn):
            parameters = {
                "MessageGroupId": message_group_id,
                "MessageDeduplicationId": sha256((message_subject + message_body).encode("utf-8")).hexdigest(),
            }
        try:
            self._client.publish(
                TopicArn=str(sns_arn),
                Message=message_body,
                Subject=message_subject,
                MessageAttributes=formatted_attributes,
                **parameters,
            )
        except ClientError as error:
            if get_error_code(error) == "NotFound":
                LOG.warning(f"SNS-Topic: {sns_arn} does not exist")
                raise TopicNotFound(sns_arn) from error
            raise error


class TopicNotFound(Exception):
    """Signals a SNS topic cannot be found."""

    def __init__(self, sns_arn: Arn):
        super().__init__(f"Sns {sns_arn} was not found")
