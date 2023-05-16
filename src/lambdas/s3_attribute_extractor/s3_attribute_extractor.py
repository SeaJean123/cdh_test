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
import concurrent.futures
import json
import os
import re
import signal
import unicodedata
from functools import lru_cache
from logging import getLogger
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union
from urllib.parse import unquote_plus

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cdh_core.log.log_safe import log_safe
from cdh_core.log.logger import configure_logging


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_sns.client import SNSClient
    from mypy_boto3_sqs.client import SQSClient
else:
    S3Client = object
    SNSClient = object
    SQSClient = object


LOG = getLogger(__name__)

PARTITION_REGEX = re.compile(r"(^|/)([^/=]+)=([^/=]+)(?=/)")
# according to https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
SNS_ATTRIBUTE_NAME_REGEX = re.compile(r"(^AWS\.|^Amazon\.|[^a-z0-9_\-.])", flags=re.IGNORECASE)


@lru_cache(maxsize=1)
def get_lambda() -> "AttributeExtractorLambda":
    """Initialize the attribute extractor lambda for the lambda handler."""
    config = Config(connect_timeout=4, read_timeout=29, retries={"max_attempts": 1})
    sns_client = boto3.client("sns", config=config)
    s3_client = boto3.client("s3", config=config)
    sqs_client = boto3.client("sqs", config=config)
    return AttributeExtractorLambda(s3_client, sns_client, sqs_client)


@log_safe()
def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """Handle an event with the attribute extractor lambda."""
    configure_logging(__name__)
    attribute_extractor_lambda = get_lambda()
    attribute_extractor_lambda.handle_event(event, context)


class AttributeExtractorLambda:
    """Extract attributes and removes special characters from payload."""

    def __init__(self, s3_client: S3Client, sns_client: SNSClient, sqs_client: SQSClient):
        """Init method for the attribute extractor lambda Class."""
        self.s3_client = s3_client
        self.sns_client = sns_client
        self.sqs_client = sqs_client
        self.sqs_url = os.environ["SQS_URL"]

    def handle_event(self, event: Dict[str, Any], context: Any) -> None:
        """Set up a watchdog and handle multiple events with a thread."""
        setup_watchdog(event, context)
        LOG.debug("input_event: %s", json.dumps(event))

        event_data_list = self.load_batch_message_content(event)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            result = executor.map(self.handle_single_event, event_data_list)

        created_exceptions = [exception for exception in result if exception is not None]
        signal.alarm(0)

        if len(created_exceptions) > 0:
            LOG.warning(f"Lambda has thrown {len(created_exceptions)} exceptions!")
            raise Exception from created_exceptions[0]  # pylint: disable=broad-exception-raised

    @staticmethod
    def extract_message_attributes(message: str, object_key: str) -> Dict[str, Any]:
        """Extract attributes from the message."""
        message_obj = json.loads(message)["Records"][0]
        message_strings = disentangle_json(message_obj, separator="###")
        message_attributes_dict = convert_to_attributes(message_strings, separator="###")

        message_attributes_dict = {
            SNS_ATTRIBUTE_NAME_REGEX.sub("_", k)[:256]: v
            for k, v in {**message_attributes_dict, **AttributeExtractorLambda.get_partition_info(object_key)}.items()
        }

        LOG.debug(f"flat_json: {str(message_attributes_dict)}")
        return message_attributes_dict

    @staticmethod
    def get_partition_info(object_key: str) -> Dict[str, Any]:
        """Look for the partitions in the object."""
        result = {}
        for match in PARTITION_REGEX.finditer(object_key):
            attribute = {
                "DataType": "String",
                "StringValue": match.group(3),
            }
            result[match.group(2)] = attribute
        return result

    @staticmethod
    def get_subject_string(object_key: str) -> str:
        """Get the subject of the string."""
        subject_string = "New Object: " + object_key
        subject_string_short = (subject_string[:95] + "...") if len(subject_string) > 98 else subject_string
        subject_string_ascii = subject_string_short.encode("ascii", errors="ignore").decode()
        subject_string_ascii_no_control_character = "".join(
            character for character in subject_string_ascii if unicodedata.category(character)[0] != "C"
        )
        return subject_string_ascii_no_control_character

    @staticmethod
    def load_message_content(event: Dict[str, Any]) -> Dict[str, Any]:
        """Return the message of the event."""
        body = json.loads(event["body"])
        message_obj = json.loads(body["Message"])["Records"][0]
        LOG.debug("message_obj: %s", message_obj)
        principal_id = message_obj["userIdentity"]["principalId"]
        bucket_name = message_obj["s3"]["bucket"]["name"]
        object_key = unquote_plus(message_obj["s3"]["object"]["key"])
        LOG.debug("bucket_name: %s", bucket_name)
        LOG.debug("object_key: %s", object_key)
        return {
            "body": body,
            "bucket_name": bucket_name,
            "object_key": object_key,
            "principal_id": principal_id,
            "receipt_handle": event["receiptHandle"],
        }

    @lru_cache(maxsize=500)  # noqa: B019 # service instantiated only once per lambda runtime
    def get_sns_topic(self, bucket_name: str) -> Any:
        """Retrieve the SNS Topic."""
        try:
            tags = self.s3_client.get_bucket_tagging(Bucket=bucket_name)["TagSet"]
        except ClientError as client_error:
            raise Exception(  # pylint: disable=broad-exception-raised
                f"No tags found for bucket {bucket_name}"
            ) from client_error
        sns_topics = [item["Value"] for item in tags if item["Key"] == "snsTopicArn"]
        if sns_topics:
            return sns_topics[0]
        raise KeyError(f"Tags for bucket {bucket_name} lack key 'snsTopicArn'")

    def load_batch_message_content(self, events: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Load the message content."""
        result = []
        for event in events["Records"]:
            try:
                result.append(self.load_message_content(event))
            except (TypeError, KeyError) as error:
                if "s3:TestEvent" in event.get("body", []):
                    LOG.info("S3 Testevent received!")
                    continue
                LOG.error("Error during json parsing in event: %s", json.dumps(event))
                LOG.error(f"Exception type: TypeError, Exception message: {str(error)}")
                raise
        return result

    def handle_single_event(self, event_data: Dict[str, Any]) -> Optional[Exception]:
        """Handle a single event."""
        try:
            self.forward_message_to_sns(
                event_data["body"]["Message"], event_data["bucket_name"], event_data["object_key"]
            )
            self.delete_sqs_message(event_data["receipt_handle"])
        except Exception as general_exception:  # pylint: disable=broad-except
            return general_exception
        return None

    def forward_message_to_sns(self, message: str, bucket_name: str, object_key: str) -> None:
        """Send the message to SNS."""
        try:
            head_object_response = self.sns_client.publish(
                TopicArn=self.get_sns_topic(bucket_name),
                Message=message,
                Subject=self.get_subject_string(object_key),
                MessageAttributes=self.extract_message_attributes(message, object_key),
            )
            LOG.debug("publish response: %s", head_object_response)
        except Exception as general_exception:  # pylint: disable=broad-except
            LOG.error(
                f"Exception type: {type(general_exception).__name__}," f"\nException message: {str(general_exception)}"
            )
            raise

    def delete_sqs_message(self, receipt_handle: str) -> None:
        """Delete a SQS message."""
        try:
            self.sqs_client.delete_message(QueueUrl=self.sqs_url, ReceiptHandle=receipt_handle)
            LOG.debug(f"SQS message with receiptHandle: {receipt_handle} was deleted.")
        except Exception as general_exception:  # pylint: disable=broad-except
            LOG.error(
                f"Exception type: {type(general_exception).__name__}," f"\nException message: {str(general_exception)}"
            )
            raise


def setup_watchdog(event: Dict[str, Any], context: Any) -> None:
    """Set up a watchdog alarm."""

    def timeout_handler(_signal: Any, _frame: Any) -> None:
        LOG.warning(f"About to timeout: event: {event} context: {vars(context)}")

    signal.alarm(int(context.get_remaining_time_in_millis() / 1000) - 1)
    signal.signal(signal.SIGALRM, timeout_handler)


def convert_to_attributes(disentangled: List[str], separator: str = "###") -> Dict[str, Dict[str, str]]:
    """Convert the disentangled list to an attributes dictionary."""

    def split(input_string: str) -> Union[Tuple[str, str], List[str]]:
        if separator in input_string:
            return input_string.rsplit(separator, 1)
        return input_string, ""

    return {
        split(entry)[0].replace(separator, "."): {"DataType": "String", "StringValue": split(entry)[1]}
        for entry in disentangled
    }


def disentangle_json(json_object: Union[Dict[str, Any], List[Any]], separator: str = "###") -> List[str]:
    """Disentangle JSON recursively."""
    result = []

    def disentangle(json_sub_object: Union[Dict[str, Any], List[Any]], prefix: str = "") -> None:
        """Recursive call to disentangle JSON and store the data in a list."""
        try:
            for json_key, json_value in tupelize(json_sub_object):
                disentangle(json_value, f"{prefix}{json_key}{separator}")
        except ValueError:
            result.append(f"{prefix}{json_sub_object}")

    disentangle(json_object)
    return result


def tupelize(
    json_object: Union[Dict[str, Any], List[Any]]
) -> Union[Iterable[Tuple[str, Any]], Iterable[Tuple[int, Any]]]:
    """Convert a list or dictionary to an Iterable."""
    if isinstance(json_object, dict):
        return json_object.items()
    if isinstance(json_object, list):
        return enumerate(json_object)
    raise ValueError


def get_error_code(error: ClientError) -> str:
    """Get the error code from an error."""
    return error.response.get("Error", {}).get("Code", "UnknownError")
