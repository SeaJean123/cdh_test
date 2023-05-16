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
import os
import random
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union
from unittest.mock import Mock
from unittest.mock import patch
from urllib.parse import unquote_plus

import boto3
import pytest
from mypy_boto3_s3.type_defs import CreateBucketConfigurationTypeDef
from s3_attribute_extractor.s3_attribute_extractor import AttributeExtractorLambda
from s3_attribute_extractor.s3_attribute_extractor import lambda_handler
from s3_attribute_extractor.s3_attribute_extractor import LOG
from s3_attribute_extractor.s3_attribute_extractor import setup_watchdog

from cdh_core.entities.arn import build_arn_string
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.fixtures import mock_s3  # pylint: disable=unused-import
from cdh_core_dev_tools.testing.fixtures import mock_sns  # pylint: disable=unused-import
from cdh_core_dev_tools.testing.fixtures import mock_sqs  # pylint: disable=unused-import
from cdh_core_dev_tools.testing.fixtures import mock_sts  # pylint: disable=unused-import

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sns import SNSClient
    from mypy_boto3_sqs import SQSClient
else:
    S3Client = object
    SNSClient = object
    SQSClient = object


@pytest.mark.usefixtures("mock_s3", "mock_sns", "mock_sqs", "mock_sts")
class TestAttributeExtractor:
    REGION = random.choice(list(Region)).value
    DATE_FORMAT = "%Y-%m-%d"
    BUCKET_BASE = "s3-attribute-extractor-test-bucket"
    BUCKET = f"{BUCKET_BASE}-{REGION}"
    DEBUG_BUCKET = "local-attribute-extractor-test-bucket"
    BODY = Builder.build_random_string(length=100).encode("UTF-8")
    SQS_QUEUE_TARGET = "sqs_queue_target"
    SQS_QUEUE_SOURCE = "sqs_queue_source"

    @pytest.fixture()
    def setup_environment_variables(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("ACCOUNT_ID", Builder.build_random_digit_string(length=12))
        monkeypatch.setenv("AWS_REGION", self.REGION)
        monkeypatch.setenv("AWS_DEFAULT_REGION", self.REGION)

    def moto_sqs(self) -> SQSClient:
        sqs_client = boto3.client("sqs", region_name=self.REGION)
        sqs_client.create_queue(QueueName=self.SQS_QUEUE_SOURCE)
        queue_url = sqs_client.get_queue_url(QueueName=self.SQS_QUEUE_SOURCE)["QueueUrl"]
        os.environ["SQS_URL"] = queue_url
        return sqs_client

    def moto_s3(self, keys: List[str]) -> S3Client:
        create_bucket_config = (
            CreateBucketConfigurationTypeDef({"LocationConstraint": self.REGION})
            if self.REGION != "us-east-1"
            else CreateBucketConfigurationTypeDef({})
        )
        s3_resource = boto3.resource("s3", region_name=self.REGION)
        s3_resource.create_bucket(
            Bucket=self.BUCKET,
            CreateBucketConfiguration=create_bucket_config,
        )
        s3_resource.create_bucket(
            Bucket=self.DEBUG_BUCKET,
            CreateBucketConfiguration=create_bucket_config,
        )
        s3_client = boto3.client("s3", region_name=self.REGION)
        for key in keys:
            s3_client.put_object(Bucket=self.BUCKET, Key=key, Body=self.BODY)
        return s3_client

    def moto_sns(self) -> SNSClient:
        sqs_client = boto3.client("sqs", region_name=self.REGION)
        sns_client = boto3.client("sns", region_name=self.REGION)
        topic_name = self.BUCKET_BASE

        # Create/Get Queue
        sqs_client.create_queue(QueueName=self.SQS_QUEUE_TARGET)
        queue_url = sqs_client.get_queue_url(QueueName=self.SQS_QUEUE_TARGET)["QueueUrl"]
        sqs_queue_attrs = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])["Attributes"]
        sqs_queue_arn = sqs_queue_attrs["QueueArn"]
        if ":sqs." in sqs_queue_arn:
            sqs_queue_arn = sqs_queue_arn.replace(":sqs.", ":")

        # Create SNS Topic
        topic_res = sns_client.create_topic(Name=topic_name)
        sns_topic_arn = topic_res["TopicArn"]
        boto3.client("s3", region_name=self.REGION).put_bucket_tagging(
            Bucket=self.BUCKET,
            Tagging={"TagSet": [{"Key": "snsTopicArn", "Value": sns_topic_arn}]},
        )

        # Subscribe SQS queue to SNS
        sns_client.subscribe(TopicArn=sns_topic_arn, Protocol="sqs", Endpoint=sqs_queue_arn)
        return sns_client

    @contextmanager
    def setup_all(self, keys: List[str]) -> Any:
        self.moto_sqs()
        self.moto_s3(keys)
        self.moto_sns()

        yield

    def mock_context(self, fake_request_id: Optional[str] = None, fake_remaining_time: int = 60000) -> Any:
        if not fake_request_id:
            fake_request_id = Builder.build_request_id()
        context = Mock()
        context.aws_request_id = fake_request_id
        context.get_remaining_time_in_millis = Mock()
        context.get_remaining_time_in_millis.return_value = fake_remaining_time
        return context

    @pytest.fixture(autouse=True)
    def patch_watchdog(self) -> Generator[Any, None, None]:
        with patch("s3_attribute_extractor.s3_attribute_extractor.setup_watchdog"):
            yield

    def get_real_receipt_handler(self, records_dict: Dict[str, Any]) -> Dict[str, Any]:
        sqs_client = boto3.client("sqs", region_name=self.REGION)
        queue_url = os.environ.get("SQS_URL", None)
        if not queue_url:
            raise ValueError
        for record in records_dict["Records"]:
            sqs_client.send_message(QueueUrl=queue_url, MessageBody=record["body"], DelaySeconds=0)

        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            AttributeNames=["All"],
            VisibilityTimeout=0,
            WaitTimeSeconds=10,
        )
        new_record_dict = {"Records": response["Messages"]}
        for record in new_record_dict["Records"]:
            keys = [*record]
            for key in keys:
                record[key[0].lower() + key[1:]] = record.pop(key)
        return new_record_dict

    def create_record_event_list(self, keys: List[str], principal_id: str) -> Dict[str, Any]:
        events: Dict[str, Any] = {"Records": []}
        for key in keys:
            events["Records"].append(self.s3_object_created_event(self.BUCKET, key, principal_id)["Records"][0])
        return events

    def s3_object_created_event(self, bucket_name: str, key: str, principal_id: Any = None) -> Dict[str, Any]:
        topic_arn = build_arn_string(
            service="sns",
            partition=Region(self.REGION).partition,
            region=Region(self.REGION),
            account=build_account_id(),
            resource=f"cdh-core-s3-attribute-extractor-trigger-{self.REGION}",
        )
        event_source_arn = ":".join(
            [
                "arn",
                "aws",
                "sqs",
                self.REGION,
                Builder.build_random_digit_string(),
                f"cdh-core-s3-attribute-extractor-event-forwarder-{self.REGION}",
            ]
        )
        message_dict = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": self.REGION,
                    "eventTime": f"{Builder.build_random_datetime().strftime(self.DATE_FORMAT)}T00:00:00.0Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": principal_id},
                    "requestParameters": {"sourceIPAddress": "0.0.0.0"},
                    "responseElements": {
                        "x-amz-request-id": Builder.build_request_id(),
                        "x-amz-id-2": Builder.build_random_string(length=50),
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": Builder.build_random_string(length=50),
                        "bucket": {
                            "name": bucket_name,
                            "ownerIdentity": {"principalId": Builder.build_random_string()},
                            "arn": "arn:aws:s3:::" + bucket_name,
                        },
                        "object": {
                            "key": key,
                            "size": int(Builder.build_random_digit_string(length=5)),
                            "eTag": Builder.build_random_string(),
                            "sequencer": Builder.build_random_string(),
                        },
                    },
                }
            ]
        }
        signing_cert_url = f"https://sns.{self.REGION}.amazonaws.com/SimpleNotificationService-xyz.pem"
        return {
            "Records": [
                {
                    "messageId": Builder.build_random_string(),
                    "receiptHandle": Builder.build_random_string(),
                    "body": json.dumps(
                        {
                            "Type": "Notification",
                            "MessageId": Builder.build_random_string(),
                            "TopicArn": topic_arn,
                            "Subject": "Amazon S3 Notification",
                            "Message": json.dumps(message_dict),
                            "Timestamp": f"{Builder.build_random_datetime().strftime(self.DATE_FORMAT)}T00:00:00.0Z",
                            "SignatureVersion": "1",
                            "Signature": Builder.build_random_string(),
                            "SigningCertURL": signing_cert_url,
                            "UnsubscribeURL": Builder.build_random_url(),
                        }
                    ),
                    "attributes": {
                        "ApproximateReceiveCount": "1",
                        "SentTimestamp": Builder.build_random_digit_string(),
                        "SenderId": Builder.build_random_string(),
                        "ApproximateFirstReceiveTimestamp": Builder.build_random_digit_string(),
                    },
                    "messageAttributes": {},
                    "md5OfBody": Builder.build_random_string(),
                    "eventSource": "aws:sqs",
                    "eventSourceARN": event_source_arn,
                    "awsRegion": self.REGION,
                }
            ]
        }

    def get_last_modified(self, *keys: Any) -> List[datetime]:
        s3_client = boto3.client("s3", region_name=self.REGION)
        last_modified = []
        for key in keys:
            response = s3_client.head_object(Bucket=self.BUCKET, Key=key)
            last_modified.append(response["LastModified"])
        return last_modified

    def get_s3_testevent(self) -> Dict[str, Any]:
        topic_arn = build_arn_string(
            service="sns",
            partition=Region(self.REGION).partition,
            region=Region(self.REGION),
            account=build_account_id(),
            resource=f"cdh-core-s3-attribute-extractor-trigger-{self.REGION}",
        )
        message = {
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": datetime.now().isoformat(),
            "Bucket": self.BUCKET,
            "RequestId": Builder.build_random_string(),
            "HostId": Builder.build_random_string(),
        }
        body = {
            "Type": "Notification",
            "MessageId": Builder.build_random_string(),
            "Subject": "Amazon S3 Notification",
            "TopicArn": topic_arn,
            "Message": json.dumps(message),
        }

        record_dict = self.s3_object_created_event("", "")
        record_dict["Records"][0]["body"] = json.dumps(body)
        return record_dict

    @contextmanager
    def call_lambda_with(self, principal_id: str, keys: Union[str, List[str]]) -> Any:
        if isinstance(keys, str):
            keys = [keys]
        records_dict = self.create_record_event_list(keys=keys, principal_id=principal_id)
        records_dict = self.get_real_receipt_handler(records_dict)
        lambda_handler(records_dict, self.mock_context())

        yield

        self.validate_empty_message(self.SQS_QUEUE_SOURCE)

    # Tests
    @pytest.mark.usefixtures("setup_environment_variables")
    def test_publish_event_for_overwritten_file(self) -> None:
        key = Builder.build_random_string()
        principal_id = Builder.build_random_digit_string(length=20)
        with self.setup_all([key]):
            with self.call_lambda_with(principal_id, key):
                self.validate_message(key, principal_id)

    @pytest.mark.usefixtures("setup_environment_variables")
    def test_random_long_key_object_created(self) -> None:
        key = Builder.build_random_string(1000)
        principal_id = Builder.build_random_digit_string(length=20)
        with self.setup_all([key]):
            with self.call_lambda_with(principal_id, key):
                self.validate_message(key, principal_id)

    @pytest.mark.usefixtures("setup_environment_variables")
    def test_multi_events_random_principal_id(self) -> None:
        keys = [Builder.build_random_string() for _ in range(5)]
        principal_id = Builder.build_random_digit_string(length=20)
        with self.setup_all(keys):
            with self.call_lambda_with(principal_id, keys):
                self.check_keys_in_messages(keys)

    @pytest.mark.usefixtures("setup_environment_variables")
    def test_key_attributes_in_message_attributes(self) -> None:
        p_date = Builder.build_random_datetime().strftime(self.DATE_FORMAT)
        p_event = Builder.build_random_string()
        p_eventtype = Builder.build_random_string()
        key = f"customer_analytics/p_date={p_date}/p_event?Type={p_event}/AWS.p_eventtype={p_eventtype}/random_object"
        principal_id = Builder.build_random_digit_string(length=20)
        with self.setup_all([key]):
            with self.call_lambda_with(principal_id, key):
                # Validate Message
                self.validate_message(
                    key,
                    principal_id,
                    [
                        ["p_date", p_date],
                        ["p_event_Type", p_event],
                        ["_p_eventtype", p_eventtype],
                    ],
                )

    @pytest.mark.usefixtures("setup_environment_variables")
    def test_handling_of_s3_test_event(self) -> None:
        with self.setup_all([]):
            # validate lambda throws no exception with s3 test events
            lambda_handler(self.get_s3_testevent(), self.mock_context())
            assert True

    @pytest.mark.usefixtures("setup_environment_variables")
    def test_multi_events_random_principal_id_s3_test_event(self) -> None:
        keys = [Builder.build_random_string() for _ in range(5)]
        principal_id = Builder.build_random_digit_string(length=20)
        with self.setup_all(keys):
            events = self.create_record_event_list(keys, principal_id)
            events = self.get_real_receipt_handler(events)
            events["Records"].append(self.get_s3_testevent()["Records"][0])
            # Run call with an event describing the files:
            lambda_handler(events, self.mock_context())

            self.check_keys_in_messages(keys)
            self.validate_empty_message(self.SQS_QUEUE_SOURCE)

    @pytest.mark.skipif(
        "PYTEST_XDIST_WORKER_COUNT" in os.environ,
        reason="`signal` does not go with parallelization",
    )
    def test_timeout(self) -> None:
        key = Builder.build_random_string(length=300)
        principal_id = Builder.build_random_string()
        LOG.warning = Mock()  # type: ignore
        context = Mock()
        context.get_remaining_time_in_millis = Mock()
        context.get_remaining_time_in_millis.return_value = 2000
        setup_watchdog(self.s3_object_created_event(self.BUCKET, key, principal_id), context)  #
        time.sleep(1.1)
        assert LOG.warning.call_count == 1

    @pytest.mark.skipif(
        "PYTEST_XDIST_WORKER_COUNT" in os.environ,
        reason="`signal` does not go with parallelization",
    )
    def test_no_timeout(self) -> None:
        key = Builder.build_random_string(length=300)
        principal_id = Builder.build_random_string()
        LOG.warning = Mock()  # type: ignore
        context = Mock()
        context.get_remaining_time_in_millis = Mock()
        context.get_remaining_time_in_millis.return_value = 2000000
        setup_watchdog(self.s3_object_created_event(self.BUCKET, key, principal_id), context)
        LOG.warning.assert_not_called()

    def test_get_subject_string_truncated_and_removes_control_characters(self) -> None:
        subject_title = Builder.build_random_string()
        subject_task = Builder.build_random_string()
        random_date = Builder.build_random_datetime().strftime(self.DATE_FORMAT)
        first_query_param_name = Builder.build_random_string(length=3)
        first_query_param_value = Builder.build_random_string(length=3)
        second_query_param_name = Builder.build_random_string(length=3)
        second_query_param_value = Builder.build_random_string(length=3)
        query_params = "+".join(
            [
                f"{first_query_param_name}%3A",
                f"{first_query_param_value}%2C",
                f"{second_query_param_name}%3A",
                second_query_param_value,
            ]
        )
        subject_str = "/".join(
            [
                subject_title,
                f"{subject_task}%3D{random_date}-00-00-00",
                f"{subject_title}-part-0+%0A{query_params}.parquet",
            ]
        )
        expected_subject_str = " ".join(
            [
                f"New Object: {subject_title}/{subject_task}={random_date}-00-00-00/{subject_title}-part-0",
                f"{first_query_param_name}:",
                f"{first_query_param_value},",
                f"{second_query_param_name}:",
                f"{second_query_param_value}.parquet",
            ]
        )
        subject = unquote_plus(subject_str)
        assert AttributeExtractorLambda.get_subject_string(subject) == expected_subject_str

    def test_get_subject_string_truncated_and_removes_non_ascii_characters(
        self,
    ) -> None:
        first_attribute = Builder.build_random_string()
        second_attribute = Builder.build_random_string()
        subject = unquote_plus(f"{first_attribute}/{second_attribute}_non_ascii_äöüßÄÜÖこんにちは你好😄.parquet")
        assert (
            AttributeExtractorLambda.get_subject_string(subject)
            == f"New Object: {first_attribute}/{second_attribute}_non_ascii_.parquet"
        )

    def test_get_subject_string_(self) -> None:
        random_string = Builder.build_random_string(length=100) + Builder.build_random_digit_string(length=20)
        assert AttributeExtractorLambda.get_subject_string(random_string) == f"New Object: {random_string[:83]}..."

    def test_load_message_content(self) -> None:
        principal_id = Builder.build_random_string()
        subject_title = Builder.build_random_string()
        subject_task = Builder.build_random_string()
        random_date = Builder.build_random_datetime().strftime(self.DATE_FORMAT)
        first_query_param_name = Builder.build_random_string(length=3)
        first_query_param_value = Builder.build_random_string(length=3)
        second_query_param_name = Builder.build_random_string(length=3)
        second_query_param_value = Builder.build_random_string(length=3)
        query_params = "+".join(
            [
                f"{first_query_param_name}%3A",
                f"{first_query_param_value}%2C",
                f"{second_query_param_name}%3A",
                second_query_param_value,
            ]
        )
        key = "/".join(
            [
                subject_title,
                f"{subject_task}%3D{random_date}-00-00-00",
                f"{subject_title}-part-0+%0A{query_params}.parquet",
            ]
        )
        message = {
            "Records": [
                {
                    "userIdentity": {"principalId": principal_id},
                    "s3": {"bucket": {"name": self.BUCKET}, "object": {"key": key}},
                }
            ]
        }
        body = {"Message": json.dumps(message)}
        event = {"body": json.dumps(body), "receiptHandle": "handle123"}
        expected_message = {
            "body": body,
            "bucket_name": self.BUCKET,
            "object_key": unquote_plus(key),
            "principal_id": principal_id,
            "receipt_handle": "handle123",
        }
        assert AttributeExtractorLambda.load_message_content(event) == expected_message

    # Validators
    def validate_empty_message(self, queue_name: str) -> None:
        sqs_queue = boto3.resource("sqs", region_name=self.REGION).get_queue_by_name(QueueName=queue_name)
        sqs_msgs = sqs_queue.receive_messages()
        assert len(sqs_msgs) == 0

    def validate_message(
        self, key: str, principal_id: str, present_attributes: Optional[List[List[str]]] = None
    ) -> None:
        sqs_obj = boto3.resource("sqs", region_name=self.REGION)
        sqs_queue = sqs_obj.get_queue_by_name(QueueName=self.SQS_QUEUE_TARGET)
        sqs_msgs = sqs_queue.receive_messages(
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            VisibilityTimeout=15,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=10,
        )
        msg_json = json.loads(sqs_msgs[0].body)
        assert len(sqs_msgs) == 1
        assert msg_json["MessageAttributes"]["s3.bucket.name"]["Value"] == self.BUCKET
        msg_body = json.loads(msg_json["Message"])
        assert msg_body["Records"][0]["s3"]["object"]["key"] == key
        assert msg_body["Records"][0]["userIdentity"]["principalId"] == principal_id

        # present attributes is a list of lists, which are [message attribute, message attribute value]
        for attribute in present_attributes or []:
            assert msg_json["MessageAttributes"][attribute[0]]["Value"] == attribute[1]

        expected_subject = "New Object: " + key
        expected_subject = (expected_subject[:95] + "...") if len(expected_subject) > 98 else expected_subject
        expected_subject = expected_subject.encode("ascii", errors="ignore").decode()
        assert msg_json["Subject"] == expected_subject

    def check_keys_in_messages(self, keys: List[str]) -> None:
        sqs_obj = boto3.resource("sqs", region_name=self.REGION)
        sqs_queue = sqs_obj.get_queue_by_name(QueueName=self.SQS_QUEUE_TARGET)
        sqs_msgs = sqs_queue.receive_messages(
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            VisibilityTimeout=1,
            WaitTimeSeconds=2,
            MaxNumberOfMessages=10,
        )
        assert len(keys) == len(sqs_msgs)
        keys_in_messages = []
        for message in sqs_msgs:
            msg_json = json.loads(message.body)
            msg_body = json.loads(msg_json["Message"])
            keys_in_messages.append(msg_body["Records"][0]["s3"]["object"]["key"])

        # check if all filenames are present
        assert set(keys_in_messages) == set(keys)
        sqs_queue.purge()
