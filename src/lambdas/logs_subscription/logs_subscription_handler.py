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
import base64
import gzip
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
from time import time
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

import boto3
from dateutil.parser import parse

from cdh_core.log.log_safe import GenericLambdaException
from cdh_core.log.log_safe import log_safe
from cdh_core.log.logger import configure_logging

LOG = getLogger(__name__)
RETENTION_TIME = 30 * 60


if TYPE_CHECKING:
    from mypy_boto3_sns import SNSClient
    from mypy_boto3_dynamodb.service_resource import Table
    from aws_lambda_typing.context.context import Context
else:
    SNSClient = object
    Table = object
    Context = object


@dataclass(frozen=True)
class EventContext:
    """Context details and origin of certain lambda events."""

    region: str
    account_id: str
    log_group: str
    log_stream: str

    @property
    def lambda_name(self) -> str:
        """Return the lambda name where the events occurred."""
        return self.log_group.rsplit("/", 1)[-1]


@dataclass(frozen=True)
class LogEvent:
    """Details about an event based on a single log line."""

    context: EventContext
    severity: str
    time_stamp: int
    description: Optional[str] = None
    request_id: Optional[str] = None
    module: Optional[str] = None
    function: Optional[str] = None

    @classmethod
    def parse(cls, raw_log_message: str, context: EventContext, time_stamp_fallback: int) -> "LogEvent":
        """Return an event based on a single log line."""
        # parse event (extract json in future.)
        # Example patterns:
        # [ERROR]	OSError: Cannot load native module bla
        # [DEBUG]	[logs_subscription]	[is_duplicate]	2020-02-10 12:25:07,732Z	53a2b081-1	my event message
        event_parsed = re.match(
            "^\\[([^] \t]+)\\]\t? ?(\\[([^]\t]+)\\]\t\\[([^]\t]+)\\]\t([^\t]+)\t([^\t]+)\t)?(.*)",
            raw_log_message,
            re.DOTALL,
        )
        if event_parsed is None:
            raise LogParseError(f"Cannot parse event {raw_log_message!r}")
        try:
            timestamp = int(parse(event_parsed.group(5)).timestamp())
        except Exception:  # pylint: disable=broad-except
            timestamp = time_stamp_fallback
        return cls(
            context=context,
            severity=event_parsed.group(1),
            time_stamp=timestamp,
            description=event_parsed.group(7),
            request_id=event_parsed.group(6),
            module=event_parsed.group(3),
            function=event_parsed.group(4),
        )

    @property
    def hash(self) -> str:
        """Return a string representation of an event to be used for de-duplication."""
        event_function_information = f" {self.module}.{self.function}" if self.module and self.function else ""
        return f"{self.severity} {self.context.lambda_name}{event_function_information}"

    def _get_uri(self) -> str:
        region = self.context.region
        human_readable_timestamp = datetime.utcfromtimestamp(self.time_stamp).strftime("%Y-%m-%dT%H:%M:%SZ")
        domain = "console.amazonaws.cn" if region == "cn-north-1" else f"{region}.console.aws.amazon.com"
        return (
            f"https://{domain}/cloudwatch/home?region={region}"
            f"#logEventViewer:group={self.context.log_group};"
            f"stream={self.context.log_stream};start={human_readable_timestamp}"
        )

    def get_message(self) -> Dict[str, Any]:
        """Return a json representation of an event."""
        return {
            "severity": self.severity,
            "hash": self.hash,
            "description": self.description,
            "time_stamp": self.time_stamp,
            "account_id": self.context.account_id,
            "region": self.context.region,
            "uri": self._get_uri(),
            "request_id": self.request_id,
        }


def _put_event(table: Table, event: LogEvent) -> None:
    table.put_item(
        Item={
            "eventHash": event.hash,
            "time_stamp": event.time_stamp,
            "event": event.get_message(),
            # This must be current time() else we risk alarm floods when old data is processed.
            "ttl": int(time()) + RETENTION_TIME,
        }
    )


def _is_duplicate(table: Table, event: LogEvent) -> bool:
    response = cast(Dict[str, Any], table.get_item(Key={"eventHash": event.hash}, ConsistentRead=True))
    if "Item" in response:
        if response["Item"]["ttl"] >= time():
            LOG.debug("Event already alerted. Ignoring")
            return True
        LOG.debug("Stale event found. Processing")
    else:
        LOG.debug("New event detected. Processing")
    return False


def _delete_event(table: Table, event: LogEvent) -> None:
    table.delete_item(Key={"eventHash": event.hash})


def _parse_logevents(
    sns_client: SNSClient, event_context: EventContext, alerts_topic_arn: str, payload: Dict[str, Any], table: Table
) -> List[str]:
    errors = []
    for raw_log_event in payload["logEvents"]:
        try:
            LOG.debug(f"LogEvent: {json.dumps(raw_log_event)}")
            try:
                time_stamp_fallback = int(raw_log_event["timestamp"] / 1000)
                raw_log_message = raw_log_event["message"]
                severity = raw_log_event["extractedFields"]["severity"]
            except Exception as err:  # pylint: disable=broad-except
                errors.append(f"Cannot parse log_event because of error ({err}): {json.dumps(raw_log_event)}\n")
                continue
            try:
                log_event = LogEvent.parse(
                    raw_log_message=raw_log_message, context=event_context, time_stamp_fallback=time_stamp_fallback
                )
            except LogParseError:
                log_event = LogEvent(severity=severity, time_stamp=time_stamp_fallback, context=event_context)

            if log_event.description and log_event.description.startswith(GenericLambdaException.__name__):
                LOG.info(f"Special exception ignored:\r{json.dumps(log_event.get_message())}")
                continue

            if _is_duplicate(table, log_event):
                LOG.info(f"Duplicate event_message ignored:\r{json.dumps(log_event.get_message())}")
                # refresh ttl on existing events
                _put_event(table, log_event)
                continue

            _put_event(table, log_event)
            sns_errors = forward_event_to_sns(
                event=log_event, sns_client=sns_client, topic_arn=alerts_topic_arn, table=table
            )
            errors.extend(sns_errors)
            LOG.debug(f"Finished processing event: {json.dumps(raw_log_event)}")

        except Exception as err:  # pylint: disable=broad-except
            errors.append(f"Error {err} for log_event: {json.dumps(raw_log_event)}")

    return errors


@log_safe()
def handler(event: Dict[str, Any], context: Context) -> None:
    """Process the given lambda event for alerting."""
    configure_logging(__name__)

    alerts_topic_arn = os.environ["ALERTS_TOPIC_ARN"]
    resource_name_prefix = os.environ["RESOURCE_NAME_PREFIX"]

    sns_client = boto3.client("sns", region_name=os.environ["AWS_REGION"])
    dynamodb_client = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
    table = dynamodb_client.Table(f"{resource_name_prefix}cdh-events-history")

    payload = json.loads(gzip.decompress(base64.b64decode(event["awslogs"]["data"])))
    LOG.debug(f"Payload: {payload}")

    event_context = EventContext(
        region=os.environ["AWS_REGION"],
        account_id=context.invoked_function_arn.split(":")[4],
        log_group=payload["logGroup"],
        log_stream=payload["logStream"],
    )

    if errors := _parse_logevents(
        sns_client=sns_client,
        event_context=event_context,
        alerts_topic_arn=alerts_topic_arn,
        payload=payload,
        table=table,
    ):
        # We can safely log here because this lambdas cloudwatch logs is not subscribed to this lambda itself
        error_msg = f"Encountered the following errors: {errors}"
        LOG.critical(error_msg)
        raise Exception(error_msg)  # pylint: disable=broad-exception-raised


def forward_event_to_sns(event: LogEvent, sns_client: SNSClient, topic_arn: str, table: Table) -> List[str]:
    """Forward the event to a sns queue for alerting."""
    LOG.info(f"Publish event_message to sns: {json.dumps(event.get_message())}")
    errors = []
    subject = f'Log Alarm for "{event.severity} {event.context.lambda_name}"'
    if len(subject) > 100:
        subject = subject[:97] + "..."
    try:
        head_object_response = sns_client.publish(
            TopicArn=topic_arn, Subject=subject, MessageAttributes={}, Message=json.dumps(event.get_message())
        )
        LOG.debug("Publish response: %s", head_object_response)
    except Exception as sns_publish_error:  # pylint: disable=broad-except
        errors.append(f"Failed to publish to sns: {sns_publish_error}")
        try:
            _delete_event(table, event)
        except Exception as event_cleanup_error:  # pylint: disable=broad-except
            errors.append(f"Rollback of dynamo failed: {event_cleanup_error}")
    return errors


class LogParseError(Exception):
    """Signals that a log line cannot be parsed."""
