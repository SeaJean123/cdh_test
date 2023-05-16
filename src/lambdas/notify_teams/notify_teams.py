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
import urllib.parse
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING

import boto3
import requests

from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.account_store import AccountStoreException
from cdh_core.enums.environment import Environment
from cdh_core.log.log_safe import log_safe
from cdh_core.log.logger import configure_logging
from cdh_core.optionals import apply_if_not_none
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
else:
    Table = object

LOG = getLogger(__name__)

# https://docs.microsoft.com/de-de/microsoftteams/limits-specifications-teams#messaging
MESSAGE_MAX_SIZE = 1024 * 20  # 20 KB
DEFAULT_FRIENDLY_NAME_FOR_UNKNOWN_ACCOUNTS = "Friendly name unknown"


@dataclass()
class Event:
    """Handling the incoming event for Microsoft Teams."""

    time_stamp: Optional[float] = None
    severity: str = ""
    hash: str = ""
    description: str = ""
    region: str = ""
    account_id: str = ""
    account_friendly_name: str = ""
    uri: str = ""
    request_id: str = ""
    uri_message: str = ""


@dataclass(frozen=True)
class SilenceInfo:
    """Describes an entry of the table cdh-silenced-alerts."""

    hash: str
    comment: Optional[str]
    valid_until: datetime


def _get_dynamo_table() -> Table:
    dynamo = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
    return dynamo.Table(os.environ["SILENCED_ALERTS_TABLE"])


def get_silence_info(event_hash: str) -> Optional[SilenceInfo]:
    """If the alert has been silenced, retrieve the relevant content from DynamoDB. Otherwise, return None."""
    table = _get_dynamo_table()
    try:
        response = table.get_item(Key={"hash": event_hash}, ConsistentRead=True)
    except (
        table.meta.client.exceptions.ProvisionedThroughputExceededException,
        table.meta.client.exceptions.InternalServerError,
    ) as error:
        LOG.error(f"Failed to retrieve silence info for {event_hash=} from Dynamo table: {error}")
        return None
    try:
        item = response["Item"]
        comment = apply_if_not_none(str)(item.get("comment"))
        ttl = int(item["ttl"])  # type: ignore
        return SilenceInfo(hash=event_hash, comment=comment, valid_until=datetime.fromtimestamp(ttl))
    except KeyError:
        # either no item was found or no ttl was specified, in which case we ignore it
        return None


def resolve_account_friendly_name(account_id: Optional[str]) -> str:
    """For the readability of the error messages we transform the account IDs into readable names."""
    if account_id:
        with suppress(AccountStoreException):
            return (
                "cdh-"
                + AccountStore()
                .query_account(account_ids=AccountId(account_id), environments=frozenset(Environment))
                .alias
            )
    return DEFAULT_FRIENDLY_NAME_FOR_UNKNOWN_ACCOUNTS


@log_safe()
def handler(event: Dict[str, Any], _: Any) -> None:
    """Handle the lambda function."""
    configure_logging(__name__)
    records = event.get("Records", [])
    failed_records: List[Tuple[Dict[str, Any], BaseException]] = []
    for record in records:
        try:
            event_region = ""
            body = json.loads(record["body"])
            LOG.debug(body)
            event_message = json.loads(body["Message"])

            if "TopicArn" in body:
                event_region = body["TopicArn"].split(":")[3]
            if raw_message := event_message.get("raw_message"):
                LOG.info(f"raw_message={raw_message}")
                send_message_to_teams(raw_message.replace("\r", "\r\r"))
            elif "hash" in event_message:
                message = process_log_subscription_or_cloudwatch_event(body, event_message, event_region)
                LOG.info(f"message={message}")
                silence_info = get_silence_info(event_message["hash"])
                send_message_to_teams(message.replace("\r", "\r\r"), silence_info)
            else:
                message = process_cloudwatch_metric_alarm(event_message, event_region)
                LOG.info(message)
                send_message_to_teams(message.replace("\r", "\r\r"))
        except Exception as err:  # pylint: disable=broad-except
            failed_records.append((record, err))

    if failed_records:
        error_msg = f"Failed to process the following records: {failed_records}"
        LOG.error(error_msg)
        raise Exception(error_msg)  # pylint: disable=broad-exception-raised


def process_cloudwatch_metric_alarm(event_message: Dict[str, Any], event_region: str) -> str:
    """Process a cloudwatch metric alarm."""
    alarm_name = event_message["AlarmName"]
    message_event = Event(
        description=event_message.get("AlarmDescription") or "???",  # handle explicit null in the json as well
        account_id=event_message["AWSAccountId"],
        account_friendly_name=resolve_account_friendly_name(event_message["AWSAccountId"]),
        uri=(
            f"https://{event_region}.console.aws.amazon.com/cloudwatch/home?"
            f"region={event_region}#alarmsV2:alarm/"
            f"{urllib.parse.quote_plus(alarm_name)}"
        ),
    )
    message = (
        f'## Metric Alarm for "{escape_markdown(alarm_name)}"\r'  # noqa: B028
        f"- Account: {message_event.account_id} ({message_event.account_friendly_name})\r"
        f"- Region: {event_region}\r"
        f"- Description: {escape_markdown(message_event.description)}\r"
        f"- Change: {escape_markdown(event_message['OldStateValue'])} -> "
        f"**{escape_markdown(event_message['NewStateValue'])}** \r"
        f"- Reason: {escape_markdown(event_message['NewStateReason'])}\r"
        f"- Time: {event_message['StateChangeTime']}\r"
        f"- URI: [alarm]({message_event.uri})"
    )
    return message


def process_log_subscription_or_cloudwatch_event(
    body: Dict[str, Any], event_message: Dict[str, Any], event_region: str
) -> str:
    """Process log subscription or cloudwatch alarm events."""
    subject = body.get("Subject") or event_message["Subject"]
    time_stamp_fallback = body["Timestamp"]
    message_event = Event(
        severity=event_message.get("severity", None),
        hash=event_message.get("hash", None),
        description=event_message.get("description", None),
        time_stamp=event_message.get("time_stamp", None),
        region=event_region or event_message.get("region", None),
        account_id=event_message.get("account_id", None),
        account_friendly_name=resolve_account_friendly_name(event_message.get("account_id", None)),
        uri=event_message.get("uri", None),
        request_id=event_message.get("request_id", None),
    )
    if message_event.time_stamp:
        event_time_stamp_human = datetime.utcfromtimestamp(message_event.time_stamp).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        event_time_stamp_human = time_stamp_fallback
    message_event.uri_message = f"[logs]({message_event.uri})" if message_event.uri else "No log link"
    message = (
        f"## {escape_markdown(subject)}\r"  # noqa: B028
        f"- Account: {message_event.account_id} ({message_event.account_friendly_name})\r"
        f"- Region: {message_event.region}\r"
        f"- Severity: {message_event.severity}\r"
        f'- Hash: "{escape_markdown(message_event.hash)}"\r'
        f'- RequestId: "{escape_markdown(message_event.request_id)}"\r'
        f"- Time: {event_time_stamp_human}\r"
        f"- URI: {message_event.uri_message}\r"
        f"- Description:\r{escape_markdown(message_event.description)}"
    )
    return message


def escape_markdown(message: Optional[str]) -> str:
    """Escaping characters for markdown."""
    if message:
        return message.replace("_", "\\_").replace("*", "\\*")
    return ""


def trim_message(message: str, message_max_size: int = MESSAGE_MAX_SIZE) -> str:
    """Trims the message to the maximum length Microsoft Teams can cope."""
    if len(message.encode(errors="ignore")) < message_max_size:
        return message
    error_msg = "The following message was too long and was cut of: \r\r"
    for i in range(message_max_size, message_max_size + 10):
        with suppress(UnicodeError):
            trimmed_message = message.encode()[:i].decode()
            LOG.error(f"{error_msg} {message}")
            return error_msg + trimmed_message
    # the following should never not happen, because of the max size of a Unicode char which is 4 byte
    error_msg = "The message was too long but couldn't be cut of properly. "
    LOG.error(f"{error_msg} {message}")
    return error_msg + "See the logfile for more information."


def send_message_to_teams(message: str, silence_info: Optional[SilenceInfo] = None) -> None:
    """Send message to Microsoft Teams webhook."""
    if os.environ.get("ENABLED", "").lower() != "true":
        LOG.info("Not forwarding the message to Teams because the Lambda is not enabled.")
        return
    if silence_info is not None:
        LOG.info(f"Not forwarding the message to Teams due to active silencing: {silence_info} ")
        return
    _do_send(message)


def _do_send(message: str) -> None:
    url = os.environ["WEBHOOK_URL"]
    data = {"text": trim_message(message)}
    response = requests.post(url=url, data=json.dumps(data).encode(), headers={"Content-Type": "application/json"})
    if not response.ok:
        raise Exception(  # pylint: disable=broad-exception-raised
            f"Sending the message to {url!r} failed with the following error: {response.status_code}, {response.text}"
        )
