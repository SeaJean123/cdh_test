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
import datetime
import json
import os
from logging import getLogger
from typing import Any
from typing import Dict

import boto3

from cdh_core.aws_clients.sqs_client import SqsClient
from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.log.log_safe import log_safe
from cdh_core.log.logger import configure_logging

DATE_FORMAT = "%Y-%m-%d"
LOG = getLogger(__name__)


def _build_sqs_client(region: Region) -> SqsClient:
    return SqsClient(boto3.client("sqs", region_name=region.value))


@log_safe()
def lambda_handler(event: Dict[str, Any], _: LambdaContext) -> None:
    """Process the given lambda event for updating account information for all accounts."""
    configure_logging(__name__)

    region = Region.preferred(Partition.default())
    sqs = _build_sqs_client(region)
    try:
        nominal_date = str(event["nominal_date"])
        LOG.info(f"nominal_date found in event data: {nominal_date}")
    except KeyError:
        LOG.warning("no nominal_date in event input, using today")
        nominal_date = datetime.date.today().strftime(DATE_FORMAT)

    base_url = os.environ["CORE_API_URL"]
    core_api_client = CoreApiClient.get_core_api_client(base_url, region)
    accounts = core_api_client.get_accounts()

    LOG.debug(accounts)
    LOG.debug(f"processing {len(accounts)} accounts")
    sqs.send_messages(
        queue_url=os.environ["QUEUE_URL"],
        messages=[
            json.dumps({"account_id": account.id, "hub": account.hub.value})
            for account in accounts
            if account.affiliation == Affiliation("cdh")
        ],
        attributes={"nominal_date": {"StringValue": nominal_date, "DataType": "String"}},
    )
