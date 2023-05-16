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
import csv
import io
import json
import os
from datetime import date
from datetime import datetime
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING

import boto3
from dateutil.relativedelta import relativedelta
from mypy_boto3_ce.type_defs import ResultByTimeTypeDef

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.arn import Arn
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.log.log_safe import log_safe
from cdh_core.log.logger import configure_logging
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_ce import CostExplorerClient
    from mypy_boto3_s3 import S3Client
else:
    CostExplorerClient = object
    S3Client = object


LOG = getLogger(__name__)
DATE_FORMAT = "%Y-%m-%d"


class CostExporter:
    """Class for exporting cost information for a single account."""

    def lambda_handler(self, event: Dict[str, Any], _: LambdaContext) -> None:
        """Process the given lambda event for updating account billing information."""
        configure_logging(__name__)
        prefix = os.environ["RESOURCE_NAME_PREFIX"]
        # The billing lambda only runs in the global scope and aggregates all billing info
        region = Region.preferred(Partition.default())
        base_url = os.environ["CORE_API_URL"]
        nominal_date = self.get_nominal_date(event)
        message_body = json.loads(event["Records"][0]["body"])
        account_id = message_body["account_id"]
        hub = Hub(message_body["hub"])
        LOG.info(f"Processing account {message_body} for nominal date {nominal_date}")

        s3_client: S3Client = boto3.client("s3", region_name=region.value)
        core_api_client = CoreApiClient.get_core_api_client(base_url, region)
        cost_explorer_client = self._get_cost_explorer(account_id=account_id, hub=hub, prefix=prefix, region=region)

        costs = self.get_costs(
            nominal_date=nominal_date,
            cost_explorer=cost_explorer_client,
        )

        self.write_costs_to_s3(
            results=costs, folder=f"MonthlyReports/{account_id}", account_id=account_id, s3_client=s3_client
        )
        self.update_account_billing(
            costs=costs,
            account_id=account_id,
            core_api_client=core_api_client,
            cost_explorer_client=cost_explorer_client,
        )

    @staticmethod
    def _get_interval_to_process(nominal_date: date) -> Tuple[str, str]:
        nominal_date_first_day_of_month = date(year=nominal_date.year, month=nominal_date.month, day=1)
        if nominal_date.day in [
            1,
            2,
            7,
            14,
        ]:  # reprocess previous month after 2/7/14 days to detect cost updates by AWS
            start = (nominal_date_first_day_of_month + relativedelta(months=-1)).strftime(DATE_FORMAT)
        else:
            start = nominal_date_first_day_of_month.strftime(DATE_FORMAT)
        end = nominal_date.strftime(DATE_FORMAT)  # hint: by aws design this specific date is excluded from report
        return end, start

    @classmethod
    def update_account_billing(
        cls,
        costs: List[ResultByTimeTypeDef],
        account_id: str,
        core_api_client: CoreApiClient,
        cost_explorer_client: CostExplorerClient,
    ) -> None:
        """Update account with billing information."""
        cost_history, estimated_cost = cls.get_history_and_estimated_cost(
            results=costs, account_id=account_id, core_api_client=core_api_client
        )
        forecasted_cost = cls.get_cost_forecast(
            cost_explorer=cost_explorer_client,
            account_id=account_id,
        )

        updated_account = core_api_client.update_account_billing(
            account_id=AccountId(account_id),
            cost_history=cost_history,
            estimated_cost=estimated_cost,
            forecasted_cost=forecasted_cost,
        )
        LOG.info(
            f"Updated account {account_id} to {cost_history=}, {estimated_cost=}, {forecasted_cost=} "
            f"resulting in {updated_account} "
        )

    @staticmethod
    def get_history_and_estimated_cost(
        results: List[ResultByTimeTypeDef], account_id: str, core_api_client: CoreApiClient
    ) -> Tuple[Optional[dict[str, float]], Optional[float]]:
        """Calculate next estimated cost and updated cost history."""
        today = datetime.now().strftime("%Y-%m-01")  # cost explorer always sets day=01

        estimated_cost = None
        cost_history = core_api_client.get_account(account_id=AccountId(account_id)).cost_history

        for result_by_time in results:
            start = date.fromisoformat(result_by_time["TimePeriod"]["Start"])
            index = start.strftime("%Y-%m")
            cost_start_time = start.strftime("%Y-%m-01")
            amount: float = 0
            for group in result_by_time["Groups"]:
                amount = amount + float(group["Metrics"]["AmortizedCost"]["Amount"])
            if cost_start_time == today:
                estimated_cost = amount
            else:
                if cost_history:
                    cost_history[index] = amount
                else:
                    cost_history = {index: amount}

        return cost_history, estimated_cost

    @staticmethod
    def write_costs_to_s3(
        results: List[ResultByTimeTypeDef], folder: str, account_id: str, s3_client: S3Client
    ) -> None:
        """Write cost CSV to S3."""
        for result_by_time in results:
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(
                [
                    "TimePeriod",
                    "LinkedAccount",
                    "Service",
                    "Amount",
                    "Unit",
                    "Estimated",
                ]
            )
            time_start = date.fromisoformat(result_by_time["TimePeriod"]["Start"])
            year = time_start.strftime("%Y")
            month = time_start.strftime("%m")
            for group in result_by_time["Groups"]:
                amount = group["Metrics"]["AmortizedCost"]["Amount"]
                unit = group["Metrics"]["AmortizedCost"]["Unit"]
                row = [
                    f"{year}-{month}",
                    group["Keys"][0],
                    group["Keys"][1],
                    amount,
                    unit,
                    str(result_by_time["Estimated"]),
                ]
                writer.writerow(row)

            file_key = f"{folder}/{year}_{month}_{account_id}.csv"
            s3_client.put_object(
                Bucket=os.environ["BUCKET"],
                Body=output.getvalue().encode(),
                Key=file_key,
                ServerSideEncryption="aws:kms",
            )
            LOG.info(f"Wrote {os.environ['BUCKET']}/{file_key}")

    @classmethod
    def get_costs(cls, nominal_date: date, cost_explorer: CostExplorerClient) -> List[ResultByTimeTypeDef]:
        """Calculate costs."""
        end, start = cls._get_interval_to_process(nominal_date)

        results: List[ResultByTimeTypeDef] = []
        next_page_token = None
        while True:
            kwargs: Dict[str, Any] = {"NextPageToken": next_page_token} if next_page_token else {}
            data = cost_explorer.get_cost_and_usage(
                TimePeriod={"Start": start, "End": end},
                Granularity="MONTHLY",
                Metrics=["AmortizedCost"],
                GroupBy=[{"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"}, {"Type": "DIMENSION", "Key": "SERVICE"}],
                **kwargs,
            )
            results += data["ResultsByTime"]
            next_page_token = data.get("NextPageToken")
            if not next_page_token:
                return results

    @staticmethod
    def get_cost_forecast(cost_explorer: CostExplorerClient, account_id: str) -> Optional[float]:
        """Fetch cost forecast from cost explorer."""
        start = datetime.now()  # the day of month does not matter as long as start >= nowget_cost_and_usage()
        end = (start + relativedelta(months=1)).replace(day=1)
        try:
            data = cost_explorer.get_cost_forecast(
                TimePeriod={"Start": start.strftime(DATE_FORMAT), "End": end.strftime(DATE_FORMAT)},
                Granularity="MONTHLY",
                Metric="AMORTIZED_COST",
            )
            return float(data["Total"]["Amount"])
        except cost_explorer.exceptions.DataUnavailableException as error:
            LOG.warning(f"Could not get cost forecast and received following error message: {error}")
            return None
        except Exception:
            LOG.exception(f"Failed to get cost forecast for account {account_id}.")
            raise

    @staticmethod
    def get_nominal_date(event: Dict[str, Any]) -> date:
        """Get nominal date for the event that triggered the lambda."""
        message_attributes = event["Records"][0]["messageAttributes"]
        nominal_date_string = message_attributes["nominal_date"]["stringValue"]
        LOG.debug(f"Nominal date found: {nominal_date_string}. Continuing with this date")
        return date.fromisoformat(nominal_date_string)

    @staticmethod
    # These arguments enable hub-specific overrides
    # pylint: disable=unused-argument
    def _get_cost_explorer(account_id: str, hub: Hub, prefix: str, region: Region) -> CostExplorerClient:
        billing_role = ConfigFileLoader().get_config().account.assumable_aws_role.billing
        role_arn = str(
            Arn.get_role_arn(
                partition=hub.partition,
                account_id=AccountId(account_id),
                path=billing_role.path,
                name=billing_role.name,
            )
        )
        LOG.debug(f"The role arn to assume: {role_arn}")

        sts_client = boto3.client("sts", region_name=region.value)
        response = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=f"export_cost_for_{account_id}")
        session = boto3.session.Session(
            aws_access_key_id=response["Credentials"]["AccessKeyId"],
            aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
            aws_session_token=response["Credentials"]["SessionToken"],
        )

        return session.client("ce", "us-east-1")  # cost explorer is only available in us-east-1


lambda_handler = log_safe()(CostExporter().lambda_handler)
