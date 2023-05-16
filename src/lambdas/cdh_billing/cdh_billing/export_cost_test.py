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
from dataclasses import replace
from datetime import date
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest
from cdh_billing.export_cost import CostExporter
from cdh_billing.export_cost import lambda_handler
from freezegun import freeze_time
from mypy_boto3_ce.type_defs import ResultByTimeTypeDef

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id_test import build_account_id

BUCKET_NAME = "my_test_bucket"
NOW = datetime.datetime(year=2020, month=3, day=10, hour=14, minute=0, second=0)


def build_fake_event(str_date: str, account: Account) -> Dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": "11d6ee51-4cc7-4302-9e22-7cd8afdaadf5",
                "body": json.dumps({"account_id": account.id, "hub": account.hub.value}),
                "messageAttributes": {"nominal_date": {"stringValue": str_date}},
            }
        ]
    }


def build_fake_cost_explorer_result(account_id: str) -> List[ResultByTimeTypeDef]:
    return [
        {
            "TimePeriod": {"Start": "2020-01-01", "End": "2020-02-01"},
            "Total": {},
            "Groups": [
                {
                    "Keys": [account_id, "AWS CloudTrail"],
                    "Metrics": {"AmortizedCost": {"Amount": "0.1", "Unit": "USD"}},
                },
                {
                    "Keys": [account_id, "AWS Cost Explorer"],
                    "Metrics": {"AmortizedCost": {"Amount": "1.1", "Unit": "USD"}},
                },
            ],
            "Estimated": False,
        },
        {
            "TimePeriod": {"Start": "2020-02-01", "End": "2020-03-01"},
            "Total": {},
            "Groups": [
                {
                    "Keys": [account_id, "AWS CloudTrail"],
                    "Metrics": {"AmortizedCost": {"Amount": "0.5", "Unit": "USD"}},
                },
                {
                    "Keys": [account_id, "AWS Cost Explorer"],
                    "Metrics": {"AmortizedCost": {"Amount": "2.25", "Unit": "USD"}},
                },
            ],
            "Estimated": True,
        },
        {
            "TimePeriod": {"Start": "2020-03-01", "End": "2020-03-03"},
            "Total": {},
            "Groups": [
                {
                    "Keys": [account_id, "AWS CloudTrail"],
                    "Metrics": {"AmortizedCost": {"Amount": "0.05", "Unit": "USD"}},
                },
                {
                    "Keys": [account_id, "AWS Cost Explorer"],
                    "Metrics": {"AmortizedCost": {"Amount": "0.25", "Unit": "USD"}},
                },
            ],
            "Estimated": True,
        },
    ]


@freeze_time(NOW)
class TestSubfunctions:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_s3: Any) -> None:  # pylint: disable=unused-argument
        self.account = build_account()
        self.s3_client = boto3.client("s3")
        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME, CreateBucketConfiguration={"LocationConstraint": build_region().value}
        )

    def test_get_history_and_estimated_cost(self) -> None:
        account_id = build_account_id()
        fake_results = build_fake_cost_explorer_result(account_id=account_id)
        core_api_client = Mock(CoreApiClient)
        account = build_account(account_id=account_id)
        core_api_client.get_account.return_value = account

        cost_history, estimated_cost = CostExporter.get_history_and_estimated_cost(
            results=fake_results, account_id=account_id, core_api_client=core_api_client
        )

        assert cost_history == {"2020-01": float(1.1 + 0.1), "2020-02": 2.75}
        assert estimated_cost == 0.3

    def test_write_costs_to_s3(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("BUCKET", BUCKET_NAME)
        account_id = build_account_id()
        fake_results = build_fake_cost_explorer_result(account_id)
        CostExporter.write_costs_to_s3(
            results=fake_results, folder=f"MonthlyReports/{account_id}", account_id=account_id, s3_client=self.s3_client
        )

        month_01_csv = self.s3_client.get_object(
            Bucket=BUCKET_NAME, Key=f"MonthlyReports/{account_id}/2020_01_{account_id}.csv"
        )
        month_02_csv = self.s3_client.get_object(
            Bucket=BUCKET_NAME, Key=f"MonthlyReports/{account_id}/2020_02_{account_id}.csv"
        )
        month_03_csv = self.s3_client.get_object(
            Bucket=BUCKET_NAME, Key=f"MonthlyReports/{account_id}/2020_03_{account_id}.csv"
        )

        assert month_01_csv["Body"].read().decode("utf-8") == (
            "TimePeriod,LinkedAccount,Service,Amount,Unit,Estimated\r\n"
            f"2020-01,{account_id},AWS CloudTrail,0.1,USD,False\r\n"
            f"2020-01,{account_id},AWS Cost Explorer,1.1,USD,False\r\n"
        )
        assert month_02_csv["Body"].read().decode("utf-8") == (
            "TimePeriod,LinkedAccount,Service,Amount,Unit,Estimated\r\n"
            f"2020-02,{account_id},AWS CloudTrail,0.5,USD,True\r\n"
            f"2020-02,{account_id},AWS Cost Explorer,2.25,USD,True\r\n"
        )
        assert month_03_csv["Body"].read().decode("utf-8") == (
            "TimePeriod,LinkedAccount,Service,Amount,Unit,Estimated\r\n"
            f"2020-03,{account_id},AWS CloudTrail,0.05,USD,True\r\n"
            f"2020-03,{account_id},AWS Cost Explorer,0.25,USD,True\r\n"
        )

    def test_get_costs(self) -> None:
        cost_explorer = Mock()
        fake_result = build_fake_cost_explorer_result(build_account_id())
        cost_explorer.get_cost_and_usage.return_value = {"ResultsByTime": fake_result, "NextPageToken": ""}
        nominal_date = date.fromisoformat("2010-01-01")

        expected_start = "2009-12-01"
        expected_end = "2010-01-01"

        result = CostExporter.get_costs(nominal_date=nominal_date, cost_explorer=cost_explorer)

        assert result == fake_result
        cost_explorer.get_cost_and_usage.assert_called_once()
        assert cost_explorer.get_cost_and_usage.call_args[1]["TimePeriod"] == {
            "Start": expected_start,
            "End": expected_end,
        }

    def test_get_nominal_date(self) -> None:
        str_date = "2010-01-01"

        result = CostExporter.get_nominal_date(build_fake_event(str_date=str_date, account=build_account()))

        assert result == date.fromisoformat("2010-01-01")

    def test_get_cost_forecast_returns_none(self) -> None:
        cost_explorer = Mock()
        exception = boto3.client("ce").exceptions.DataUnavailableException
        cost_explorer.exceptions.DataUnavailableException = exception
        cost_explorer.get_cost_forecast.side_effect = [
            exception(
                operation_name="foo",
                error_response={
                    "Error": {"Code": "DataUnavailableException", "Message": "Cannot pull data for cost forecast"}
                },
            )
        ]

        # pylint: disable=protected-access
        result = CostExporter.get_cost_forecast(cost_explorer=cost_explorer, account_id="1234")

        assert result is None


@freeze_time(NOW)
@pytest.mark.usefixtures("mock_sts")
class TestCostExporter:
    @pytest.fixture(autouse=True)
    def setup_environment_variables(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("CORE_API_URL", "https://mocked_core_api_url.com")
        monkeypatch.setenv("RESOURCE_NAME_PREFIX", "my-prefix")

    @pytest.fixture(autouse=True)
    def service_setup(self, mock_s3: Any) -> None:  # pylint: disable=unused-argument
        self.account = build_account()

        self.s3_client = boto3.client("s3")
        self.s3_client.create_bucket(
            Bucket=BUCKET_NAME, CreateBucketConfiguration={"LocationConstraint": build_region().value}
        )

        self.core_api_client = Mock(CoreApiClient)
        self.core_api_client.get_account.return_value = self.account
        self.core_api_client.update_account_billing.side_effect = self._update_account

    def _update_account(
        self,
        account_id: str,
        cost_history: Optional[Dict[str, float]] = None,
        estimated_cost: Optional[float] = None,
        forecasted_cost: Optional[float] = None,
    ) -> Account:
        assert account_id == self.account.id

        updated_cost_history = self.account.cost_history if cost_history is None else cost_history
        updated_estimated_cost = self.account.estimated_cost if estimated_cost is None else estimated_cost
        updated_forecasted_cost = self.account.forecasted_cost if forecasted_cost is None else forecasted_cost
        updated_account = replace(
            self.account,
            cost_history=updated_cost_history,
            estimated_cost=updated_estimated_cost,
            forecasted_cost=updated_forecasted_cost,
        )
        self.account = updated_account

        return self.account

    @patch.object(CostExporter, "get_costs")
    @patch.object(CostExporter, "get_cost_forecast")
    @patch.object(CoreApiClient, "get_core_api_client")
    def test_lambda_handler_end_to_end(
        self,
        get_core_api_client: Mock,
        get_cost_forecast: Mock,
        get_costs: Mock,
        monkeypatch: Any,
    ) -> None:
        monkeypatch.setenv("BUCKET", BUCKET_NAME)
        event = build_fake_event("2020-03-01", account=self.account)
        get_costs.return_value = build_fake_cost_explorer_result(account_id=self.account.id)
        get_cost_forecast.return_value = 10.1
        account_id = self.account.id

        get_core_api_client.return_value = self.core_api_client

        lambda_handler(event, LambdaContext())
        month_01_csv = self.s3_client.get_object(
            Bucket=BUCKET_NAME, Key=f"MonthlyReports/{account_id}/2020_01_{account_id}.csv"
        )
        month_02_csv = self.s3_client.get_object(
            Bucket=BUCKET_NAME, Key=f"MonthlyReports/{account_id}/2020_02_{account_id}.csv"
        )
        month_03_csv = self.s3_client.get_object(
            Bucket=BUCKET_NAME, Key=f"MonthlyReports/{account_id}/2020_03_{account_id}.csv"
        )

        assert month_01_csv["Body"].read().decode("utf-8") == (
            "TimePeriod,LinkedAccount,Service,Amount,Unit,Estimated\r\n"
            f"2020-01,{account_id},AWS CloudTrail,0.1,USD,False\r\n"
            f"2020-01,{account_id},AWS Cost Explorer,1.1,USD,False\r\n"
        )
        assert month_02_csv["Body"].read().decode("utf-8") == (
            "TimePeriod,LinkedAccount,Service,Amount,Unit,Estimated\r\n"
            f"2020-02,{account_id},AWS CloudTrail,0.5,USD,True\r\n"
            f"2020-02,{account_id},AWS Cost Explorer,2.25,USD,True\r\n"
        )
        assert month_03_csv["Body"].read().decode("utf-8") == (
            "TimePeriod,LinkedAccount,Service,Amount,Unit,Estimated\r\n"
            f"2020-03,{account_id},AWS CloudTrail,0.05,USD,True\r\n"
            f"2020-03,{account_id},AWS Cost Explorer,0.25,USD,True\r\n"
        )
        assert self.account.id == account_id
        assert self.account.cost_history
        assert self.account.cost_history["2020-01"] == float(1.1 + 0.1)
        assert self.account.cost_history["2020-02"] == 2.75
        assert "2020-03" not in self.account.cost_history
        assert self.account.estimated_cost == 0.3
        assert self.account.forecasted_cost == get_cost_forecast.return_value
