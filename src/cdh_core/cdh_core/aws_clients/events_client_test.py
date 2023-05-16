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
from typing import Any

import boto3
import pytest
from asserts import assert_count_equal

from cdh_core.aws_clients.events_client import EventsClient
from cdh_core.aws_clients.events_client import LambdaTarget
from cdh_core.aws_clients.events_client import RuleNotFound
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.aws_test import build_region
from cdh_core_dev_tools.testing.builder import Builder


class TestEventsClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_events: Any) -> None:  # pylint: disable=unused-argument
        self.boto_client = boto3.client("events", region_name=build_region().value)
        self.client = EventsClient(self.boto_client)

    def check_rule_exists(self, name: str) -> bool:
        all_rules = self.boto_client.list_rules()["Rules"]
        return any(item["Name"] == name for item in all_rules)

    def test_get_rule_name_from_arn(self) -> None:
        rule_arn = build_arn(service="events", resource="rule/of3")
        assert EventsClient.get_rule_name_from_arn(rule_arn) == "of3"

    def test_get_rule_name_from_arn_error(self) -> None:
        rule_arn = build_arn(service="events", resource=Builder.build_random_string())
        with pytest.raises(ValueError):
            EventsClient.get_rule_name_from_arn(rule_arn)

    def test_create_periodic_rule(self) -> None:
        rule_name = Builder.build_random_string()
        arn = self.client.create_or_update_periodic_rule(rule_name, 60)
        all_rules = self.boto_client.list_rules()["Rules"]
        assert len(all_rules) == 1
        assert all_rules[0]["Arn"] == str(arn)
        assert all_rules[0]["Name"] == rule_name
        assert all_rules[0]["ScheduleExpression"] == "rate(60 minutes)"

    def test_add_target(self) -> None:
        rule_name = Builder.build_random_string()
        target = LambdaTarget(target_id="123", lambda_arn=build_arn("lambda"), input={"foo": "bar"})
        arn = self.client.create_or_update_periodic_rule(rule_name, 60)

        self.client.add_target(arn, target)

        all_targets = self.client.list_targets(rule_name)
        assert len(all_targets) == 1
        assert all_targets[0]["Arn"] == str(target.lambda_arn)
        assert all_targets[0]["Input"] == '{"foo": "bar"}'
        assert self.boto_client.list_rule_names_by_target(
            TargetArn=str(target.lambda_arn),
        )[
            "RuleNames"
        ] == [rule_name]

    def test_add_target_rule_does_not_exist(self) -> None:
        target = LambdaTarget(target_id="123", lambda_arn=build_arn("lambda"), input={"foo": "bar"})
        with pytest.raises(RuleNotFound):
            self.client.add_target(rule_arn=build_arn(service="events", resource="rule/myrule"), target=target)

    def test_list_multiple_targets(self) -> None:
        rule_name = Builder.build_random_string()
        target1 = LambdaTarget(target_id="1", lambda_arn=build_arn("lambda"), input={"foo": "foo"})
        target2 = LambdaTarget(target_id="2", lambda_arn=build_arn("lambda"), input={"bar": "bar"})
        arn = self.client.create_or_update_periodic_rule(rule_name, 60)
        self.client.add_target(arn, target1)
        self.client.add_target(arn, target2)

        all_targets = self.client.list_targets(rule_name)
        assert_count_equal(all_targets, [target1.to_dict(), target2.to_dict()])
        assert (
            self.boto_client.list_rule_names_by_target(
                TargetArn=str(target.lambda_arn),
            )["RuleNames"]
            == [rule_name]
            for target in [target1, target2]
        )

    def test_delete_rule(self) -> None:
        rule_name = Builder.build_random_string()
        arn = self.client.create_or_update_periodic_rule(rule_name, 60)
        assert self.check_rule_exists(rule_name)
        self.client.delete_rule(arn)
        assert not self.check_rule_exists(rule_name)

    def test_delete_rule_does_not_exist(self) -> None:
        with pytest.raises(RuleNotFound):
            self.client.delete_rule(build_arn(service="events", resource="rule/myrule"))

    def test_delete_rule_with_targets(self) -> None:
        rule_name = Builder.build_random_string()
        arn = self.client.create_or_update_periodic_rule(rule_name, 60)
        target = LambdaTarget(target_id="123", lambda_arn=build_arn("lambda"), input={"foo": "bar"})
        self.client.add_target(rule_arn=arn, target=target)

        self.client.delete_rule(arn)
        assert not self.check_rule_exists(rule_name)
        assert (
            len(
                self.boto_client.list_rule_names_by_target(
                    TargetArn=str(target.lambda_arn),
                )["RuleNames"]
            )
            == 0
        )
