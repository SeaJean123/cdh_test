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
# pylint: disable=unused-argument
from typing import Optional
from uuid import uuid4

import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.config.config_file_test import PARTITION_AWS
from cdh_core.config.config_file_test import REGION_VALUE_CN_NORTH_1
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn import build_arn_string
from cdh_core.entities.arn import MalformedArnException
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def build_arn(
    service: str,
    resource: Optional[str] = None,
    *,
    account_id: Optional[str] = None,  # we accept str here on purpose to facilitate using this function
    region: Optional[Region] = None,
    partition: Optional[Partition] = None,
) -> Arn:
    default_resource_name = Builder.build_random_string(4)
    resource = resource or (default_resource_name if service != "states" else f"stateMachine:{default_resource_name}")

    account_id = account_id if account_id is not None else build_account_id()
    if region and partition:
        assert region.partition == partition
    partition = partition or (region.partition if region is not None else build_partition())
    region = region or build_region(partition)
    if service == "lambda":
        return Arn(f"arn:{partition.value}:{service}:{region.value}:{account_id}:function:{resource}")
    if service == "states":
        return Arn(f"arn:{partition.value}:{service}:{region.value}:{account_id}:{resource}")
    if service == "sts":
        return Arn(f"arn:{partition.value}:{service}::{account_id}:{resource}")
    if service == "s3":
        return Arn(f"arn:{partition.value}:{service}:::{resource}")
    if service == "iam":
        return Arn(f"arn:{partition.value}:{service}::{account_id}:role/{resource}")
    return Arn(f"arn:{partition.value}:{service}:{region.value}:{account_id}:{resource}")


def build_role_arn(
    name: Optional[str] = None, account_id: Optional[AccountId] = None, partition: Optional[Partition] = None
) -> Arn:
    return build_arn(service="iam", resource=name, account_id=account_id, partition=partition)


def build_sts_assumed_role_arn(account_id: Optional[AccountId] = None, role_name: Optional[str] = None) -> Arn:
    role_name = role_name or Builder.build_random_string()
    return build_arn(
        service="sts",
        resource=f"assumed-role/{role_name}/session-name",
        account_id=account_id,
    )


def build_kms_key_arn(
    account_id: Optional[str] = None,
    region: Optional[Region] = None,
    key_id: Optional[str] = None,
) -> Arn:
    account_id = account_id or build_account_id()
    region = region or build_region()
    return Arn(f"arn:{build_partition(region).value}:kms:{region.value}:{account_id}:key/{key_id or uuid4()}")


class TestArnValidator:
    partition = build_partition()

    def test_regular_arn(self) -> None:
        arn = Arn(
            f"arn:{self.partition.value}:sts::some-cdh-account-number:assumed-role/SomeRole/some-caller-assume-role-id"
        )
        assert arn.service == "sts"
        assert arn.account_id == "some-cdh-account-number"
        assert arn.partition == self.partition

    def test_lambda_arn(self) -> None:
        arn = Arn(f"arn:{self.partition.value}:lambda:some-region:some-cdh-account-number:function:some-lambda-name")
        assert arn.service == "lambda"
        assert arn.account_id == "some-cdh-account-number"
        assert arn.partition == self.partition
        assert arn.identifier == "function:some-lambda-name"

    def test_lambda_arn_with_version(self) -> None:
        arn = Arn(
            f"arn:{self.partition.value}:lambda:some-region:some-cdh-account-number:function:some-lambda-name:version"
        )
        assert arn.service == "lambda"
        assert arn.account_id == "some-cdh-account-number"

    def test_state_machine_arn(self) -> None:
        arn = Arn(
            f"arn:{self.partition.value}:"
            "states:some-region:some-cdh-account-number:stateMachine:some-state-machine-name"
        )
        assert arn.service == "states"
        assert arn.account_id == "some-cdh-account-number"

    def test_state_machine_execution_arn(self) -> None:
        arn = Arn(
            f"arn:{self.partition.value}:states:some-region:some-cdh-account-number:"
            "execution:some-state-machine-name:some-execution-name"
        )
        assert arn.service == "states"
        assert arn.account_id == "some-cdh-account-number"

    @pytest.mark.parametrize(
        "arn",
        [
            *[f"arn:{partition}:this:is:an:invalid:arn:because:it_has_too_many_fields" for partition in Partition],
            "invalid",
            "arn:nosuchpartition:lambda:some-region:some-cdh-account-number:function:some-lambda-name",
        ],
    )
    def test_invalid_arn(self, arn: str) -> None:
        with pytest.raises(MalformedArnException):
            Arn(arn)


@pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
class TestArnString:
    account_id = build_account_id()

    def test_region(self, mock_config_file: ConfigFile) -> None:
        region = build_region()
        arn = build_arn_string(
            service="myservice",
            region=region,
            account=self.account_id,
            resource="myresource",
            partition=region.partition,
        )
        assert arn == f"arn:{region.partition.value}:myservice:{region.value}:{self.account_id}:myresource"

    def test_without_region(self, mock_config_file: ConfigFile) -> None:
        partition = build_partition()
        arn = build_arn_string(
            service="myservice",
            region=None,
            account=self.account_id,
            resource="myresource",
            partition=partition,
        )
        assert arn == f"arn:{partition.value}:myservice::{self.account_id}:myresource"

    def test_region_and_partition_consistency(self, mock_config_file: ConfigFile) -> None:
        with pytest.raises(AssertionError):
            build_arn_string(
                service="myservice",
                region=Region(REGION_VALUE_CN_NORTH_1),
                account=self.account_id,
                resource="myresource",
                partition=Partition(PARTITION_AWS),
            )


class TestAssumedRoleName:
    def test_valid_assumed_role_name(self) -> None:
        arn = Arn("arn:aws:sts::some-cdh-account-number:assumed-role/SomeRole/session-name")
        assert arn.get_assumed_role_name() == "SomeRole"

    @pytest.mark.parametrize(
        "arn",
        [
            pytest.param(build_arn(service="not-sts", resource="assumed-role/SomeRole/session-name"), id="not-sts"),
            pytest.param(
                build_arn(service="sts", resource="not-an-assumed-role/SomeRole/session-name"),
                id="not-an-assumed-role",
            ),
            pytest.param(build_arn(service="sts", resource="assumed-role/SomeRole"), id="too-few-components"),
            pytest.param(
                build_arn(service="sts", resource="assumed-role/SomeRole/session-name/for-good-luck"),
                id="too-many-components",
            ),
        ],
    )
    def test_invalid_assumed_role_name(self, arn: Arn) -> None:
        with pytest.raises(ValueError):
            arn.get_assumed_role_name()
