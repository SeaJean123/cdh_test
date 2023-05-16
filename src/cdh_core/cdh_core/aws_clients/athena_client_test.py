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
from unittest.mock import Mock

import boto3
import pytest

from cdh_core.aws_clients.athena_client import AthenaClient
from cdh_core.aws_clients.athena_client import AthenaWorkgroup
from cdh_core.aws_clients.kms_client import KmsKey
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class TestAthenaWorkgroup:
    def test_arn(self) -> None:
        name = Builder.build_random_string()
        account_id = build_account_id()
        partition = build_partition()
        assert AthenaWorkgroup(name=name, account_id=account_id, partition=partition).arn == Arn(
            f"arn:{partition.value}:athena:*:{account_id}:workgroup/{name}"
        )


class TestAthenaClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_athena: Any) -> None:  # pylint: disable=unused-argument
        self._work_group_name = Builder.build_random_string()
        self._output_location = f"s3://athena-query-results/{Builder.build_random_string()}/"
        self._kms_arn = build_arn("kms")
        self._boto_athena_client = boto3.client("athena", region_name=build_region().value)
        self._athena_client = AthenaClient(self._boto_athena_client)

    def test_create_work_group_moto(self) -> None:
        with self._athena_client.create_work_group_transaction(
            self._work_group_name, self._output_location, self._kms_arn
        ):
            pass
        result = self._boto_athena_client.get_work_group(WorkGroup=self._work_group_name)
        assert self._work_group_name == result["WorkGroup"]["Name"]
        assert self._output_location == result["WorkGroup"]["Configuration"]["ResultConfiguration"]["OutputLocation"]
        assert (
            str(self._kms_arn)
            == result["WorkGroup"]["Configuration"]["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"]
        )

    def test_rollback_creation_of_work_group(self) -> None:
        boto_athena_client = Mock()  # moto does not yet support delete
        client = AthenaClient(boto_athena_client)

        with pytest.raises(ValueError):
            with client.create_work_group_transaction(self._work_group_name, self._output_location, self._kms_arn):
                raise ValueError()

        boto_athena_client.delete_work_group.assert_called_once_with(
            WorkGroup=self._work_group_name, RecursiveDeleteOption=True
        )

    def test_delete_work_group(self) -> None:
        boto_athena_client = Mock()  # moto does not yet support delete
        client = AthenaClient(boto_athena_client)

        client.delete_workgroup(name=self._work_group_name)
        boto_athena_client.delete_work_group.assert_called_once_with(
            WorkGroup=self._work_group_name, RecursiveDeleteOption=True
        )

    def test_update_work_group(self) -> None:
        boto_athena_client = Mock()  # moto does not yet support update
        client = AthenaClient(boto_athena_client)
        new_kms_key_arn = build_arn("kmw", resource=f"{Builder.build_random_string()}/{Builder.build_random_string()}")
        client.update_kms_key_for_workgroup(
            workgroup_name=self._work_group_name, kms_key=KmsKey.parse_from_arn(new_kms_key_arn)
        )
        boto_athena_client.update_work_group.assert_called_once_with(
            WorkGroup=self._work_group_name,
            Description="string",
            ConfigurationUpdates={
                "ResultConfigurationUpdates": {
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_KMS",
                        "KmsKey": str(new_kms_key_arn),
                    }
                }
            },
        )
