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
import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.credential import Credential
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core_dev_tools.testing.builder import Builder


@pytest.mark.parametrize(
    "mock_config_file",
    [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS],
    indirect=True,
)
class TestCredential:
    def setup_method(self) -> None:
        self.access_key_id = Builder.build_random_string()
        self.secret_access_key = Builder.build_random_string()

    def test_region(self, mock_config_file: ConfigFile) -> None:
        for partition in mock_config_file.partition.instances.values():
            credential = Credential(
                access_key_id=self.access_key_id,
                secret_access_key=self.secret_access_key,
                partition=Partition(partition.value),
            )
            assert credential.region is Region.preferred(credential.partition)

    def test_tf_env_format(self, mock_config_file: ConfigFile) -> None:
        for partition in mock_config_file.partition.instances.values():
            credential = Credential(
                access_key_id=self.access_key_id,
                secret_access_key=self.secret_access_key,
                partition=Partition(partition.value),
            )
            if partition.value == "aws":
                assert credential.tf_env_format == {
                    "TF_VAR_tf_access_key": self.access_key_id,
                    "TF_VAR_tf_secret_key": self.secret_access_key,
                }
            elif partition.value == "aws-cn":
                assert credential.tf_env_format == {
                    "TF_VAR_cn_tf_access_key": self.access_key_id,
                    "TF_VAR_cn_tf_secret_key": self.secret_access_key,
                }
