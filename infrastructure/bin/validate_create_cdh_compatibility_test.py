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
import os
import random
import subprocess
from argparse import Namespace
from pathlib import Path
from typing import Optional

import pytest

from .render import TemplateRenderer  # noqa: ABS101
from cdh_core.enums.accounts import AccountPurpose  # pylint: disable=C0411
from cdh_core.enums.aws import Partition  # pylint: disable=C0411
from cdh_core.enums.environment import Environment  # pylint: disable=C0411
from cdh_core_dev_tools.testing.builder import Builder  # pylint: disable=C0411

# flake8: noqa: B028

CDH_CORE_CONFIG_FILE_PATH = os.environ["CDH_CORE_CONFIG_FILE_PATH"]
_BIN_PATH = os.path.dirname(os.path.realpath(__file__))
expected_cdh_core_config_file_path = Path(
    os.path.join(_BIN_PATH, "create-cdh-compatibility-test-config.yaml")
).resolve()

assert CDH_CORE_CONFIG_FILE_PATH == expected_cdh_core_config_file_path.as_posix()


def _call_shell(cmd: str, cwd: Optional[Path] = None) -> None:
    subprocess.check_call(cmd, shell=True, cwd=cwd)


@pytest.mark.parametrize("account_purpose", ["security", "api", "resources"])
@pytest.mark.parametrize("resource_name_prefix", ["", "cdhxtst"])
def test_deployment_infrastructure_compatible_with_create_cdh(
    account_purpose: str, tmp_path: Path, resource_name_prefix: str
) -> None:
    partition = Partition.default()
    args = Namespace(
        check_only=False,
        partition=partition.value,
        template_directory="cdh-oss.bmw.cloud",
        account_purpose=AccountPurpose(account_purpose),
        output_path=tmp_path,
    )

    renderer = TemplateRenderer(args)
    renderer.render_terraform_files()

    vars_file = os.path.join(tmp_path, f"{account_purpose}.tfvars")
    with open(vars_file, "w", encoding="utf-8") as file:
        file.writelines(
            [
                f'resource_name_prefix      = "{resource_name_prefix}"\n',
                f'environment               = "{random.choice(list(Environment)).value}"\n',
                f'cdh_core_config_file_path = "{os.path.relpath(CDH_CORE_CONFIG_FILE_PATH, tmp_path)}"\n',
                "new_cloudwatch_role       = true\n",
                f'cloudwatch_role_name      = "{Builder.build_random_string()}"\n',
                f'jwt_cookie_name           = "{Builder.build_random_string()}"\n',
                f'authorization_api_url     = "{Builder.build_random_string()}"\n',
                f'users_api_url             = "{Builder.build_random_string()}"\n',
                "trusted_org_ids           = []\n",
                "user_account_ids          = []\n",
            ]
        )

    repo_path = Path(os.path.join(_BIN_PATH, "..", "..")).resolve()
    _call_shell(f"cp {str(repo_path)}/.terraform-version {tmp_path}/.terraform-version")
    _call_shell("terraform init -reconfigure", cwd=tmp_path)
    _call_shell(f"terraform plan -var-file={vars_file} -refresh=false", cwd=tmp_path)
