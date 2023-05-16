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
from argparse import Namespace
from pathlib import Path
from typing import Any
from typing import cast
from typing import Dict

import pytest
import yaml
from cdh_core_api.validate_openapi_spec_test import fixture_generated_openapi_spec  # pylint: disable=W0611

from .render import TemplateRenderer  # noqa: ABS101
from cdh_core.enums.accounts import AccountPurpose  # pylint: disable=C0411


_CDH_DEPLOYMENT_PATH = Path(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "cdh-oss.bmw.cloud")
).resolve()
_CDH_DEPLOYMENT_CONFIG_FILE_PATH = _CDH_DEPLOYMENT_PATH / "cdh-core-config-test-deployment.yaml"
_CDH_DEPLOYMENT_OPENAPI_SPEC_FILE_PATH = _CDH_DEPLOYMENT_PATH / "openapi.yml"


def read_yaml_file(filename: str) -> Dict[str, Any]:
    with open(filename, encoding="utf-8") as file:
        data = yaml.load(file, yaml.CSafeLoader)
        return cast(Dict[str, Any], json.loads(json.dumps(data)))


@pytest.mark.parametrize("account_purpose", ["api", "security", "test", "resources"])
def test_deployment_infrastructure_templates_match(account_purpose: str) -> None:
    """Currently only a subset of AccountPurposes are supported."""
    args = Namespace(
        check_only=True,
        template_directory="cdh-oss.bmw.cloud",
        partition="aws",
        account_purpose=AccountPurpose(account_purpose),
        output_path=None,
    )

    renderer = TemplateRenderer(args)
    renderer.render_terraform_files()
    renderer.render_config_files()


def test_current_test_deployment_openapi_spec_is_checked_in(generated_openapi_spec: Dict[str, Any]) -> None:
    openapi_spec = read_yaml_file(str(_CDH_DEPLOYMENT_OPENAPI_SPEC_FILE_PATH))
    assert openapi_spec == generated_openapi_spec, (
        f"The checked-in OpenAPI spec at {_CDH_DEPLOYMENT_OPENAPI_SPEC_FILE_PATH} is not up-to-date.\n"
        "==========\n"
        "Create a new spec with python src/lambdas/cdh_core_api/cdh_core_api/create_openapi_spec.py "
        "and check the diff.\n"
        "Store the new file with python src/lambdas/cdh_core_api/cdh_core_api/create_openapi_spec.py --store --path "
        f"{_CDH_DEPLOYMENT_OPENAPI_SPEC_FILE_PATH}.\n"
        "=========="
    )
