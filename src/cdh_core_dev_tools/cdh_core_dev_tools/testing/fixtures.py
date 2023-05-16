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
# pylint: disable=redefined-outer-name
import random
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Generator
from typing import Optional
from typing import Tuple

import moto
import pytest
from aws_xray_sdk.core import xray_recorder
from freezegun import freeze_time

from cdh_core_dev_tools.testing.builder import Builder


@pytest.fixture()
def time_travel() -> Generator[None, None, None]:
    """Freeze the time using a random timestamp."""
    date_time = datetime.fromtimestamp(random.randint(1, 10**9))
    with freeze_time(date_time):
        yield


@pytest.fixture(autouse=True, name="resource_name_prefix")
def fixture_resource_name_prefix(monkeypatch: Any) -> str:
    """Create a random resource prefix and set it in the environment."""
    prefix = Builder.build_resource_name_prefix()
    monkeypatch.setenv("RESOURCE_NAME_PREFIX", prefix)
    return prefix


# The list of variables was taken from
# https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
AWS_ENVIRONMENT_VARIABLES = {
    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "AWS_SESSION_TOKEN": "",
    "AWS_DEFAULT_REGION": "eu-central-1",
    "AWS_DEFAULT_OUTPUT": "text",
    "AWS_PROFILE": "default",
    "AWS_ROLE_SESSION_NAME": "test-session",
    "AWS_CA_BUNDLE": "",
    # AWS_SHARED_CREDENTIALS_FILE and AWS_CONFIG_FILE are set below
}


@pytest.fixture(autouse=True, name="aws_config")
def fixture_aws_config(tmp_path: Path) -> Optional[Tuple[Path, Path]]:
    """Offers AWS config file as a fixture."""
    config_file = tmp_path / "config"
    credentials_file = tmp_path / "credentials"

    with open(config_file, "w", encoding="utf-8") as file:
        file.write("[default]")
    with open(credentials_file, "w", encoding="utf-8") as file:
        file.write("[default]\naws_access_key_id=AKIA123\naws_secret_access_key=xyz")

    return config_file, credentials_file


@pytest.fixture(autouse=True)
def aws_environment_variables(monkeypatch: Any, aws_config: Tuple[Path, Path]) -> None:
    """Offers AWS environment variables as a fixture."""
    for key, value in AWS_ENVIRONMENT_VARIABLES.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(aws_config[0]))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(aws_config[1]))


@pytest.fixture()
def mock_athena() -> Generator[None, None, None]:
    """Offers the moto athena mock as fixture."""
    with moto.mock_athena():
        yield


@pytest.fixture()
def mock_iam() -> Generator[None, None, None]:
    """Offers the moto iam mock as fixture."""
    with moto.mock_iam():
        yield


@pytest.fixture()
def mock_cloudformation() -> Generator[None, None, None]:
    """Offers the moto cloud formation mock as fixture."""
    with moto.mock_cloudformation():
        yield


@pytest.fixture()
def mock_glue() -> Generator[None, None, None]:
    """Offers the moto glue mock as fixture."""
    with moto.mock_glue():
        yield


@pytest.fixture()
def mock_kms() -> Generator[None, None, None]:
    """Offers the moto kms mock as fixture."""
    with moto.mock_kms():
        yield


@pytest.fixture()
def mock_logs() -> Generator[None, None, None]:
    """Offers the moto logs mock as fixture."""
    with moto.mock_logs():
        yield


@pytest.fixture()
def mock_ram() -> Generator[None, None, None]:
    """Offers the moto ses mock as fixture."""
    with moto.mock_ram():
        yield


@pytest.fixture()
def mock_ses() -> Generator[None, None, None]:
    """Offers the moto ses mock as fixture."""
    with moto.mock_ses():
        yield


@pytest.fixture()
def mock_sns() -> Generator[None, None, None]:
    """Offers the moto sns mock as fixture."""
    with moto.mock_sns():
        yield


@pytest.fixture()
def mock_stepfunctions() -> Generator[None, None, None]:
    """Offers the moto step functions mock as fixture."""
    with moto.mock_stepfunctions():
        yield


@pytest.fixture()
def mock_sqs() -> Generator[None, None, None]:
    """Offers the moto sqs mock as fixture."""
    with moto.mock_sqs():
        yield


@pytest.fixture()
def mock_events() -> Generator[None, None, None]:
    """Offers the moto events mock as fixture."""
    with moto.mock_events():
        yield


@pytest.fixture()
def mock_s3() -> Generator[None, None, None]:
    """Offers the moto s3 mock as fixture."""
    with moto.mock_s3():
        yield


@pytest.fixture()
def mock_dynamodb() -> Generator[None, None, None]:
    """Offers the moto dynamodb mock as fixture."""
    with moto.mock_dynamodb():
        yield


@pytest.fixture()
def mock_xray() -> Generator[None, None, None]:
    """Offers the moto xray mock as fixture."""
    with moto.mock_xray():
        xray_recorder.begin_segment(name=Builder.build_random_string())
        yield
        xray_recorder.end_segment()


@pytest.fixture()
def mock_sts() -> Generator[Any, None, None]:
    """Offers the moto sts mock as fixture."""
    with moto.mock_sts():
        yield
