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
from functools import lru_cache
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config

if TYPE_CHECKING:
    from mypy_boto3_ssm import SSMClient
else:
    SSMClient = object


class SsmClient:
    """Abstracts the boto3 SSM client."""

    def __init__(self, boto_ssm_client: SSMClient):
        self._client = boto_ssm_client

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def get_parameter(self, name: str, decryption: bool = True) -> str:
        """Get the value of a parameter."""
        return str(self._client.get_parameter(Name=name, WithDecryption=decryption)["Parameter"]["Value"])

    @staticmethod
    def get_local_ssm_client() -> "SsmClient":
        """Return a basic SSM client."""
        return SsmClient(boto3.client("ssm", config=Config(connect_timeout=4, read_timeout=10)))

    @staticmethod
    def get_thread_safe_client() -> "SsmClient":
        """Return a SSM client with a session and therefore thread safe."""
        thread_safe_session = boto3.session.Session()
        return SsmClient(thread_safe_session.client("ssm", config=Config(connect_timeout=4, read_timeout=10)))
