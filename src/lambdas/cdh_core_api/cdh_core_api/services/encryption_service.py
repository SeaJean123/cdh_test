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
from typing import Callable
from typing import Protocol

from cdh_core_api.config import Config

from cdh_core.aws_clients.ssm_client import SsmClient


class Cryptographer(Protocol):
    """Protocol for any cryptographic implementation. See https://www.python.org/dev/peps/pep-0544/."""

    def encrypt(self, plain: bytes) -> bytes:
        """Encrypts a given plain input."""

    def decrypt(self, encrypted: bytes) -> bytes:
        """Decrypts a secret input."""


class EncryptionService:
    """This class can be used to encrypt sensitive information."""

    def __init__(
        self, config: Config, ssm_client: SsmClient, cryptographer_factory: Callable[[str], Cryptographer]
    ) -> None:
        self._config = config
        self._ssm = ssm_client
        self._cryptographer_factory = cryptographer_factory

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def _get_cryptographer(self) -> Cryptographer:
        encryption_key = self._ssm.get_parameter(name=self._config.encryption_key, decryption=True)
        return self._cryptographer_factory(encryption_key)

    def encrypt(self, plain: str) -> str:
        """Encrypt a plain string input."""
        cryptographer = self._get_cryptographer()
        try:
            return cryptographer.encrypt(plain.encode("utf-8")).decode("utf-8")
        except Exception as error:  # noqa: E722 (bare-except)
            raise CryptographyError from error

    def decrypt(self, encrypted: str) -> str:
        """Decrypt a secret string input."""
        cryptographer = self._get_cryptographer()
        try:
            return cryptographer.decrypt(encrypted.encode("utf-8")).decode("utf-8")
        except Exception as error:  # noqa: E722 (bare-except)
            raise CryptographyError from error


class CryptographyError(Exception):
    """Raised if the underlying Cryptographer encountered an error."""
