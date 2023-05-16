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
from unittest.mock import Mock

import pytest
from cdh_core_api.config_test import build_config
from cdh_core_api.services.encryption_service import Cryptographer
from cdh_core_api.services.encryption_service import CryptographyError
from cdh_core_api.services.encryption_service import EncryptionService
from cryptography.fernet import Fernet

from cdh_core.aws_clients.ssm_client import SsmClient
from cdh_core_dev_tools.testing.builder import Builder


class TestEncryptionService:
    def setup_method(self) -> None:
        self.ssm = Mock(SsmClient)
        self.encryption_key = Builder.build_random_string()
        self.encryption_key_param = Builder.build_random_string()
        self.ssm.get_parameter.return_value = self.encryption_key
        self.config = build_config(encryption_key=self.encryption_key_param)
        self.cryptographer = Mock(Cryptographer)
        self.cryptographer_factory = Mock(return_value=self.cryptographer)
        self.encryption_service = EncryptionService(
            config=self.config, ssm_client=self.ssm, cryptographer_factory=self.cryptographer_factory
        )

    def test_encrypt(self) -> None:
        plain = Builder.build_random_string()
        encrypted_value = Builder.build_random_string().encode("utf-8")
        self.cryptographer.encrypt.return_value = encrypted_value

        assert self.encryption_service.encrypt(plain) == encrypted_value.decode("utf-8")
        self.cryptographer.encrypt.assert_called_once_with(plain.encode("utf-8"))
        self.cryptographer_factory.assert_called_once_with(self.encryption_key)
        self.ssm.get_parameter.assert_called_once_with(name=self.encryption_key_param, decryption=True)

    def test_encrypt_fail(self) -> None:
        self.cryptographer.encrypt.side_effect = Exception("cryptographer error")

        with pytest.raises(CryptographyError):
            self.encryption_service.encrypt(Builder.build_random_string())

    def test_decrypt(self) -> None:
        encrypted = Builder.build_random_string()
        decrypted_value = Builder.build_random_string().encode("utf-8")
        self.cryptographer.decrypt.return_value = decrypted_value

        assert self.encryption_service.decrypt(encrypted) == decrypted_value.decode("utf-8")
        self.cryptographer.decrypt.assert_called_once_with(encrypted.encode("utf-8"))
        self.cryptographer_factory.assert_called_once_with(self.encryption_key)
        self.ssm.get_parameter.assert_called_once_with(name=self.encryption_key_param, decryption=True)

    def test_decrypt_fail(self) -> None:
        self.cryptographer.decrypt.side_effect = Exception("cryptographer error")

        with pytest.raises(CryptographyError):
            self.encryption_service.decrypt(Builder.build_random_string())

    def test_cache_encryption_key(self) -> None:
        self.encryption_service.encrypt(Builder.build_random_string())
        self.encryption_service.encrypt(Builder.build_random_string())

        assert self.ssm.get_parameter.call_count == 1

    def test_encrypt_decrypt_with_fernet(self) -> None:
        encryption_key = Fernet.generate_key().decode("utf-8")
        self.ssm.get_parameter.return_value = encryption_key
        encryption_service = EncryptionService(
            config=self.config,
            ssm_client=self.ssm,
            cryptographer_factory=Fernet,
        )
        plain = Builder.build_random_string()

        encrypted = encryption_service.encrypt(plain)
        assert plain not in encrypted
        assert encryption_service.decrypt(encrypted) == plain

        with pytest.raises(CryptographyError):
            encryption_service.decrypt("invalid token")
