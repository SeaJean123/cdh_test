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
from cdh_core_api.catalog.base_test import build_last_evaluated_key
from cdh_core_api.services.encryption_service import CryptographyError
from cdh_core_api.services.encryption_service import EncryptionService
from cdh_core_api.services.pagination_service import NextPageTokenContext
from cdh_core_api.services.pagination_service import PaginationService
from marshmallow import ValidationError

from cdh_core_dev_tools.testing.builder import Builder


class TestPaginationService:
    def setup_method(self) -> None:
        self.encryption_service = Mock(EncryptionService)
        self.pagination_service = PaginationService(self.encryption_service)

    def test_decode(self) -> None:
        last_evaluated_key = build_last_evaluated_key()
        context = Builder.get_random_element(list(NextPageTokenContext))
        self.encryption_service.decrypt.return_value = PaginationService.NextPageToken(
            last_evaluated_key=last_evaluated_key, context=context
        ).to_json()
        next_page_token = Builder.build_random_string()
        assert (
            self.pagination_service.decode_token(
                next_page_token=next_page_token,
                context=context,
            )
            == last_evaluated_key
        )
        self.encryption_service.decrypt.assert_called_once_with(next_page_token)

    def test_decode_none(self) -> None:
        context = Builder.get_random_element(list(NextPageTokenContext))
        assert (
            self.pagination_service.decode_token(
                next_page_token=None,
                context=context,
            )
            is None
        )
        self.encryption_service.decrypt.assert_not_called()

    def test_decode_invalid_token(self) -> None:
        context = Builder.get_random_element(list(NextPageTokenContext))
        self.encryption_service.decrypt.side_effect = CryptographyError("invalid token")
        with pytest.raises(ValidationError):
            self.pagination_service.decode_token(
                next_page_token=Builder.build_random_string(),
                context=context,
            )

    def test_decode_wrong_context(self) -> None:
        context, other_context = Builder.choose_without_repetition(list(NextPageTokenContext), 2)
        last_evaluated_key = build_last_evaluated_key()
        self.encryption_service.decrypt.return_value = PaginationService.NextPageToken(
            last_evaluated_key=last_evaluated_key, context=other_context
        ).to_json()

        with pytest.raises(ValidationError):
            self.pagination_service.decode_token(
                next_page_token=Builder.build_random_string(),
                context=context,
            )

    def test_issue_token(self) -> None:
        encrypted_token = Builder.build_random_string()
        self.encryption_service.encrypt.return_value = encrypted_token
        last_evaluated_key = build_last_evaluated_key()
        context = Builder.get_random_element(list(NextPageTokenContext))

        assert (
            self.pagination_service.issue_token(
                last_evaluated_key=last_evaluated_key,
                context=context,
            )
            == encrypted_token
        )
        self.encryption_service.encrypt.assert_called_once_with(
            PaginationService.NextPageToken(last_evaluated_key=last_evaluated_key, context=context).to_json()
        )

    def test_issue_token_none(self) -> None:
        context = Builder.get_random_element(list(NextPageTokenContext))

        assert (
            self.pagination_service.issue_token(
                last_evaluated_key=None,
                context=context,
            )
            is None
        )
        self.encryption_service.encrypt.assert_not_called()
