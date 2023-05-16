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
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from cdh_core_api.catalog.base import LastEvaluatedKey
from cdh_core_api.services.encryption_service import CryptographyError
from cdh_core_api.services.encryption_service import EncryptionService
from marshmallow import ValidationError

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin


class NextPageTokenContext(Enum):
    """Describes the various contexts in which a NextPageToken can be issued."""

    RESOURCES = "resources"
    DATASETS = "datasets"
    ACCOUNTS = "accounts"


class PaginationService:
    """Manages the encoding and decoding of NextPageTokens."""

    @dataclass
    class NextPageToken(DataClassJsonCDHMixin):
        """Describes the content encoded in a NextPageToken as returned to the client."""

        last_evaluated_key: LastEvaluatedKey
        context: NextPageTokenContext

    def __init__(self, encryption_service: EncryptionService) -> None:
        self._encryption_service = encryption_service

    def decode_token(self, next_page_token: Optional[str], context: NextPageTokenContext) -> Optional[LastEvaluatedKey]:
        """Reconstruct DynamoDB's LastEvaluatedKey from an encrypted NextPageToken."""
        if next_page_token is None:
            return None
        try:
            decrypted = self._encryption_service.decrypt(next_page_token)
        except CryptographyError:
            raise ValidationError(  # pylint: disable=raise-missing-from
                f"The provided nextPageToken {next_page_token!r} is invalid"
            )
        token = PaginationService.NextPageToken.from_json(decrypted)
        if token.context is not context:
            raise ValidationError(f"The provided nextPageToken {next_page_token!r} was issued in a different context")
        return token.last_evaluated_key

    def issue_token(
        self, last_evaluated_key: Optional[LastEvaluatedKey], context: NextPageTokenContext
    ) -> Optional[str]:
        """Encode and encrypt DynamoDB's LastEvaluatedKey."""
        if last_evaluated_key is None:
            return None
        token = PaginationService.NextPageToken(last_evaluated_key=last_evaluated_key, context=context)
        return self._encryption_service.encrypt(token.to_json())
