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
from http import HTTPStatus
from typing import Dict
from typing import Optional


class HttpError(Exception):
    """
    This class represents HTTP error codes.

    Subclasses should overwrite STATUS to specify the HTTP status code of the response.
    """

    STATUS = HTTPStatus.INTERNAL_SERVER_ERROR

    def to_dict(self, request_id: Optional[str] = None) -> Dict[str, str]:
        """Return the error as standardized dict, which can be converted to JSON."""
        error_dict = {"Code": type(self).__name__, "Message": str(self)}
        if request_id:
            error_dict["RequestId"] = request_id
        return error_dict


class BadRequestError(HttpError):
    """Signals HTTP code 400."""

    STATUS = HTTPStatus.BAD_REQUEST


class UnauthorizedError(HttpError):
    """Signals HTTP code 401."""

    STATUS = HTTPStatus.UNAUTHORIZED


class ForbiddenError(HttpError):
    """Signals HTTP code 403."""

    STATUS = HTTPStatus.FORBIDDEN


class NotFoundError(HttpError):
    """Signals HTTP code 404."""

    STATUS = HTTPStatus.NOT_FOUND


class MethodNotAllowedError(HttpError):
    """Signals HTTP code 405."""

    STATUS = HTTPStatus.METHOD_NOT_ALLOWED


class ConflictError(HttpError):
    """Signals HTTP code 409."""

    STATUS = HTTPStatus.CONFLICT


class RoleOutOfSyncError(ConflictError):
    """Signals HTTP code 409 but it is internally used to signal an error within the glue sync."""

    def __init__(self, role_name: str) -> None:
        super().__init__(f"Role: {role_name} out of sync, try resyncing the role")


class UnsupportedMediaTypeError(HttpError):
    """Signals HTTP code 415."""

    STATUS = HTTPStatus.UNSUPPORTED_MEDIA_TYPE


class UnprocessableEntityError(HttpError):
    """Signals HTTP code 422."""

    STATUS = HTTPStatus.UNPROCESSABLE_ENTITY


class TooManyRequestsError(HttpError):
    """Signals HTTP code 429."""

    STATUS = HTTPStatus.TOO_MANY_REQUESTS


class InternalError(HttpError):
    """Signals HTTP code 500."""

    STATUS = HTTPStatus.INTERNAL_SERVER_ERROR


class LockError(HttpError):
    """Signals HTTP code 423."""

    STATUS = HTTPStatus.LOCKED


class ServiceUnavailableError(HttpError):
    """Signals HTTP code 503."""

    STATUS = HTTPStatus.SERVICE_UNAVAILABLE


class GatewayTimeoutError(HttpError):
    """Signals HTTP code 504."""

    STATUS = HTTPStatus.GATEWAY_TIMEOUT
