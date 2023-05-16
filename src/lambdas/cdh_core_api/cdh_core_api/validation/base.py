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
import re
import string
from typing import Any
from typing import Optional

from cdh_core_api.api.validation import field
from cdh_core_api.api.validation import register_for_type
from cdh_core_api.config import ValidationContext
from cdh_core_api.validation.abstract import create_enum_validator
from cdh_core_api.validation.abstract import StringValidator
from marshmallow import fields
from marshmallow import ValidationError
from marshmallow.validate import Length
from marshmallow.validate import Regexp
from marshmallow_enum import EnumField

from cdh_core.entities.dataset_participants import DatasetParticipantId
from cdh_core.enums.aws import Region
from cdh_core.enums.hubs import Hub
from cdh_core.primitives.account_id import AccountId

_validate_hub = create_enum_validator(Hub, "Invalid hub", hide_sensitive_data=True)
_validate_region = create_enum_validator(Region, "Invalid region", hide_sensitive_data=False)


def validate_hub(context: ValidationContext, hub_name: object) -> Hub:
    """Return a hub if the name is valid."""
    hub = _validate_hub(hub_name)
    if hub in context.config.hubs:
        return hub
    raise ValidationError(f"Invalid hub {hub_name!r}")


account_id_validator = StringValidator(
    min_length=12,
    max_length=12,
    characters=string.digits,
    characters_description="Digits",
)


def validate_account_id(account_input: Any) -> AccountId:
    """Return a AccountId if it is valid."""
    return AccountId(account_id_validator(account_input))


@register_for_type(Hub)
class HubField(EnumField):
    """Validates the Hub enum."""

    def __init__(self, **kwargs: Any):
        def validation(context: ValidationContext, hub_name: object) -> Optional[Hub]:
            if not kwargs.get("required", False) and hub_name is None:
                return None
            return validate_hub(context, hub_name)

        super().__init__(
            enum=Hub,
            validator_with_context=validation,
            _jsonschema_type_mapping={"$ref": "#/components/schemas/Hub"},
            **kwargs,
        )


def validate_region_in_hub(current_hub: Optional[Hub], region_name: object) -> Region:
    """Return a region if it is valid and if the the region is in the given hub."""
    region = _validate_region(region_name)
    if current_hub is None:
        return region
    if region in current_hub.regions:
        return region
    raise ValidationError(f"Invalid region for hub {current_hub}. Must be one of {current_hub.regions}")


def validate_region(context: ValidationContext, region_name: object) -> Region:
    """Return a region if it is a valid one."""
    return validate_region_in_hub(context.current_hub, region_name)


@register_for_type(Region)
class RegionField(EnumField):
    """Validates the Region enum."""

    def __init__(self, **kwargs: Any):
        def validation(context: ValidationContext, region_name: object) -> Optional[Region]:
            if kwargs.get("required", False) is False and region_name is None:
                return None
            return validate_region(context, region_name)

        super().__init__(
            enum=Region,
            validator_with_context=validation,
            _jsonschema_type_mapping={"$ref": "#/components/schemas/Region"},
            **kwargs,
        )


@register_for_type(AccountId)
class AccountIdField(fields.String):
    """Validates the AccountId class."""

    def __init__(self, **kwargs: Any):
        super().__init__(validate=account_id_validator, **kwargs)


next_page_token_field = field(
    validator=StringValidator(max_length=10000),
    metadata={
        "description": "The 'nextPageToken' returned in the header of the response to a previous request on the "
        "same endpoint.",
    },
    default=None,
)

EMAIL_REGEX = re.compile(r"^[äöüÄÖÜßa-zA-Z0-9_.-]+@([A-Za-z0-9.-]+\.[A-Za-z0-9]{2,})$")


@register_for_type(DatasetParticipantId)
class ResponsibleField(fields.String):
    """Validates responsibles."""

    def __init__(self, **kwargs: Any):
        super().__init__(
            validate=Regexp(EMAIL_REGEX), pattern=EMAIL_REGEX.pattern, example="someone@example.com", **kwargs
        )


def list_field(inner_field: Any, allow_empty: bool = True, can_be_none: bool = False, **kwargs: Any) -> Any:
    """Return a field that validates that its elements are valid inner_field items."""
    if can_be_none:
        return field(metadata={"marshmallow_field": fields.List(inner_field)}, default=None, **kwargs)
    if allow_empty:
        return field(metadata={"marshmallow_field": fields.List(inner_field, required=True)}, **kwargs)
    return field(
        metadata={"marshmallow_field": fields.List(inner_field, required=True, validate=Length(min=1))}, **kwargs
    )


def responsibles_field(allow_empty_responsible: bool = True, can_be_none: bool = False, **kwargs: Any) -> Any:
    """Return a field that validates a list of responsible email addresses."""
    inner_field = ResponsibleField
    return list_field(inner_field=inner_field, allow_empty=allow_empty_responsible, can_be_none=can_be_none, **kwargs)


def owner_account_id_field() -> Any:
    """Return a dataclasses.Field instance with the relevant metadata for the owner_account_id."""
    return field(metadata={"description": "create resources on behalf of the specified account"}, default=None)
