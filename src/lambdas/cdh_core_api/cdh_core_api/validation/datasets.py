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
import string
from typing import Any
from typing import Callable
from typing import Dict
from typing import NewType
from typing import Optional
from typing import Set
from typing import Type
from typing import Union

from cdh_core_api.api.validation import field
from cdh_core_api.api.validation import register_for_type
from cdh_core_api.validation.abstract import InvalidType
from cdh_core_api.validation.abstract import StringValidator
from marshmallow import fields
from marshmallow import validate
from marshmallow.validate import Validator

from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import DatasetTags
from cdh_core.entities.dataset import SourceIdentifier
from cdh_core.entities.dataset import SupportGroup


def generate_string_validation_field(
    validator: Union[Validator, Callable[[Any], Any]], default_metadata: Optional[Dict[str, Any]] = None
) -> Type[fields.String]:
    """Generate a marshmallow Field that can be used in conjunction with register_for_type."""

    class _BaseValidationField(fields.String):
        def __init__(self, **kwargs: Any):
            if "metadata" not in kwargs and default_metadata is not None:
                kwargs["metadata"] = default_metadata
            super().__init__(validate=validator, **kwargs)

    return _BaseValidationField


validate_dataset_id = StringValidator(
    min_length=5,
    max_length=255,
    characters=string.ascii_lowercase + string.digits + "_",
    characters_description="ASCII letters, digits and _",
)

DatasetIdField = generate_string_validation_field(
    validator=validate_dataset_id,
    default_metadata={
        "description": "ID of the dataset",
        "example": "hr_data_src",
    },
)
register_for_type(DatasetId)(DatasetIdField)


def get_short_string_validator(allow_empty: bool = False) -> StringValidator:
    """Build a validator for a short string attribute."""
    return StringValidator(
        min_length=0 if allow_empty else 1,
        max_length=255,
        characters=string.ascii_letters + string.digits + "_-:",
        characters_description="ASCII letters, digits, _, -, and :",
    )


DeletableSupportGroup = NewType("DeletableSupportGroup", SupportGroup)
register_for_type(SupportGroup)(generate_string_validation_field(validator=get_short_string_validator()))
register_for_type(DeletableSupportGroup)(generate_string_validation_field(validator=get_short_string_validator(True)))

DATASET_SOURCE_IDENTIFIER_METADATA = {"description": "ID of the data source system."}
DeletableSourceIdentifier = NewType("DeletableSourceIdentifier", SourceIdentifier)
register_for_type(SourceIdentifier)(
    generate_string_validation_field(
        validator=get_short_string_validator(), default_metadata=DATASET_SOURCE_IDENTIFIER_METADATA
    )
)
register_for_type(DeletableSourceIdentifier)(
    generate_string_validation_field(
        validator=get_short_string_validator(True), default_metadata=DATASET_SOURCE_IDENTIFIER_METADATA
    )
)

validate_tag_key = StringValidator(
    min_length=1,
    max_length=45,
    characters=string.ascii_lowercase + string.digits + "-",
    characters_description="lowercase ASCII letters, digits, and -",
)

validate_tag_value = StringValidator(
    min_length=1,
    max_length=100,
    characters=string.ascii_letters + string.digits + "-" + " " + "&",
    characters_description="ASCII letters, digits, space, &, and -",
)


@register_for_type(DatasetTags)
class DatasetTagsField(fields.Dict):
    """Validates dataset tags."""

    def __init__(self, **kwargs: Any):
        super().__init__(
            keys=fields.String(validate=validate_tag_key), values=fields.String(validate=validate_tag_value), **kwargs
        )


DATASET_LABELS_DESCRIPTION = (
    "Can be used to track the legal entities or markets from which the contained data originates."
)


def dataset_labels_field(**kwargs: Any) -> Any:
    """Return a dataclasses.Field instance with the relevant metadata for the dataset_label."""
    return field(
        default=None,
        metadata={
            "marshmallow_field": fields.List(
                fields.Str(validate=validate_dataset_label),
                required=False,
                validate=validate.Length(max=100),
                metadata={
                    "description": DATASET_LABELS_DESCRIPTION,
                },
            ),
        },
        **kwargs,
    )


validate_dataset_name = StringValidator(
    min_length=3,
    max_length=20,
    characters=string.ascii_lowercase + string.digits + "_",
    characters_description="lowercase ASCII letters, digits, and underscores",
)
validate_dataset_friendly_name = StringValidator(
    min_length=1,
    max_length=40,
    characters=string.ascii_letters + string.digits + "_- ",
    characters_description="ASCII letters, digits, spaces, -, and _",
)
validate_dataset_description = StringValidator(min_length=5, max_length=1000, allow_newlines=True)
# maximum item size: 400KB (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)
# utf-8 characters need between 1 and 4 byte each
validate_dataset_documentation = StringValidator(max_length=50_000, allow_newlines=True)
validate_dataset_label = StringValidator(
    min_length=1,
    max_length=100,
    characters=string.ascii_letters + string.digits + "_",
    characters_description="ASCII letters, digits, and _",
)


def validate_dataset_lineage(lineage: object) -> Set[DatasetId]:
    """Validate the dataset_lineage."""
    if not isinstance(lineage, set):
        raise InvalidType(type(lineage), set)
    for dataset_id in lineage:
        validate_dataset_id(dataset_id)
    return lineage
