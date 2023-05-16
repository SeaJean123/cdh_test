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
from typing import Optional

import pytest
from cdh_core_api.validation.abstract import InvalidType
from cdh_core_api.validation.datasets import DeletableSourceIdentifier
from cdh_core_api.validation.datasets import DeletableSupportGroup
from cdh_core_api.validation.datasets import get_short_string_validator
from cdh_core_api.validation.datasets import validate_dataset_friendly_name
from cdh_core_api.validation.datasets import validate_dataset_id
from marshmallow import ValidationError

from cdh_core.entities.dataset_test import build_source_identifier
from cdh_core.entities.dataset_test import build_support_group
from cdh_core_dev_tools.testing.assert_raises import assert_raises


def build_deletable_source_identifier(source_identifier: Optional[str] = None) -> DeletableSourceIdentifier:
    return DeletableSourceIdentifier(build_source_identifier(source_identifier))


def build_deletable_support_group(support_group: Optional[str] = None) -> DeletableSupportGroup:
    return DeletableSupportGroup(build_support_group(support_group))


class TestValidateDatasetId:
    def test_valid_ids(self) -> None:
        assert validate_dataset_id("hr_test_sem") == "hr_test_sem"
        assert validate_dataset_id("hr_test_with_underscore_sem") == "hr_test_with_underscore_sem"
        assert validate_dataset_id("mars_hr_test_with_underscore_sem") == "mars_hr_test_with_underscore_sem"
        assert validate_dataset_id("dataset_with_digits_123") == "dataset_with_digits_123"

    def test_invalid_type(self) -> None:
        with assert_raises(InvalidType(int, str)):
            validate_dataset_id(123)

    @pytest.mark.parametrize("character", ["-", "/", "!", "%", "&", "A"])
    def test_invalid_chars(self, character: str) -> None:
        with pytest.raises(ValidationError):
            validate_dataset_id(f"my{character}dataset")


class TestValidateShortString:
    def test_valid_short_string(self) -> None:
        assert get_short_string_validator()("foobar") == "foobar"
        assert get_short_string_validator()("FooBar") == "FooBar"
        assert get_short_string_validator()("42") == "42"
        assert get_short_string_validator()("foo:bar") == "foo:bar"
        assert get_short_string_validator()("foo-bar") == "foo-bar"
        assert get_short_string_validator()("foo_bar") == "foo_bar"
        assert get_short_string_validator(allow_empty=True)("") == ""

    def test_disallowed_empty(self) -> None:
        with pytest.raises(ValidationError):
            get_short_string_validator()("")

    def test_invalid_type(self) -> None:
        with assert_raises(InvalidType(int, str)):
            validate_dataset_id(123)


class TestValidateDatasetFriendlyName:
    def test_validate_friendly_names(self) -> None:
        assert validate_dataset_friendly_name("test_dataset_name") == "test_dataset_name"
        assert validate_dataset_friendly_name("test-dataset-name") == "test-dataset-name"

    def test_invalid_friendly_names(self) -> None:
        with pytest.raises(ValidationError):
            validate_dataset_friendly_name("  \n test_dataset_name")
        with pytest.raises(ValidationError):
            validate_dataset_friendly_name("test_dataset_name ")

    def test_invalid_type(self) -> None:
        with assert_raises(InvalidType(int, str)):
            validate_dataset_friendly_name(123)
