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
from logging import getLogger
from typing import Iterator
from unittest.mock import patch

from cdh_applications.cleanup.cleanup_utils import always_delete_filter
from cdh_applications.cleanup.cleanup_utils import ask_user_filter
from cdh_applications.cleanup.cleanup_utils import chunked
from cdh_applications.cleanup.cleanup_utils import has_prefix
from cdh_applications.cleanup.cleanup_utils import has_resource_prefix
from cdh_applications.cleanup.cleanup_utils import never_delete_filter
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.hubs_test import build_non_default_hub

PREFIX = "cdhxtest"


class TestChunked:
    def make_iterator(self, end_value: int) -> Iterator[int]:
        yield from range(1, end_value + 1)

    def test_empty(self) -> None:
        assert not list(chunked(self.make_iterator(0), 2))

    def test_without_remainder(self) -> None:
        assert list(chunked(self.make_iterator(4), 2)) == [[1, 2], [3, 4]]

    def test_with_remainder(self) -> None:
        assert list(chunked(self.make_iterator(5), 2)) == [[1, 2], [3, 4], [5]]

    def test_has_prefix(self) -> None:
        assert has_prefix(name=f"{PREFIX}cdh-my-awesome-name", prefix=PREFIX)
        assert not has_prefix(name="cdh-my-awesome-name", prefix="")
        assert not has_prefix(name="cdh-my-awesome-name", prefix="none")
        assert not has_prefix(name="cdhxprscdh-my-awesome-name", prefix=PREFIX)
        assert not has_prefix(name="cdh-my-awesome-name", prefix="cdhxprs")
        assert not has_prefix(name="cdh-my-awesome-name", prefix="cdh")
        assert not has_prefix(name="cdh-core-my-awesome-name", prefix="cdh-core")
        assert not has_prefix(name="mars_test-my-awesome-name", prefix="mars")
        assert not has_prefix(name="mars_test-my-awesome-name", prefix="mars_test")
        assert not has_prefix(name="humres-test-cdh-core-my-awesome-name", prefix="humres")
        assert not has_prefix(name="humres-test-cdh-core-my-awesome-name", prefix="humres-test")

    def test_has_resource_prefix(self) -> None:
        bo_value = build_business_object().value
        hub_value = build_non_default_hub().value
        assert has_resource_prefix(name=f"{PREFIX}cdh-{bo_value}-name", prefix=PREFIX)
        assert has_resource_prefix(name=f"{PREFIX}cdh-{hub_value}-{bo_value}-name", prefix=PREFIX)
        assert not has_resource_prefix(name=f"{PREFIX}cdh-{bo_value}name", prefix=PREFIX)
        assert not has_resource_prefix(name=f"cdh-{bo_value}name", prefix=PREFIX)
        assert not has_resource_prefix(name=f"othercdh-{bo_value}name", prefix=PREFIX)
        assert not has_resource_prefix(name=f"{PREFIX}cdh-nobo-name", prefix=PREFIX)
        assert not has_resource_prefix(name=f"{PREFIX}cdh-{bo_value}-name", prefix="")


class TestFilters:
    def test_always_delete_filter(self) -> None:
        assert always_delete_filter(identifier="some_identifier", resource_type="some_resource_type")
        assert always_delete_filter("some_identifier", "some_resource_type", getLogger())

    def test_never_delete_filter(self) -> None:
        assert not never_delete_filter(identifier="some_identifier", resource_type="some_resource_type")
        assert not never_delete_filter("some_identifier", "some_resource_type", getLogger())

    def test_ask_user_filter(self) -> None:
        with patch("builtins.input", return_value="y"):
            assert ask_user_filter(identifier="some_identifier", resource_type="some_resource_type")
            assert ask_user_filter("some_identifier", "some_resource_type")
        with patch("builtins.input", return_value="N"):
            assert not ask_user_filter(identifier="some_identifier", resource_type="some_resource_type")
        with patch("builtins.input", return_value="n"):
            assert not ask_user_filter(identifier="some_identifier", resource_type="some_resource_type")
