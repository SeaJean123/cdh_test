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
from unittest.mock import call
from unittest.mock import Mock

import pytest

from cdh_core.iterables import chunks_of_bounded_weight
from cdh_core.iterables import unwrap_singleton


def _identity(identity: int) -> int:
    return identity


class TestChunksOfBoundedWeight:
    def test_empty_iterator(self) -> None:
        assert not list(chunks_of_bounded_weight([], 10, _identity))

    def test_all_items_fit_into_one_chunk(self) -> None:
        assert list(chunks_of_bounded_weight(range(5), 10, _identity)) == [[0, 1, 2, 3, 4]]

    def test_several_chunks_necessary(self) -> None:
        assert list(chunks_of_bounded_weight([5, 6, 4, 1], 10, _identity)) == [[5], [6, 4], [1]]

    def test_get_weight_function_is_called(self) -> None:
        get_weight = Mock(side_effect=len)
        assert list(chunks_of_bounded_weight(["123", "456", "78"], 5, get_weight)) == [["123"], ["456", "78"]]
        get_weight.assert_has_calls([call("123"), call("456"), call("78")])

    def test_item_weight_surpasses_max_weight(self) -> None:
        with pytest.raises(ValueError):
            list(chunks_of_bounded_weight([20], 10, _identity))


class TestUnwrapSingleton:
    def test_empty_collection_fails(self) -> None:
        with pytest.raises(ValueError):
            unwrap_singleton(set())

    def test_multiple_elements_fails(self) -> None:
        with pytest.raises(ValueError):
            unwrap_singleton({1, 2})

    def test_single_element_success(self) -> None:
        assert unwrap_singleton({1}) == 1
