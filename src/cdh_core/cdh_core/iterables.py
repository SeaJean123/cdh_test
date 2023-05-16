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
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import List
from typing import TypeVar

T = TypeVar("T")  # pylint: disable=invalid-name


def unique(items: List[T]) -> List[T]:
    """Return a new list with the same items, but without duplicates. Keep the order of items."""
    return list(dict.fromkeys(items))  # dict insertion order is guaranteed to be stable in Python 3.6+


def chunks_of_bounded_weight(
    items: Iterable[T], max_weight: int, get_weight: Callable[[T], int] = lambda x: 1
) -> Iterator[List[T]]:
    """
    Split *items* into chunks such that each chunk has a total weight of at most *max_weight*.

    The 'weight' of an item is whatever *get_weight* returns.
    """
    chunk = []
    chunk_weight = 0
    for item in items:
        item_weight = get_weight(item)
        if chunk_weight + item_weight <= max_weight:
            chunk.append(item)
            chunk_weight += item_weight
        elif item_weight > max_weight:
            raise ValueError("Item weight must not surpass max weight")
        else:
            yield chunk
            chunk = [item]
            chunk_weight = item_weight
    if chunk:
        yield chunk


def unwrap_singleton(items: Iterable[T]) -> T:
    """
    Retrieve the single element of a collection.

    Raises a ValueError if the collection is empty or has more than one element.
    """
    item_list = list(items)
    if len(item_list) == 0:
        raise ValueError("Expected to find a single item, but the collection as empty")
    if len(item_list) > 1:
        raise ValueError(f"Expected to find a single item, but found {len(item_list)} items")
    return item_list[0]
