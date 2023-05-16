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

from cdh_core.aws_clients.utils import repeat_continuation_call
from cdh_core.aws_clients.utils import repeat_while_truncated


class TestRepeatWhileTruncated:
    def test_not_truncated(self) -> None:
        function = Mock(return_value={"key": [1, 2, 3], "IsTruncated": False})

        assert list(repeat_while_truncated(function, "key")) == [1, 2, 3]
        function.assert_called_once_with()

    def test_truncated(self) -> None:
        function = Mock(
            side_effect=[
                {"key": [1], "IsTruncated": True, "Marker": "t1"},
                {"key": [2], "IsTruncated": True, "Marker": "t2"},
                {"key": [3], "IsTruncated": False},
            ]
        )

        assert list(repeat_while_truncated(function, "key")) == [1, 2, 3]
        function.assert_has_calls([call(), call(Marker="t1"), call(Marker="t2")])


class TestRepeatContinuationCall:
    def test_no_next_token_present(self) -> None:
        function = Mock(return_value={"key": [1, 2, 3]})

        assert list(repeat_continuation_call(function, "key")) == [1, 2, 3]
        function.assert_called_once_with()

    def test_next_token_present(self) -> None:
        function = Mock(
            side_effect=[
                {"key": [1], "NextToken": "t1"},
                {"key": [2], "NextToken": "t2"},
                {"key": [3]},
            ]
        )

        assert list(repeat_continuation_call(function, "key")) == [1, 2, 3]
        function.assert_has_calls([call(), call(NextToken="t1"), call(NextToken="t2")])
