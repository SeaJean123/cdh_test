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
import pytest

from cdh_core.log.measure_time import MeasureTimeContextManager


class TestMeasureTimeContextManager:
    def test_exception_does_not_get_caught(self) -> None:
        error = Exception("my error")
        with pytest.raises(Exception) as exc_info:
            with MeasureTimeContextManager("foo"):
                raise error
        assert exc_info.value == error
