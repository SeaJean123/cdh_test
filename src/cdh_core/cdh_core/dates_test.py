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
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from cdh_core.dates import date_output


@pytest.mark.parametrize(
    "date_time", [datetime.now(), datetime.now(tz=timezone.utc), datetime.now(tz=timezone(offset=timedelta(hours=3)))]
)
def test_date_output(date_time: datetime) -> None:
    result = datetime.fromisoformat(date_output(date_time))
    assert result == date_time.replace(tzinfo=timezone.utc)
