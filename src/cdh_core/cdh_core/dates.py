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
from datetime import timezone


def date_output(date_time: datetime) -> str:
    """Return datetime in UTC timezone reformatted to isoformat and set the time zone to UTC."""
    return datetime.replace(date_time, tzinfo=timezone.utc).isoformat()


def date_input(date: str) -> datetime:
    """Return datetime in local timezone reformatted to fromisoformat but remove the time zone information."""
    return datetime.replace(datetime.fromisoformat(date), tzinfo=None)
