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
import time
from logging import getLogger
from typing import Any

LOG = getLogger(__name__)


class MeasureTimeContextManager:
    """Can be used to measure the time a context takes to finish and logs afterwards to INFO."""

    def __init__(self, message: str):
        self.message = message
        self.start_time = 0.0

    def __enter__(self) -> None:
        """Store the current time when entering the context."""
        self.start_time = time.perf_counter()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Print the time on log level info it took for completing the context."""
        LOG.info(f"{self.message} took {int(time.perf_counter() - self.start_time)}s")
