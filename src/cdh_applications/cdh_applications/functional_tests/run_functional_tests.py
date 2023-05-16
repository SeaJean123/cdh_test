#!/usr/bin/env python3
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

from cdh_applications.functional_tests.utils import FunctionalTestEnvironment
from cdh_core.log.logger import configure_logging

configure_logging(__name__)
LOG = getLogger(__name__)

BASE_PATH = "src/functional_tests"

BASE_TEST_COMMAND = ["pytest", "-vv"]


def main() -> None:
    """Collect and run all functional tests."""
    test_env = FunctionalTestEnvironment(BASE_PATH, BASE_TEST_COMMAND)
    test_env.log()

    test_suite = test_env.get_test_suite()
    test_suite.execute()


if __name__ == "__main__":
    main()
