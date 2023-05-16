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
import asyncio
import glob
import logging
import os
import subprocess
import sys
from asyncio import FIRST_EXCEPTION
from dataclasses import dataclass
from logging import getLogger
from typing import List
from typing import Optional

from cdh_core.enums.environment import Environment
from cdh_core.log.logger import configure_logging

configure_logging(__name__)
LOG = getLogger(__name__)
logging.info("For some reason no log output gets created if this line is omitted!")


class TestExecutionFailed(Exception):
    """Signals that the execution of a functional test failed."""


@dataclass(frozen=True)
class TestSpec:
    """Represents the test specification for a functional test."""

    path: str
    file_name: str
    parameters: Optional[List[str]] = None


class TestSuite:
    """Class for executing a collection of prerequisite and parallel tests."""

    def __init__(
        self, base_test_command: List[str], prerequisite_tests: List[TestSpec], parallel_tests: List[TestSpec]
    ) -> None:
        self.base_test_command = base_test_command
        self.prerequisite_tests = prerequisite_tests
        self.parallel_tests = parallel_tests
        self.logs_list: List[str] = []

    def execute(self) -> None:
        """
        Execute all prerequisite and parallel tests contained in the test suite.

        Prints the logs of all completed and failed tests.
        """
        try:
            self._execute_prerequisite_tests()
            asyncio.run(self._execute_parallel_tests())
        except TestExecutionFailed:
            self.logs_list.append("\nERROR: Killing remaining tests because previous tests have failed")
            sys.exit(1)
        finally:
            self.print_logs()

    def _execute_prerequisite_tests(self) -> None:
        for test_spec in self.prerequisite_tests:
            process = subprocess.run(
                self._get_test_command(test_spec),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            self.logs_list += [process.stdout]
            if process.returncode != 0:
                raise TestExecutionFailed

    async def _execute_parallel_tests(self) -> None:
        tasks = [asyncio.create_task(self._spawn_subprocess(test_spec)) for test_spec in self.parallel_tests]

        done_tasks, pending_tasks = await asyncio.wait(tasks, return_when=FIRST_EXCEPTION)

        for task in pending_tasks:
            task.cancel()
        if any(task.exception() for task in done_tasks):
            raise TestExecutionFailed

    async def _spawn_subprocess(self, test_spec: TestSpec) -> None:
        proc = await asyncio.create_subprocess_exec(
            *self._get_test_command(test_spec),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        result = await proc.wait()
        self.logs_list += [(await proc.stdout.read()).decode(encoding="utf-8")]  # type: ignore
        if result != 0:
            raise TestExecutionFailed

    def print_logs(self) -> None:
        """Log all logs collected during the test suite execution."""
        logs = "\n".join(self.logs_list)
        LOG.info(f"\n{logs}\n")

    def _get_test_command(self, test_spec: TestSpec) -> List[str]:
        command = self.base_test_command + [f"{test_spec.path}/{test_spec.file_name}"]
        if test_spec.parameters:
            command += test_spec.parameters
        return command


class FunctionalTestEnvironment:
    """Class for collecting all environment variables needed to execute functional tests."""

    def __init__(self, base_path: str, base_test_command: List[str]) -> None:
        self.base_url = os.environ["BASE_URL"]
        self.environment = Environment(os.environ["ENVIRONMENT"])
        self.cleanup_enabled: bool = os.environ["CLEANUP_PREFIX_DEPLOYMENT"].lower() == "true"
        self.is_integrated_deployment: bool = os.environ["IS_INTEGRATED_DEPLOYMENT"].lower() == "true"
        self.contains_test_data: bool = os.environ["CONTAINS_TEST_DATA"].lower() == "true"
        self.base_path = base_path
        self.base_test_command = base_test_command

    def log(self) -> None:
        """Log all environment variables used in the functional tests."""
        LOG.info(
            f"\nExecuting tests with BASE_URL={self.base_url}, ENVIRONMENT={self.environment.value}, "
            f"CLEANUP_PREFIX_DEPLOYMENT={self.cleanup_enabled}, "
            f"IS_INTEGRATED_DEPLOYMENT={self.is_integrated_deployment}, CONTAINS_TEST_DATA={self.contains_test_data}\n"
        )

    def get_test_suite(self) -> TestSuite:
        """Collect all required functional tests and return them as a test suite object."""
        prerequisite_tests: List[TestSpec] = []
        parallel_tests: List[TestSpec] = []

        test_type = "non_mutating_basic"
        enabled_test_types = [test_type]
        self._append_parallel_tests(parallel_tests=parallel_tests, test_type=test_type)

        if self.cleanup_enabled:
            test_type = "mutating_basic"
            enabled_test_types += [test_type]
            self._append_prerequisite_tests(prerequisite_tests=prerequisite_tests, test_type=test_type)
            self._append_parallel_tests(
                parallel_tests=parallel_tests,
                test_type=test_type,
                test_files_to_skip=[test.file_name for test in prerequisite_tests],
            )

        if self.is_integrated_deployment and self.environment.is_test_environment:
            test_type = "mutating_integration"
            enabled_test_types += [test_type]
            self._append_parallel_tests(parallel_tests=parallel_tests, test_type=test_type)

        if self.contains_test_data:
            test_type = "non_mutating_known_data"
            enabled_test_types += [test_type]
            self._append_parallel_tests(parallel_tests=parallel_tests, test_type=test_type)

        LOG.info(f"\nEnabled test types: {enabled_test_types}\n")

        return TestSuite(
            base_test_command=self.base_test_command,
            prerequisite_tests=prerequisite_tests,
            parallel_tests=parallel_tests,
        )

    def _append_parallel_tests(
        self, parallel_tests: List[TestSpec], test_type: str, test_files_to_skip: Optional[List[str]] = None
    ) -> None:
        test_path = f"{self.base_path}/{test_type}"
        for file_name in self._get_test_file_names(test_path):
            if test_files_to_skip and file_name in test_files_to_skip:
                continue
            parameters = ["-n", "auto", "-v"] if test_type == "mutating_basic" else []
            parallel_tests.append(TestSpec(test_path, file_name, parameters))

    def _append_prerequisite_tests(self, prerequisite_tests: List[TestSpec], test_type: str) -> None:
        test_path = f"{self.base_path}/{test_type}"
        prerequisite_tests += [TestSpec(test_path, "test_prerequisite.py")]

    @staticmethod
    def _get_test_file_names(path: str) -> List[str]:
        """Return all python test files in the given path."""
        return [file_path.split("/")[-1] for file_path in glob.glob(f"{path}/test*.py")]
