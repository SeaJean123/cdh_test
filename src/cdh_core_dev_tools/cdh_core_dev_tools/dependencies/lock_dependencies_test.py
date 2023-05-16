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
import os
import shutil
import subprocess
from tempfile import mktemp
from typing import Set
from typing import Tuple
from unittest import mock

from cdh_core_dev_tools.dependencies.lock_dependencies import _start_sub_process
from cdh_core_dev_tools.dependencies.lock_dependencies import ensure_complete_references_in_root_requirements
from cdh_core_dev_tools.dependencies.lock_dependencies import generate_requirements_files
from cdh_core_dev_tools.dependencies.lock_dependencies import LOG
from cdh_core_dev_tools.dependencies.lock_dependencies import validate_requirement_files
from cdh_core_dev_tools.testing.builder import Builder


class TestLockDependencies:
    tmp_folders: Set[str] = set()
    root_requirements_txt_content = ["foo==1.0\n", "bar==0.2\n"]

    def teardown_method(self) -> None:
        for folder in self.tmp_folders:
            try:
                shutil.rmtree(folder)
            except FileNotFoundError:
                pass

    def _build_tmp_structure(self) -> Tuple[str, str]:
        tmp_dir_name = mktemp()
        self.tmp_folders.add(tmp_dir_name)
        sub_folder = Builder.build_random_string()
        sub_folder_full_path = os.path.join(tmp_dir_name, sub_folder)
        os.makedirs(sub_folder_full_path)
        with open(os.path.join(tmp_dir_name, "requirements.in"), "w", encoding="utf-8") as root_requirements_in_handler:
            root_requirements_in_handler.writelines(f"-r {sub_folder}/requirements.in")
        with open(
            os.path.join(sub_folder_full_path, "requirements.in"), "w", encoding="utf-8"
        ) as root_requirements_in_handler:
            root_requirements_in_handler.writelines("mock")
        with open(
            os.path.join(sub_folder_full_path, "requirements.txt"), "w", encoding="utf-8"
        ) as root_requirements_in_handler:
            root_requirements_in_handler.writelines(["mock", "file:foo"])
        with open(
            os.path.join(tmp_dir_name, "requirements-dev.in"), "w", encoding="utf-8"
        ) as root_requirements_dev_in_handler:
            root_requirements_dev_in_handler.writelines("pytest")
        with open(
            os.path.join(tmp_dir_name, "requirements.txt"), "w", encoding="utf-8"
        ) as root_requirements_txt_handler:
            root_requirements_txt_handler.writelines(TestLockDependencies.root_requirements_txt_content)
        return tmp_dir_name, sub_folder

    def test_ensure_complete_references_in_root_requirements_good_case(self) -> None:
        LOG.error = mock.Mock()  # type: ignore
        tmp_dir_name, _ = self._build_tmp_structure()
        ensure_complete_references_in_root_requirements(tmp_dir_name)
        assert LOG.error.call_count == 0

    def test_ensure_complete_references_in_root_requirements_bad_case(self) -> None:
        LOG.error = mock.Mock()  # type: ignore
        tmp_dir_name, _ = self._build_tmp_structure()
        with open(os.path.join(tmp_dir_name, "requirements.in"), "w", encoding="utf-8") as root_requirements_in_handler:
            root_requirements_in_handler.writelines("-r foo/requirements.in")
        ensure_complete_references_in_root_requirements(tmp_dir_name)
        assert LOG.error.call_count == 1

    @mock.patch("subprocess.run")
    def test_subprocess_retry_on_error(self, mock_subprocess_run: mock.Mock) -> None:
        mock_subprocess_run.side_effect = [
            subprocess.CompletedProcess(
                returncode=128, args="", stdout="stdout".encode("utf-8"), stderr="stderr".encode("utf-8")
            ),
            subprocess.CompletedProcess(
                returncode=128, args="", stdout="stdout".encode("utf-8"), stderr="stderr".encode("utf-8")
            ),
            subprocess.CompletedProcess(
                returncode=0, args="", stdout="stdout".encode("utf-8"), stderr="stderr".encode("utf-8")
            ),
        ]
        command_args = ["something"]

        assert _start_sub_process(command_args)
        assert len(mock_subprocess_run.call_args_list) == 3

    @mock.patch("subprocess.run")
    def test_subprocess_retry_on_error_fail(self, mock_subprocess_run: mock.Mock) -> None:
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            returncode=128, args="", stdout="stdout".encode("utf-8"), stderr="stderr".encode("utf-8")
        )
        command_args = ["something"]

        assert not _start_sub_process(command_args)
        assert len(mock_subprocess_run.call_args_list) == 3

    @mock.patch("subprocess.run")
    def test_generate_requirements_files(self, mock_subprocess_run: mock.Mock) -> None:
        tmp_dir_name, sub_folder = self._build_tmp_structure()

        generate_requirements_files(tmp_dir_name)
        mock_subprocess_run.assert_any_call(
            [
                "pip-compile",
                os.path.join(tmp_dir_name, "requirements.in"),
                "--output-file",
                os.path.join(tmp_dir_name, "requirements.txt"),
                "--no-annotate",
                "--no-header",
                "--upgrade",
                "--allow-unsafe",
            ],
            capture_output=True,
            check=False,
        )
        mock_subprocess_run.assert_any_call(
            [
                "pip-compile",
                os.path.join(tmp_dir_name, "requirements-dev.in"),
                "--output-file",
                os.path.join(tmp_dir_name, "requirements-dev.txt"),
                "--no-annotate",
                "--no-header",
                "--upgrade",
                "--allow-unsafe",
            ],
            capture_output=True,
            check=False,
        )
        assert os.stat(os.path.join(tmp_dir_name, "requirements.txt")).st_size == 0
        mock_subprocess_run.assert_any_call(
            [
                "pip-compile",
                os.path.join(os.path.join(tmp_dir_name, sub_folder), "requirements.in"),
                "--output-file",
                os.path.join(os.path.join(tmp_dir_name, sub_folder), "requirements.txt"),
                "--no-annotate",
                "--no-header",
                "--allow-unsafe",
            ],
            capture_output=True,
            check=False,
        )

    @mock.patch("subprocess.run")
    def test_generate_requirements_files_with_update(self, mock_subprocess_run: mock.Mock) -> None:
        tmp_dir_name, sub_folder = self._build_tmp_structure()
        generate_requirements_files(tmp_dir_name)
        mock_subprocess_run.assert_any_call(
            [
                "pip-compile",
                os.path.join(tmp_dir_name, "requirements.in"),
                "--output-file",
                os.path.join(tmp_dir_name, "requirements.txt"),
                "--no-annotate",
                "--no-header",
                "--upgrade",
                "--allow-unsafe",
            ],
            capture_output=True,
            check=False,
        )
        mock_subprocess_run.assert_any_call(
            [
                "pip-compile",
                os.path.join(os.path.join(tmp_dir_name, sub_folder), "requirements.in"),
                "--output-file",
                os.path.join(os.path.join(tmp_dir_name, sub_folder), "requirements.txt"),
                "--no-annotate",
                "--no-header",
                "--allow-unsafe",
            ],
            capture_output=True,
            check=False,
        )

    @mock.patch("subprocess.run")
    def test_validate_requirement_files(self, mock_subprocess_run: mock.Mock) -> None:
        mock_subprocess_run.side_effect = [
            subprocess.CompletedProcess(
                returncode=0, args="", stdout="stdout".encode("utf-8"), stderr="stderr".encode("utf-8")
            ),
        ]
        tmp_dir_name, sub_folder = self._build_tmp_structure()
        validate_requirement_files(tmp_dir_name)
        assert len(mock_subprocess_run.call_args_list) == 1
        assert mock_subprocess_run.call_args_list[0][0][0][0] == "pip-extra-reqs"
        assert mock_subprocess_run.call_args_list[0][0][0][-1].endswith(sub_folder)
