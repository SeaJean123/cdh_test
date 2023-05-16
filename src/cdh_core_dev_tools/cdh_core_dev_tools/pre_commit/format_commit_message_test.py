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
import tempfile
from unittest import mock

import pytest

from cdh_core_dev_tools.pre_commit.format_commit_message import format_commit_message
from cdh_core_dev_tools.testing.utils import disable_stdout


@pytest.mark.parametrize(
    "branch,message,expected_message",
    [
        pytest.param("ABC-123-#45-test-branch", "msg", "ABC-123 #45 msg", id="internal and issue number missing"),
        pytest.param(
            "ABC-123-#45-test-branch",
            "msg ABC-123 fixes #45",
            "msg ABC-123 fixes #45",
            id="internal and issue number already present",
        ),
        pytest.param(
            "ABC-123-#45-test-branch",
            "ABC-123 msg",
            "#45 ABC-123 msg",
            id="internal number already present, issue missing",
        ),
        pytest.param(
            "ABC-123-#45-test-branch",
            "msg #45",
            "ABC-123 msg #45",
            id="issue already present, internal number missing",
        ),
        pytest.param("test-branch", "msg", "msg", id="internal number and issue missing in branch"),
        pytest.param("test-ABC-123-branch", "msg", "ABC-123 msg", id="issue missing in branch"),
        pytest.param("test-#45-branch", "msg", "#45 msg", id="internal number missing in branch"),
    ],
)
def test_format_commit_message(branch: str, message: str, expected_message: str) -> None:
    with mock.patch("subprocess.check_output") as mock_subprocess_check_output:
        mock_subprocess_check_output.return_value = bytes(branch, encoding="utf-8")
        with tempfile.NamedTemporaryFile() as tmp_file:
            with open(tmp_file.name, "r+", encoding="utf-8") as commit_message_fh:
                commit_message_fh.write(message)
                commit_message_fh.seek(0)
                with disable_stdout():
                    format_commit_message(tmp_file.name)
                commit_message_fh.seek(0)
                assert commit_message_fh.read() == expected_message
