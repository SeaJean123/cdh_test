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

import pytest

from cdh_core_dev_tools.pre_commit.check_for_signed_off_message import ensure_commit_is_signed_off
from cdh_core_dev_tools.testing.utils import disable_stdout


def test_ensure_commit_is_signed_off_fails() -> None:
    with tempfile.NamedTemporaryFile() as tmp_file:
        with open(tmp_file.name, "w", encoding="utf-8") as commit_message:
            commit_message.write("Some test commit message without being signed off")
        with pytest.raises(SystemExit):
            with disable_stdout():
                ensure_commit_is_signed_off(tmp_file.name)


def test_ensure_commit_is_signed_off_success() -> None:
    with tempfile.NamedTemporaryFile() as tmp_file:
        with open(tmp_file.name, "w", encoding="utf-8") as commit_message:
            commit_message.writelines("Some test commit message being signed off")
            commit_message.writelines("\n")
            commit_message.writelines("\n")
            commit_message.writelines("Signed-off-by: Test User <Test@user.com>")

        ensure_commit_is_signed_off(tmp_file.name)
