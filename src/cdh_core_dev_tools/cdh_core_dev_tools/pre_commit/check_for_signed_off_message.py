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
import re
import sys

SIGNED_OFF_REGEX = r"^Signed-off-by: .* <.*@.*>$"


def ensure_commit_is_signed_off(commit_message_file: str) -> None:
    """We want to that every commit message has been signed off before pushing it into the repo."""
    with open(commit_message_file, "r", encoding="utf-8") as file:
        commit_message_lines = file.readlines()
    signed_off = any(re.search(SIGNED_OFF_REGEX, commit_message) for commit_message in commit_message_lines)
    if not signed_off:
        print("Missing signature for commit! Please use the option '-s'")  # noqa: T201
        sys.exit(2)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Expecting exactly one argument (filename of the commit message)")  # noqa: T201
        sys.exit(1)
    ensure_commit_is_signed_off(commit_message_file=sys.argv[1])
