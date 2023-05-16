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
import subprocess
import sys
from typing import Any
from typing import Optional


INTERNAL_TICKET_REGEX = r"[A-Z]{3,}-\d+"
ISSUE_REGEX = r"#\d+"


def _git_current_branch() -> str:
    return subprocess.check_output(["git rev-parse --abbrev-ref HEAD"], shell=True).decode().strip()


def _print_local(to_print: Any) -> None:
    print(f"[commit message hook] {to_print}")  # noqa: T201


def _extract_ticket_number(message: str, regex: str) -> Optional[str]:
    ticket = re.search(regex, message)
    return ticket.group(0) if ticket else None


def _prefix_commit(commit_message_file: str, old_commit_message: str, prefix: str, current_branch: str) -> None:
    _print_local(f"Prepending ticket ID(s) from current branch: {current_branch}")
    with open(commit_message_file, "w", encoding="utf-8") as file:
        file.write(prefix + old_commit_message)


def format_commit_message(commit_message_file: str) -> None:
    """Format the given commit message based on the current git branch."""
    with open(commit_message_file, "r", encoding="utf-8") as file:
        old_commit_message = file.read()
    current_branch = _git_current_branch()
    internal_ticket = _extract_ticket_number(current_branch, INTERNAL_TICKET_REGEX)
    issue = _extract_ticket_number(current_branch, ISSUE_REGEX)
    prefix = ""
    if internal_ticket and not _extract_ticket_number(old_commit_message, INTERNAL_TICKET_REGEX):
        prefix += f"{internal_ticket} "
    if issue and not _extract_ticket_number(old_commit_message, ISSUE_REGEX):
        prefix += f"{issue} "
    if prefix:
        _prefix_commit(commit_message_file, old_commit_message, prefix, current_branch)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        _print_local("Expecting at least one argument (filename of the commit message)")
        sys.exit(1)
    format_commit_message(commit_message_file=sys.argv[1])
