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
import datetime
import json
import os
import re
import subprocess
from collections import defaultdict
from typing import Any
from typing import Optional
from typing import Tuple


class NotifyTeams:
    """Build message and send to teams."""

    def __init__(self) -> None:
        """Initialize variables from environment."""
        context = self._dict_to_defaultdict(json.loads(os.environ["GITHUB_CONTEXT"]))
        self.webhook_alert_url = os.environ["WEBHOOK_ALERT_URL"]
        self.webhook_url = os.environ["WEBHOOK_URL"]
        self.successful = os.getenv("SUCCESSFUL", "false").lower() in ("true", "1")
        self.author = context["actor"] or ""
        self.author_name = context["event"]["head_commit"]["author"]["name"] or ""
        self.workflow_name = context["workflow"] or ""
        self.repository_url = context["event"]["repository"]["html_url"] or ""
        self.repository_name = context["repository"] or ""
        self.workflow_id = context["run_id"] or ""
        self.workflow_url = f"{self.repository_url}/actions/runs/{self.workflow_id}"
        commit_message_pr_id, commit_message_pr_branch = self._parse_commit_message(
            context["event"]["head_commit"]["message"] or ""
        )
        self.branch = commit_message_pr_branch or context["ref_name"] or ""
        self.pr_id = context["event"]["pull_request"]["number"] or commit_message_pr_id
        self.pr_url = f"{self.repository_url}/pull/{self.pr_id}"
        self.branch_url = f"{self.repository_url}/tree/{self.branch}"
        self.url = self.pr_url if self.pr_id else self.branch_url
        self.info = (
            context["event"]["pull_request"]["title"] or context["event"]["push"]["head_commit"]["message"] or ""
        )
        self.time = context["event"]["repository"]["pushed_at"] or datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S%z")

    @staticmethod
    def _dict_to_defaultdict(item: Any) -> Any:
        """Convert recursively dict to defaultdict."""

        def dict_maker() -> defaultdict[str, Any]:
            return defaultdict(dict_maker)

        if isinstance(item, dict):
            return defaultdict(dict_maker, {k: NotifyTeams._dict_to_defaultdict(v) for k, v in item.items()})
        return item

    @staticmethod
    def _parse_commit_message(message: str) -> Tuple[Optional[int], str]:
        """Extract information from commit message."""
        pr_id = None
        pr_branch = ""
        # example: "Merge pull request #264 from xxx-cdh/enhanced-stats-endpoint\n\nEnhanced stats endpoint"
        if match := re.match("^[^#]+#([0-9]+) from ([^\n\\ ]+)", message):
            pr_id = int(match.group(1))
            pr_branch = match.group(2)
        return pr_id, pr_branch

    def build_message(self) -> str:
        """Build message to send."""
        status = "succeded ❤" if self.successful else "failed 💔"
        summary_detail = f"({self.info})" if self.info else ""
        author_detail = f"({self.author_name})" if self.author_name else ""
        message = [
            f"## 🤖 Deployment of Github action `{self.repository_name}` {status}:",
            f"- Repo      : [{self.repository_name}]({self.repository_url})",
            f"- Workflow  : [{self.workflow_name}]({self.workflow_url})",
            f"- Summary   : [{self.branch}]({self.url}) {summary_detail}",
            f"- Time      : {self.time}",
            f"- Author    : {self.author} {author_detail}",
        ]
        return "\n".join(message)

    def send(self, message: str) -> None:
        """Send message to webhook."""
        data = json.dumps({"text": message}).encode()
        webhook_url = self.webhook_url if self.successful else self.webhook_alert_url
        subprocess.run(
            [
                "curl",
                "-X",
                "POST",
                webhook_url,
                "--data",
                data,
                "--silent",
                "--header",
                "Content-Type: application/json",
            ],
            check=True,
        )


def main() -> None:
    """Send a message to the webhook."""
    print("\n".join([f"{k}: {v}" for k, v in sorted(os.environ.items())]))  # noqa: T201
    notify_teams = NotifyTeams()
    message = notify_teams.build_message()
    print(message)  # noqa: T201
    notify_teams.send(message)


if __name__ == "__main__":
    main()
