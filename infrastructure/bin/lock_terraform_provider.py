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
"""This script re-generates .terraform.lock.hcl files."""
import os
import subprocess
from typing import List


def terraform_init_and_lock(directory: str) -> None:
    """Init and lock terraform dependency file."""
    print(f"Processing {directory}:")  # noqa: T201
    try:
        command = [
            "terraform",
            "init",
            "-upgrade",
            "-backend=false",
        ]
        process = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True, cwd=directory
        )
        print(process.stdout)  # noqa: T201

        command = [
            "terraform",
            "providers",
            "lock",
            "-platform=darwin_amd64",
            "-platform=darwin_arm64",
            "-platform=linux_amd64",
        ]
        process = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True, cwd=directory
        )
        print(process.stdout)  # noqa: T201

    except subprocess.CalledProcessError as error:
        print(error.output)  # noqa: T201
        raise


def process_project_directory(project_directory: str, subfolders: List[str]) -> None:
    """Process project directory."""
    project_directory_abs = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", project_directory)
    for subfolder in subfolders:
        terraform_init_and_lock(os.path.join(project_directory_abs, subfolder))


if __name__ == "__main__":
    print(  # noqa: T201
        'INFO: If there are "error configuring S3 Backend" errors, then delete your local ".terraform/" folders.'
    )
    print("")  # noqa: T201

    process_project_directory("cdh-oss.bmw.cloud", ["api", "bootstrap", "resources", "security", "test"])
