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
import argparse
import concurrent
import logging
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from glob import glob
from pathlib import Path
from typing import List
from typing import Set
from typing import Tuple

logging.basicConfig(stream=sys.stdout, level=os.environ.get("LOG_LEVEL", "INFO").upper())
LOG = logging.getLogger("lock_dependencies")


def _find_requirements_in_files(base_path: str) -> Tuple[str, Set[str]]:
    root_requirements_in = os.path.join(base_path, "requirements.in")
    all_requirements_in = set(glob(base_path + "/**/requirements.in", recursive=True))
    if root_requirements_in in all_requirements_in:
        all_requirements_in.remove(root_requirements_in)
    return root_requirements_in, all_requirements_in


def _start_sub_process(command_args: List[str]) -> bool:
    for attempt in range(3):
        LOG.info(f"Starting subprocess #{attempt}: {' '.join(command_args)}")
        process = subprocess.run(command_args, capture_output=True, check=False)
        stdout = process.stdout.decode().strip()
        stderr = process.stderr.decode().strip()
        if process.returncode == 0:
            return True
        LOG.warning(f"Process returned with {process.returncode}")
        LOG.warning(f"Stdout: {stdout}")
        LOG.warning(f"Stderr: {stderr}")
    return False


def validate_requirement_files(base_path: str) -> None:
    """Validate all requirements.txt und .in files."""

    def validate_requirements_in_file(requirements_in: str) -> bool:
        package_path = os.path.dirname(os.path.realpath(requirements_in))
        with tempfile.NamedTemporaryFile() as tmp_file:
            original_requirements_in = requirements_in
            with open(original_requirements_in, "r", encoding="utf-8") as original_requirements_in_handler:
                lines = [
                    bytes(line, encoding=original_requirements_in_handler.encoding)
                    for line in original_requirements_in_handler.readlines()
                    if not line.startswith("-")
                ]
            tmp_file.writelines(lines)
            return _start_sub_process(["pip-extra-reqs", f"--requirements-file={tmp_file.name}", package_path])

    _, all_requirements_in = _find_requirements_in_files(base_path)
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(validate_requirements_in_file, requirements_in=requirements_in)
            for requirements_in in all_requirements_in
        ]
        concurrent.futures.as_completed(futures)
        if not all(future.result() for future in futures):
            LOG.critical("At least one requirement was not ideal, please fix it.")


def _remove_local_file_reference(requirements_file: str) -> None:
    with open(requirements_file, "r+", encoding="utf-8") as requirements_handler:
        lines = requirements_handler.readlines()
        requirements_handler.seek(0)
        requirements_handler.writelines([line for line in lines if not line.startswith("file:")])
        requirements_handler.truncate()


def generate_requirements_files(base_path: str) -> None:
    """Generate requirements.txt from requirements.in."""
    dev_requirements_in = os.path.join(base_path, "requirements-dev.in")
    root_requirements_in, all_requirements_in = _find_requirements_in_files(base_path)
    root_requirements_txt = root_requirements_in.replace(".in", ".txt")
    with open(root_requirements_txt, "w", encoding="utf-8"):
        pass  # empty file to avoid unnecessary version constraints
    for requirements_in in [root_requirements_in, dev_requirements_in]:
        if Path(requirements_in).is_file():
            root_command = [
                "pip-compile",
                requirements_in,
                "--output-file",
                requirements_in.replace(".in", ".txt"),
                "--no-annotate",
                "--no-header",
                "--upgrade",
                "--allow-unsafe",
            ]
            _start_sub_process(root_command)
    _remove_local_file_reference(root_requirements_txt)

    # Due to rate-limiting max_workers=3 is used
    with ThreadPoolExecutor(max_workers=3) as executor:
        concurrent.futures.as_completed(
            [
                executor.submit(
                    _start_sub_process,
                    command_args=[
                        "pip-compile",
                        requirements_in,
                        "--output-file",
                        requirements_in.replace(".in", ".txt"),
                        "--no-annotate",
                        "--no-header",
                        "--allow-unsafe",
                    ],
                )
                for requirements_in in all_requirements_in
            ]
        )
    for requirements_in in all_requirements_in:
        _remove_local_file_reference(requirements_in.replace(".in", ".txt"))


def ensure_complete_references_in_root_requirements(base_path: str) -> None:
    """Check if the root requirements.in contains a link to every other requirements.in in the repository."""
    root_requirements_in, all_requirements_in = _find_requirements_in_files(base_path)
    listed_requirements_in_files = set()
    with open(root_requirements_in, "r", encoding="utf-8") as root_requirements_in_handler:
        for line in root_requirements_in_handler.readlines():
            if line.startswith("-r "):
                listed_requirements_in_files.add(
                    os.path.join(base_path, line.replace("-r ", "").replace(os.linesep, ""))
                )
    if not all_requirements_in.issubset(listed_requirements_in_files):
        LOG.error(
            f"The required file ({root_requirements_in}) is missing at least the following entry:"
            f" {', '.join(all_requirements_in.difference(listed_requirements_in_files))}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="manages the python dependencies for the whole project")
    parser.add_argument("--validate", action="store_true", help="only validate requirements files")
    parser.add_argument("--lock", action="store_true", help="lock and upgrade requirements")
    parser.add_argument(
        "--base-path",
        help="path to the root of this repository",
        default=os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../")),
    )
    args = parser.parse_args()
    if args.lock == args.validate:  # this is the default behaviour
        generate_requirements_files(args.base_path)
        ensure_complete_references_in_root_requirements(args.base_path)
        validate_requirement_files(args.base_path)
    elif args.lock:
        generate_requirements_files(args.base_path)
    elif args.validate:
        ensure_complete_references_in_root_requirements(args.base_path)
        validate_requirement_files(args.base_path)
