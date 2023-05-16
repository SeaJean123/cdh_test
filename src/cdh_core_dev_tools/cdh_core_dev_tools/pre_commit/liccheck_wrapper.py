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
import logging
import os
import subprocess
import sys
import tempfile

logging.basicConfig(stream=sys.stdout, level=os.environ.get("LOG_LEVEL", "INFO").upper())
LOG = logging.getLogger("liccheck_wrapper")


def log(msg: str, level: int) -> None:
    """Write a multiline string to the log."""
    for line in msg.splitlines():
        LOG.log(msg=line, level=level)


def main() -> None:
    """Check if the used packages licences conform to our whitelist."""
    debug = False
    try:
        sys.argv.remove("--debug")
        debug = True
    except ValueError:
        pass
    if len(sys.argv) < 2:
        LOG.error("The filepath to the requirements.txt is missing")
        sys.exit(1)
    for requirements in sys.argv[1:]:
        with tempfile.NamedTemporaryFile() as tmp_file:
            with open(requirements, "r", encoding="utf-8") as original_requirements_handler:
                lines = original_requirements_handler.readlines()
                tmp_file.writelines(
                    [
                        bytes(line, encoding=original_requirements_handler.encoding)
                        for line in lines
                        if "file:" not in line
                    ]
                )
                tmp_file.flush()
            process = subprocess.run(
                ["liccheck", "-s", "oss-license-configuration.ini", "-r", tmp_file.name],
                capture_output=True,
                check=False,
            )
        stdout = process.stdout.decode().strip()
        stderr = process.stderr.decode().strip()
        if process.returncode:
            LOG.info(f"Checking: {requirements}")
            log(stdout, logging.INFO)
            log(stderr, logging.ERROR)
            sys.exit(process.returncode)
        elif debug:
            LOG.info(f"Checking: {requirements}")
            log(stdout, logging.INFO)
            log(stderr, logging.ERROR)


if __name__ == "__main__":
    main()
