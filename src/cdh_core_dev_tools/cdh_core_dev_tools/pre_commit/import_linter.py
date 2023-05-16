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
import io
import logging
import os
import sys
from pathlib import Path

from importlinter.cli import lint_imports

logging.basicConfig(stream=sys.stdout, level=os.environ.get("LOG_LEVEL", "INFO").upper())
LOG = logging.getLogger("import_linter")


def parse_args() -> argparse.Namespace:
    """Parse the commandline arguments."""
    parser = argparse.ArgumentParser(description="checks python imports")
    parser.add_argument("-f", "--folders", nargs="+", help="python packages relative to the base path", default=[])
    parser.add_argument(
        "--base-path",
        help="path to the root of this repository",
        default=Path(__file__).parents[3].absolute(),
    )
    parser.add_argument("--no-cache", action="store_true", help="disable caching")
    parser.add_argument("--debug", action="store_true", help="prints the output of lint-imports")

    return parser.parse_args()


def main() -> None:
    """Check that the import rules are kept."""
    args = parse_args()

    for folder in args.folders:
        sys.path.append(os.path.join(args.base_path, folder))

    # start lint-import and capture the output
    buffer = io.StringIO()
    default_stdout = sys.stdout
    sys.stdout = buffer
    result = lint_imports(no_cache=args.no_cache)
    sys.stdout = default_stdout
    buffer.seek(0)

    if result or args.debug:
        LOG.info(buffer.read())
    sys.exit(result)


if __name__ == "__main__":
    main()
