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
"""Basic templating idea.

- Only use jinja templating features when TF is missing functionality (eg. dynamic creation of provider)
  - Jinja context variables should be used sparingly. Use tf data definitions instead.
  - Jinja should not be used on terraform module/ level.
- Templating functionality will be extended only when required (removal of secondary regions, multiple env support,..)
- To support easier development Jinja templating must be callable standalone.

Example usage:
 $ export CDH_CORE_CONFIG_FILE_PATH=/XXX/cdh-core/infrastructure/cdh-oss.bmw.cloud/cdh-core-config-test-deployment.yaml
 $ python render.py --partition "aws" --account-purpose api
 $ python render.py --partition "aws" --account-purpose api --output-path "path/relative/to/cwd"
 $ python render.py --partition "aws" --account-purpose api --output-path "/absolute/path" \
      --template-directory "path/relative/to/the/parent/folder/of/this/file's/folder"
 $ TF_DATA_DIR=/tmp/terraform_validate terraform init && terraform apply
"""
import argparse
import glob
import os
from pathlib import Path
from typing import Any
from typing import Dict

from jinja2 import Environment as JEnvironment
from jinja2 import FileSystemLoader

from cdh_core.entities.account_store import AccountStore
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub


def render(template: str, context: Dict[str, Any]) -> str:
    """Render Jinja templates."""
    path, filename = os.path.split(template)
    return (
        JEnvironment(
            loader=FileSystemLoader(path or "./", encoding="utf-8", followlinks=False),
            lstrip_blocks=True,
            trim_blocks=True,
            keep_trailing_newline=True,
        )
        .get_template(filename)
        .render(context)
    )


def _parse_arguments() -> argparse.Namespace:
    """Parse arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--template-directory",
        default="cdh-oss.bmw.cloud",
    )
    parser.add_argument(
        "--output-path",
        default=None,
    )
    parser.add_argument(
        "--skip-config",
        help="do not render the backend config and vars",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--check-only",
        help="check generated .tf files are up-to-date",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--partition",
        choices=[enum.value for enum in Partition],
        required=True,
    )
    parser.add_argument(
        "--account-purpose",
        choices=[enum.value for enum in AccountPurpose],
        required=True,
    )
    args = parser.parse_args()
    return args


class TemplateRenderer:
    """Generate or validate terraform infrastructure code."""

    def __init__(self, args: argparse.Namespace):
        self.check_only = args.check_only
        self.partition = Partition(args.partition)
        self.account_purpose = AccountPurpose(args.account_purpose)
        self.region_primary = Region.preferred(partition=self.partition)

        abs_base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
        self.template_path = os.path.join(abs_base_path, args.template_directory)
        self.input_path = os.path.join(self.template_path, self.account_purpose.value)
        self.output_path = args.output_path or self.input_path
        self.base_path = os.path.relpath(abs_base_path, self.output_path)
        if not os.path.isdir(self.template_path):
            raise FileNotFoundError(f"Infrastructure path {self.template_path } does not exist.")

    def render_terraform_files(self) -> None:
        """Render terraform files from root-level templates for a given account purpose."""
        for template in glob.glob(os.path.join(self.input_path, "*.jinja")):
            context = {
                "region_primary": self.region_primary,
                "regions": {region.name: region.value for region in Region if region.partition is self.partition},
                "base_path": self.base_path,
            }
            rendered_filename = Path(template).with_suffix(".tf").name
            rendered_path = Path(os.path.join(self.output_path, rendered_filename))
            self.render_file(template, rendered_path, context)

    def render_config_files(self) -> None:
        """Render terraform backend config files for a given account purpose for every available environment."""
        for template in glob.glob(os.path.join(self.input_path, "vars", "*.jinja")):
            for environment in list(Environment):
                accounts = AccountStore().query_accounts(
                    environments=environment,
                    account_purposes=self.account_purpose,
                    partitions=self.partition,
                    hubs=Hub.default(self.partition) if self.account_purpose is not AccountPurpose("api") else None,
                )

                for account in accounts:
                    context = {
                        "region_primary": self.region_primary,
                        "account_id": account.id,
                        "environment": environment.value,
                    }
                    rendered_filename = Path(template).stem
                    rendered_path = Path(os.path.join(self.output_path, "vars", account.id, rendered_filename))
                    self.render_file(template, rendered_path, context)

    def render_file(self, template: str, rendered_filename: Path, context: Dict[str, Any]) -> None:
        """Render a given jinja template or check if the current template and file matches."""
        if self.check_only:
            with open(rendered_filename, "r", encoding="utf-8") as file:
                if file.read() != render(template, context):
                    raise RuntimeError(
                        f"The checked-in terraform infrastructure for " f"{rendered_filename} is not up-to-date."
                    )
        else:
            os.makedirs(os.path.dirname(rendered_filename), exist_ok=True)
            with open(rendered_filename, "w", encoding="utf-8") as file:
                file.write(render(template, context))


if __name__ == "__main__":
    argz = _parse_arguments()
    renderer = TemplateRenderer(argz)
    renderer.render_terraform_files()
    if not argz.skip_config:
        renderer.render_config_files()
