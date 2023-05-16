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
import json
import os.path
import sys

from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecGenerator
from cdh_core_api.app import openapi

OPENAPI_SPEC_FILENAME = "openapi.yml"


def create_openapi(args: argparse.Namespace) -> None:
    """Create a openapi spec based on the registered routes."""
    generator = OpenApiSpecGenerator.from_collector(
        openapi,
        remap_integration_timeout=args.remap_integration_timeout,
        partition=args.partition,
        region=args.region,
        lambda_arn=args.lambda_arn,
        accounts=args.accounts,
        org_ids=args.org_ids,
        options_arn=args.options_arn,
    )
    openapi_spec = generator.generate(args.url)
    if args.json:
        json.dump({"openapiSpec": openapi_spec}, sys.stdout)
    elif args.store:
        with open(args.path, "w", encoding="UTF-8") as file:
            file.write(openapi_spec)
    else:
        print(openapi_spec)  # noqa: T201


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Create an OpenAPI specification for the Core API")
    parser.add_argument(
        "--json", help="Output the spec as JSON suited for Terraform's external data protocol", action="store_true"
    )
    parser.add_argument("--store", help="Store the generated spec in cdh_core_api/openapi.yml", action="store_true")
    parser.add_argument(
        "--path",
        help="Output path if '--store' has been selected",
        default=os.path.abspath(os.path.join(os.path.dirname(__file__), OPENAPI_SPEC_FILENAME)),
    )
    parser.add_argument("--url", help="The URL for the open api spec", default="https://api.example.com")
    parser.add_argument("--partition", required=False)
    parser.add_argument("--region", required=False)
    parser.add_argument("--lambda-arn", required=False)
    parser.add_argument("--accounts", required=False)
    parser.add_argument("--org-ids", required=False)
    parser.add_argument("--options-arn", required=False)
    parser.add_argument(
        "--remap-integration-timeout",
        dest="remap_integration_timeout",
        help="Map gateway response to 429 statuscode",
        action="store_true",
    )
    args = parser.parse_args()
    if args.json and args.store:
        raise ValueError("Cannot combine options --json and --store")
    return args


if __name__ == "__main__":
    create_openapi(_parse_arguments())
