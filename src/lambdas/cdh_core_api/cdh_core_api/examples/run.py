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
import json
import os
import sys
from argparse import ArgumentParser
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import quote_plus
from urllib.parse import urlparse

import requests
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

from cdh_core.enums.aws import Region

NEXT_PAGE_TOKEN_KEY = "nextPageToken"


def _ensure_https(url: str) -> str:
    return url if url.startswith("https://") else "https://" + url


def run_example(
    region: Optional[Region] = None, base_url: Optional[str] = None, example_folders: Optional[List[str]] = None
) -> None:
    """Execute a example file against the given API."""
    try:
        file_path, path_parameters, show_headers, query_string, verbose, region, base_url = _parse_arguments(
            region, base_url
        )
        base_url = _ensure_https(base_url)
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        if not example_folders:
            example_folders = [str(Path(__file__).parent)]
        else:
            for folder in example_folders:
                if Path.is_file(Path(folder)):
                    raise ValueError(f"This is not a valid path: {folder}")

        spec = _load_spec_from_file(file_path, example_folders)
        _make_request(
            spec=spec,
            path_parameters=path_parameters,
            show_headers=show_headers,
            query_string=query_string,
            verbose=verbose,
            base_url=base_url,
            region=region,
        )
    except Exception as error:  # pylint: disable=broad-except
        print(str(error), file=sys.stderr)  # noqa: T201
        sys.exit(1)


def _parse_arguments(
    region: Optional[Region], base_url: Optional[str]
) -> Tuple[str, Dict[str, str], bool, str, bool, Region, str]:
    parser = ArgumentParser("Send an example request to the Core API")
    parser.add_argument("file", type=str, help="File that contains the JSON data for the request body")
    parser.add_argument(
        "--param",
        type=str,
        help="Overwrite path parameters, e.g. id=test1234",
        action="append",
        dest="params",
        default=[],
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase verbosity level")
    parser.add_argument(
        "--query",
        type=str,
        help="Add a query string",
    )
    parser.add_argument("--stage", "-s", type=str, help="Specify the stage to use, e.g. dev")
    parser.add_argument("--show-headers", help="Print response headers", action="store_true")
    parser.add_argument("--region", help="AWS region", default=region, type=Region)
    parser.add_argument("--base-url", help="CDH base url", default=base_url)
    args = parser.parse_args()
    for assignment in args.params:
        if assignment.count("=") != 1:
            raise ValueError(f'Invalid value for --param: {assignment!r}. A valid value is for example "id=test1234".')
    path_parameters = dict([assignment.split("=") for assignment in args.params])
    if args.stage:
        path_parameters["stage"] = args.stage

    return args.file, path_parameters, args.show_headers, args.query, args.verbose, args.region, args.base_url


@dataclass(frozen=True)
class _RequestSpec:
    path: str
    method: str
    body: Optional[Dict[str, Any]]
    default_path_parameters: Dict[str, Any]
    default_query_parameters: Dict[str, Union[str, List[str]]]


def _load_spec_from_file(filepath: str, example_folders: List[str]) -> _RequestSpec:
    filename = os.path.basename(filepath)
    for folder in example_folders:
        new_file_path = Path(os.path.join(folder, filename))
        if new_file_path.is_file():
            try:
                with open(new_file_path, encoding="UTF-8") as file:
                    content = json.load(file)
            except (OSError, json.JSONDecodeError) as error:
                raise RuntimeError(f"Could not open file {new_file_path}: {error}") from error

            return _RequestSpec(
                path=content["path"],
                method=content["method"],
                body=content.get("body"),
                default_path_parameters=content.get("defaultPathParameters", {}),
                default_query_parameters=content.get("defaultQueryParameters", {}),
            )
    raise RuntimeError(f"Could not find a file named {filepath!r} in one of the following paths: {example_folders}")


def _make_request(  # pylint: disable=too-many-arguments, too-many-locals
    spec: _RequestSpec,
    path_parameters: Dict[str, Any],
    show_headers: bool,
    query_string: str,
    verbose: bool,
    base_url: str,
    region: Region,
) -> None:
    url = _build_url(spec, path_parameters, query_string, base_url)
    params = {} if query_string else spec.default_query_parameters
    max_number_of_fetches = 100
    if verbose:
        print(f"Sending {spec.method} request to {url}")  # noqa: T201
    auth = BotoAWSRequestsAuth(urlparse(url).hostname, region.value, "execute-api")
    result: Dict[str, Any] = {}
    next_page_token = None
    for _ in range(max_number_of_fetches):
        # mypy does not acknowledge an BotoAwsRequestsAuth as an AuthBase
        response = requests.request(spec.method, url, json=spec.body, params=params, auth=auth)  # type: ignore
        if verbose:
            print(  # noqa: T201
                "Response status code:",
                response.status_code,
                HTTPStatus(response.status_code).name,  # pylint: disable=no-member
            )
        if show_headers:
            for key, value in response.headers.items():
                print(f"{key}: {value}")  # noqa: T201
            print("")  # noqa: T201
        if response.text:
            _json_merge(result, response.json())

        if not (next_page_token := response.headers.get(NEXT_PAGE_TOKEN_KEY)):
            break
        params[NEXT_PAGE_TOKEN_KEY] = next_page_token

    if next_page_token:
        raise MaximumFetchesExceeded(f"Maximum number of fetches (={max_number_of_fetches}) exceeded")

    if result:
        print(json.dumps(result, indent=4))  # noqa: T201


def _json_merge(json_base: Dict[str, Any], json_add: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two JSON objects."""
    for key, value in json_add.items():
        if key in json_base:
            if isinstance(value, dict):
                json_base[key] = _json_merge(json_base[key], value)
            elif isinstance(value, list):
                json_base[key].extend(value)
            else:
                json_base[key] = value
        else:
            json_base[key] = value
    return json_base


def _build_url(spec: _RequestSpec, path_parameters: Dict[str, Any], query_string: str, base_url: str) -> str:
    parameters = {
        **spec.default_path_parameters,
        **path_parameters,
    }
    path = spec.path.format(**parameters)
    url = base_url + path
    if query_string:
        url += "?" + quote_plus(string=query_string, safe="&=")
    return url


if __name__ == "__main__":
    run_example()


class MaximumFetchesExceeded(Exception):
    """The maximum number of fetches has been exceeded."""
