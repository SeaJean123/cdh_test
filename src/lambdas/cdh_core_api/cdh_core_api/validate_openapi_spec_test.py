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
import copy
import inspect
import json
import os
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional

import pytest
import yaml
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecGenerator
from cdh_core_api.api.validation import SchemaValidator
from cdh_core_api.app import coreapi
from cdh_core_api.app import openapi
from cdh_core_api.config_test import build_config
from cdh_core_api.config_test import build_validation_context
from openapi_spec_validator import openapi_v30_spec_validator

from cdh_core.enums.environment import Environment
from cdh_core.enums.http import HttpVerb


@pytest.fixture(name="generated_openapi_spec", scope="module")
def fixture_generated_openapi_spec() -> Dict[str, Any]:
    generator = OpenApiSpecGenerator.from_collector(openapi, remap_integration_timeout=False)
    return cast(Dict[str, Any], yaml.safe_load(generator.generate("https://api.example.com")))


def test_coreapi_openapi_spec_is_valid(generated_openapi_spec: Dict[str, Any]) -> None:
    # make sure we don't validate a mostly empty document
    assert "/api-info" in generated_openapi_spec["paths"]
    # enable when there are more endpoints
    assert len(generated_openapi_spec["components"]["schemas"]) > 10
    generated_openapi_spec = copy.deepcopy(generated_openapi_spec)  # for some reason validate modifies the spec
    openapi_v30_spec_validator.validate(generated_openapi_spec)  # type: ignore


EXAMPLE_DIR = os.path.join(os.path.dirname(__file__), "examples")
EXAMPLE_FILES = [
    os.path.join(EXAMPLE_DIR, file_name) for file_name in os.listdir(EXAMPLE_DIR) if file_name.endswith(".json")
]


def check_if_files_contain_examples_for_all_routes(
    generated_openapi_spec: Dict[str, Any], file_names: List[str]
) -> None:
    endpoints_with_example = set()
    for file_name in file_names:
        with open(file_name, "r", encoding="UTF-8") as file:
            example_data = json.load(file)
        endpoints_with_example.add((example_data["path"], example_data["method"]))

    endpoints_in_spec = set()
    for path, path_spec in generated_openapi_spec["paths"].items():
        for method in path_spec:
            if method in ["get", "post", "put", "delete", "patch"]:
                endpoints_in_spec.add((path, method.upper()))

    assert endpoints_with_example == endpoints_in_spec


def test_all_routes_have_at_least_one_example(generated_openapi_spec: Dict[str, Any]) -> None:
    check_if_files_contain_examples_for_all_routes(generated_openapi_spec, EXAMPLE_FILES)


def check_if_example_is_valid(file_name: str, prefix: Optional[str] = None) -> None:
    with open(file_name, "r", encoding="utf-8") as file:
        example_data = json.load(file)
    handler = coreapi.get_route(path=example_data["path"], method=HttpVerb[example_data["method"]])
    signature = inspect.signature(handler)
    if "path" in signature.parameters:
        validator = SchemaValidator(
            signature.parameters["path"].annotation,
            context=build_validation_context(build_config(environment=Environment("prod"), prefix=prefix), None),
        )
        validator(example_data["defaultPathParameters"])
    if "body" in signature.parameters:
        validator = SchemaValidator(
            signature.parameters["body"].annotation,
            context=build_validation_context(build_config(environment=Environment("prod"), prefix=prefix), None),
        )
        validator(example_data["body"])
    if "query" in signature.parameters:
        validator = SchemaValidator(
            signature.parameters["query"].annotation,
            context=build_validation_context(build_config(environment=Environment("prod"), prefix=prefix), None),
        )
        validator(example_data.get("defaultQueryParameters", {}))


@pytest.mark.parametrize("file_name", EXAMPLE_FILES)
def test_all_examples_contain_valid_requests(file_name: str) -> None:
    check_if_example_is_valid(file_name)
