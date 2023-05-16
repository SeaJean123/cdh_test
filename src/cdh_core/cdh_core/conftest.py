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
# pylint: disable=unused-import
import inspect
import sys
import warnings
from contextlib import suppress
from enum import Enum
from enum import EnumMeta
from types import ModuleType
from typing import Any
from typing import cast
from typing import Dict
from typing import Generator
from typing import List
from typing import Set
from typing import Tuple
from typing import Type
from unittest.mock import patch

import pytest
from _pytest.fixtures import SubRequest

import cdh_core.enums.accounts
import cdh_core.enums.aws
import cdh_core.enums.dataset_properties
import cdh_core.enums.environment
import cdh_core.enums.hubs
from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file import validate_config_file
from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.config.config_file_test import SIMPLE_CONFIG_FILE
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core_dev_tools.testing.fixtures import mock_athena
from cdh_core_dev_tools.testing.fixtures import mock_cloudformation
from cdh_core_dev_tools.testing.fixtures import mock_dynamodb
from cdh_core_dev_tools.testing.fixtures import mock_events
from cdh_core_dev_tools.testing.fixtures import mock_glue
from cdh_core_dev_tools.testing.fixtures import mock_iam
from cdh_core_dev_tools.testing.fixtures import mock_kms
from cdh_core_dev_tools.testing.fixtures import mock_logs
from cdh_core_dev_tools.testing.fixtures import mock_s3
from cdh_core_dev_tools.testing.fixtures import mock_ses
from cdh_core_dev_tools.testing.fixtures import mock_sns
from cdh_core_dev_tools.testing.fixtures import mock_sqs
from cdh_core_dev_tools.testing.fixtures import mock_stepfunctions

_real_region = cdh_core.enums.aws.Region
_real_partition = cdh_core.enums.aws.Partition
_real_environment = cdh_core.enums.environment.Environment
_real_hub = cdh_core.enums.hubs.Hub
_real_business_object = cdh_core.enums.dataset_properties.BusinessObject
_real_dataset_purpose = cdh_core.enums.dataset_properties.DatasetPurpose
_real_dataset_external_link_type = cdh_core.enums.dataset_properties.ExternalLinkType
_real_account_purpose = cdh_core.enums.accounts.AccountPurpose
_real_affiliation = cdh_core.enums.accounts.Affiliation


@pytest.fixture()
def mock_config_file(request: SubRequest) -> Generator[ConfigFile, None, None]:
    """
    Patch configurable Enums and return the patched ConfigFile.

    If no configuration is provided the default test configuration will be used.
    Attention: this fixture does not work with DataClassJsonCDHMixin, see: #62.
    """
    config_file = (
        request.param if hasattr(request, "param") and isinstance(request.param, ConfigFile) else SIMPLE_CONFIG_FILE
    )
    validate_config_file(config_file)
    with patch.object(ConfigFileLoader, "get_config") as get_config:
        get_config.return_value = config_file

        real_to_mock = _build_mock_classes_from_config_file(config_file)
        original_attributes = _mock_objects_in_modules({id(real): mock for real, mock in real_to_mock.items()})

        yield config_file

        for module, attribute_name, attribute_value in original_attributes:
            setattr(module, attribute_name, attribute_value)


def _build_mock_classes_from_config_file(config_file: ConfigFile) -> Dict[Type[Any], Type[Any]]:
    return {
        _real_region: cast(
            Type[cdh_core.enums.aws.RegionMixin],
            Enum(
                cdh_core.enums.aws.__name__ + ".Region",
                {instance: entry.value for instance, entry in config_file.region.instances.items()},
                type=cdh_core.enums.aws.RegionMixin,
                module=cdh_core.enums.aws.__name__,
            ),
        ),
        _real_partition: cast(
            Type[cdh_core.enums.aws.PartitionMixin],
            Enum(
                cdh_core.enums.aws.__name__ + ".Partition",
                {instance: entry.value for instance, entry in config_file.partition.instances.items()},
                type=cdh_core.enums.aws.PartitionMixin,
                module=cdh_core.enums.aws.__name__,
            ),
        ),
        _real_environment: cast(
            Type[cdh_core.enums.environment.EnvironmentMixin],
            Enum(
                cdh_core.enums.environment.__name__ + ".Environment",
                {instance: entry.value for instance, entry in config_file.environment.instances.items()},
                type=cdh_core.enums.environment.EnvironmentMixin,
                module=cdh_core.enums.environment.__name__,
            ),
        ),
        _real_hub: cast(
            Type[cdh_core.enums.hubs.HubMixin],
            Enum(
                cdh_core.enums.hubs.__name__ + ".Hub",
                {instance: entry.value for instance, entry in config_file.hub.instances.items()},
                type=cdh_core.enums.hubs.HubMixin,
                module=cdh_core.enums.hubs.__name__,
            ),
        ),
        _real_business_object: cast(
            Type[cdh_core.enums.dataset_properties.BusinessObjectMixin],
            Enum(
                cdh_core.enums.dataset_properties.__name__ + ".BusinessObject",
                {instance: entry.value for instance, entry in config_file.business_object.instances.items()},
                type=cdh_core.enums.dataset_properties.BusinessObjectMixin,
                module=cdh_core.enums.dataset_properties.__name__,
            ),
        ),
        _real_dataset_purpose: cast(
            Type[cdh_core.enums.dataset_properties.DatasetPurposeMixin],
            Enum(
                cdh_core.enums.dataset_properties.__name__ + ".DatasetPurpose",
                {instance: entry.value for instance, entry in config_file.dataset_purpose.instances.items()},
                type=cdh_core.enums.dataset_properties.DatasetPurposeMixin,
                module=cdh_core.enums.dataset_properties.__name__,
            ),
        ),
        _real_dataset_external_link_type: cast(
            Type[cdh_core.enums.dataset_properties.ExternalLinkTypeMixin],
            Enum(
                cdh_core.enums.dataset_properties.__name__ + ".ExternalLinkType",
                {instance: entry.value for instance, entry in config_file.dataset_external_link_type.instances.items()},
                type=cdh_core.enums.dataset_properties.ExternalLinkTypeMixin,
                module=cdh_core.enums.dataset_properties.__name__,
            ),
        ),
        _real_account_purpose: cast(
            Type[cdh_core.enums.accounts.AccountPurpose],
            Enum(
                cdh_core.enums.accounts.__name__ + ".AccountPurpose",
                {instance: entry.value for instance, entry in config_file.account.purpose.instances.items()},
                type=cdh_core.enums.accounts.AccountPurposeMixin,
                module=cdh_core.enums.accounts.__name__,
            ),
        ),
        _real_affiliation: cast(
            Type[cdh_core.enums.accounts.AffiliationMixin],
            Enum(
                cdh_core.enums.accounts.__name__ + ".Affiliation",
                {instance: entry.value for instance, entry in config_file.affiliation.instances.items()},
                type=cdh_core.enums.accounts.AffiliationMixin,
                module=cdh_core.enums.accounts.__name__,
            ),
        ),
    }


def _mock_objects_in_modules(real_id_to_mock: Dict[int, Any]) -> Set[Tuple[Any, str, Any]]:
    original_attributes = set()
    for module_name, module in list(sys.modules.items()):
        if not module_name or not module:
            continue
        for attribute_name, attribute_instance in _get_attributes(module):
            if mock := real_id_to_mock.get(id(attribute_instance)):
                original_attributes.add((module, attribute_name, attribute_instance))
                setattr(module, attribute_name, mock)
            else:
                _test_for_dynamic_internal_enum(attribute_instance)

    return original_attributes


def _test_for_dynamic_internal_enum(instance: Any) -> None:
    """Raise an exception if the instance is an Enum from this package and is dynamically created."""
    if isinstance(instance, EnumMeta) and instance.__module__.startswith(__name__.split(".", maxsplit=1)[0]):
        try:
            inspect.getsource(instance)
        except OSError:
            raise Exception(  # pylint: disable=raise-missing-from
                f"The following class {instance} is not mocked, based in: {inspect.getmodule(instance)}"
            )


def _get_attributes(module: ModuleType) -> List[Tuple[str, Any]]:
    result: List[Tuple[str, Any]] = []
    for attribute_name in dir(module):
        with suppress(AttributeError, ModuleNotFoundError):
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                result.append((attribute_name, getattr(module, attribute_name)))
    return result
