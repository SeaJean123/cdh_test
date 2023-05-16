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
# pylint: disable=too-many-lines
from dataclasses import replace
from typing import Optional
from typing import Type

import marshmallow_dataclass
import pytest
from marshmallow import ValidationError

from cdh_core.config.config_file import AWSRole
from cdh_core.config.config_file import ConfigFile
from cdh_core_dev_tools.testing.builder import Builder

PARTITION_AWS = "aws"
PARTITION_AWS_CN = "aws-cn"
REGION_VALUE_EU_CENTRAL_1 = "eu-central-1"
REGION_KEY_EU_CENTRAL_1 = "eu_central_1"
REGION_VALUE_US_EAST_1 = "us-east-1"
REGION_KEY_US_EAST_1 = "us_east_1"
REGION_VALUE_CN_NORTH_1 = "cn-north-1"
REGION_KEY_CN_NORTH_1 = "cn_north_1"

SIMPLE_CONFIG_FILE = ConfigFile(
    business_object=ConfigFile.BusinessObject(
        {
            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                value=Builder.build_random_string(), friendly_name=Builder.build_random_string()
            )
        }
    ),
    dataset_purpose=ConfigFile.DatasetPurpose(
        {
            Builder.build_random_string(): ConfigFile.DatasetPurpose.Entry(
                value=Builder.build_random_string(),
                friendly_name=Builder.build_random_string(),
            )
        }
    ),
    dataset_external_link_type=ConfigFile.DatasetExternalLinkType(
        {
            Builder.build_random_string(): ConfigFile.DatasetExternalLinkType.Entry(
                value=Builder.build_random_string(),
                friendly_name=Builder.build_random_string(),
            )
        }
    ),
    partition=ConfigFile.Partition(
        instances={
            Builder.build_random_string(): ConfigFile.Partition.Entry(
                value=PARTITION_AWS,
                friendly_name=Builder.build_random_string(),
                default_hub="global",
                default_region=REGION_VALUE_US_EAST_1,
                regions={REGION_VALUE_US_EAST_1, REGION_VALUE_EU_CENTRAL_1},
            ),
        },
        default_value=PARTITION_AWS,
    ),
    region=ConfigFile.Region(
        instances={
            Builder.build_random_string(): ConfigFile.Region.Entry(
                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
            ),
            Builder.build_random_string(): ConfigFile.Region.Entry(
                value=REGION_VALUE_EU_CENTRAL_1, friendly_name=Builder.build_random_string()
            ),
        }
    ),
    environment=ConfigFile.Environment(
        instances={
            Builder.build_random_string(): ConfigFile.Environment.Entry(
                value="prod", friendly_name="Production", domain={PARTITION_AWS: "prod.example.com"}
            )
        }
    ),
    hub=ConfigFile.Hub(
        instances={
            Builder.build_random_string(): ConfigFile.Hub.Entry(
                value="global", environments={"prod"}, regions={REGION_VALUE_EU_CENTRAL_1}
            )
        }
    ),
    account=ConfigFile.Account(
        instances_per_purpose={
            "api": ConfigFile.Account.PurposeEntry(
                account_instances={
                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                        id="123456789012",
                        partition="aws",
                        environment="prod",
                    ),
                },
            ),
            "security": ConfigFile.Account.PurposeEntry(
                account_instances={
                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                        id="123456789012", partition="aws", environment="prod", hub="global"
                    ),
                },
            ),
        },
    ),
    aws_service=ConfigFile.AWSService(
        iam=ConfigFile.AWSService.IAM(ConfigFile.AWSService.IAM.ConfiguredLimits(max_managed_policies_per_role=15)),
        s3=ConfigFile.AWSService.S3(allowed_origins=["https://www.example.com"]),
    ),
    affiliation=ConfigFile.Affiliation(
        {
            Builder.build_random_string(): ConfigFile.Affiliation.Entry(
                value=Builder.build_random_string(),
                friendly_name=Builder.build_random_string(),
                access_management=Builder.get_random_bool(),
            )
        }
    ),
    stage_by_origin=ConfigFile.StageByOrigin(instances={Builder.build_random_url(): "int"}),
)


CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS = replace(
    SIMPLE_CONFIG_FILE,
    partition=ConfigFile.Partition(
        instances={
            Builder.build_random_string(): ConfigFile.Partition.Entry(
                value=PARTITION_AWS,
                friendly_name=Builder.build_random_string(),
                default_hub="global",
                default_region=REGION_VALUE_EU_CENTRAL_1,
                regions={REGION_VALUE_EU_CENTRAL_1, REGION_VALUE_US_EAST_1},
            ),
            Builder.build_random_string(): ConfigFile.Partition.Entry(
                value=PARTITION_AWS_CN,
                friendly_name=Builder.build_random_string(),
                default_hub="cn",
                default_region=REGION_VALUE_CN_NORTH_1,
                regions={REGION_VALUE_CN_NORTH_1},
            ),
        },
        default_value=PARTITION_AWS,
    ),
    environment=ConfigFile.Environment(
        {
            Builder.build_random_string(): ConfigFile.Environment.Entry(
                value="prod",
                friendly_name="Production",
                domain={
                    PARTITION_AWS: Builder.build_random_string(),
                    PARTITION_AWS_CN: Builder.build_random_string(),
                },
            ),
            Builder.build_random_string(): ConfigFile.Environment.Entry(
                value="dev",
                friendly_name="Development",
                domain={
                    PARTITION_AWS: Builder.build_random_string(),
                    PARTITION_AWS_CN: Builder.build_random_string(),
                },
                is_test_environment=True,
            ),
        }
    ),
    region=ConfigFile.Region(
        instances={
            REGION_KEY_EU_CENTRAL_1: ConfigFile.Region.Entry(
                value=REGION_VALUE_EU_CENTRAL_1, friendly_name=Builder.build_random_string()
            ),
            REGION_KEY_CN_NORTH_1: ConfigFile.Region.Entry(
                value=REGION_VALUE_CN_NORTH_1, friendly_name=Builder.build_random_string()
            ),
            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
            ),
        }
    ),
    hub=ConfigFile.Hub(
        instances={
            Builder.build_random_string(): ConfigFile.Hub.Entry(
                value="global",
                environments={"dev", "prod"},
                regions={REGION_VALUE_EU_CENTRAL_1},
            ),
            Builder.build_random_string(): ConfigFile.Hub.Entry(
                value="dev", environments={"dev"}, regions={REGION_VALUE_US_EAST_1}
            ),
            Builder.build_random_string(): ConfigFile.Hub.Entry(
                value="cn",
                environments={"prod"},
                regions={REGION_VALUE_CN_NORTH_1},
            ),
        }
    ),
    account=ConfigFile.Account(
        instances_per_purpose={
            "api": ConfigFile.Account.PurposeEntry(
                account_instances={
                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                        id="123456789012",
                        partition="aws",
                        environment="prod",
                    ),
                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                        id="987654321098",
                        partition="aws-cn",
                        environment="prod",
                    ),
                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                        id="123412341234",
                        partition="aws",
                        environment="dev",
                    ),
                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                        id="432143214321",
                        partition="aws-cn",
                        environment="dev",
                    ),
                },
            ),
            "security": ConfigFile.Account.PurposeEntry(
                account_instances={
                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                        id="123456789012", partition="aws", environment="prod", hub="global"
                    ),
                },
            ),
        },
    ),
)


class TestConfigFile:
    @pytest.mark.parametrize(
        "to_test,expected_exception,expected_error_message",
        [
            pytest.param(SIMPLE_CONFIG_FILE, None, None, id="good case"),
            pytest.param(CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS, None, None, id="more complex good case"),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            Builder.build_random_string(13): ConfigFile.BusinessObject.Entry(
                                value=Builder.build_random_string(), friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                None,
                None,
                id="business object name key is long but not checked",
            ),
            pytest.param(
                replace(SIMPLE_CONFIG_FILE, region=ConfigFile.Region(instances={})),
                ValidationError,
                "{'region': {'_schema': ['Dict cannot be empty.']}}",
                id="region cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    region=ConfigFile.Region(
                        instances={
                            Builder.build_random_string(): ConfigFile.Region.Entry(
                                value=REGION_VALUE_EU_CENTRAL_1, friendly_name=Builder.build_random_string()
                            ),
                            Builder.build_random_string(): ConfigFile.Region.Entry(
                                value=REGION_VALUE_EU_CENTRAL_1, friendly_name=Builder.build_random_string()
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique.",
                id="region values have to be unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    region=ConfigFile.Region(
                        instances={
                            Builder.build_random_string(): ConfigFile.Region.Entry(
                                value=REGION_VALUE_EU_CENTRAL_1, friendly_name="foo"
                            ),
                            Builder.build_random_string(): ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'friendly_name' are not unique.",
                id="region values have to be unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    region=ConfigFile.Region(
                        instances={
                            Builder.build_random_string(): ConfigFile.Region.Entry(
                                value="foo", friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                "The given region 'foo' is not a valid AWS region.",
                id="region value has to be valid",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    region=ConfigFile.Region(
                        instances={
                            "global": ConfigFile.Region.Entry(value="foo", friendly_name=Builder.build_random_string())
                        }
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="region name has to be none reserved word",
            ),
            pytest.param(
                replace(SIMPLE_CONFIG_FILE, dataset_purpose=ConfigFile.DatasetPurpose({})),
                ValidationError,
                "{'dataset_purpose': {'_schema': ['Dict cannot be empty.']}}",
                id="dataset purpose cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    dataset_purpose=ConfigFile.DatasetPurpose(
                        {
                            Builder.build_random_string(): ConfigFile.DatasetPurpose.Entry(
                                value="foo", friendly_name=Builder.build_random_string()
                            ),
                            Builder.build_random_string(): ConfigFile.DatasetPurpose.Entry(
                                value="foo", friendly_name=Builder.build_random_string()
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique.",
                id="dataset purpose value is duplicate",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    dataset_purpose=ConfigFile.DatasetPurpose(
                        {
                            "global": ConfigFile.DatasetPurpose.Entry(
                                value=Builder.build_random_string(), friendly_name=Builder.build_random_string()
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="dataset purpose key cannot be reserved word",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    dataset_purpose=ConfigFile.DatasetPurpose(
                        {
                            Builder.build_random_string(): ConfigFile.DatasetPurpose.Entry(
                                value=Builder.build_random_string(), friendly_name="foo"
                            ),
                            Builder.build_random_string(): ConfigFile.DatasetPurpose.Entry(
                                value=Builder.build_random_string(), friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'friendly_name' are not unique.",
                id="dataset purpose friendly name is duplicate",
            ),
            pytest.param(
                replace(SIMPLE_CONFIG_FILE, dataset_external_link_type=ConfigFile.DatasetExternalLinkType({})),
                ValidationError,
                "{'dataset_external_link_type': {'_schema': ['Dict cannot be empty.']}}",
                id="dataset external link type cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    dataset_external_link_type=ConfigFile.DatasetExternalLinkType(
                        {
                            Builder.build_random_string(): ConfigFile.DatasetExternalLinkType.Entry(
                                value="foo", friendly_name=Builder.build_random_string()
                            ),
                            Builder.build_random_string(): ConfigFile.DatasetExternalLinkType.Entry(
                                value="foo", friendly_name=Builder.build_random_string()
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique.",
                id="dataset external link type value is duplicate",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    dataset_external_link_type=ConfigFile.DatasetExternalLinkType(
                        {
                            "global": ConfigFile.DatasetExternalLinkType.Entry(
                                value=Builder.build_random_string(), friendly_name=Builder.build_random_string()
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="dataset external link type key cannot be reserved word",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    dataset_external_link_type=ConfigFile.DatasetExternalLinkType(
                        {
                            Builder.build_random_string(): ConfigFile.DatasetExternalLinkType.Entry(
                                value=Builder.build_random_string(), friendly_name="foo"
                            ),
                            Builder.build_random_string(): ConfigFile.DatasetExternalLinkType.Entry(
                                value=Builder.build_random_string(), friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'friendly_name' are not unique.",
                id="dataset external link type friendly name is duplicate",
            ),
            pytest.param(
                replace(SIMPLE_CONFIG_FILE, business_object=ConfigFile.BusinessObject({})),
                ValidationError,
                "{'business_object': {'_schema': ['Dict cannot be empty.']}}",
                id="business object cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value=Builder.build_random_string(13), friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                "{'value': ['String does not match expected pattern.']}",
                id="business object value is too long",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value="foo", friendly_name=Builder.build_random_string()
                            ),
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value="foo", friendly_name=Builder.build_random_string()
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique.",
                id="business object value is duplicate",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value=Builder.build_random_string(), friendly_name="foo"
                            ),
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value=Builder.build_random_string(), friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'friendly_name' are not unique.",
                id="business object friendly name is duplicate",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            "global": ConfigFile.BusinessObject.Entry(
                                value=Builder.build_random_string(), friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="business object key cannot be reserved word",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value="foo-bar", friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "String does not match expected pattern.",
                id="business object must not contain hyphens",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value="foo_bar", friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "String does not match expected pattern.",
                id="business object must not contain underscore",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    business_object=ConfigFile.BusinessObject(
                        {
                            Builder.build_random_string(): ConfigFile.BusinessObject.Entry(
                                value="FOO", friendly_name="foo"
                            ),
                        }
                    ),
                ),
                ValidationError,
                "String does not match expected pattern.",
                id="business object must not contain uppercase letters",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(instances={}, default_value=Builder.build_random_string()),
                ),
                ValidationError,
                "Dict cannot be empty.",
                id="partition cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value="aws",
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_EU_CENTRAL_1,
                                regions={REGION_VALUE_EU_CENTRAL_1},
                            ),
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value="aws",
                                friendly_name=Builder.build_random_string(),
                                default_hub="cn",
                                default_region=REGION_VALUE_EU_CENTRAL_1,
                                regions={REGION_VALUE_EU_CENTRAL_1},
                            ),
                        },
                        default_value="aws",
                    ),
                    hub=ConfigFile.Hub(
                        instances={
                            Builder.build_random_string(): ConfigFile.Hub.Entry(
                                value="global", environments={"prod"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                            Builder.build_random_string(): ConfigFile.Hub.Entry(
                                value="cn",
                                environments={"dev"},
                                regions={REGION_VALUE_CN_NORTH_1},
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique.",
                id="partition value is duplicate",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value=PARTITION_AWS,
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_EU_CENTRAL_1,
                                regions={REGION_VALUE_EU_CENTRAL_1, REGION_VALUE_US_EAST_1},
                            ),
                        },
                        default_value="non-existent",
                    ),
                ),
                ValidationError,
                "The default partition 'non-existent' is not listed in the available partitions",
                id="default partition is not in available partitions",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value=PARTITION_AWS,
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_US_EAST_1,
                                regions={REGION_VALUE_EU_CENTRAL_1},
                            ),
                        },
                        default_value=PARTITION_AWS,
                    ),
                    region=ConfigFile.Region(
                        instances={
                            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                f"The default region {REGION_VALUE_US_EAST_1!r} is not in the corresponding regions.",
                id="partition default region is not in the regions",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value=PARTITION_AWS,
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_EU_CENTRAL_1,
                                regions={REGION_VALUE_EU_CENTRAL_1},
                            ),
                        },
                        default_value=PARTITION_AWS,
                    ),
                    region=ConfigFile.Region(
                        instances={
                            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                f"Partition: the default_region {REGION_VALUE_EU_CENTRAL_1!r} is not configured as region value.",
                id="partition default region is not in the global regions",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    region=ConfigFile.Region(
                        instances={
                            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
                            ),
                            REGION_KEY_EU_CENTRAL_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_EU_CENTRAL_1, friendly_name=Builder.build_random_string()
                            ),
                            "outlier": ConfigFile.Region.Entry(
                                value="us-west-1", friendly_name=Builder.build_random_string()
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Region: region 'us-west-1' is not part of any partition",
                id="region does not belong to any partition",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value=PARTITION_AWS,
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_US_EAST_1,
                                regions={REGION_VALUE_US_EAST_1, REGION_VALUE_EU_CENTRAL_1},
                            ),
                        },
                        default_value=PARTITION_AWS,
                    ),
                    region=ConfigFile.Region(
                        instances={
                            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                f"Partition: regions entry {REGION_VALUE_EU_CENTRAL_1!r} is not configured at region values.",
                id="partition regions is not valid",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value="foo",
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_US_EAST_1,
                                regions={REGION_VALUE_US_EAST_1},
                            ),
                        },
                        default_value="foo",
                    ),
                    region=ConfigFile.Region(
                        instances={
                            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                "The given partition 'foo' is not a valid AWS partition.",
                id="partition value is not valid",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value=PARTITION_AWS_CN,
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_US_EAST_1,
                                regions={REGION_VALUE_US_EAST_1},
                            ),
                        },
                        default_value=PARTITION_AWS_CN,
                    ),
                    region=ConfigFile.Region(
                        instances={
                            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                f"The default region {REGION_VALUE_US_EAST_1!r} is not in the partition {PARTITION_AWS_CN!r}.",
                id="partition region does not match the partition.",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            "global": ConfigFile.Partition.Entry(
                                value=PARTITION_AWS,
                                friendly_name=Builder.build_random_string(),
                                default_hub="global",
                                default_region=REGION_VALUE_US_EAST_1,
                                regions={REGION_VALUE_US_EAST_1},
                            ),
                        },
                        default_value=PARTITION_AWS,
                    ),
                    region=ConfigFile.Region(
                        instances={
                            REGION_KEY_US_EAST_1: ConfigFile.Region.Entry(
                                value=REGION_VALUE_US_EAST_1, friendly_name=Builder.build_random_string()
                            )
                        }
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="partition key cannot be reserved word.",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    partition=ConfigFile.Partition(
                        instances={
                            Builder.build_random_string(): ConfigFile.Partition.Entry(
                                value=PARTITION_AWS,
                                friendly_name=Builder.build_random_string(),
                                default_hub="non-existent-hub",
                                default_region=REGION_VALUE_US_EAST_1,
                                regions={REGION_VALUE_US_EAST_1, REGION_VALUE_EU_CENTRAL_1},
                            ),
                        },
                        default_value=PARTITION_AWS,
                    ),
                ),
                ValidationError,
                "Partition: the default_hub 'non-existent-hub' is not configured as hub value.",
                id="default hub is not in available hubs",
            ),
            pytest.param(
                replace(SIMPLE_CONFIG_FILE, environment=ConfigFile.Environment({})),
                ValidationError,
                "{'environment': {'_schema': ['Dict cannot be empty.']}}",
                id="environment cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    environment=ConfigFile.Environment(
                        {
                            Builder.build_random_string(): ConfigFile.Environment.Entry(
                                value="foo",
                                friendly_name=Builder.build_random_string(),
                                domain={PARTITION_AWS: Builder.build_random_string()},
                            ),
                            Builder.build_random_string(): ConfigFile.Environment.Entry(
                                value="foo",
                                friendly_name=Builder.build_random_string(),
                                domain={PARTITION_AWS: Builder.build_random_string()},
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique.",
                id="environment value is not unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    environment=ConfigFile.Environment(
                        {
                            Builder.build_random_string(): ConfigFile.Environment.Entry(
                                value=Builder.build_random_string(),
                                friendly_name="foo",
                                domain={PARTITION_AWS: Builder.build_random_string()},
                            ),
                            Builder.build_random_string(): ConfigFile.Environment.Entry(
                                value=Builder.build_random_string(),
                                friendly_name="foo",
                                domain={PARTITION_AWS: Builder.build_random_string()},
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'friendly_name' are not unique.",
                id="environment friendly name is not unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    environment=ConfigFile.Environment(
                        {
                            "global": ConfigFile.Environment.Entry(
                                value=Builder.build_random_string(),
                                friendly_name=Builder.build_random_string(),
                                domain={PARTITION_AWS: Builder.build_random_string()},
                            )
                        }
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="environment value is not valid",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    environment=ConfigFile.Environment(
                        {
                            "prod": ConfigFile.Environment.Entry(
                                value="prod",
                                friendly_name=Builder.build_random_string(),
                                domain={"not-existent": Builder.build_random_string()},
                            )
                        }
                    ),
                ),
                ValidationError,
                "The given partition 'not-existent' is not a valid AWS partition.",
                id="environment partition is not valid",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    environment=ConfigFile.Environment(
                        {
                            "prod": ConfigFile.Environment.Entry(
                                value="prod",
                                friendly_name=Builder.build_random_string(),
                                domain={},
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Environment: 'prod' does not have domains configured for every possible partition.",
                id="environment partition is missing",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    environment=ConfigFile.Environment(
                        {
                            "prod": ConfigFile.Environment.Entry(
                                value=Builder.build_random_string(),
                                friendly_name=Builder.build_random_string(),
                                domain={PARTITION_AWS: Builder.build_random_string()},
                                stages_with_extended_metrics={"invalid-stage"},
                            ),
                        }
                    ),
                ),
                ValidationError,
                "'stages_with_extended_metrics': {0: ['Must be one of: dev, int, prod.']}",
                id="environment stages_with_extended_metrics does not contain valid stage",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    environment=ConfigFile.Environment(
                        instances={
                            "prod": ConfigFile.Environment.Entry(
                                value="prod",
                                friendly_name=Builder.build_random_string(),
                                domain={PARTITION_AWS: "prod.example.com"},
                            ),
                            "dev": ConfigFile.Environment.Entry(
                                value="dev",
                                friendly_name=Builder.build_random_string(),
                                domain={PARTITION_AWS: "dev.example.com"},
                            ),
                        }
                    ),
                    hub=ConfigFile.Hub(
                        instances={
                            "GLOBAL": ConfigFile.Hub.Entry(
                                value="global", environments={"prod"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            )
                        },
                    ),
                ),
                ValidationError,
                "Environment: environments entry 'dev' is not assigned to any hub.",
                id="environment is not assigned to at least one hub",
            ),
            pytest.param(
                replace(SIMPLE_CONFIG_FILE, hub=ConfigFile.Hub(instances={})),
                ValidationError,
                "{'hub': {'_schema': ['Dict cannot be empty.']}}",
                id="hub can not be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            "GLOBAL": ConfigFile.Hub.Entry(
                                value="global", environments={"prod"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                            "MARS": ConfigFile.Hub.Entry(
                                value="global", environments={"prod"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique",
                id="hub value is not unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            "global": ConfigFile.Hub.Entry(
                                value="global", environments={"dev"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="hub key is reserved word",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            "GLOBAL": ConfigFile.Hub.Entry(
                                value="my_hub", environments={"dev"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                        },
                    ),
                ),
                ValidationError,
                "{'value': ['String does not match expected pattern.']}",
                id="hub value may not contain underscores",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            "GLOBAL": ConfigFile.Hub.Entry(
                                value="my-hub", environments={"dev"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                        },
                    ),
                ),
                ValidationError,
                "{'value': ['String does not match expected pattern.']}",
                id="hub value may not contain dashes",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            "GLOBAL": ConfigFile.Hub.Entry(
                                value="GLOBAL", environments={"dev"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                        },
                    ),
                ),
                ValidationError,
                "{'value': ['String does not match expected pattern.']}",
                id="hub value may not contain uppercase characters",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            "GLOBAL": ConfigFile.Hub.Entry(
                                value="global", environments={"prod"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                            Builder.build_random_string(): ConfigFile.Hub.Entry(
                                value=Builder.build_random_string(length=8),
                                environments={"prod"},
                                regions={REGION_VALUE_EU_CENTRAL_1},
                            ),
                        },
                    ),
                ),
                ValidationError,
                "{'value': ['String does not match expected pattern.']}",
                id="hub value too long",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            "GLOBAL": ConfigFile.Hub.Entry(
                                value="global", environments={"prod"}, regions={"us-west-1"}
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Hub: regions entry 'us-west-1' is not configured at region values.",
                id="hub region is not listed in regions",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    hub=ConfigFile.Hub(
                        instances={
                            **SIMPLE_CONFIG_FILE.hub.instances,
                            "MARS": ConfigFile.Hub.Entry(
                                value="mars", environments={"unknown-environment"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Hub: environments entry 'unknown-environment' is not configured at environment values.",
                id="hub environment is not listed in environments",
            ),
            pytest.param(
                replace(
                    CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
                    hub=ConfigFile.Hub(
                        instances={
                            **CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.hub.instances,
                            "MARS": ConfigFile.Hub.Entry(
                                value="mars",
                                environments={"dev"},
                                regions={REGION_VALUE_CN_NORTH_1, REGION_VALUE_EU_CENTRAL_1},
                            ),
                        },
                    ),
                ),
                ValidationError,
                "are associated with different partitions.",
                id="hub has regions with different partitions",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(SIMPLE_CONFIG_FILE.account, purpose=ConfigFile.Account.Purpose({})),
                ),
                ValidationError,
                "Dict cannot be empty.",
                id="account purpose empty dict not allowed",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        purpose=ConfigFile.Account.Purpose(
                            {
                                "global": ConfigFile.Account.Purpose.Entry(
                                    value=Builder.build_random_string(),
                                    deployed_by_cdh_core=True,
                                    hub_specific=False,
                                    can_be_owner=True,
                                )
                            }
                        ),
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="account purpose is reserved word",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        assumable_aws_role=ConfigFile.Account.AssumableAWSRole(
                            billing=AWSRole(name="123", path="/"),
                        ),
                    ),
                ),
                ValidationError,
                "'billing': {'name': ['String does not match expected pattern.']}",
                id="billing role name does not match regex",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        assumable_aws_role=ConfigFile.Account.AssumableAWSRole(
                            billing=AWSRole(name="allowed-name", path=""),
                        ),
                    ),
                ),
                ValidationError,
                "'billing': {'path': ['String does not match expected pattern.']}",
                id="billing role path does not match regex",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        assumable_aws_role=ConfigFile.Account.AssumableAWSRole(
                            billing=AWSRole(name="allowed-name", path="invalid/"),
                        ),
                    ),
                ),
                ValidationError,
                "'billing': {'path': ['String does not match expected pattern.']}",
                id="billing role path does not match regex",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        assumable_aws_role=ConfigFile.Account.AssumableAWSRole(
                            billing=AWSRole(name="allowed-name", path="/invalid"),
                        ),
                    ),
                ),
                ValidationError,
                "'billing': {'path': ['String does not match expected pattern.']}",
                id="billing role path does not match regex",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        assumable_aws_role=ConfigFile.Account.AssumableAWSRole(
                            billing=AWSRole(name="allowed-name", path="/valid/"),
                        ),
                    ),
                ),
                None,
                None,
                id="billing role path is valid",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        admin_role_name="123",
                    ),
                ),
                ValidationError,
                "'admin_role_name': ['String does not match expected pattern.']",
                id="admin role name does not match regex",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(SIMPLE_CONFIG_FILE.account, instances_per_purpose={}),
                ),
                ValidationError,
                "instances_per_purpose': ['Shorter than minimum length 1.']",
                id="account instances_per_purpose cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "test": ConfigFile.Account.PurposeEntry(
                                account_instances={},
                            ),
                        },
                    ),
                ),
                ValidationError,
                "'account_instances': ['Shorter than minimum length 1.']",
                id="account account_instances cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "does_not_exist": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "The account purpose 'does_not_exist' is not listed in the available purposes",
                id="account purpose does not exist",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456123456",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "The account '123456123456' is missing a hub definition.",
                id="account with missing hub",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012", partition="aws", environment="prod", hub="global"
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "The account '123456789012' with purpose 'api' must not specify a hub.",
                id="account hub not needed",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "resources": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "The resource account '123456789012' is missing a stage definition.",
                id="resource account with missing stage",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "resources": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                        stage="dev",
                                        stage_priority=0,
                                    ),
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="222222222222",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                        stage="dev",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Some resource accounts contain ambiguous account priorities.",
                id="stage priority of two resource is not unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="222222222222",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Only one security account can be defined per hub.",
                id="more than one security account per hub",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111",
                                        partition="not-existent",
                                        environment="prod",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012", partition="aws", environment="prod", hub="global"
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "The given partition 'not-existent' is not a valid AWS partition.",
                id="account with invalid partition",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111",
                                        partition="aws",
                                        environment="not-existent",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012", partition="aws", environment="prod", hub="global"
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Account: environments entry 'not-existent' is not configured at environment values.",
                id="account with invalid environment",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            **SIMPLE_CONFIG_FILE.account.instances_per_purpose,
                            "test": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111",
                                        partition="aws",
                                        environment="prod",
                                        hub="not-existent",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Account: hub entry 'not-existent' is not configured at hub values.",
                id="account with invalid hub",
            ),
            pytest.param(
                replace(
                    CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
                    account=replace(
                        CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account,
                        instances_per_purpose={
                            **CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account.instances_per_purpose,
                            "test": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111", partition="aws-cn", environment="dev", hub="cn"
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Account: environment entry 'dev' does not match configured hub environments",
                id="account hub not matching environment",
            ),
            pytest.param(
                replace(
                    CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
                    account=replace(
                        CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account,
                        instances_per_purpose={
                            **CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account.instances_per_purpose,
                            "test": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111",
                                        partition="aws-cn",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Account: partition entry 'aws-cn' does not match partition 'aws' associated with the regions of the "
                "hub 'global'.",
                id="account hub not matching partition",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "test": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="111111111111",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012", partition="aws", environment="prod", hub="global"
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Account: api accounts have to be defined for each environment/partition combination.",
                id="missing api account",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="987654321098",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012", partition="aws", environment="prod", hub="global"
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Account: multiple api accounts defined per environment/partition combination.",
                id="too many api accounts per environment/partition combination",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                            "resources": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                        stage="dev",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                            "iam": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                            "portal": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                None,
                None,
                id="valid single account setup",
            ),
            pytest.param(
                replace(
                    CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
                    account=replace(
                        CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account,
                        instances_per_purpose={
                            **CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account.instances_per_purpose,
                            "resources": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="dev",
                                        hub="global",
                                        stage="dev",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "A single account id can only be used in one environment.",
                id="account id can only be used in one environment",
            ),
            pytest.param(
                replace(
                    CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
                    hub=ConfigFile.Hub(
                        instances={
                            **CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.hub.instances,
                            "MARS": ConfigFile.Hub.Entry(
                                value="mars", environments={"prod"}, regions={REGION_VALUE_EU_CENTRAL_1}
                            ),
                        },
                    ),
                    account=replace(
                        CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account,
                        instances_per_purpose={
                            **CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account.instances_per_purpose,
                            "resources": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                        stage="dev",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="mars",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "A single account id can only be used in one hub.",
                id="account id can only be used in one hub",
            ),
            pytest.param(
                replace(
                    CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS,
                    account=replace(
                        CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account,
                        instances_per_purpose={
                            **CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS.account.instances_per_purpose,
                            "resources": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws-cn",
                                        environment="prod",
                                        hub="cn",
                                        stage="dev",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="mars",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "A single account id can only be used in one partition.",
                id="account id can only be used in one partition",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                            "resources": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                        stage="dev",
                                        stage_priority=0,
                                    ),
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                        stage="dev",
                                        stage_priority=1,
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "A single account id can only be used as one resource account.",
                id="account id can only be used for one resource account",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,  # COMPLEX FILE
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "A security account for the default hub needs to be defined.",
                id="security account for the default hub needs to be defined",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    account=replace(
                        SIMPLE_CONFIG_FILE.account,
                        instances_per_purpose={
                            "api": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                    ),
                                },
                            ),
                            "security": ConfigFile.Account.PurposeEntry(
                                account_instances={
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456789012",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                    Builder.build_random_string(): ConfigFile.Account.PurposeEntry.AccountEntry(
                                        id="123456123456",
                                        partition="aws",
                                        environment="prod",
                                        hub="global",
                                    ),
                                },
                            ),
                        },
                    ),
                ),
                ValidationError,
                "Only one security account can be defined per hub.",
                id="more than one security account per hub",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    aws_service=replace(
                        SIMPLE_CONFIG_FILE.aws_service,
                        iam=ConfigFile.AWSService.IAM(
                            configured_limits=ConfigFile.AWSService.IAM.ConfiguredLimits(
                                max_managed_policies_per_role=30
                            )
                        ),
                    ),
                ),
                ValidationError,
                "{'aws_service': {'iam': {'configured_limits': {'max_managed_policies_per_role': "
                "['Must be greater than or equal to 10 and less than or equal to 20.']}}}}",
                id="max_managed_policies_per_role must be in range",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    aws_service=replace(
                        SIMPLE_CONFIG_FILE.aws_service,
                        s3=ConfigFile.AWSService.S3(allowed_origins=["http://www.example.com"]),
                    ),
                ),
                None,
                None,
                id="CORES allowed_origins plain URL",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    aws_service=replace(
                        SIMPLE_CONFIG_FILE.aws_service,
                        s3=ConfigFile.AWSService.S3(allowed_origins=["*"]),
                    ),
                ),
                None,
                None,
                id="CORES allowed_origins single star",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    aws_service=replace(
                        SIMPLE_CONFIG_FILE.aws_service,
                        s3=ConfigFile.AWSService.S3(allowed_origins=["http://*.example.com"]),
                    ),
                ),
                None,
                None,
                id="CORES allowed_origins single star within URL",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    aws_service=replace(
                        SIMPLE_CONFIG_FILE.aws_service,
                        s3=ConfigFile.AWSService.S3(
                            allowed_origins=["http://www.example.com", "http://*.example.com", "*"]
                        ),
                    ),
                ),
                None,
                None,
                id="CORES allowed_origins multiple valid URLs",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    aws_service=replace(
                        SIMPLE_CONFIG_FILE.aws_service,
                        s3=ConfigFile.AWSService.S3(allowed_origins=["http://*.foo.*.example.com"]),
                    ),
                ),
                ValidationError,
                "{'aws_service': {'s3': {'allowed_origins': {0: ['Not a valid URL.']}}}}",
                id="CORES allowed_origins multiple stars within URL",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    aws_service=replace(
                        SIMPLE_CONFIG_FILE.aws_service,
                        s3=ConfigFile.AWSService.S3(
                            allowed_origins=["http://www.example.com", "http://*.foo.*.example.com"]
                        ),
                    ),
                ),
                ValidationError,
                "{'aws_service': {'s3': {'allowed_origins': {1: ['Not a valid URL.']}}}}",
                id="CORES allowed_origins multiple URLs, one is malformed",
            ),
            pytest.param(
                replace(SIMPLE_CONFIG_FILE, affiliation=ConfigFile.Affiliation({})),
                ValidationError,
                "{'affiliation': {'_schema': ['Dict cannot be empty.']}}",
                id="affiliation cannot be empty",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    affiliation=ConfigFile.Affiliation(
                        {
                            Builder.build_random_string(): ConfigFile.Affiliation.Entry(
                                value="foo",
                                friendly_name=Builder.build_random_string(),
                                access_management=Builder.get_random_bool(),
                            ),
                            Builder.build_random_string(): ConfigFile.Affiliation.Entry(
                                value="foo",
                                friendly_name=Builder.build_random_string(),
                                access_management=Builder.get_random_bool(),
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'value' are not unique.",
                id="affiliation value is not unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    affiliation=ConfigFile.Affiliation(
                        {
                            Builder.build_random_string(): ConfigFile.Affiliation.Entry(
                                value=Builder.build_random_string(),
                                friendly_name="foo",
                                access_management=Builder.get_random_bool(),
                            ),
                            Builder.build_random_string(): ConfigFile.Affiliation.Entry(
                                value=Builder.build_random_string(),
                                friendly_name="foo",
                                access_management=Builder.get_random_bool(),
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Values of field 'friendly_name' are not unique.",
                id="affiliation friendly name is not unique",
            ),
            pytest.param(
                replace(
                    SIMPLE_CONFIG_FILE,
                    affiliation=ConfigFile.Affiliation(
                        {
                            "global": ConfigFile.Affiliation.Entry(
                                value=Builder.build_random_string(),
                                friendly_name=Builder.build_random_string(),
                                access_management=Builder.get_random_bool(),
                            ),
                        }
                    ),
                ),
                ValidationError,
                "Enum name 'global' is not valid.",
                id="affiliation key is reserved word",
            ),
        ],
    )
    def test_validate_config(
        self,
        to_test: ConfigFile,
        expected_exception: Optional[Type[Exception]],
        expected_error_message: Optional[str],
    ) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile)()
        if expected_exception:
            with pytest.raises(expected_exception=expected_exception) as exc_info:
                config_class.load(config_class.dump(to_test))
            assert expected_error_message is not None
            assert expected_error_message in str(exc_info.value)
        else:
            assert to_test == config_class.load(config_class.dump(to_test))

    def test_optional_assumable_role_field(self) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile.Account)()
        config_obj = config_class.dump(
            ConfigFile.Account(
                instances_per_purpose=SIMPLE_CONFIG_FILE.account.instances_per_purpose,
            )
        )
        del config_obj["assumable_aws_role"]
        loaded_config_without_option_values: ConfigFile.Account = config_class.load(config_obj)
        expected_aws_roles = {
            "billing": AWSRole("cdh-assumable-billing", "/cdh/"),
            "metadata": AWSRole("cdh-assumable-metadata", "/cdh/"),
        }
        assert all(
            getattr(loaded_config_without_option_values.assumable_aws_role, attr) == aws_role
            for attr, aws_role in expected_aws_roles.items()
        )

        new_aws_roles = {
            attr: AWSRole(Builder.build_random_string(), f"/{Builder.build_random_string()}/")
            for attr in expected_aws_roles
        }
        loaded_config_with_option_values: ConfigFile.Account = config_class.load(
            config_class.dump(
                ConfigFile.Account(
                    assumable_aws_role=ConfigFile.Account.AssumableAWSRole(
                        billing=new_aws_roles["billing"],
                        metadata=new_aws_roles["metadata"],
                    ),
                    instances_per_purpose=SIMPLE_CONFIG_FILE.account.instances_per_purpose,
                )
            )
        )
        assert all(
            getattr(loaded_config_with_option_values.assumable_aws_role, attr) == aws_role
            for attr, aws_role in new_aws_roles.items()
        )

    def test_set_only_one_of_the_optional_assumable_role_fields(self) -> None:
        billing_role_name = Builder.build_random_string()
        billing_role_path = f"/[{Builder.build_random_string()}/"
        config_class = marshmallow_dataclass.class_schema(ConfigFile.Account)()
        config_obj = config_class.dump(
            ConfigFile.Account(
                assumable_aws_role=ConfigFile.Account.AssumableAWSRole(
                    billing=AWSRole(name=billing_role_name, path=billing_role_path),
                ),
                instances_per_purpose=SIMPLE_CONFIG_FILE.account.instances_per_purpose,
            )
        )
        loaded_config: ConfigFile.Account = config_class.load(config_obj)

        assert loaded_config.assumable_aws_role.billing.name
        assert loaded_config.assumable_aws_role.metadata.name
        assert loaded_config.assumable_aws_role.billing.name == billing_role_name
        assert loaded_config.assumable_aws_role.billing.path == billing_role_path
        assert loaded_config.assumable_aws_role.metadata.name == "cdh-assumable-metadata"

    def test_optional_purpose_field_is_not_set(self) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile.Account)()
        config_obj = config_class.dump(
            ConfigFile.Account(
                instances_per_purpose=SIMPLE_CONFIG_FILE.account.instances_per_purpose,
            )
        )
        del config_obj["purpose"]
        loaded_config_without_option_value: ConfigFile.Account = config_class.load(config_obj)
        assert loaded_config_without_option_value.purpose == ConfigFile.Account.get_default_account_purpose()

    def test_optional_purpose_field_is_set(self) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile.Account)()
        purpose_key = Builder.build_random_string()
        config_obj = config_class.dump(
            ConfigFile.Account(
                purpose=ConfigFile.Account.Purpose(
                    instances={
                        purpose_key: ConfigFile.Account.Purpose.Entry(
                            value=Builder.build_random_string(),
                            deployed_by_cdh_core=False,
                            hub_specific=False,
                            can_be_owner=False,
                        ),
                    },
                ),
                instances_per_purpose=SIMPLE_CONFIG_FILE.account.instances_per_purpose,
            )
        )
        loaded_config_with_option_values: ConfigFile.Account = config_class.load(config_obj)

        assert sorted(loaded_config_with_option_values.purpose.instances.keys()) == sorted(
            [*ConfigFile.Account.get_default_account_purpose().instances.keys(), purpose_key]
        )

    def test_optional_admin_role_field_is_not_set(self) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile.Account)()
        config_obj = config_class.dump(
            ConfigFile.Account(
                instances_per_purpose=SIMPLE_CONFIG_FILE.account.instances_per_purpose,
            )
        )
        del config_obj["admin_role_name"]
        loaded_config_without_option_value: ConfigFile.Account = config_class.load(config_obj)
        assert loaded_config_without_option_value.admin_role_name == "CDHX-DevOps"

    def test_set_aws_limits(self) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile)()
        config_obj = config_class.dump(SIMPLE_CONFIG_FILE)
        loaded_config_with_option_values: ConfigFile = config_class.load(config_obj)
        assert loaded_config_with_option_values.aws_service.iam.configured_limits.max_managed_policies_per_role == 15

    def test_optional_configured_aws_limits(self) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile)()
        config_obj = config_class.dump(SIMPLE_CONFIG_FILE)
        del config_obj["aws_service"]["iam"]["configured_limits"]["max_managed_policies_per_role"]
        loaded_config_with_option_values: ConfigFile = config_class.load(config_obj)
        assert loaded_config_with_option_values.aws_service.iam.configured_limits.max_managed_policies_per_role == 10

    def test_access_managements_in_affiliation(self) -> None:
        config_class = marshmallow_dataclass.class_schema(ConfigFile)()
        adjusted_simple_config_file = replace(
            SIMPLE_CONFIG_FILE,
            affiliation=ConfigFile.Affiliation(
                instances={
                    "access_management_not_set": ConfigFile.Affiliation.Entry(
                        value=Builder.build_random_string(),
                        friendly_name=Builder.build_random_string(),
                    ),
                    "access_management_set_false": ConfigFile.Affiliation.Entry(
                        value=Builder.build_random_string(),
                        friendly_name=Builder.build_random_string(),
                        access_management=False,
                    ),
                    "access_management_set_true": ConfigFile.Affiliation.Entry(
                        value=Builder.build_random_string(),
                        friendly_name=Builder.build_random_string(),
                        access_management=True,
                    ),
                }
            ),
        )
        config_obj = config_class.dump(adjusted_simple_config_file)
        loaded_config_with_option_values: ConfigFile = config_class.load(config_obj)
        for instance_key, instance_affiliation in loaded_config_with_option_values.affiliation.instances.items():
            if instance_key in {"access_management_not_set", "access_management_set_true"}:
                assert instance_affiliation.access_management is True
            else:
                assert instance_affiliation.access_management is False
