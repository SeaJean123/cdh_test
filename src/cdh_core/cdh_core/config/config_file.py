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
import itertools
import keyword
from dataclasses import dataclass
from dataclasses import field
from functools import lru_cache
from typing import Any
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import marshmallow_dataclass
from boto3.session import Session
from marshmallow import post_load
from marshmallow import validate
from marshmallow import validates_schema
from marshmallow import ValidationError


def _is_unique(iterable: Collection[Any]) -> bool:
    return len(iterable) == len(set(iterable))


@lru_cache()
def _get_boto_session() -> Session:
    return Session()


@lru_cache()
def _get_available_partitions() -> List[str]:
    return _get_boto_session().get_available_partitions()


@lru_cache()
def _get_available_regions(partition: str) -> List[str]:
    return _get_boto_session().get_available_regions("s3", partition_name=partition)


@lru_cache()
def _validate_partition_against_boto(partition: str) -> None:
    if partition not in _get_available_partitions():
        raise ValidationError(f"The given partition {partition!r} is not a valid AWS partition.")


@lru_cache()
def _validate_region_against_boto(region: str) -> None:
    if region not in {
        aws_region for partition in _get_available_partitions() for aws_region in _get_available_regions(partition)
    }:
        raise ValidationError(f"The given region {region!r} is not a valid AWS region.")


def _validate_enum_name(name: str) -> None:
    if not name.isidentifier() or keyword.iskeyword(name):
        raise ValidationError(f"Enum name {name!r} is not valid.")


def _validate_unique_dict_values(dictionary: Dict[Any, Any]) -> None:
    if not dictionary:
        raise ValidationError("Dict cannot be empty.")
    if not _is_unique(dictionary.values()):
        raise ValidationError("Dict values are not unique.")


def _validate_s3_cores_url(url: str) -> None:
    """See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html#cors-allowed-origin ."""
    if url != "*":
        validate.URL(schemes=["http", "https"])(url.replace("*.", "", 1))


RegionValueType = marshmallow_dataclass.NewType("RegionValueType", str, validate=_validate_region_against_boto)
PartitionValueType = marshmallow_dataclass.NewType("PartitionValueType", str, validate=_validate_partition_against_boto)
EnumNameType = marshmallow_dataclass.NewType("EnumNameType", str, validate=_validate_enum_name)
AccountIdType = marshmallow_dataclass.NewType("AllowedAccountIdType", str, validate=validate.Regexp(r"^[0-9]{12}$"))
# The regex for role names and paths is based on https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateRole.html
AWSRoleName = marshmallow_dataclass.NewType("AWSRoleName", str, validate=validate.Regexp(r"^[a-zA-Z_+=,.@-]{1,64}$"))
AWSRolePath = marshmallow_dataclass.NewType("AWSRolePath", str, validate=validate.Regexp(r"^\/(?:[\!-\~]{1,510}\/)?$"))
S3URL = marshmallow_dataclass.NewType("S3URL", str, validate=_validate_s3_cores_url)
Stage = marshmallow_dataclass.NewType("Stage", str, validate=validate.OneOf(["dev", "int", "prod"]))
URL = marshmallow_dataclass.NewType("URL", str, validate=validate.URL(schemes=["http", "https"]))


@dataclass(frozen=True)
class AWSRole:  # noqa: D106
    """Dataclass to represent the configurable AWS roles."""

    name: AWSRoleName
    path: AWSRolePath


def _create_validation_class(attributes_to_be_unique: List[str]) -> Any:
    class _HasInstanceWithUniqueValue:
        @validates_schema
        def _validate_has_instance_with_value(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            if not data["instances"]:
                raise ValidationError("Dict cannot be empty.")
            for attribute in attributes_to_be_unique:
                if not _is_unique([getattr(instance, attribute) for instance in data["instances"].values()]):
                    raise ValidationError(f"Values of field {attribute!r} are not unique.")

    return _HasInstanceWithUniqueValue


@dataclass(frozen=True)
class ConfigFile:
    """Dataclass to represent the configuration file."""

    @dataclass(frozen=True)
    class Partition(  # noqa: D106
        _create_validation_class(attributes_to_be_unique=["value", "friendly_name"])  # type: ignore
    ):
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            value: PartitionValueType
            friendly_name: str
            default_hub: str
            default_region: RegionValueType
            regions: Set[RegionValueType] = field(metadata={"validate": validate.Length(min=1)})

        instances: Dict[EnumNameType, Entry]
        default_value: str

        @property
        def partitions(self) -> Collection[str]:
            """Return all available partitions."""
            return frozenset(entry.value for entry in self.instances.values())

        @property
        def default_hub(self) -> str:
            """Return the default_hub of the default_partition."""
            return next(entry.default_hub for entry in self.instances.values() if entry.value == self.default_value)

        @validates_schema
        def _validate_regions_match_partition(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            for partition in data["instances"].values():
                valid_regions = set(_get_available_regions(partition.value))
                if partition.default_region not in valid_regions:
                    raise ValidationError(
                        f"The default region {partition.default_region!r} is not in the partition {partition.value!r}."
                    )
                if partition.default_region not in partition.regions:
                    raise ValidationError(
                        f"The default region {partition.default_region!r} is not in the corresponding regions."
                    )

        @validates_schema
        def _validate_partition_default_value_in_instances(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            partition_values = [partition.value for partition in data["instances"].values()]
            if (default_value := data["default_value"]) not in partition_values:
                raise ValidationError(
                    f"The default partition {default_value!r} is not listed in the available partitions "
                    f"{partition_values!r}."
                )

    @dataclass(frozen=True)
    class Region(  # noqa: D106
        _create_validation_class(attributes_to_be_unique=["value", "friendly_name"])  # type: ignore
    ):
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            value: RegionValueType
            friendly_name: str

        instances: Dict[EnumNameType, Entry]

    @dataclass(frozen=True)
    class BusinessObject(  # noqa: D106
        _create_validation_class(attributes_to_be_unique=["value", "friendly_name"])  # type: ignore
    ):
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            # Our naming scheme for S3 buckets requires that these names are at most 12 characters long.
            value: str = field(metadata={"validate": validate.Regexp("^([a-z0-9]{1,12})$")})
            friendly_name: str

        instances: Dict[EnumNameType, Entry]

    @dataclass(frozen=True)
    class DatasetPurpose(  # noqa: D106
        _create_validation_class(attributes_to_be_unique=["value", "friendly_name"])  # type: ignore
    ):
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            value: str
            friendly_name: str

        instances: Dict[EnumNameType, Entry]

    @dataclass(frozen=True)
    class DatasetExternalLinkType(  # noqa: D106
        _create_validation_class(attributes_to_be_unique=["value", "friendly_name"])  # type: ignore
    ):
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            value: str
            friendly_name: str

        instances: Dict[EnumNameType, Entry]

    @dataclass(frozen=True)
    class Environment(  # noqa: D106
        _create_validation_class(attributes_to_be_unique=["value", "friendly_name"])  # type: ignore
    ):
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            value: str
            friendly_name: str
            domain: Dict[PartitionValueType, str]
            is_test_environment: bool = False
            stages_with_extended_metrics: Set[Stage] = field(default_factory=set)

        instances: Dict[EnumNameType, Entry]

        @property
        def environments(self) -> Collection[str]:
            """Return all available environments."""
            return frozenset(entry.value for entry in self.instances.values())

        @property
        def partitions(self) -> Collection[str]:
            """Return all available partitions."""
            return frozenset(next(entry.domain.keys() for entry in self.instances.values()))

    @dataclass(frozen=True)
    class Hub(_create_validation_class(attributes_to_be_unique=["value"])):  # type: ignore # noqa: D106
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            value: str = field(metadata={"validate": validate.Regexp("^([a-z0-9]{1,7})$")})
            environments: Set[str] = field(metadata={"validate": validate.Length(min=1)})
            regions: Set[RegionValueType] = field(metadata={"validate": validate.Length(min=1)})

        instances: Dict[EnumNameType, Entry]

        @property
        def hubs(self) -> Collection[str]:
            """Return all available hubs."""
            return frozenset(entry.value for entry in self.instances.values())

    @dataclass(frozen=True)
    class Account:  # noqa: D106
        @dataclass(frozen=True)
        class Purpose(  # noqa: D106
            _create_validation_class(attributes_to_be_unique=["value"])  # type: ignore
        ):
            @dataclass(frozen=True)
            class Entry:  # noqa: D106
                value: str
                deployed_by_cdh_core: bool
                hub_specific: bool
                can_be_owner: bool

            instances: Dict[EnumNameType, Entry] = field(metadata={"data_key": "additional_instances"})

            @post_load
            def _extend_via_default_entries(  # pylint: disable=unused-argument
                self, data: Dict[str, Dict[EnumNameType, Entry]], **kwargs: Dict[str, Any]
            ) -> Dict[str, Dict[EnumNameType, Entry]]:
                data["instances"].update(ConfigFile.Account.get_default_account_purpose().instances)
                return data

        @dataclass(frozen=True)
        class PurposeEntry:  # noqa: D106
            @dataclass(frozen=True)
            class AccountEntry:  # noqa: D106
                id: AccountIdType  # pylint: disable=invalid-name
                partition: PartitionValueType
                environment: str
                hub: Optional[str] = None
                stage: Optional[Stage] = None
                stage_priority: int = 0

            account_instances: Dict[str, AccountEntry] = field(metadata={"validate": validate.Length(min=1)})

        @dataclass(frozen=True)
        class AssumableAWSRole:  # noqa: D106
            billing: AWSRole = field(default=AWSRole(name="cdh-assumable-billing", path="/cdh/"))
            metadata: AWSRole = field(default=AWSRole(name="cdh-assumable-metadata", path="/cdh/"))

        @staticmethod
        def get_default_account_purpose() -> Purpose:
            """Return the default account purpose.

            Its instances are added to the additional instances specified in the config.
            """
            return ConfigFile.Account._default_account_purpose  # pylint: disable=protected-access

        _default_account_purpose = Purpose(
            instances={
                "resources": Purpose.Entry(
                    value="resources", deployed_by_cdh_core=True, hub_specific=True, can_be_owner=False
                ),
                "test": Purpose.Entry(value="test", deployed_by_cdh_core=True, hub_specific=True, can_be_owner=True),
                "api": Purpose.Entry(value="api", deployed_by_cdh_core=True, hub_specific=False, can_be_owner=False),
                "security": Purpose.Entry(
                    value="security", deployed_by_cdh_core=False, hub_specific=True, can_be_owner=False
                ),
                "iam": Purpose.Entry(value="iam", deployed_by_cdh_core=False, hub_specific=False, can_be_owner=False),
                "portal": Purpose.Entry(
                    value="portal", deployed_by_cdh_core=False, hub_specific=False, can_be_owner=True
                ),
            }
        )

        instances_per_purpose: Dict[str, PurposeEntry] = field(metadata={"validate": validate.Length(min=1)})
        purpose: Purpose = _default_account_purpose
        assumable_aws_role: AssumableAWSRole = field(default=AssumableAWSRole())
        admin_role_name: AWSRoleName = field(default=AWSRoleName("CDHX-DevOps"))

        @validates_schema
        def _validate_account_purpose_in_purpose(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            purpose_values = [purpose.value for purpose in data["purpose"].instances.values()]
            for purpose in data["instances_per_purpose"].keys():
                if purpose not in purpose_values:
                    raise ValidationError(
                        f"The account purpose {purpose!r} is not listed in the available purposes {purpose_values}'."
                    )

        @validates_schema
        # pylint: disable=unused-argument
        def _validate_account_only_used_in_one_partition_environment_hub_combination(
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            account_ids = frozenset(
                account_entry.id
                for purpose_entry in data["instances_per_purpose"].values()
                for account_entry in purpose_entry.account_instances.values()
            )

            partitions_by_id: Dict[str, Set[str]] = {account_id: set() for account_id in account_ids}
            envs_by_id: Dict[str, Set[str]] = {account_id: set() for account_id in account_ids}
            hubs_by_id: Dict[str, Set[str]] = {account_id: set() for account_id in account_ids}
            for purpose_entry in data["instances_per_purpose"].values():
                for account_entry in purpose_entry.account_instances.values():
                    partitions_by_id[account_entry.id].add(account_entry.partition)
                    envs_by_id[account_entry.id].add(account_entry.environment)
                    if account_entry.hub:
                        hubs_by_id[account_entry.id].add(account_entry.hub)

            if any(len(partitions) != 1 for partitions in partitions_by_id.values()):
                raise ValidationError("A single account id can only be used in one partition.")
            if any(len(envs) != 1 for envs in envs_by_id.values()):
                raise ValidationError("A single account id can only be used in one environment.")
            if any(len(hubs) > 1 for hubs in hubs_by_id.values()):
                raise ValidationError("A single account id can only be used in one hub.")

        @validates_schema
        def _validate_account_hub_defined_iff_needed(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            for purpose_value, purpose_entry in data["instances_per_purpose"].items():
                for account_entry in purpose_entry.account_instances.values():
                    try:
                        purpose_hub_specific = next(
                            purpose.hub_specific
                            for purpose in data["purpose"].instances.values()
                            if purpose_value == purpose.value
                        )
                    except StopIteration:
                        continue
                    if account_entry.hub and not purpose_hub_specific:
                        raise ValidationError(
                            f"The account {account_entry.id!r} with purpose {purpose_value!r} must not specify a hub."
                        )
                    if not account_entry.hub and purpose_hub_specific:
                        raise ValidationError(f"The account {account_entry.id!r} is missing a hub definition.")

        @validates_schema
        def _validate_account_only_used_as_one_resource_account(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            resource_account_ids = [
                account_entry.id
                for purpose_value, purpose_entry in data["instances_per_purpose"].items()
                for account_entry in purpose_entry.account_instances.values()
                if purpose_value == "resources"
            ]
            if len(resource_account_ids) != len(set(resource_account_ids)):
                raise ValidationError("A single account id can only be used as one resource account.")

        @validates_schema
        def _validate_resource_accounts_contain_a_stage(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            for purpose_value, purpose_entry in data["instances_per_purpose"].items():
                if purpose_value == "resources":
                    for entry in purpose_entry.account_instances.values():
                        if not entry.stage:
                            raise ValidationError(f"The resource account {entry.id!r} is missing a stage definition.")

        @validates_schema
        def _validate_resource_account_priorities(  # pylint: disable=unused-argument
            self, data: Dict[str, Any], **kwargs: Dict[str, Any]
        ) -> None:
            for purpose_value, purpose_entry in data["instances_per_purpose"].items():
                if purpose_value == "resources":
                    resource_account_info = [
                        (entry.environment, entry.hub, entry.stage, entry.stage_priority)
                        for entry in purpose_entry.account_instances.values()
                    ]
                    if len(resource_account_info) != len(set(resource_account_info)):
                        raise ValidationError("Some resource accounts contain ambiguous account priorities.")

    @dataclass(frozen=True)
    class AWSService:  # noqa: D106
        @dataclass(frozen=True)
        class IAM:  # noqa: D106
            @dataclass(frozen=True)
            class ConfiguredLimits:  # noqa: D106
                max_managed_policies_per_role: int = field(
                    default=10, metadata={"validate": validate.Range(min=10, max=20)}
                )

            configured_limits: ConfiguredLimits = ConfiguredLimits()

        @dataclass(frozen=True)
        class S3:  # noqa: D106
            @dataclass(frozen=True)
            class ConfiguredLimits:  # noqa: D106
                resource_account_bucket_limit: int = field(
                    # AWS has a hard limit of 1000 for buckets per account
                    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/BucketRestrictions.html
                    default=100,
                    metadata={"validate": validate.Range(min=100, max=1000)},
                )

            configured_limits: ConfiguredLimits = ConfiguredLimits()
            allowed_origins: List[S3URL] = field(default_factory=lambda: ["*"])

        iam: IAM = IAM()
        s3: S3 = S3()  # pylint: disable=invalid-name

    @dataclass(frozen=True)
    class Affiliation(  # noqa: D106
        _create_validation_class(attributes_to_be_unique=["value", "friendly_name"])  # type: ignore
    ):
        @dataclass(frozen=True)
        class Entry:  # noqa: D106
            value: str
            friendly_name: str
            access_management: bool = True

        instances: Dict[EnumNameType, Entry]

    @dataclass(frozen=True)
    class StageByOrigin:  # noqa: D106
        instances: Dict[URL, Optional[Stage]] = field(default_factory=dict)

    partition: Partition
    region: Region
    business_object: BusinessObject
    environment: Environment
    hub: Hub
    account: Account
    affiliation: Affiliation
    dataset_purpose: DatasetPurpose
    dataset_external_link_type: DatasetExternalLinkType
    stage_by_origin: StageByOrigin = StageByOrigin()
    aws_service: AWSService = AWSService()

    @validates_schema
    def _validate(self, data: Dict[str, Any], **kwargs: Dict[str, Any]) -> None:  # pylint: disable=unused-argument
        """Validate across several entries."""
        if (
            isinstance(data["partition"], ConfigFile.Partition)
            and isinstance(data["region"], ConfigFile.Region)
            and isinstance(data["environment"], ConfigFile.Environment)
            and isinstance(data["hub"], ConfigFile.Hub)
            and isinstance(data["account"], ConfigFile.Account)
        ):
            ConfigFile._validate_partition_against_region(data["partition"], data["region"])
            ConfigFile._validate_partition_against_hub(data["partition"], data["hub"])
            ConfigFile._validate_region_against_partition(data["region"], data["partition"])
            ConfigFile._validate_environment_against_partition(data["environment"], data["partition"])
            ConfigFile._validate_environment_against_hub(data["environment"], data["hub"])
            ConfigFile._validate_hub_against_region(data["hub"], data["region"])
            ConfigFile._validate_hub_against_partition(data["hub"], data["partition"])
            ConfigFile._validate_hub_against_environment(data["hub"], data["environment"])
            ConfigFile._validate_account_against_partition(data["account"], data["partition"])
            ConfigFile._validate_account_against_environment_and_partition(
                data["account"], data["environment"], data["partition"]
            )
            ConfigFile._validate_account_against_hub_and_partition(data["account"], data["hub"], data["partition"])

    @staticmethod
    def _validate_partition_against_region(partition_config: Partition, region_config: Region) -> None:
        for entry in partition_config.instances.values():
            if entry.default_region not in {e.value for e in region_config.instances.values()}:
                raise ValidationError(
                    f"Partition: the default_region {entry.default_region!r} is not configured as region value."
                )
            for region in entry.regions:
                if region not in {e.value for e in region_config.instances.values()}:
                    raise ValidationError(f"Partition: regions entry {region!r} is not configured at region values.")

    @staticmethod
    def _validate_partition_against_hub(partition_config: Partition, hub_config: Hub) -> None:
        hub_values = [hub.value for hub in hub_config.instances.values()]
        for entry in partition_config.instances.values():
            if entry.default_hub not in hub_values:
                raise ValidationError(
                    f"Partition: the default_hub {entry.default_hub!r} is not configured as hub value."
                )

    @staticmethod
    def _validate_region_against_partition(region_config: Region, partition_config: Partition) -> None:
        regions_defined_by_a_partition = set.union(
            *[partition.regions for partition in partition_config.instances.values()]
        )
        for entry in {e.value for e in region_config.instances.values()}:
            if entry not in regions_defined_by_a_partition:
                raise ValidationError(f"Region: region {entry!r} is not part of any partition")

    @staticmethod
    def _validate_environment_against_partition(environment_config: Environment, partition_config: Partition) -> None:
        for instance_name, instance_values in environment_config.instances.items():
            if instance_values.domain.keys() != partition_config.partitions:
                raise ValidationError(
                    f"Environment: {instance_name!r} does not have domains configured for every possible partition."
                )

    @staticmethod
    def _validate_environment_against_hub(environment_config: Environment, hub_config: Hub) -> None:
        for environment in environment_config.environments:
            if environment not in {env for entry in hub_config.instances.values() for env in entry.environments}:
                raise ValidationError(f"Environment: environments entry {environment!r} is not assigned to any hub.")

    @staticmethod
    def _validate_hub_against_region(hub_config: Hub, region_config: Region) -> None:
        for entry in hub_config.instances.values():
            for region in entry.regions:
                if region not in {e.value for e in region_config.instances.values()}:
                    raise ValidationError(f"Hub: regions entry {region!r} is not configured at region values.")

    @staticmethod
    def _validate_hub_against_partition(hub_config: Hub, partition_config: Partition) -> None:
        def partition_from_region(region: str) -> str:
            return next(
                partition.value for partition in partition_config.instances.values() if region in partition.regions
            )

        for entry in hub_config.instances.values():
            if len(set(partition_from_region(region) for region in entry.regions)) != 1:
                raise ValidationError(f"Hub: the regions {entry.regions!r} are associated with different partitions.")

    @staticmethod
    def _validate_hub_against_environment(hub_config: Hub, environment_config: Environment) -> None:
        for entry in hub_config.instances.values():
            for environment in entry.environments:
                if environment not in environment_config.environments:
                    raise ValidationError(
                        f"Hub: environments entry {environment!r} is not configured at environment values."
                    )

    @staticmethod
    def _validate_account_against_partition(account_config: Account, partition_config: Partition) -> None:
        for purpose_entry in account_config.instances_per_purpose.values():
            for account_entry in purpose_entry.account_instances.values():
                if account_entry.partition not in partition_config.partitions:
                    raise ValidationError(
                        f"Account: partitions entry {account_entry.partition!r} is not configured at partition values."
                    )

        security_account_hubs = [
            account_entry.hub
            for purpose_value, purpose_entry in account_config.instances_per_purpose.items()
            for account_entry in purpose_entry.account_instances.values()
            if purpose_value == "security" and account_entry.hub == partition_config.default_hub
        ]
        if partition_config.default_hub not in security_account_hubs:
            raise ValidationError("A security account for the default hub needs to be defined.")
        if len(security_account_hubs) > len(set(security_account_hubs)):
            raise ValidationError("Only one security account can be defined per hub.")

    @staticmethod
    def _validate_account_against_environment_and_partition(
        account_config: Account, environment_config: Environment, partition_config: Partition
    ) -> None:
        for purpose_entry in account_config.instances_per_purpose.values():
            for account_entry in purpose_entry.account_instances.values():
                if account_entry.environment not in environment_config.environments:
                    raise ValidationError(
                        f"Account: environments entry {account_entry.environment!r} is not configured at environment "
                        f"values."
                    )

        try:
            api_purpose_entry = next(
                purpose_entry
                for purpose_value, purpose_entry in account_config.instances_per_purpose.items()
                if purpose_value == "api"
            )
            api_accounts_info = [
                (entry.environment, entry.partition) for entry in api_purpose_entry.account_instances.values()
            ]
        except StopIteration:
            api_accounts_info = []
        if len(api_accounts_info) != len(set(api_accounts_info)):
            raise ValidationError("Account: multiple api accounts defined per environment/partition combination.")
        if len(api_accounts_info) != len(
            list(itertools.product(environment_config.environments, partition_config.partitions))
        ):
            raise ValidationError(
                "Account: api accounts have to be defined for each environment/partition combination."
            )

    @staticmethod
    def _validate_account_against_hub_and_partition(
        account_config: Account, hub_config: Hub, partition_config: Partition
    ) -> None:
        for purpose_entry in account_config.instances_per_purpose.values():
            for account_entry in purpose_entry.account_instances.values():
                if hub_value := account_entry.hub:
                    if hub_value not in hub_config.hubs:
                        raise ValidationError(
                            f"Account: hub entry {account_entry.hub!r} is not configured at hub values."
                        )

                    valid_hub_environments = next(
                        hub.environments for hub in hub_config.instances.values() if hub.value == account_entry.hub
                    )
                    if account_entry.environment not in valid_hub_environments:
                        raise ValidationError(
                            f"Account: environment entry {account_entry.environment!r} does not match configured hub "
                            f"environments {valid_hub_environments!r}."
                        )
                    valid_hub_regions = next(
                        hub.regions for hub in hub_config.instances.values() if hub.value == account_entry.hub
                    )
                    valid_hub_partition = next(
                        partition.value
                        for partition in partition_config.instances.values()
                        if list(valid_hub_regions)[0] in partition.regions
                    )
                    if account_entry.partition != valid_hub_partition:
                        raise ValidationError(
                            f"Account: partition entry {account_entry.partition!r} does not match partition "
                            f"{valid_hub_partition!r} associated with the regions of the hub {hub_value!r}."
                        )


def validate_config_file(config_file: ConfigFile) -> None:
    """Raise an exception if the given config is not valid."""
    config_class = marshmallow_dataclass.class_schema(ConfigFile)()
    config_class.load(config_class.dump(config_file))
