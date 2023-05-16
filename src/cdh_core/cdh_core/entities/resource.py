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
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from datetime import datetime
from typing import Callable
from typing import cast
from typing import ClassVar
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Type
from typing import TYPE_CHECKING
from typing import TypeVar

from dataclasses_json import config
from dataclasses_json import dataclass_json
from dataclasses_json.core import Json
from marshmallow import fields as mm_fields

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.dates import date_input
from cdh_core.dates import date_output
from cdh_core.entities.arn import Arn
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.glue_database import GlueDatabase
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId

if TYPE_CHECKING:
    from mypy_boto3_lakeformation.type_defs import ResourceTypeDef
else:
    ResourceTypeDef = object


@dataclass(frozen=True)
class _ResourceBody(ABC):  # noqa: B024
    dataset_id: DatasetId
    arn: Arn = field(metadata=config(encoder=str, decoder=Arn))
    creation_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )
    creator_user_id: Optional[str]
    hub: Hub
    owner_account_id: AccountId
    region: Region
    resource_account_id: AccountId
    stage: Stage
    update_date: datetime = field(
        metadata=config(encoder=date_output, decoder=date_input, mm_field=mm_fields.DateTime(format="iso"))
    )


@dataclass(frozen=True)
class Resource(_ResourceBody, ABC):
    """
    The abstraction for user-owned resources located in the Cloud Data Hub.

    Resources belong to a Dataset. Of each type, there can be at most one resource in each stage/region combination.
    The AWS resources associated with these resources are located in resource accounts.
    """

    @property
    @abstractmethod
    def type(self) -> ResourceType:
        """Return the type of the resource."""
        raise NotImplementedError

    @property
    def name(self) -> str:
        """Provide a human-readable name for the resource."""
        return self.arn.identifier

    @abstractmethod
    def to_payload(self) -> "ResourcePayload":
        """Convert resource to the dataclass sent to a client."""
        raise NotImplementedError


AnyResourcePayloadClass = TypeVar("AnyResourcePayloadClass", bound="Type[ResourcePayload]")


@dataclass(frozen=True)
class ResourcePayload(_ResourceBody, DataClassJsonCDHMixin, ABC):  # noqa: B024
    """Abstract class containing the information about a resource that is returned to a client."""

    _registered_subclasses: ClassVar[Dict[ResourceType, Type["ResourcePayload"]]] = {}

    name: str
    type: ResourceType
    attributes: Optional[DataClassJsonCDHMixin] = None

    @classmethod
    def from_dict(
        cls, kvs: Json, *, infer_missing: bool = False  # pylint: disable=unused-argument
    ) -> "ResourcePayload":
        """Infer the correct subclass from the internal registry."""
        if not isinstance(kvs, dict):
            raise ValueError(f"Cannot obtain resource from given input {kvs!r}. Must be a dict.")
        try:
            resource_type = ResourceType(kvs["type"])
        except ValueError:
            raise ValueError(  # pylint: disable=raise-missing-from
                f"Cannot obtain resource from given input {kvs!r}. Type {kvs['type']!r} unknown."
            )
        except KeyError:
            raise ValueError(  # pylint: disable=raise-missing-from
                f"Cannot obtain resource from given input {kvs!r}. Must specify 'type'."
            )
        try:
            payload_class = cls._registered_subclasses[resource_type]
        except KeyError:
            raise ValueError(  # pylint: disable=raise-missing-from
                f"Cannot obtain resource from given input {kvs!r}. Type {kvs['type']!r} not supported."
            )
        return payload_class.from_dict(kvs)

    @classmethod
    def register_for_resource_type(
        cls, resource_type: ResourceType, force: bool = False
    ) -> Callable[[AnyResourcePayloadClass], AnyResourcePayloadClass]:
        """Register a subclass for a specific resource type. Needed to reconstruct 'ResourcePayload' from dict."""
        if cls != ResourcePayload:
            raise RuntimeError(
                f"Must register resource payload at class {ResourcePayload.__name__!r}, not subclass {cls.__name__!r}"
            )

        def register_class(subcls: AnyResourcePayloadClass) -> AnyResourcePayloadClass:
            if already_registered := cls._registered_subclasses.get(resource_type):
                if not force:
                    raise ResourceTypeAlreadyRegistered(
                        f"Cannot register multiple subclasses for resourceType {resource_type.value!r}: "
                        f"attempted to register classes {already_registered.__name__!r} and {subcls.__name__!r}"
                    )
            cls._registered_subclasses[resource_type] = subcls
            # overwrite the inherited `from_dict` method with the implementation given by DataClassJsonCDHMixin
            return cast(AnyResourcePayloadClass, dataclass_json(subcls))

        return register_class


@dataclass(frozen=True)
class ResourcesPayload(DataClassJsonCDHMixin):
    """This class is used to return multiple resources to a client."""

    resources: Sequence[ResourcePayload]

    @classmethod
    def from_resources(cls, resources: Sequence[Resource]) -> "ResourcesPayload":
        """Create the payload for multiple resources."""
        return ResourcesPayload(resources=[resource.to_payload() for resource in resources])

    @classmethod
    def from_dict(
        cls, kvs: Json, *, infer_missing: bool = False  # pylint: disable=unused-argument
    ) -> "ResourcesPayload":
        """Create the resources payload from a plain dict."""
        if not isinstance(kvs, dict):
            raise TypeError(f"Cannot obtain resources from given input {kvs!r}. Must be a dict.")
        return ResourcesPayload(resources=[ResourcePayload.from_dict(resource) for resource in kvs["resources"]])


@dataclass(frozen=True)
class S3Resource(Resource):
    """
    An S3 resource is the basic building block for storing data in Cloud Data Hub.

    It is backed by an AWS S3 bucket and an AWS SNS topic. The latter can be listened on to be notified about updates.
    """

    kms_key_arn: Arn
    sns_topic_arn: Arn

    @property
    def type(self) -> ResourceType:
        """Return S3 type."""
        return ResourceType.s3

    def to_payload(self) -> "S3ResourcePayload":
        """Convert S3 resource to the dataclass sent to a client."""
        return S3ResourcePayload(
            **{field.name: getattr(self, field.name) for field in fields(Resource)},
            name=self.name,
            type=self.type,
            attributes=S3ResourcePayload.S3ResourceAttributes(
                **{field.name: getattr(self, field.name) for field in fields(S3ResourcePayload.S3ResourceAttributes)}
            ),
        )

    @property
    def to_lake_formation_data_location(self) -> ResourceTypeDef:
        """Convert S3 resource to the format used for granting and revoking Lake Formation permissions."""
        return {"DataLocation": {"CatalogId": self.resource_account_id, "ResourceArn": str(self.arn)}}


@ResourcePayload.register_for_resource_type(ResourceType.s3)
@dataclass(frozen=True)
class S3ResourcePayload(ResourcePayload):
    """The information about an S3 resource that is returned to a client."""

    @dataclass(frozen=True)
    class S3ResourceAttributes(DataClassJsonCDHMixin):
        """The special attributes of S3 resources."""

        kms_key_arn: Arn = field(metadata=config(encoder=str, decoder=Arn))
        sns_topic_arn: Arn = field(metadata=config(encoder=str, decoder=Arn))

    attributes: S3ResourceAttributes

    @classmethod
    def from_dict(
        cls, kvs: Json, *, infer_missing: bool = False  # pylint: disable=unused-argument
    ) -> "S3ResourcePayload":
        """
        Fix mypy's incorrectly inferred type.

        This method is actually defined via the '@dataclass_json'-decorator, but mypy erroneously assumes
        it was inherited from 'ResourcePayload.'
        """
        raise RuntimeError("It should not be possible to call this method")


@dataclass(frozen=True)
class GlueSyncResource(Resource):
    """Class for the GlueSyncResource."""

    database_name: DatabaseName
    sync_type: SyncType

    @property
    def glue_database(self) -> GlueDatabase:
        """Return the database information."""
        return GlueDatabase(
            name=self.database_name,
            account_id=self.resource_account_id,
            region=self.region,
        )

    @property
    def name(self) -> str:
        """Return the database name."""
        return self.database_name

    @property
    def type(self) -> ResourceType:
        """Return GlueSync type."""
        return ResourceType.glue_sync

    def to_payload(self) -> "GlueSyncResourcePayload":
        """Convert GlueSync resource to the dataclass sent to a client."""
        return GlueSyncResourcePayload(
            **{field.name: getattr(self, field.name) for field in fields(Resource)},
            name=self.name,
            type=self.type,
            attributes=GlueSyncResourcePayload.GlueSyncResourceAttributes(
                **{
                    field.name: getattr(self, field.name)
                    for field in fields(GlueSyncResourcePayload.GlueSyncResourceAttributes)
                }
            ),
        )


@ResourcePayload.register_for_resource_type(ResourceType.glue_sync)
@dataclass(frozen=True)
class GlueSyncResourcePayload(ResourcePayload):
    """The information about a GlueSync resource that is returned to a client."""

    @dataclass(frozen=True)
    class GlueSyncResourceAttributes(DataClassJsonCDHMixin):
        """The special attributes of GlueSync resources."""

        sync_type: SyncType

    attributes: GlueSyncResourceAttributes

    @classmethod
    def from_dict(
        cls, kvs: Json, *, infer_missing: bool = False  # pylint: disable=unused-argument
    ) -> "GlueSyncResourcePayload":
        """
        Fix mypy's incorrectly inferred type.

        This method is actually defined via the '@dataclass_json'-decorator, but mypy erroneously assumes
        it was inherited from 'ResourcePayload.'
        """
        raise RuntimeError("It should not be possible to call this method")


class ResourceTypeAlreadyRegistered(Exception):
    """Signals that a subclass has already been registered for the given resource type."""
