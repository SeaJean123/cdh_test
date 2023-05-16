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
# pylint: disable=no-member
from abc import abstractmethod
from dataclasses import replace
from datetime import datetime
from typing import Any
from typing import cast
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

from cdh_core_api.catalog.base import BaseTable
from cdh_core_api.catalog.base import conditional_check_failed
from cdh_core_api.catalog.base import create_model
from cdh_core_api.catalog.base import DateTimeAttribute
from cdh_core_api.catalog.base import DynamoItemIterator
from cdh_core_api.catalog.base import LastEvaluatedKey
from cdh_core_api.catalog.base import LazyEnumAttribute
from cdh_core_api.generic_types import GenericGlueSyncResource
from cdh_core_api.generic_types import GenericS3Resource
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.attributes import UnicodeSetAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.exceptions import PutError
from pynamodb.exceptions import UpdateError
from pynamodb.expressions.condition import Comparison
from pynamodb.expressions.update import Action
from pynamodb.models import Model
from pynamodb.pagination import ResultIterator
from pynamodb_attributes import UnicodeEnumAttribute

from cdh_core.entities.arn import Arn
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import Resource
from cdh_core.entities.resource import S3Resource
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.optionals import apply_if_not_none
from cdh_core.primitives.account_id import AccountId

GenericModel = TypeVar("GenericModel")


class S3ResourceAttribute(MapAttribute[str, str]):
    """S3 Resource Attributes for the resource table."""

    sns_topic_arn = UnicodeAttribute()
    kms_key_arn = UnicodeAttribute()


class GlueSyncResourceAttribute(MapAttribute[str, Any]):
    """GlueSync Resource Attributes for the resource table."""

    database_name = UnicodeAttribute()
    sync_type = UnicodeEnumAttribute(SyncType)


class _ResourceAttributeContainer(Model):
    dataset_id = UnicodeAttribute(hash_key=True)
    id = UnicodeAttribute(range_key=True)
    hub = LazyEnumAttribute[Hub](lambda: Hub)
    resource_account_id = UnicodeAttribute()
    arn = UnicodeAttribute()
    creator_user_id = UnicodeAttribute(null=True)
    creation_date = DateTimeAttribute()
    glue_sync = GlueSyncResourceAttribute(null=True)  # type: ignore[no-untyped-call]
    region = LazyEnumAttribute[Region](lambda: Region)
    stage = LazyEnumAttribute[Stage](lambda: Stage)
    s3 = S3ResourceAttribute(null=True)  # type: ignore[no-untyped-call]
    type = UnicodeEnumAttribute(ResourceType)
    update_date = DateTimeAttribute()
    owner_account_id = UnicodeAttribute()


class GenericResourceModel(Generic[GenericS3Resource, GenericGlueSyncResource], _ResourceAttributeContainer):
    """Generic model for resources.

    Inherit from this class to create a model for a specific GlueSyncResource class.
    """

    @staticmethod
    def get_range_key(resource_type: ResourceType, stage: Stage, region: Region) -> str:
        """Generate Dynamo range key."""
        return f"{resource_type.value}_{stage.value}_{region.value}"

    @abstractmethod
    def to_resource(self) -> Union[GenericS3Resource, GenericGlueSyncResource]:
        """Convert a resource model to a resource instance."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_resource(
        cls: Type[GenericModel], resource: Union[GenericS3Resource, GenericGlueSyncResource]
    ) -> GenericModel:
        """Convert a resource instance to a resource model."""
        raise NotImplementedError


class GenericResourcesTable(Generic[GenericS3Resource, GenericGlueSyncResource], BaseTable):
    """Class for DynamoDB Resource Tables."""

    def __init__(
        self, model_cls: Type[GenericResourceModel[GenericS3Resource, GenericGlueSyncResource]], prefix: str = ""
    ) -> None:
        self._model = create_model(table=f"{prefix}cdh-resources", model=model_cls, module=__name__)

    def get_s3(self, dataset_id: str, stage: Stage, region: Region) -> GenericS3Resource:
        """Create S3 Resource."""
        return cast(GenericS3Resource, self.get(ResourceType.s3, dataset_id, stage, region))

    def get_glue_sync(self, dataset_id: str, stage: Stage, region: Region) -> GenericGlueSyncResource:
        """Create GlueSync Resource."""
        return cast(GenericGlueSyncResource, self.get(ResourceType.glue_sync, dataset_id, stage, region))

    def get(
        self, resource_type: ResourceType, dataset_id: str, stage: Stage, region: Region
    ) -> Union[GenericS3Resource, GenericGlueSyncResource]:
        """Search for a resource."""
        key = self._model.get_range_key(resource_type, stage, region)
        return self._get_by_range_key(dataset_id, key).to_resource()

    def _get_by_range_key(
        self, dataset_id: str, range_key: str
    ) -> GenericResourceModel[GenericS3Resource, GenericGlueSyncResource]:
        try:
            return self._model.get(hash_key=dataset_id, range_key=range_key, consistent_read=True)
        except DoesNotExist as error:
            raise ResourceNotFound(dataset_id, range_key) from error

    def exists(self, resource_type: ResourceType, dataset_id: str, stage: Stage, region: Region) -> bool:
        """Check if Resource exists."""
        try:
            self.get(resource_type, dataset_id, stage, region)
            return True
        except ResourceNotFound:
            return False

    def list_s3(  # pylint: disable=too-many-arguments
        self,
        region: Optional[Region] = None,
        resource_account: Optional[AccountId] = None,
        stage: Optional[Stage] = None,
        dataset_id: Optional[str] = None,
        hub: Optional[Hub] = None,  # This must be "None" because DataExplorerSync must be able to see all buckets
        consistent_read: bool = True,
    ) -> List[GenericS3Resource]:
        """List S3 Resources and filter via the parameters passed."""
        return cast(
            List[GenericS3Resource],
            self.list(
                region=region,
                resource_account=resource_account,
                stage=stage,
                dataset_id=dataset_id,
                hub=hub,
                resource_type=ResourceType.s3,
                consistent_read=consistent_read,
            ),
        )

    def list_glue_sync(  # pylint: disable=too-many-arguments
        self,
        region: Optional[Region] = None,
        resource_account: Optional[AccountId] = None,
        stage: Optional[Stage] = None,
        dataset_id: Optional[str] = None,
        hub: Optional[Hub] = None,
        consistent_read: bool = True,
    ) -> List[GenericGlueSyncResource]:
        """List GlueSync Resources and filter via the parameters passed."""
        return cast(
            List[GenericGlueSyncResource],
            self.list(
                region=region,
                resource_account=resource_account,
                stage=stage,
                dataset_id=dataset_id,
                hub=hub,
                resource_type=ResourceType.glue_sync,
                consistent_read=consistent_read,
            ),
        )

    def list(  # pylint: disable=too-many-arguments
        self,
        region: Optional[Region] = None,
        resource_account: Optional[AccountId] = None,
        stage: Optional[Stage] = None,
        dataset_id: Optional[str] = None,
        resource_type: Optional[ResourceType] = None,
        hub: Optional[Hub] = None,  # This must be "None" because DataExplorerSync must be able to see all buckets
        owner: Optional[AccountId] = None,
        consistent_read: bool = True,
    ) -> List[Resource]:
        """List S3 and GlueSync Resources and filter via the parameters passed."""
        return list(
            self.get_resources_iterator(
                region=region,
                resource_account=resource_account,
                stage=stage,
                dataset_id=dataset_id,
                resource_type=resource_type,
                hub=hub,
                owner=owner,
                consistent_read=consistent_read,
            )
        )

    def get_resources_iterator(  # pylint: disable=too-many-arguments
        self,
        region: Optional[Region] = None,
        resource_account: Optional[AccountId] = None,
        stage: Optional[Stage] = None,
        dataset_id: Optional[str] = None,
        resource_type: Optional[ResourceType] = None,
        hub: Optional[Hub] = None,  # This must be "None" because DataExplorerSync must be able to see all buckets
        owner: Optional[AccountId] = None,
        consistent_read: bool = True,
        last_evaluated_key: Optional[LastEvaluatedKey] = None,
    ) -> DynamoItemIterator[Union[GenericS3Resource, GenericGlueSyncResource]]:
        """Get an iterator over all matching resources."""
        filter_expression: Optional[Comparison] = None
        if hub:
            filter_expression &= GenericResourceModel.hub == hub
        if region:
            filter_expression &= GenericResourceModel.region == region
        if stage:
            filter_expression &= GenericResourceModel.stage == stage
        if resource_account:
            filter_expression &= GenericResourceModel.resource_account_id == resource_account
        if resource_type:
            filter_expression &= GenericResourceModel.type == resource_type
        if owner:
            filter_expression &= GenericResourceModel.owner_account_id == owner
        if dataset_id:
            result_iterator: ResultIterator[Any] = self._model.query(
                hash_key=dataset_id,
                consistent_read=consistent_read,
                filter_condition=filter_expression,
                last_evaluated_key=last_evaluated_key,
            )
        else:
            result_iterator = self._model.scan(
                consistent_read=consistent_read,
                filter_condition=filter_expression,
                last_evaluated_key=last_evaluated_key,
            )
        return DynamoItemIterator(
            items=(model.to_resource() for model in result_iterator),
            get_last_evaluated_key=lambda: apply_if_not_none(LastEvaluatedKey)(result_iterator.last_evaluated_key),
        )

    def create(self, resource: Union[GenericS3Resource, GenericGlueSyncResource]) -> None:
        """Create a model and save it to dynamo db."""
        model = self._model.from_resource(resource)
        try:
            model.save(GenericResourceModel.id.does_not_exist())
        except PutError as error:
            if conditional_check_failed(error):
                raise ResourceAlreadyExists(resource.dataset_id, model.id) from error
            raise error

    def update_owner_account_id(
        self, resource: Union[GenericS3Resource, GenericGlueSyncResource], new_owner_account_id: AccountId
    ) -> Union[GenericS3Resource, GenericGlueSyncResource]:
        """Update owner account id."""
        range_key = self._model.get_range_key(resource_type=resource.type, stage=resource.stage, region=resource.region)
        try:
            actions: List[Action] = [self._model.owner_account_id.set(new_owner_account_id)]
            self._model.from_resource(resource).update(actions=actions, condition=self._model.id.exists())
        except UpdateError as error:
            if conditional_check_failed(error):
                raise ResourceNotFound(resource.dataset_id, range_key) from error
            raise

        return replace(resource, owner_account_id=new_owner_account_id)

    def update_kms_key_arn(self, resource: GenericS3Resource, new_kms_key_arn: Arn) -> Resource:
        """Update KMS-Key arn in resource."""
        range_key = self._model.get_range_key(resource_type=resource.type, stage=resource.stage, region=resource.region)
        try:
            field = cast(UnicodeSetAttribute, self._model.s3.kms_key_arn)
            actions: List[Action] = [field.set(str(new_kms_key_arn))]
            self._model.from_resource(resource).update(actions=actions, condition=self._model.id.exists())
        except UpdateError as error:
            if conditional_check_failed(error):
                raise ResourceNotFound(resource.dataset_id, range_key) from error
            raise

        return replace(resource, kms_key_arn=new_kms_key_arn)

    def update_glue_sync(
        self,
        resource: GenericGlueSyncResource,
        update_date: datetime,
        sync_type: Optional[SyncType] = None,
    ) -> GenericGlueSyncResource:
        """Update GlueSync resource."""
        updated_resource = resource
        actions: List[Action] = []

        if sync_type:
            actions.append(cast(UnicodeEnumAttribute[SyncType], self._model.glue_sync.sync_type).set(sync_type))
            updated_resource = replace(updated_resource, sync_type=sync_type)

        if actions:
            actions.append(self._model.update_date.set(update_date))
            updated_resource = replace(updated_resource, update_date=update_date)

            try:
                self._model.from_resource(resource).update(
                    actions=actions,
                    condition=(self._model.id.exists() & self._model.dataset_id.exists()),
                )
            except UpdateError as error:
                if conditional_check_failed(error):
                    raise ResourceNotFound(
                        resource.dataset_id,
                        self._model.get_range_key(resource.type, resource.stage, resource.region),
                    ) from error
                raise

        return updated_resource

    def delete(self, resource_type: ResourceType, dataset_id: str, stage: Stage, region: Region) -> None:
        """Delete resource from DynamoDB."""
        range_key = self._model.get_range_key(resource_type, stage, region)
        try:
            model: Any = self._model.get(hash_key=dataset_id, range_key=range_key, consistent_read=True)
            model.delete()
        except DoesNotExist as error:
            raise ResourceNotFound(dataset_id, range_key) from error


class ResourceModel(GenericResourceModel[S3Resource, GlueSyncResource]):
    """Model for resources."""

    def _get_from_class_common_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "hub": self.hub,
            "resource_account_id": AccountId(self.resource_account_id),
            "arn": Arn(self.arn),
            "creation_date": self.creation_date,
            "creator_user_id": self.creator_user_id,
            "region": self.region,
            "stage": self.stage,
            "update_date": self.update_date,
            "owner_account_id": AccountId(self.owner_account_id),
        }

    @classmethod
    def _get_from_resource_common_dict(cls, resource: Resource) -> Dict[str, Any]:
        return {
            "dataset_id": resource.dataset_id,
            "id": cls.get_range_key(resource.type, resource.stage, resource.region),
            "hub": resource.hub,
            "resource_account_id": resource.resource_account_id,
            "arn": str(resource.arn),
            "creator_user_id": resource.creator_user_id,
            "creation_date": resource.creation_date,
            "region": resource.region,
            "stage": resource.stage,
            "type": resource.type,
            "update_date": resource.update_date,
            "owner_account_id": resource.owner_account_id,
        }

    def to_resource(self) -> Union[GlueSyncResource, S3Resource]:
        """Create S3- or GlueSync-Resource."""
        common = self._get_from_class_common_dict()
        if self.type is ResourceType.glue_sync:
            return GlueSyncResource(
                database_name=DatabaseName(self.glue_sync.database_name),
                sync_type=self.glue_sync.sync_type,
                **common,
            )
        if self.type is ResourceType.s3:
            return S3Resource(
                sns_topic_arn=Arn(self.s3.sns_topic_arn),
                kms_key_arn=Arn(self.s3.kms_key_arn),
                **common,
            )
        raise TypeError(f"resources is of invalid type {type(self.type)}")

    @classmethod
    def from_resource(cls, resource: Resource) -> "ResourceModel":
        """Create a ResourceModel from an S3- or GlueSyncResource."""
        common = cls._get_from_resource_common_dict(resource=resource)
        if isinstance(resource, GlueSyncResource):
            glue_sync = GlueSyncResourceAttribute(  # type: ignore[no-untyped-call]
                database_name=resource.database_name, sync_type=resource.sync_type
            )
            return cls(
                glue_sync=glue_sync,
                **common,
            )
        if isinstance(resource, S3Resource):
            s3_resource_attribute = S3ResourceAttribute(
                sns_topic_arn=str(resource.sns_topic_arn),
                kms_key_arn=str(resource.kms_key_arn),
            )  # type: ignore[no-untyped-call]
            return cls(
                s3=s3_resource_attribute,
                **common,
            )
        raise ValueError(f"Resource of type {type(resource)} not supported!")


class ResourcesTable(GenericResourcesTable[S3Resource, GlueSyncResource]):
    """Table for resources."""

    def __init__(self, prefix: str = "") -> None:
        super().__init__(model_cls=ResourceModel, prefix=prefix)


class ResourceNotFound(Exception):
    """The Resource was not found."""

    def __init__(self, dataset_id: str, range_key: str):
        super().__init__(f"Resource {range_key} was not found in dataset {dataset_id}")


class ResourceAlreadyExists(Exception):
    """A create request was fired, but the resource already exists."""

    def __init__(self, dataset_id: str, range_key: str):
        super().__init__(f"Resource {range_key} already exists in dataset {dataset_id}")
