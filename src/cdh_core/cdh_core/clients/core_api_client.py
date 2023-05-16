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
# pylint: disable=duplicate-code
import os
from http import HTTPStatus
from logging import getLogger
from typing import Any
from typing import cast
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Type
from typing import TypeVar
from urllib.parse import urlparse

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

from cdh_core.clients.http_client import HttpClient
from cdh_core.clients.http_client import NonRetryableConflictError
from cdh_core.entities.accounts import AccountRole
from cdh_core.entities.accounts import ResponseAccount
from cdh_core.entities.accounts import ResponseAccounts
from cdh_core.entities.accounts import ResponseAccountWithoutCosts
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset import ResponseDataset
from cdh_core.entities.dataset import ResponseDatasetPermissions
from cdh_core.entities.dataset import ResponseDatasets
from cdh_core.entities.dataset import SourceIdentifier
from cdh_core.entities.dataset import SupportGroup
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.filter_package import FilterPackage
from cdh_core.entities.filter_package import FilterPackages
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.hub_business_object import HubBusinessObject
from cdh_core.entities.hub_business_object import HubBusinessObjectList
from cdh_core.entities.resource import GlueSyncResourcePayload
from cdh_core.entities.resource import ResourcePayload
from cdh_core.entities.resource import ResourcesPayload
from cdh_core.entities.resource import S3ResourcePayload
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetPurpose
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties import RetentionPeriod
from cdh_core.enums.dataset_properties import SupportLevel
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId

LOG = getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

_JsonDictType = Dict[str, Any]

NEXT_PAGE_TOKEN_KEY = "nextPageToken"

T = TypeVar("T", bound="CoreApiClient")


class CoreApiClient:  # pylint: disable=too-many-arguments,too-many-public-methods
    """The client to talk with the cdh-core-api."""

    def __init__(self, http_client: HttpClient):
        self._client = http_client

    @classmethod
    def get_core_api_client(cls: Type[T], base_url: str, region: Region) -> T:
        """Get core api client for a given base url and region."""
        return cls(
            http_client=HttpClient(
                base_url=base_url,
                credentials=BotoAWSRequestsAuth(urlparse(base_url).hostname, region.value, "execute-api"),
            ),
        )

    def create_dataset(  # pylint: disable=too-many-locals
        self,
        hub: Hub,
        business_object: BusinessObject,
        name: str,
        layer: Layer,
        description: str,
        engineers: List[DatasetParticipant],
        confidentiality: Confidentiality,
        documentation: Optional[str] = None,
        external_links: Optional[List[ExternalLink]] = None,
        friendly_name: Optional[str] = None,
        hub_visibility: Optional[Set[Hub]] = None,
        labels: Optional[List[str]] = None,
        preview_available: Optional[bool] = None,
        purpose: Optional[List[DatasetPurpose]] = None,
        retention_period: Optional[RetentionPeriod] = None,
        source_identifier: Optional[SourceIdentifier] = None,
        stewards: Optional[List[DatasetParticipant]] = None,
        support_group: Optional[SupportGroup] = None,
        support_level: Optional[SupportLevel] = None,
        tags: Optional[Dict[str, str]] = None,
        upstream_lineage: Optional[Set[DatasetId]] = None,
    ) -> ResponseDataset:
        """Create a CDH dataset."""
        body = CoreApiClient.RequestBuilder.build_new_dataset_body(
            business_object=business_object,
            name=name,
            layer=layer,
            description=description,
            confidentiality=confidentiality,
            documentation=documentation,
            engineers=engineers,
            external_links=external_links,
            friendly_name=friendly_name,
            hub_visibility=hub_visibility,
            labels=labels,
            preview_available=preview_available,
            purpose=purpose,
            retention_period=retention_period,
            source_identifier=source_identifier,
            stewards=stewards,
            support_group=support_group,
            support_level=support_level,
            tags=tags,
            upstream_lineage=upstream_lineage,
        )
        try:
            return ResponseDataset.from_dict(
                self._client.post(
                    f"/{hub.value}/datasets",
                    body=body,
                    expected_status_codes=[HTTPStatus.CREATED],
                )
            )
        except NonRetryableConflictError:
            LOG.info(
                f"NonRetryableConflictError detected. Trying to recover: {hub.value}/{business_object}/{name}/{layer}"
            )
            try:
                return self.get_dataset(
                    hub=hub,
                    dataset_id=Dataset.build_id(business_object, name, layer, hub),
                )
            except Exception as error:  # pylint: disable=broad-except
                LOG.error(error)
            raise

    def create_s3_resource(
        self,
        hub: Hub,
        dataset_id: DatasetId,
        stage: Stage,
        region: Region,
        seconds_between_retries: Optional[int] = None,
    ) -> S3ResourcePayload:
        """Create a S3 resource (bucket)."""
        return S3ResourcePayload.from_dict(
            self._create_new_resource_raw(
                hub=hub,
                dataset_id=dataset_id,
                stage=stage,
                region=region,
                resource_type=ResourceType.s3,
                seconds_between_retries=seconds_between_retries,
            )
        )

    def create_glue_sync(
        self,
        hub: Hub,
        dataset_id: DatasetId,
        stage: Stage,
        region: Region,
        sync_type: Optional[SyncType] = None,
        seconds_between_retries: Optional[int] = None,
    ) -> GlueSyncResourcePayload:
        """Create a Glue-Sync resource."""
        return GlueSyncResourcePayload.from_dict(
            self._create_new_resource_raw(
                hub=hub,
                dataset_id=dataset_id,
                stage=stage,
                region=region,
                resource_type=ResourceType.glue_sync,
                sync_type=sync_type,
                seconds_between_retries=seconds_between_retries,
            )
        )

    def _create_new_resource_raw(
        self,
        hub: Hub,
        dataset_id: DatasetId,
        stage: Stage,
        region: Region,
        resource_type: ResourceType,
        sync_type: Optional[SyncType] = None,
        seconds_between_retries: Optional[int] = None,
    ) -> _JsonDictType:
        body = CoreApiClient.RequestBuilder.build_new_resource_body(dataset_id=dataset_id, stage=stage, region=region)
        if resource_type is ResourceType.glue_sync and sync_type:
            body["syncType"] = sync_type.value

        try:
            return self._client.post(
                f"/{hub.value}/resources/{resource_type.value}",
                body=body,
                expected_status_codes=[HTTPStatus.CREATED],
                seconds_between_retries=seconds_between_retries,
            )
        except NonRetryableConflictError:
            try:
                return self._get_resource_raw(
                    hub=hub, dataset_id=dataset_id, stage=stage, region=region, resource_type=resource_type
                )
            except Exception:  # pylint: disable=broad-except
                LOG.exception("Failed to create the resource and failed to check if it really failed.")
            raise

    def grant_dataset_permission(
        self, hub: Hub, dataset_id: DatasetId, account_id: AccountId, stage: Stage, region: Region
    ) -> DatasetAccountPermission:
        """Add the permission to the dataset."""
        body = CoreApiClient.RequestBuilder.build_dataset_permission_post_body(
            account_id=account_id, stage=stage, region=region
        )
        return self._send_dataset_permission_request(hub, dataset_id, body)

    def _send_dataset_permission_request(
        self, hub: Hub, dataset_id: DatasetId, body: _JsonDictType
    ) -> DatasetAccountPermission:
        """Send the request for adding the permission to the dataset."""
        try:
            return DatasetAccountPermission.from_dict(
                self._client.post(
                    path=f"/{hub.value}/datasets/{dataset_id}/permissions",
                    body=body,
                    expected_status_codes=[HTTPStatus.CREATED],
                )
            )
        except NonRetryableConflictError:
            try:
                permissions = self.get_dataset_permissions(hub=hub, dataset_id=dataset_id)
                for permission in permissions:
                    if (
                        permission.account_id == body["accountId"]
                        and permission.stage.value == body["stage"]
                        and permission.region.value == body["region"]
                    ):
                        return permission
            except Exception:  # pylint: disable=broad-except
                LOG.exception("Failed to update the permissions and cannot return the original ones.")
            raise

    def revoke_dataset_permission(
        self, hub: Hub, dataset_id: DatasetId, account_id: AccountId, stage: Stage, region: Region
    ) -> None:
        """Remove the permission of a dataset."""
        body = CoreApiClient.RequestBuilder.build_dataset_permission_delete_body(
            account_id=account_id, stage=stage, region=region
        )
        try:
            self._client.delete(
                f"/{hub.value}/datasets/{dataset_id}/permissions", body=body, expected_status_codes=[HTTPStatus.OK]
            )
        except NonRetryableConflictError:
            if any(
                remaining_permission.account_id == account_id
                and remaining_permission.stage == stage
                and remaining_permission.region == region
                for remaining_permission in self.get_dataset_permissions(hub, dataset_id)
            ):
                raise

    def get_datasets(self, hub: Hub) -> List[ResponseDataset]:
        """Get all datasets."""
        return ResponseDatasets.from_dict(
            self._client.get_with_pagination(
                f"/{hub.value}/datasets",
                next_page_key=NEXT_PAGE_TOKEN_KEY,
                item_key="datasets",
            )
        ).datasets

    def get_datasets_cross_hub(self, dataset_ids: List[DatasetId]) -> List[ResponseDataset]:
        """Get all datasets with the given IDs, across hubs, ignoring missing items."""
        return ResponseDatasets.from_dict(
            self._client.get(
                "/datasets", params={"ids": cast(List[str], dataset_ids)}, expected_status_codes=[HTTPStatus.OK]
            )
        ).datasets

    def get_dataset(self, hub: Hub, dataset_id: DatasetId) -> ResponseDataset:
        """Get a single dataset."""
        return ResponseDataset.from_dict(
            self._client.get(
                f"/{hub.value}/datasets/{dataset_id}",
                expected_status_codes=[HTTPStatus.OK],
            )
        )

    def get_dataset_permissions(self, hub: Hub, dataset_id: DatasetId) -> FrozenSet[DatasetAccountPermission]:
        """Get the permissions for a dataset."""
        return ResponseDatasetPermissions.from_dict(
            self._client.get(
                f"/{hub.value}/datasets/{dataset_id}/permissions",
                expected_status_codes=[HTTPStatus.OK],
            )
        ).permissions

    def delete_dataset(self, hub: Hub, dataset_id: DatasetId) -> None:
        """Delete a dataset."""
        try:
            self._client.delete(
                f"/{hub.value}/datasets/{dataset_id}",
                expected_status_codes=[HTTPStatus.NO_CONTENT],
            )
        except NonRetryableConflictError:
            self._client.get(
                f"/{hub.value}/datasets/{dataset_id}",
                expected_status_codes=[HTTPStatus.NOT_FOUND],
            )

    def get_resources(
        self,
        hub: Hub,
        dataset_id: Optional[DatasetId] = None,
        stage: Optional[Stage] = None,
        region: Optional[Region] = None,
        resource_account_id: Optional[AccountId] = None,
    ) -> Sequence[ResourcePayload]:
        """Get all resources regardless of their type."""
        return ResourcesPayload.from_dict(
            self._client.get_with_pagination(
                f"/{hub.value}/resources",
                params=CoreApiClient.RequestBuilder.build_resources_query_param(
                    dataset_id=dataset_id, stage=stage, region=region, resource_account_id=resource_account_id
                ),
                next_page_key=NEXT_PAGE_TOKEN_KEY,
                item_key="resources",
            )
        ).resources

    def get_filter_packages(
        self,
        hub: Hub,
        dataset_id: DatasetId,
        stage: Stage,
        region: Region,
    ) -> List[FilterPackage]:
        """Get all filter packages."""
        return FilterPackages.from_dict(
            self._client.get(
                f"/{hub.value}/resources/glue-sync/{dataset_id}/{stage.value}/{region.value}/filter-packages",
                expected_status_codes=[HTTPStatus.OK],
            )
        ).filter_packages

    def get_filter_package(
        self,
        hub: Hub,
        dataset_id: DatasetId,
        stage: Stage,
        region: Region,
        package_id: PackageId,
    ) -> FilterPackage:
        """Get a single filter package."""
        return FilterPackage.from_dict(
            self._client.get(
                f"/{hub.value}/resources/glue-sync/{dataset_id}/{stage.value}/{region.value}"
                f"/filter-packages/{package_id}",
                expected_status_codes=[HTTPStatus.OK],
            )
        )

    def delete_resource(
        self,
        hub: Hub,
        dataset_id: DatasetId,
        resource_type: ResourceType,
        stage: Stage,
        region: Region,
        fail_if_not_found: bool = True,
    ) -> None:
        """Delete a dataset resource."""
        expected_status_codes = (
            [HTTPStatus.NO_CONTENT] if fail_if_not_found else [HTTPStatus.NO_CONTENT, HTTPStatus.NOT_FOUND]
        )
        self._client.delete(
            f"/{hub.value}/resources/{resource_type.value}/{dataset_id}/{stage.value}/{region.value}",
            expected_status_codes=expected_status_codes,
        )

    def get_s3_resource(self, hub: Hub, dataset_id: DatasetId, stage: Stage, region: Region) -> S3ResourcePayload:
        """Get a single S3 resource."""
        return S3ResourcePayload.from_dict(
            self._get_resource_raw(
                hub=hub, dataset_id=dataset_id, stage=stage, region=region, resource_type=ResourceType.s3
            )
        )

    def get_s3_resource_by_bucket_name(self, bucket_name: str) -> S3ResourcePayload:
        """Get a single S3 resource."""
        return S3ResourcePayload.from_dict(
            self._client.get("/resources/s3", params={"bucketName": bucket_name}, expected_status_codes=[HTTPStatus.OK])
        )

    def get_glue_resource(
        self, hub: Hub, dataset_id: DatasetId, stage: Stage, region: Region
    ) -> GlueSyncResourcePayload:
        """Get a single Glue resource."""
        return GlueSyncResourcePayload.from_dict(
            self._get_resource_raw(
                hub=hub, dataset_id=dataset_id, stage=stage, region=region, resource_type=ResourceType.glue_sync
            )
        )

    def _get_resource_raw(
        self, hub: Hub, dataset_id: DatasetId, stage: Stage, region: Region, resource_type: ResourceType
    ) -> _JsonDictType:
        """Get a single resource."""
        return self._client.get(
            f"/{hub.value}/resources/{resource_type.value}/{dataset_id}/{stage.value}/{region.value}",
            expected_status_codes=[HTTPStatus.OK],
        )

    def get_accounts(self) -> List[ResponseAccountWithoutCosts]:
        """Return all accounts."""
        return ResponseAccounts.from_dict(
            self._client.get_with_pagination(
                "/accounts",
                next_page_key=NEXT_PAGE_TOKEN_KEY,
                item_key="accounts",
            )
        ).accounts

    def get_account(self, account_id: AccountId) -> ResponseAccount:
        """Return a single account."""
        return ResponseAccount.from_dict(
            self._client.get(f"/accounts/{account_id}", expected_status_codes=[HTTPStatus.OK])
        )

    def register_account(  # pylint: disable=too-many-locals
        self,
        account_id: str,
        affiliation: Affiliation,
        business_objects: List[BusinessObject],
        hub: Hub,
        layers: List[Layer],
        stages: List[Stage],
        type: AccountType,  # pylint: disable=redefined-builtin
        visible_in_hubs: List[Hub],
        friendly_name: str,
        admin_roles: Optional[List[str]] = None,
        group: Optional[str] = None,
        responsibles: Optional[List[str]] = None,
        request_id: Optional[str] = None,
        roles: Optional[List[AccountRole]] = None,
        fail_if_exists: bool = True,
    ) -> Optional[ResponseAccount]:
        """Register a new account with the CDH.

        If fail_if_exists is set to False, the call will not fail if the account is already registered.
        """
        body = CoreApiClient.RequestBuilder.build_new_account_body(
            account_id=account_id,
            admin_roles=admin_roles,
            affiliation=affiliation,
            business_objects=business_objects,
            hub=hub,
            layers=layers,
            stages=stages,
            type=type,
            visible_in_hubs=visible_in_hubs,
            friendly_name=friendly_name,
            group=group,
            responsibles=responsibles,
            request_id=request_id,
            roles=roles,
        )

        if fail_if_exists:
            response = self._client.post("/accounts", body=body, expected_status_codes=[HTTPStatus.CREATED])
        else:
            response = self._client.post(
                "/accounts", body=body, expected_status_codes=[HTTPStatus.CREATED, HTTPStatus.CONFLICT]
            )
            if response.get("Code") == "ConflictError":
                return None

        return ResponseAccount.from_dict(response)

    def deregister_account(self, account_id: AccountId) -> None:
        """Deregister an account from the CDH."""
        self._client.delete(f"/accounts/{account_id}", expected_status_codes=[HTTPStatus.NO_CONTENT])

    def update_account(
        self,
        account_id: str,
        admin_roles: Optional[List[str]] = None,
        affiliation: Optional[Affiliation] = None,
        business_objects: Optional[List[BusinessObject]] = None,
        friendly_name: Optional[str] = None,
        group: Optional[str] = None,
        layers: Optional[List[Layer]] = None,
        responsibles: Optional[List[str]] = None,
        roles: Optional[List[AccountRole]] = None,
        stages: Optional[List[Stage]] = None,
        type: Optional[AccountType] = None,  # pylint: disable=redefined-builtin
        visible_in_hubs: Optional[List[Hub]] = None,
    ) -> ResponseAccount:
        """Update the specified parameters of an account.

        Hint: Optional values and None are handled identically by core_api and will not change data
        """
        body = CoreApiClient.RequestBuilder.build_update_account_body(
            admin_roles=admin_roles,
            affiliation=affiliation,
            business_objects=business_objects,
            friendly_name=friendly_name,
            group=group,
            layers=layers,
            responsibles=responsibles,
            roles=roles,
            stages=stages,
            type=type,
            visible_in_hubs=visible_in_hubs,
        )
        return ResponseAccount.from_dict(
            self._client.put(f"/accounts/{account_id}", body=body, expected_status_codes=[HTTPStatus.OK])
        )

    def update_account_billing(
        self,
        account_id: str,
        cost_history: Optional[Dict[str, float]] = None,
        estimated_cost: Optional[float] = None,
        forecasted_cost: Optional[float] = None,
    ) -> ResponseAccount:
        """Update the specified billing information of an account.

        Hint: Optional values and None are handled identically by core_api and will not change data
        """
        body = CoreApiClient.RequestBuilder.build_update_account_billing_body(
            cost_history=cost_history, estimated_cost=estimated_cost, forecasted_cost=forecasted_cost
        )
        return ResponseAccount.from_dict(
            self._client.put(f"/accounts/{account_id}/billing", body=body, expected_status_codes=[HTTPStatus.OK])
        )

    def rename_dataset(
        self,
        hub: Hub,
        new_friendly_name: str,
        dataset_id: DatasetId,
    ) -> ResponseDataset:
        """Change the dataset name."""
        return self.update_dataset(hub=hub, dataset_id=dataset_id, friendly_name=new_friendly_name)

    def update_dataset(  # pylint: disable=too-many-locals
        self,
        hub: Hub,
        dataset_id: DatasetId,
        confidentiality: Optional[Confidentiality] = None,
        contains_pii: Optional[bool] = None,
        description: Optional[str] = None,
        documentation: Optional[str] = None,
        engineers: Optional[List[DatasetParticipant]] = None,
        external_links: Optional[List[ExternalLink]] = None,
        friendly_name: Optional[str] = None,
        hub_visibility: Optional[Set[Hub]] = None,
        labels: Optional[List[str]] = None,
        preview_available: Optional[bool] = None,
        purpose: Optional[List[DatasetPurpose]] = None,
        retention_period: Optional[RetentionPeriod] = None,
        source_identifier: Optional[SourceIdentifier] = None,
        stewards: Optional[List[DatasetParticipant]] = None,
        support_group: Optional[SupportGroup] = None,
        support_level: Optional[SupportLevel] = None,
        tags: Optional[Dict[str, str]] = None,
        upstream_lineage: Optional[Set[DatasetId]] = None,
        quality_score: Optional[int] = None,
    ) -> ResponseDataset:
        """Update the specified parameters of a dataset."""
        body = CoreApiClient.RequestBuilder.build_update_dataset_body(
            description=description,
            confidentiality=confidentiality,
            contains_pii=contains_pii,
            documentation=documentation,
            engineers=engineers,
            external_links=external_links,
            friendly_name=friendly_name,
            hub_visibility=hub_visibility,
            labels=labels,
            preview_available=preview_available,
            purpose=purpose,
            retention_period=retention_period,
            source_identifier=source_identifier,
            stewards=stewards,
            support_group=support_group,
            support_level=support_level,
            tags=tags,
            upstream_lineage=upstream_lineage,
            quality_score=quality_score,
        )
        return ResponseDataset.from_dict(
            self._client.put(
                f"/{hub.value}/datasets/{dataset_id}",
                body=body,
                expected_status_codes=[HTTPStatus.OK],
            )
        )

    def get_hub_business_object(self, hub: Hub, business_object: BusinessObject) -> HubBusinessObject:
        """Get a single HubBusinessObject."""
        return HubBusinessObject.from_dict(
            self._client.get(
                f"/{hub.value}/businessObjects/{business_object.value}",
                expected_status_codes=[HTTPStatus.OK],
            )
        )

    def get_hub_business_objects(self, hub: Hub) -> List[HubBusinessObject]:
        """Get all HubBusinessObjects for a given hub."""
        return HubBusinessObjectList.from_dict(
            self._client.get(
                f"/{hub.value}/businessObjects",
                expected_status_codes=[HTTPStatus.OK],
            )
        ).business_objects

    class RequestBuilder:  # this class should disappear with #44
        """Builds dicts which can be used to generate JSON objects."""

        @staticmethod
        def build_new_account_body(  # noqa: D102
            account_id: str,
            affiliation: Affiliation,
            business_objects: List[BusinessObject],
            friendly_name: str,
            hub: Hub,
            layers: List[Layer],
            stages: List[Stage],
            type: AccountType,  # pylint: disable=redefined-builtin
            visible_in_hubs: List[Hub],
            admin_roles: Optional[List[str]] = None,
            group: Optional[str] = None,
            responsibles: Optional[List[str]] = None,
            request_id: Optional[str] = None,
            roles: Optional[List[AccountRole]] = None,
        ) -> _JsonDictType:
            json = {
                "id": account_id,
                "affiliation": affiliation.value,
                "businessObjects": [business_object.value for business_object in business_objects],
                "friendlyName": friendly_name,
                "hub": hub.value,
                "layers": [layer.value for layer in layers],
                "stages": [stage.value for stage in stages],
                "type": type.value,
                "visibleInHubs": [hub.value for hub in visible_in_hubs],
                "group": group,
                "responsibles": responsibles or [],
                "requestId": request_id,
            }
            if admin_roles is not None:
                json["adminRoles"] = admin_roles
            if roles is not None:
                json["roles"] = [
                    {"name": role.name, "path": role.path, "type": role.type.value, "friendlyName": role.friendly_name}
                    for role in roles
                ]
            return json

        @staticmethod
        def build_update_account_body(  # noqa: D102
            admin_roles: Optional[List[str]] = None,
            affiliation: Optional[Affiliation] = None,
            business_objects: Optional[List[BusinessObject]] = None,
            friendly_name: Optional[str] = None,
            group: Optional[str] = None,
            layers: Optional[List[Layer]] = None,
            responsibles: Optional[List[str]] = None,
            roles: Optional[List[AccountRole]] = None,
            stages: Optional[List[Stage]] = None,
            type: Optional[AccountType] = None,  # pylint: disable=redefined-builtin
            visible_in_hubs: Optional[List[Hub]] = None,
        ) -> _JsonDictType:
            json: _JsonDictType = {}
            if admin_roles is not None:
                json["adminRoles"] = admin_roles
            if affiliation:
                json["affiliation"] = affiliation.value
            if business_objects is not None:
                json["businessObjects"] = [bo.value for bo in business_objects]
            if friendly_name is not None:
                json["friendlyName"] = friendly_name
            if group is not None:
                json["group"] = group
            if layers is not None:
                json["layers"] = [layer.value for layer in layers]
            if stages is not None:
                json["stages"] = [stage.value for stage in stages]
            if type:
                json["type"] = type.value
            if responsibles is not None:
                json["responsibles"] = responsibles
            if roles is not None:
                json["roles"] = [
                    {"name": role.name, "path": role.path, "type": role.type.value, "friendlyName": role.friendly_name}
                    for role in roles
                ]
            if visible_in_hubs is not None:
                json["visibleInHubs"] = [hub.value for hub in visible_in_hubs]
            return json

        @staticmethod
        def build_new_resource_body(  # noqa: D102
            dataset_id: DatasetId, stage: Stage, region: Region
        ) -> _JsonDictType:  # noqa: D102
            return {
                "datasetId": dataset_id,
                "stage": stage.value,
                "region": region.value,
            }

        @staticmethod
        def build_dataset_permission_post_body(  # noqa: D102
            account_id: AccountId, stage: Stage, region: Region
        ) -> _JsonDictType:
            return {"accountId": account_id, "stage": stage.value, "region": region.value}

        @staticmethod
        def build_dataset_permission_delete_body(  # noqa: D102
            account_id: AccountId,
            stage: Stage,
            region: Region,
        ) -> _JsonDictType:
            return {"accountId": account_id, "stage": stage.value, "region": region.value}

        @staticmethod
        def build_new_dataset_body(  # noqa: D102, pylint: disable=too-many-locals,too-many-branches
            business_object: BusinessObject,
            name: str,
            layer: Layer,
            description: str,
            engineers: List[DatasetParticipant],
            confidentiality: Confidentiality,
            documentation: Optional[str] = None,
            external_links: Optional[List[ExternalLink]] = None,
            friendly_name: Optional[str] = None,
            hub_visibility: Optional[Set[Hub]] = None,
            labels: Optional[List[str]] = None,
            preview_available: Optional[bool] = None,
            purpose: Optional[List[DatasetPurpose]] = None,
            retention_period: Optional[RetentionPeriod] = None,
            source_identifier: Optional[SourceIdentifier] = None,
            stewards: Optional[List[DatasetParticipant]] = None,
            support_group: Optional[SupportGroup] = None,
            support_level: Optional[SupportLevel] = None,
            tags: Optional[Dict[str, str]] = None,
            upstream_lineage: Optional[Set[DatasetId]] = None,
        ) -> _JsonDictType:
            body = {
                "name": name,
                "businessObject": business_object.value,
                "confidentiality": confidentiality.value,
                "containsPii": False,
                "description": description or "",
                "engineers": [{"id": engineer.id, "idp": engineer.idp} for engineer in engineers],
                "friendlyName": friendly_name or name,
                "layer": layer.value,
                "tags": tags or {},
            }
            if documentation is not None:
                body["documentation"] = documentation
            if external_links is not None:
                body["externalLinks"] = (
                    [{"type": link.type.value, "name": link.name, "url": link.url} for link in external_links],
                )
            if hub_visibility is not None:
                body["hubVisibility"] = [hub.value for hub in hub_visibility]
            if labels is not None:
                body["labels"] = labels
            if preview_available is not None:
                body["previewAvailable"] = preview_available
            if purpose is not None:
                body["purpose"] = [pur.value for pur in purpose]
            if retention_period is not None:
                body["retentionPeriod"] = retention_period.value
            if source_identifier is not None:
                body["sourceIdentifier"] = source_identifier
            if stewards is not None:
                body["stewards"] = [{"id": steward.id, "idp": steward.idp} for steward in stewards]
            if support_group is not None:
                body["supportGroup"] = support_group
            if support_level is not None:
                body["supportLevel"] = support_level.value
            if upstream_lineage is not None:
                body["upstreamLineage"] = list(upstream_lineage)
            return body

        @staticmethod
        def build_update_account_billing_body(  # noqa: D102
            cost_history: Optional[Dict[str, float]], estimated_cost: Optional[float], forecasted_cost: Optional[float]
        ) -> _JsonDictType:
            return {"costHistory": cost_history, "estimatedCost": estimated_cost, "forecastedCost": forecasted_cost}

        @staticmethod
        def build_update_dataset_body(  # noqa: D102, pylint: disable=too-many-locals,too-many-branches
            confidentiality: Optional[Confidentiality] = None,
            contains_pii: Optional[bool] = None,
            description: Optional[str] = None,
            documentation: Optional[str] = None,
            engineers: Optional[List[DatasetParticipant]] = None,
            external_links: Optional[List[ExternalLink]] = None,
            friendly_name: Optional[str] = None,
            hub_visibility: Optional[Set[Hub]] = None,
            labels: Optional[List[str]] = None,
            preview_available: Optional[bool] = None,
            purpose: Optional[List[DatasetPurpose]] = None,
            retention_period: Optional[RetentionPeriod] = None,
            source_identifier: Optional[SourceIdentifier] = None,
            stewards: Optional[List[DatasetParticipant]] = None,
            support_group: Optional[SupportGroup] = None,
            support_level: Optional[SupportLevel] = None,
            tags: Optional[Dict[str, str]] = None,
            upstream_lineage: Optional[Set[DatasetId]] = None,
            quality_score: Optional[int] = None,
        ) -> _JsonDictType:
            body: _JsonDictType = {}
            if confidentiality is not None:
                body["confidentiality"] = confidentiality.value
            if contains_pii is not None:
                body["containsPii"] = contains_pii
            if description is not None:
                body["description"] = description
            if documentation is not None:
                body["documentation"] = documentation
            if engineers is not None:
                body["engineers"] = [{"id": engineer.id, "idp": engineer.idp} for engineer in engineers]
            if external_links is not None:
                body["externalLinks"] = (
                    [{"type": link.type.value, "name": link.name, "url": link.url} for link in external_links],
                )
            if friendly_name is not None:
                body["friendlyName"] = friendly_name
            if hub_visibility is not None:
                body["hubVisibility"] = [hub.value for hub in hub_visibility]
            if labels is not None:
                body["labels"] = labels
            if preview_available is not None:
                body["previewAvailable"] = preview_available
            if purpose is not None:
                body["purpose"] = [pur.value for pur in purpose]
            if retention_period is not None:
                body["retentionPeriod"] = retention_period.value
            if source_identifier is not None:
                body["sourceIdentifier"] = source_identifier
            if stewards is not None:
                body["stewards"] = [{"id": steward.id, "idp": steward.idp} for steward in stewards]
            if support_group is not None:
                body["supportGroup"] = support_group
            if support_level is not None:
                body["supportLevel"] = support_level.value
            if tags is not None:
                body["tags"] = tags
            if upstream_lineage is not None:
                body["upstreamLineage"] = list(upstream_lineage)
            if quality_score is not None:
                body["qualityScore"] = quality_score
            return body

        @staticmethod
        def build_resources_query_param(  # noqa: D102
            dataset_id: Optional[DatasetId],
            stage: Optional[Stage],
            region: Optional[Region],
            resource_account_id: Optional[AccountId],
        ) -> Dict[str, str]:
            body: Dict[str, str] = {}
            if dataset_id is not None:
                body["datasetId"] = dataset_id
            if stage is not None:
                body["stage"] = stage.value
            if region is not None:
                body["region"] = region.value
            if resource_account_id is not None:
                body["resourceAccountId"] = resource_account_id
            return body
