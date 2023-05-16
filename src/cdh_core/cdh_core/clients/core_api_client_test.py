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
import random
import re
from contextlib import suppress
from dataclasses import fields
from dataclasses import is_dataclass
from datetime import datetime
from enum import Enum
from http import HTTPStatus
from inspect import getfullargspec
from inspect import ismethod
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import get_args
from typing import get_origin
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from typing import Union
from unittest.mock import Mock

import pytest
import requests
from marshmallow import validate

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.clients.http_client import HttpClient
from cdh_core.clients.http_client import NonRetryableConflictError
from cdh_core.entities.accounts import ResponseAccounts
from cdh_core.entities.accounts_test import build_response_account
from cdh_core.entities.accounts_test import build_response_account_without_costs
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset import ResponseDataset
from cdh_core.entities.dataset import ResponseDatasets
from cdh_core.entities.dataset import SourceIdentifier
from cdh_core.entities.dataset import SupportGroup
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.dataset_participants_test import build_dataset_participant
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.dataset_test import build_external_link
from cdh_core.entities.dataset_test import build_response_dataset
from cdh_core.entities.dataset_test import build_source_identifier
from cdh_core.entities.dataset_test import build_support_group
from cdh_core.entities.filter_package import FilterPackages
from cdh_core.entities.filter_package import PackageId
from cdh_core.entities.filter_package_test import build_filter_package
from cdh_core.entities.hub_business_object_test import build_hub_business_object
from cdh_core.entities.resource import ResourcePayload
from cdh_core.entities.resource import ResourcesPayload
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_sync_type
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def get_response_body(data: Any) -> Dict[str, Any]:
    return json.loads(JsonResponse(data).to_dict()["body"])  # type: ignore


class TestCoreApiClient:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: Any) -> None:  # pylint: disable=unused-argument
        self.hub = build_hub()
        self.dataset_id = DatasetId(Builder.build_random_string())
        self.stage = build_stage()
        self.region = build_region()
        self.account_id = build_account_id()
        self.sync_type, self.other_sync_type = Builder.choose_without_repetition(SyncType, 2)
        self.http_client = Mock(spec_set=HttpClient)
        self.core_api_client = CoreApiClient(http_client=self.http_client)

    def test_grant_dataset_permission_ok(
        self,
    ) -> None:
        dataset_permission_response_json = {
            "accountId": self.account_id,
            "stage": self.stage.value,
            "region": self.region.value,
            "syncType": self.sync_type.value,
        }
        self.http_client.post.side_effect = [dataset_permission_response_json]

        response = self.core_api_client.grant_dataset_permission(
            hub=self.hub,
            dataset_id=self.dataset_id,
            account_id=self.account_id,
            stage=self.stage,
            region=self.region,
        )
        assert get_response_body(response) == dataset_permission_response_json

    def test_grant_dataset_permission_non_retryable_conflict_recovered(
        self,
    ) -> None:
        dataset_permission_response_json = {
            "permissions": [
                {
                    "accountId": build_account_id(),
                    "stage": self.stage.value,
                    "region": self.region.value,
                    "syncType": build_sync_type().value,
                },
                {
                    "accountId": self.account_id,
                    "stage": self.stage.value,
                    "region": self.region.value,
                    "syncType": self.other_sync_type.value,
                },
            ]
        }
        self.http_client.get.return_value = dataset_permission_response_json
        self.http_client.post.side_effect = NonRetryableConflictError(
            "Account ... already has access to dataset ... in stage ... and region ..."
        )
        response = self.core_api_client.grant_dataset_permission(
            hub=self.hub,
            dataset_id=self.dataset_id,
            account_id=self.account_id,
            stage=self.stage,
            region=self.region,
        )
        assert get_response_body(response) == dataset_permission_response_json["permissions"][1]

    def test_grant_dataset_permission_non_retryable_conflict_with_not_found(
        self,
    ) -> None:
        self.http_client.post.side_effect = NonRetryableConflictError("Some error occurred")
        self.http_client.get.return_value = {"permissions": []}

        with pytest.raises(NonRetryableConflictError):
            self.core_api_client.grant_dataset_permission(
                hub=self.hub,
                dataset_id=self.dataset_id,
                account_id=self.account_id,
                stage=self.stage,
                region=self.region,
            )
        self.http_client.get.assert_called_once_with(
            f"/{self.hub.value}/datasets/{self.dataset_id}/permissions", expected_status_codes=[HTTPStatus.OK]
        )

    def test_grant_dataset_permission_non_retryable_conflict_with_error(
        self,
    ) -> None:
        self.http_client.post.side_effect = NonRetryableConflictError("Some error occurred")
        self.http_client.get.side_effect = Exception("test error")

        with pytest.raises(NonRetryableConflictError):
            self.core_api_client.grant_dataset_permission(
                hub=self.hub,
                dataset_id=self.dataset_id,
                account_id=self.account_id,
                stage=self.stage,
                region=self.region,
            )
        self.http_client.get.assert_called_once_with(
            f"/{self.hub.value}/datasets/{self.dataset_id}/permissions", expected_status_codes=[HTTPStatus.OK]
        )

    def test_revoke_dataset_permission_non_retryable_conflict_but_succeeded(self) -> None:
        self.http_client.delete.side_effect = NonRetryableConflictError("Some error occurred")
        dataset_permission_response = {
            "permissions": [
                {
                    "accountId": "some_other_account_id",
                    "stage": self.stage.value,
                    "region": self.region.value,
                    "syncType": self.sync_type.value,
                }
            ]
        }
        self.http_client.get.return_value = dataset_permission_response
        self.core_api_client.revoke_dataset_permission(
            hub=self.hub,
            dataset_id=self.dataset_id,
            account_id=self.account_id,
            stage=self.stage,
            region=self.region,
        )
        self.http_client.get.assert_called_once_with(
            f"/{self.hub.value}/datasets/{self.dataset_id}/permissions", expected_status_codes=[HTTPStatus.OK]
        )

    def test_revoke_dataset_permission_non_retryable_conflict_but_fails(self) -> None:
        self.http_client.delete.side_effect = NonRetryableConflictError("Some error occurred")
        dataset_permission_response = {
            "permissions": [
                {
                    "accountId": self.account_id,
                    "stage": self.stage.value,
                    "region": self.region.value,
                    "syncType": self.sync_type.value,
                }
            ]
        }
        self.http_client.get.return_value = dataset_permission_response
        with pytest.raises(NonRetryableConflictError):
            self.core_api_client.revoke_dataset_permission(
                hub=self.hub,
                dataset_id=self.dataset_id,
                account_id=self.account_id,
                stage=self.stage,
                region=self.region,
            )
        self.http_client.get.assert_called_once_with(
            f"/{self.hub.value}/datasets/{self.dataset_id}/permissions", expected_status_codes=[HTTPStatus.OK]
        )

    def test_revoke_dataset_permission_only_for_one_stage(self) -> None:
        self.http_client.delete.side_effect = NonRetryableConflictError("Some error occurred")
        dataset_permission_response = {
            "permissions": [
                {
                    "accountId": self.account_id,
                    "stage": self.stage.value,
                    "region": Builder.get_random_element(set(Region), {self.region}).value,
                    "syncType": self.sync_type.value,
                }
            ]
        }
        self.http_client.get.return_value = dataset_permission_response
        self.core_api_client.revoke_dataset_permission(
            hub=self.hub,
            dataset_id=self.dataset_id,
            account_id=self.account_id,
            stage=self.stage,
            region=self.region,
        )
        self.http_client.get.assert_called_once_with(
            f"/{self.hub.value}/datasets/{self.dataset_id}/permissions", expected_status_codes=[HTTPStatus.OK]
        )


class TestCoreApiClientNoneMocked:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.stage = build_stage()
        self.region = build_region()
        self.dataset_id = build_dataset().id
        self.http_client = Mock(spec=HttpClient)
        self.core_api_client = CoreApiClient(http_client=self.http_client)

    @pytest.mark.parametrize(
        "dataset_participants",
        [
            DatasetParticipants([], []),
            DatasetParticipants([build_dataset_participant()], []),
            DatasetParticipants([build_dataset_participant()], [build_dataset_participant()]),
        ],
    )
    def test_get_dataset(self, dataset_participants: DatasetParticipants) -> None:
        dataset = build_dataset(hub=self.hub, update_date=datetime.today())
        response_dataset = ResponseDataset.from_dataset(dataset, dataset_participants=dataset_participants)
        self.http_client.get.return_value = get_response_body(response_dataset)
        assert self.core_api_client.get_dataset(dataset_id=dataset.id, hub=self.hub) == response_dataset

    def test_get_datasets(self) -> None:
        datasets = [build_response_dataset() for _ in range(5)]
        self.http_client.get_with_pagination.return_value = get_response_body(ResponseDatasets(datasets=datasets))
        assert self.core_api_client.get_datasets(hub=self.hub) == datasets

    def test_create_dataset(self) -> None:
        dataset = build_response_dataset()
        self.http_client.post.return_value = get_response_body(dataset)

        assert (
            self.core_api_client.create_dataset(
                hub=dataset.hub,
                business_object=dataset.business_object,
                confidentiality=dataset.confidentiality,
                name=dataset.name,
                layer=dataset.layer,
                engineers=dataset.engineers,
                description=dataset.description,
            )
            == dataset
        )

    def test_create_dataset_failure(self) -> None:
        dataset = build_response_dataset()
        self.http_client.post.side_effect = NonRetryableConflictError
        self.http_client.get.side_effect = Exception("recovery impossible")

        with pytest.raises(NonRetryableConflictError):
            self.core_api_client.create_dataset(
                hub=dataset.hub,
                business_object=dataset.business_object,
                confidentiality=dataset.confidentiality,
                name=dataset.name,
                layer=dataset.layer,
                engineers=dataset.engineers,
                description=dataset.description,
            )

    def test_create_dataset_failure_recovery(self) -> None:
        dataset = build_response_dataset()
        self.http_client.post.side_effect = NonRetryableConflictError
        self.http_client.get.return_value = get_response_body(dataset)

        assert (
            self.core_api_client.create_dataset(
                hub=dataset.hub,
                business_object=dataset.business_object,
                confidentiality=dataset.confidentiality,
                name=dataset.name,
                layer=dataset.layer,
                description=dataset.description,
                engineers=dataset.engineers,
            )
            == dataset
        )

    def test_update_dataset(self) -> None:
        dataset = build_response_dataset()
        self.http_client.put.return_value = get_response_body(dataset)
        assert (
            self.core_api_client.update_dataset(
                dataset_id=dataset.id, hub=dataset.hub, friendly_name=Builder.build_random_string()
            )
            == dataset
        )

    def test_get_datasets_cross_hub(self) -> None:
        datasets = [build_response_dataset() for _ in range(5)]
        self.http_client.get.return_value = get_response_body(ResponseDatasets(datasets=datasets))
        assert self.core_api_client.get_datasets_cross_hub(dataset_ids=[dataset.id for dataset in datasets]) == datasets
        self.http_client.get.call_args.kwargs["params"] = {"ids": [dataset.id for dataset in datasets]}

    def test_get_bucket(self) -> None:
        resource_payload = build_s3_resource().to_payload()
        self.http_client.get.return_value = get_response_body(resource_payload)
        assert (
            self.core_api_client.get_s3_resource(
                hub=resource_payload.hub,
                dataset_id=resource_payload.dataset_id,
                stage=resource_payload.stage,
                region=resource_payload.region,
            )
            == resource_payload
        )

    def test_create_bucket_success(self) -> None:
        resource_payload = build_s3_resource().to_payload()
        self.http_client.post.return_value = get_response_body(resource_payload)
        assert (
            self.core_api_client.create_s3_resource(
                hub=resource_payload.hub,
                dataset_id=resource_payload.dataset_id,
                stage=resource_payload.stage,
                region=resource_payload.region,
            )
            == resource_payload
        )

    def test_create_bucket_failure(self) -> None:
        resource_payload = build_s3_resource().to_payload()
        self.http_client.post.side_effect = NonRetryableConflictError
        self.http_client.get.side_effect = Exception
        with pytest.raises(NonRetryableConflictError):
            self.core_api_client.create_s3_resource(
                hub=resource_payload.hub,
                dataset_id=resource_payload.dataset_id,
                stage=resource_payload.stage,
                region=resource_payload.region,
            )

    def test_create_bucket_failure_recovery(self) -> None:
        resource_payload = build_s3_resource().to_payload()
        self.http_client.post.side_effect = NonRetryableConflictError
        self.http_client.get.return_value = resource_payload
        assert (
            self.core_api_client.create_s3_resource(
                hub=resource_payload.hub,
                dataset_id=resource_payload.dataset_id,
                stage=resource_payload.stage,
                region=resource_payload.region,
            )
            == resource_payload
        )

    def test_create_glue_sync_success(self) -> None:
        resource_payload = build_glue_sync_resource().to_payload()
        self.http_client.post.return_value = get_response_body(resource_payload)
        assert (
            self.core_api_client.create_glue_sync(
                hub=resource_payload.hub,
                dataset_id=resource_payload.dataset_id,
                stage=resource_payload.stage,
                region=resource_payload.region,
            )
            == resource_payload
        )

    def test_create_glue_sync_failure(self) -> None:
        resource_payload = build_glue_sync_resource().to_payload()
        self.http_client.post.side_effect = NonRetryableConflictError
        self.http_client.get.side_effect = Exception
        with pytest.raises(NonRetryableConflictError):
            self.core_api_client.create_glue_sync(
                hub=resource_payload.hub,
                dataset_id=resource_payload.dataset_id,
                stage=resource_payload.stage,
                region=resource_payload.region,
            )

    def test_create_glue_sync_failure_recovery(self) -> None:
        resource_payload = build_glue_sync_resource().to_payload()
        self.http_client.post.side_effect = NonRetryableConflictError
        self.http_client.get.return_value = resource_payload
        assert (
            self.core_api_client.create_glue_sync(
                hub=resource_payload.hub,
                dataset_id=resource_payload.dataset_id,
                stage=resource_payload.stage,
                region=resource_payload.region,
            )
            == resource_payload
        )

    def test_get_resources(self) -> None:
        resources = [cast(ResourcePayload, build_s3_resource().to_payload()) for _ in range(3)]
        self.http_client.get_with_pagination.return_value = get_response_body(ResourcesPayload(resources))
        assert self.core_api_client.get_resources(hub=self.hub) == resources

    def test_get_account(self) -> None:
        response_account = build_response_account()
        self.http_client.get.return_value = get_response_body(response_account)
        assert self.core_api_client.get_account(account_id=response_account.id) == response_account

    def test_get_accounts(self) -> None:
        accounts = [build_response_account_without_costs() for _ in range(5)]
        self.http_client.get_with_pagination.return_value = get_response_body(ResponseAccounts(accounts=accounts))
        assert self.core_api_client.get_accounts() == accounts

    @pytest.mark.parametrize("fail_if_exists", [True, False])
    def test_register_account_success(self, fail_if_exists: bool) -> None:
        response_account = build_response_account()
        self.http_client.post.return_value = get_response_body(response_account)
        assert (
            self.core_api_client.register_account(
                account_id=response_account.id,
                affiliation=response_account.affiliation,
                business_objects=response_account.business_objects,
                hub=response_account.hub,
                layers=response_account.layers,
                stages=response_account.stages,
                type=response_account.type,
                visible_in_hubs=response_account.visible_in_hubs,
                admin_roles=response_account.admin_roles,
                friendly_name=response_account.friendly_name,
                responsibles=response_account.responsibles,
                group=response_account.group,
                request_id=response_account.request_id,
                roles=response_account.roles,
                fail_if_exists=fail_if_exists,
            )
            == response_account
        )

    def test_register_account_does_not_fail_on_conflict_error_when_fail_if_exists_false(self) -> None:
        response_account = build_response_account()
        self.http_client.post.return_value = {"Code": "ConflictError"}
        assert (
            self.core_api_client.register_account(
                account_id=response_account.id,
                affiliation=response_account.affiliation,
                business_objects=response_account.business_objects,
                hub=response_account.hub,
                layers=response_account.layers,
                stages=response_account.stages,
                type=response_account.type,
                visible_in_hubs=response_account.visible_in_hubs,
                admin_roles=response_account.admin_roles,
                friendly_name=response_account.friendly_name,
                responsibles=response_account.responsibles,
                group=response_account.group,
                request_id=response_account.request_id,
                roles=response_account.roles,
                fail_if_exists=False,
            )
            is None
        )

    def test_update_account_billing(self) -> None:
        response_account = build_response_account()
        self.http_client.put.return_value = response_account.to_plain_dict()
        assert (
            self.core_api_client.update_account_billing(
                account_id=response_account.id, forecasted_cost=random.uniform(0, 1)
            )
            == response_account
        )

    def test_get_filter_packages(self) -> None:
        filter_packages = [
            build_filter_package(hub=self.hub, stage=self.stage, region=self.region, dataset_id=self.dataset_id)
            for _ in range(5)
        ]
        self.http_client.get.return_value = get_response_body(FilterPackages(filter_packages=filter_packages))
        assert (
            self.core_api_client.get_filter_packages(
                hub=self.hub, dataset_id=self.dataset_id, stage=self.stage, region=self.region
            )
            == filter_packages
        )

    def test_get_filter_package(self) -> None:
        filter_package = build_filter_package(
            hub=self.hub, stage=self.stage, region=self.region, dataset_id=self.dataset_id
        )

        self.http_client.get.return_value = get_response_body(filter_package)
        assert (
            self.core_api_client.get_filter_package(
                hub=self.hub,
                stage=self.stage,
                region=self.region,
                dataset_id=self.dataset_id,
                package_id=filter_package.id,
            )
            == filter_package
        )


class ClientResponseBuilder:
    """
    Provides expected response to the Core Api client.

    Custom responses can be specified depending on the method and path.
    In order of appearance in the list, the first entry to regex-match the method and path is used.
    """

    class _FakeResponse(Dict[str, str]):
        """A dict which contains every key."""

        def __getitem__(self, key: object) -> str:
            return Builder.build_random_string()

        def __contains__(self, key: object) -> bool:
            return True

    def __init__(self, path_response_mapping: Optional[List[Tuple[str, Dict[str, Any]]]] = None) -> None:
        default_mapping: List[Tuple[str, Dict[str, Any]]] = [
            (r"^GET/.*?/datasets/.+", get_response_body(build_response_dataset())),
            (r"^GET/datasets(\?.*)?$", {"datasets": [get_response_body(build_response_dataset())]}),
            (r"^GET/.*?/datasets$", {"datasets": [get_response_body(build_response_dataset())]}),
            (r"^GET/.*?/resources/s3/.*", get_response_body(build_s3_resource().to_payload())),
            (r"^GET/.*?/filter-packages$", {"filterPackages": [get_response_body(build_filter_package())]}),
            (r"^GET/.*?/filter-packages/.+", get_response_body(build_filter_package())),
            (r"^GET/resources/s3\?.*$", get_response_body(build_s3_resource().to_payload())),
            (r"^GET/.*?/resources/glue-sync/.*", get_response_body(build_glue_sync_resource().to_payload())),
            (r"^GET/.*?/resources(\?.*)?$", {"resources": [get_response_body(build_s3_resource().to_payload())]}),
            (r"^POST/.*?/resources/s3$", get_response_body(build_s3_resource().to_payload())),
            (r"^POST/.*?/resources/glue-sync", get_response_body(build_glue_sync_resource().to_payload())),
            (r".*?/datasets/.*?/permissions$", get_response_body(build_dataset_account_permission())),
            (r"^POST/.*?/datasets(\?.*)?$", get_response_body(build_response_dataset())),
            (r"^PUT/.*?/datasets/.+", get_response_body(build_response_dataset())),
            (r"^GET/accounts/.+", get_response_body(build_response_account())),
            (r"^GET/accounts$", {"accounts": [get_response_body(build_response_account_without_costs())]}),
            (r"^POST/accounts", get_response_body(build_response_account())),
            (r"^PUT/accounts", get_response_body(build_response_account())),
            (r"^PUT/accounts/.+/billing", get_response_body(build_response_account())),
            (r"^GET/.*?/businessObjects$", {"businessObjects": [get_response_body(build_hub_business_object())]}),
            (r"^GET/.*?/businessObjects/.+", get_response_body(build_hub_business_object())),
        ]
        self.path_response_mapping: List[Tuple[str, Dict[str, Any]]] = (path_response_mapping or []) + default_mapping

    def __call__(self, method_path: str) -> Dict[str, Any]:
        for expression, response in self.path_response_mapping:
            if re.match(expression, method_path):
                return response
        return self._FakeResponse()


class ClientRequestBuilder:
    """
    Provides arguments to methods of CoreApiClient.

    Extend the factories
    """

    def __init__(self, factories: Optional[Dict[type, Callable[[], Any]]] = None) -> None:
        default_factories: Dict[type, Callable[[], Any]] = {
            type(None): lambda: None,
            str: Builder.build_random_string,
            bool: lambda: Builder.get_random_element([False, True]),
            int: lambda: random.randint(0, 10),
            float: lambda: random.uniform(0, 10),
            AccountId: build_account_id,
            DatasetId: lambda: build_dataset().id,
            DatasetAccountPermission: build_dataset_account_permission,
            DatasetParticipant: build_dataset_participant,
            ExternalLink: build_external_link,
            PackageId: lambda: build_filter_package().id,
            SourceIdentifier: build_source_identifier,
            SupportGroup: build_support_group,
        }
        self.factories = default_factories | (factories or {})

    def generate_arguments(self, func: Callable[..., Any]) -> Dict[str, Any]:
        return {
            arg: self._generate_value_for_type(getfullargspec(func).annotations[arg])
            for arg in getfullargspec(func).args
            if arg not in {"self", "cls"}
        }

    def _generate_value_for_type(  # pylint: disable=too-many-branches, too-many-return-statements
        self,
        class_type: Any,
    ) -> Any:
        if factory := self.factories.get(class_type):
            return factory()
        if get_origin(class_type) is list:
            return [self._generate_value_for_type(get_args(class_type)[0])]
        if get_origin(class_type) is Union:
            subtype = Builder.get_random_element(get_args(class_type), exclude={type(None)})
            return self._generate_value_for_type(subtype)
        if get_origin(class_type) is dict:
            return {
                self._generate_value_for_type(get_args(class_type)[0]): self._generate_value_for_type(
                    get_args(class_type)[1]
                )
            }
        if get_origin(class_type) is set:
            return {self._generate_value_for_type(get_args(class_type)[0])}
        with suppress(TypeError):
            if issubclass(class_type, Enum):
                return Builder.get_random_element(list(class_type))
        if is_dataclass(class_type):
            return class_type(**{field.name: self._generate_value_for_type(field.type) for field in fields(class_type)})

        raise NotImplementedError(f"Cannot build object of type {class_type.__name__!r}: please add a factory")


def is_class_method(cls: Type[Any], method_name: str) -> bool:
    return ismethod(getattr(cls, method_name)) and getattr(cls, method_name).__self__ is cls


def get_client_methods(cls: Type[CoreApiClient]) -> Set[str]:
    return {
        method_name
        for method_name in dir(cls)
        if callable(getattr(cls, method_name))
        and not method_name.startswith("_")
        and not is_class_method(cls, method_name)
    }


class ValidationClient(HttpClient):
    """This class is used to validate http requests."""

    def __init__(self, response_builder: ClientResponseBuilder) -> None:
        super().__init__(base_url="https://example.com", credentials=None)
        self._response_builder = response_builder

    def raw(  # pylint: disable=unused-argument)
        self,
        method: str,
        path: str,
        *,
        expected_status_codes: Optional[List[HTTPStatus]] = None,
        retry_status_codes: Optional[List[HTTPStatus]] = None,
        min_bytes: Optional[int] = None,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Mapping[str, Union[str, List[str]]]] = None,
        retries: Optional[int] = None,
        seconds_between_retries: Optional[int] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        # The following asserts make sure that all http requests do match our expectations
        assert json.dumps(body)
        query_string = "?" + "&".join([f"{key}={value}" for key, value in params.items()]) if params else ""
        validate.URL()(self._base_url + path + query_string)
        response = Mock(requests.Response)
        response.json.return_value = self._response_builder(method_path=method + path + query_string)
        response.headers = {}

        return response


class TestValidateHTTPCalls:
    @pytest.mark.parametrize("method_name", sorted(get_client_methods(CoreApiClient)))
    def test_every_call_makes_valid_http_requests(self, method_name: str) -> None:
        validation_client = ValidationClient(ClientResponseBuilder())
        core_api_client = CoreApiClient(validation_client)
        method = getattr(core_api_client, method_name)

        method(**ClientRequestBuilder().generate_arguments(method))
