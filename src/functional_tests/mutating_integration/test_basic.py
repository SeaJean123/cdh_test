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
"""These functional tests need to run on fully-integrated deployments (interacting with the auth and users api).

They may modify the data on those deployments.
"""
import os
import random
from contextlib import contextmanager
from dataclasses import fields
from http import HTTPStatus
from logging import getLogger
from random import randint
from typing import Iterator
from typing import List
from typing import Optional

import pytest
from waiting import wait

from cdh_core.clients.http_client import HttpStatusCodeNotInExpectedCodes
from cdh_core.entities.accounts import ResponseAccount
from cdh_core.entities.accounts_test import build_account_role
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import ResponseDataset
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants_test import build_dataset_participant_id
from cdh_core.entities.resource import GlueSyncResourcePayload
from cdh_core.entities.resource import S3ResourcePayload
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.primitives.account_id import AccountId
from cdh_core_dev_tools.testing.builder import Builder
from functional_tests.mutating_integration.conftest import IntegrationTestConfig
from functional_tests.mutating_integration.conftest import IntegrationTestSetup
from functional_tests.utils import get_stages

LOG = getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

TIMEOUT_IN_SEC = 5


# This function should not be used when tests are run in parallel with mutating basic tests,
# since they both make changes to our test account for mutating tests
@contextmanager
def register_account(integration_test_setup: IntegrationTestSetup) -> Iterator[ResponseAccount]:
    account = integration_test_setup.core_api_client.register_account(
        account_id=integration_test_setup.account_id_to_be_registered,
        affiliation=Affiliation("cdh"),
        business_objects=[],
        hub=Hub("global"),
        layers=[],
        stages=[],
        type=AccountType.provider,
        visible_in_hubs=[],
        request_id="some-id",
        friendly_name="registered by integration test",
        responsibles=[],
        roles=integration_test_setup.roles,
    )
    wait(
        lambda: integration_test_setup.core_api_client.get_account(integration_test_setup.account_id_to_be_registered),
        expected_exceptions=HttpStatusCodeNotInExpectedCodes,
        timeout_seconds=TIMEOUT_IN_SEC,
        sleep_seconds=0.2,
    )
    try:
        yield account  # type: ignore
    finally:
        try:
            integration_test_setup.core_api_client.deregister_account(
                integration_test_setup.account_id_to_be_registered
            )
            with pytest.raises(HttpStatusCodeNotInExpectedCodes) as exc_info:
                integration_test_setup.core_api_client.get_account(integration_test_setup.account_id_to_be_registered)
            assert exc_info.value.status_code is HTTPStatus.NOT_FOUND
        except:  # noqa: E722 (bare-except)
            LOG.warning(
                f"Deregistering account {integration_test_setup.account_id_to_be_registered} failed. Please deregister "
                f"it manually by calling the DELETE /accounts/{integration_test_setup.account_id_to_be_registered} "
                f"endpoint."
            )
            raise


@contextmanager
def create_dataset(
    integration_test_setup: IntegrationTestSetup,
    layer: Optional[Layer] = None,
    engineers: Optional[List[DatasetParticipant]] = None,
    stewards: Optional[List[DatasetParticipant]] = None,
) -> Iterator[ResponseDataset]:
    dataset = integration_test_setup.core_api_client.create_dataset(
        hub=integration_test_setup.hub,
        business_object=build_business_object(),
        name=f"{integration_test_setup.name_prefix}_{randint(1, 999):03d}",
        layer=layer or Builder.get_random_element(to_choose_from=set(Layer), exclude={Layer.sem}),
        confidentiality=build_confidentiality(),
        engineers=engineers
        or [DatasetParticipant(id=build_dataset_participant_id("some.engineer@example.com"), idp="some")],
        stewards=stewards,
        description="Created by functional test",
        preview_available=random.choice([None, True, False]),
    )
    try:
        yield dataset
    finally:
        try:
            integration_test_setup.core_api_client.delete_dataset(dataset.hub, dataset.id)
            with pytest.raises(HttpStatusCodeNotInExpectedCodes) as exc_info:
                integration_test_setup.core_api_client.get_dataset(dataset.hub, dataset.id)
            assert exc_info.value.status_code is HTTPStatus.NOT_FOUND
        except:  # noqa: E722 (bare-except)
            LOG.warning(
                f"Deleting dataset {dataset.id} failed. Please delete it manually by calling the DELETE "
                f"/{dataset.hub.value}/datasets/{dataset.id} endpoint."
            )
            raise


@contextmanager
def create_s3_resource(
    integration_test_setup: IntegrationTestSetup, dataset: ResponseDataset
) -> Iterator[S3ResourcePayload]:
    s3_resource = integration_test_setup.core_api_client.create_s3_resource(
        hub=dataset.hub, dataset_id=dataset.id, stage=integration_test_setup.stage, region=integration_test_setup.region
    )
    try:
        yield s3_resource
    finally:
        try:
            integration_test_setup.core_api_client.delete_resource(
                hub=s3_resource.hub,
                dataset_id=s3_resource.dataset_id,
                resource_type=ResourceType.s3,
                stage=s3_resource.stage,
                region=s3_resource.region,
            )
            with pytest.raises(HttpStatusCodeNotInExpectedCodes) as exc_info:
                integration_test_setup.core_api_client.get_s3_resource(
                    hub=s3_resource.hub,
                    dataset_id=s3_resource.dataset_id,
                    stage=s3_resource.stage,
                    region=s3_resource.region,
                )
            assert exc_info.value.status_code is HTTPStatus.NOT_FOUND
        except:  # noqa: E722 (bare-except)
            LOG.warning(
                f"Deleting s3 resource failed. Please delete it manually by calling the DELETE /{s3_resource.hub.value}"
                f"/resources/s3/{s3_resource.dataset_id}/{s3_resource.stage.value}/{s3_resource.region.value} endpoint."
            )
            raise


@contextmanager
def create_glue_sync(
    integration_test_setup: IntegrationTestSetup, dataset: ResponseDataset
) -> Iterator[GlueSyncResourcePayload]:
    glue_sync = integration_test_setup.core_api_client.create_glue_sync(
        hub=dataset.hub,
        dataset_id=dataset.id,
        stage=integration_test_setup.stage,
        region=integration_test_setup.region,
    )
    try:
        yield glue_sync
    finally:
        try:
            wait(
                lambda: integration_test_setup.core_api_client.delete_resource(
                    hub=glue_sync.hub,
                    dataset_id=glue_sync.dataset_id,
                    resource_type=ResourceType.glue_sync,
                    stage=glue_sync.stage,
                    region=glue_sync.region,
                )  # type: ignore
                is None,
                expected_exceptions=HttpStatusCodeNotInExpectedCodes,
                timeout_seconds=60,
                sleep_seconds=5,
                waiting_for="association of resources after resource share creation",
            )
            with pytest.raises(HttpStatusCodeNotInExpectedCodes) as exc_info:
                integration_test_setup.core_api_client.get_glue_resource(
                    hub=glue_sync.hub,
                    dataset_id=glue_sync.dataset_id,
                    stage=glue_sync.stage,
                    region=glue_sync.region,
                )
            assert exc_info.value.status_code is HTTPStatus.NOT_FOUND
        except:  # noqa: E722 (bare-except)
            LOG.warning(
                f"Deleting glue sync resource failed. Please delete it manually by calling the DELETE /"
                f"{glue_sync.hub.value}/resources/glue-sync/{glue_sync.dataset_id}/{glue_sync.stage.value}/"
                f"{glue_sync.region.value} endpoint."
            )
            raise


@contextmanager
def create_permission(
    integration_test_setup: IntegrationTestSetup,
    dataset_id: DatasetId,
    account_id: AccountId,
) -> Iterator[DatasetAccountPermission]:
    permission = integration_test_setup.core_api_client.grant_dataset_permission(
        dataset_id=dataset_id,
        account_id=account_id,
        stage=integration_test_setup.stage,
        region=integration_test_setup.region,
        hub=integration_test_setup.hub,
    )
    try:
        yield permission
    finally:
        try:
            integration_test_setup.core_api_client.revoke_dataset_permission(
                dataset_id=dataset_id,
                account_id=account_id,
                stage=integration_test_setup.stage,
                region=integration_test_setup.region,
                hub=integration_test_setup.hub,
            )
            assert permission not in integration_test_setup.core_api_client.get_dataset_permissions(
                integration_test_setup.hub, dataset_id
            )
        except:  # noqa: E722 (bare-except)
            LOG.warning(
                f"Removing permission ({permission.account_id}, {permission.region.value} {permission.stage.value}, "
                f"{permission.sync_type.value}) failed. Please remove it manually by calling the DELETE "
                f"/{integration_test_setup.hub.value}/datasets/{dataset_id}/permissions endpoint."
            )
            raise


class TestAccounts:
    @staticmethod
    def _deregister_temporary_account_if_necessary(integration_test_setup: IntegrationTestSetup) -> None:
        try:
            integration_test_setup.core_api_client.deregister_account(
                integration_test_setup.account_id_to_be_registered
            )
        except HttpStatusCodeNotInExpectedCodes as err:
            if err.status_code is not HTTPStatus.NOT_FOUND:
                raise

    def test_register_update_delete_account(
        self, integration_test_setup: IntegrationTestSetup, integration_test_config: IntegrationTestConfig
    ) -> None:
        self._deregister_temporary_account_if_necessary(integration_test_setup)
        with register_account(integration_test_setup) as created_account:
            account_after_create = integration_test_setup.core_api_client.get_account(
                integration_test_setup.account_id_to_be_registered
            )
            assert created_account.id == account_after_create.id
            assert account_after_create.group is None
            assert account_after_create.roles == integration_test_setup.roles
            assert account_after_create.type == AccountType.provider

            all_accounts = integration_test_setup.core_api_client.get_accounts()
            assert created_account.id in {account.id for account in all_accounts}

            roles = integration_test_setup.roles + [build_account_role()]
            integration_test_setup.core_api_client.update_account(
                account_id=integration_test_setup.account_id_to_be_registered,
                affiliation=Affiliation("cdh"),
                business_objects=list(BusinessObject),
                layers=list(Layer),
                stages=get_stages(integration_test_setup.hub, integration_test_config.environment),
                type=AccountType.internal,
                group="functional-test",
                roles=roles,
            )
            account_after_update = integration_test_setup.core_api_client.get_account(
                integration_test_setup.account_id_to_be_registered
            )
            assert account_after_update.group == "functional-test"
            assert account_after_update.roles == roles
            assert account_after_update.type == AccountType.internal


class TestDatasets:
    def setup_method(self) -> None:
        self.initial_engineers = [
            DatasetParticipant(id=build_dataset_participant_id("some.engineer@example.com"), idp="some")
        ]
        self.initial_stewards = [
            DatasetParticipant(id=build_dataset_participant_id("some.steward@example.com"), idp="some")
        ]

    @staticmethod
    def _datasets_equal(first_dataset: ResponseDataset, second_dataset: ResponseDataset) -> bool:
        fields_with_ignored_order = {"stewards", "engineers"}
        if any(
            getattr(first_dataset, field) != getattr(second_dataset, field)
            for field in {field.name for field in fields(ResponseDataset)} - fields_with_ignored_order
        ):
            return False
        return all(
            set(getattr(first_dataset, field)) == set(getattr(second_dataset, field))
            for field in fields_with_ignored_order
        )

    def test_create_update_delete_dataset(  # pylint: disable=unused-argument
        self, integration_test_setup: IntegrationTestSetup, integration_test_config: IntegrationTestConfig
    ) -> None:
        with create_dataset(
            integration_test_setup, layer=Layer.sem, engineers=self.initial_engineers, stewards=self.initial_stewards
        ) as created_dataset:
            dataset_after_create = integration_test_setup.core_api_client.get_dataset(
                integration_test_setup.hub, created_dataset.id
            )
            assert self._datasets_equal(dataset_after_create, created_dataset)
            if not integration_test_config.resource_name_prefix:
                assert dataset_after_create.engineers == self.initial_engineers
                assert dataset_after_create.stewards == self.initial_stewards

            all_datasets = integration_test_setup.core_api_client.get_datasets(hub=integration_test_setup.hub)
            assert any(self._datasets_equal(dataset, created_dataset) for dataset in all_datasets)
            datasets_cross_hub = integration_test_setup.core_api_client.get_datasets_cross_hub(
                dataset_ids=[created_dataset.id]
            )
            assert any(self._datasets_equal(dataset, created_dataset) for dataset in datasets_cross_hub)

            new_confidentiality = Builder.get_random_element(
                to_choose_from=set(Confidentiality), exclude={created_dataset.confidentiality}
            )
            new_description = "Updated by functional tests"
            new_friendly_name = "My updated Dataset"
            new_engineers = [
                DatasetParticipant(id=build_dataset_participant_id("someone.else@example.com"), idp="other")
            ]
            new_preview_available = not created_dataset.preview_available
            updated_dataset = integration_test_setup.core_api_client.update_dataset(
                hub=integration_test_setup.hub,
                dataset_id=created_dataset.id,
                confidentiality=new_confidentiality,
                description=new_description,
                friendly_name=new_friendly_name,
                engineers=new_engineers,
                preview_available=new_preview_available,
            )
            dataset_after_update = integration_test_setup.core_api_client.get_dataset(
                integration_test_setup.hub, created_dataset.id
            )
            assert self._datasets_equal(updated_dataset, dataset_after_update)
            if not integration_test_config.resource_name_prefix:
                assert dataset_after_update.engineers == new_engineers
                assert dataset_after_update.stewards == self.initial_stewards


class TestDatasetPermissions:
    def test_add_remove_dataset_permission(  # pylint: disable=unused-argument
        self, integration_test_setup: IntegrationTestSetup, integration_test_config: IntegrationTestConfig
    ) -> None:
        with create_dataset(integration_test_setup) as dataset:
            with create_s3_resource(integration_test_setup, dataset):
                with create_glue_sync(integration_test_setup, dataset):
                    with register_account(integration_test_setup) as consumer_account:
                        with create_permission(integration_test_setup, dataset.id, consumer_account.id) as permission:
                            assert permission == DatasetAccountPermission(
                                account_id=consumer_account.id,
                                region=integration_test_setup.region,
                                stage=integration_test_setup.stage,
                                sync_type=SyncType.resource_link,
                            )
                            assert permission in integration_test_setup.core_api_client.get_dataset_permissions(
                                hub=integration_test_setup.hub, dataset_id=dataset.id
                            )


class TestResources:
    def test_create_delete_s3_and_glue_sync(  # pylint: disable=unused-argument
        self, integration_test_setup: IntegrationTestSetup, integration_test_config: IntegrationTestConfig
    ) -> None:
        with create_dataset(integration_test_setup) as dataset:
            with create_s3_resource(integration_test_setup, dataset) as s3_resource:
                s3_resource_after_create = integration_test_setup.core_api_client.get_s3_resource(
                    hub=dataset.hub,
                    dataset_id=dataset.id,
                    stage=integration_test_setup.stage,
                    region=integration_test_setup.region,
                )
                assert s3_resource == s3_resource_after_create
                assert s3_resource_after_create.type is ResourceType.s3
                assert s3_resource_after_create.hub is dataset.hub
                assert s3_resource_after_create.stage is integration_test_setup.stage
                assert s3_resource_after_create.region is integration_test_setup.region

                with create_glue_sync(integration_test_setup, dataset) as glue_sync:
                    glue_sync_after_create = integration_test_setup.core_api_client.get_glue_resource(
                        hub=dataset.hub,
                        dataset_id=dataset.id,
                        stage=integration_test_setup.stage,
                        region=integration_test_setup.region,
                    )
                    assert glue_sync == glue_sync_after_create
                    assert glue_sync_after_create.type is ResourceType.glue_sync
                    assert glue_sync_after_create.hub is dataset.hub
                    assert glue_sync_after_create.stage is integration_test_setup.stage
                    assert glue_sync_after_create.region is integration_test_setup.region

                    resources = integration_test_setup.core_api_client.get_resources(hub=dataset.hub)
                    assert any(resource == s3_resource for resource in resources)
                    assert any(resource == glue_sync for resource in resources)
