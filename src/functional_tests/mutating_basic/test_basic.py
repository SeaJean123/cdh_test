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
import datetime
import random
import time
from http import HTTPStatus
from typing import Any
from typing import Dict

import boto3
import pytest
import requests
from waiting import wait

from cdh_core.clients.core_api_client import CoreApiClient
from cdh_core.clients.core_api_client import NEXT_PAGE_TOKEN_KEY
from cdh_core.clients.http_client import HttpStatusCodeNotInExpectedCodes
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.entities.accounts_test import build_account_role
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants_test import build_dataset_participant_id
from cdh_core.entities.hub_business_object import HubBusinessObject
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.log.measure_time import MeasureTimeContextManager
from cdh_core_dev_tools.testing.builder import Builder
from functional_tests.mutating_basic.conftest import MutatingBasicTestConfig
from functional_tests.mutating_basic.conftest import MutatingBasicTestSetup
from functional_tests.mutating_basic.conftest import ResourceList
from functional_tests.mutating_basic.conftest import TestDataset
from functional_tests.mutating_basic.conftest import TestDatasetDefinition
from functional_tests.utils import get_stages


class TestAccounts:
    def test_registered_account(
        self, mutating_basic_test_setup: MutatingBasicTestSetup, mutating_basic_test_config: MutatingBasicTestConfig
    ) -> None:
        existing_accounts = {
            account.id: account for account in mutating_basic_test_setup.core_api_client.get_accounts()
        }
        expected_account_ids = {
            account.id
            for account in AccountStore().query_accounts(
                account_purposes=AccountPurpose("test"),
                environments=mutating_basic_test_config.environment,
            )
        }
        assert set(account_id for account_id in existing_accounts) >= expected_account_ids
        for account in mutating_basic_test_config.test_accounts:
            registered_account = existing_accounts[account.id]
            assert registered_account.hub == account.hub
            assert registered_account.affiliation == Affiliation("cdh")
            assert set(registered_account.stages) == set(
                get_stages(account.hub, mutating_basic_test_config.environment)
            )
            assert set(registered_account.business_objects) == set(BusinessObject)
            assert (
                registered_account.type == AccountType.usecase
                if account.id == mutating_basic_test_setup.test_consumer_account.id
                else AccountType.provider
            )

    # this test can significantly affect all other tests since it modifies the dynamo entry of the account
    # that is also used for all other functional tests.
    def test_update_account(self, mutating_basic_test_setup: MutatingBasicTestSetup) -> None:
        roles = [build_account_role(account_type=AccountRoleType.WRITE), build_account_role()]

        mutating_basic_test_setup.core_api_client.update_account(
            account_id=mutating_basic_test_setup.test_provider_account.id, group="functional-test", roles=roles
        )
        account_after_update = mutating_basic_test_setup.core_api_client.get_account(
            mutating_basic_test_setup.test_provider_account.id
        )
        assert account_after_update.group == "functional-test"
        assert account_after_update.roles == roles
        assert account_after_update.type == AccountType.provider


class TestDatasets:
    def test_create_and_delete_dataset(
        self,
        test_dataset: TestDataset,
        mutating_basic_test_setup: MutatingBasicTestSetup,
    ) -> None:
        assert test_dataset.dataset_name in test_dataset.dataset_id

        response = mutating_basic_test_setup.core_api_client.get_dataset(
            hub=mutating_basic_test_setup.hub, dataset_id=test_dataset.dataset_id
        )
        assert response.id == test_dataset.dataset_id

        all_datasets = mutating_basic_test_setup.core_api_client.get_datasets(hub=mutating_basic_test_setup.hub)
        assert any(dataset.id == test_dataset.dataset_id for dataset in all_datasets)

        datasets = mutating_basic_test_setup.core_api_client.get_datasets_cross_hub(
            dataset_ids=[DatasetId(test_dataset.dataset_id)]
        )
        assert [dataset.id for dataset in datasets] == [test_dataset.dataset_id]

        mutating_basic_test_setup.core_api_client.delete_dataset(
            hub=mutating_basic_test_setup.hub, dataset_id=test_dataset.dataset_id
        )
        mutating_basic_test_setup.http_client.get(
            f"/{mutating_basic_test_setup.hub.value}/datasets/{test_dataset.dataset_id}",
            expected_status_codes=[HTTPStatus.NOT_FOUND],
        )

    def test_delete_updates_lineage(self, mutating_basic_test_setup: MutatingBasicTestSetup) -> None:
        test_dataset = mutating_basic_test_setup.core_api_client.create_dataset(
            business_object=mutating_basic_test_setup.business_object,
            name=Builder.build_random_string(),
            layer=mutating_basic_test_setup.layer,
            hub=mutating_basic_test_setup.hub,
            confidentiality=build_confidentiality(),
            description="Created for functional test - Testdataset",
            engineers=[DatasetParticipant(id=build_dataset_participant_id("someone@example.com"), idp="example")],
        )
        downstream_dataset = mutating_basic_test_setup.core_api_client.create_dataset(
            business_object=mutating_basic_test_setup.business_object,
            name=Builder.build_random_string(),
            layer=mutating_basic_test_setup.layer,
            hub=mutating_basic_test_setup.hub,
            confidentiality=build_confidentiality(),
            description="Created for functional test - Testdataset",
            engineers=[DatasetParticipant(id=build_dataset_participant_id("someone@example.com"), idp="example")],
            upstream_lineage={test_dataset.id},
        )
        assert downstream_dataset.lineage.upstream == {test_dataset.id}

        with MeasureTimeContextManager("Time until all dataset lineages are scanned and updated."):
            mutating_basic_test_setup.core_api_client.delete_dataset(
                hub=mutating_basic_test_setup.hub, dataset_id=test_dataset.id
            )

        updated_downstream_dataset = mutating_basic_test_setup.core_api_client.get_dataset(
            hub=downstream_dataset.hub, dataset_id=downstream_dataset.id
        )
        assert updated_downstream_dataset.lineage.upstream == set()

    def test_get_multiple_datasets(self, mutating_basic_test_setup: MutatingBasicTestSetup) -> None:
        dataset_ids = [
            mutating_basic_test_setup.core_api_client.create_dataset(
                business_object=mutating_basic_test_setup.business_object,
                name=Builder.build_random_string(),
                layer=mutating_basic_test_setup.layer,
                hub=mutating_basic_test_setup.hub,
                description="Created for functional test",
                engineers=[DatasetParticipant(id=build_dataset_participant_id("someone@example.com"), idp="example")],
                confidentiality=build_confidentiality(),
            ).id
            for _ in range(3)
        ]
        all_datasets = mutating_basic_test_setup.core_api_client.get_datasets(hub=mutating_basic_test_setup.hub)
        assert set(dataset_ids) <= {dataset.id for dataset in all_datasets}

        ids_to_query = dataset_ids[1:] + [DatasetId(f"does_not_exist_{Builder.build_random_string()}")]
        selected_datasets = mutating_basic_test_setup.core_api_client.get_datasets_cross_hub(dataset_ids=ids_to_query)
        assert {dataset.id for dataset in selected_datasets} == set(dataset_ids[1:])

    def test_update_dataset(
        self,
        test_dataset: TestDataset,
        mutating_basic_test_setup: MutatingBasicTestSetup,
    ) -> None:
        new_confidentiality = build_confidentiality()
        new_contains_pii = True
        new_description = "Updated by functional tests"
        new_friendly_name = "My updated Dataset"
        new_preview_available = random.choice([True, False])
        new_quality_score = random.randint(0, 100)
        response = mutating_basic_test_setup.core_api_client.update_dataset(
            hub=mutating_basic_test_setup.hub,
            dataset_id=test_dataset.dataset_id,
            confidentiality=new_confidentiality,
            contains_pii=new_contains_pii,
            description=new_description,
            friendly_name=new_friendly_name,
            tags={},
            preview_available=new_preview_available,
            quality_score=new_quality_score,
        )

        assert response.confidentiality == new_confidentiality
        assert response.contains_pii == new_contains_pii
        assert response.description == new_description
        assert response.friendly_name == new_friendly_name
        assert response.tags == {}
        assert response.preview_available == new_preview_available
        assert response.quality_score == new_quality_score

    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_dataset_permission(
        self,
        test_dataset: TestDataset,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        resources_to_clean_up: ResourceList,
        sync_type: SyncType,
    ) -> None:
        for region in mutating_basic_test_setup.hub.regions:
            s3_resource = mutating_basic_test_setup.core_api_client.create_s3_resource(
                dataset_id=test_dataset.dataset_id,
                stage=mutating_basic_test_setup.stage,
                region=region,
                hub=mutating_basic_test_setup.hub,
            )
            resources_to_clean_up.append(s3_resource)
            glue_resource = mutating_basic_test_setup.core_api_client.create_glue_sync(
                dataset_id=test_dataset.dataset_id,
                stage=mutating_basic_test_setup.stage,
                region=region,
                hub=mutating_basic_test_setup.hub,
                sync_type=sync_type,
            )
            resources_to_clean_up.append(glue_resource)

        for region in mutating_basic_test_setup.hub.regions:
            mutating_basic_test_setup.core_api_client.grant_dataset_permission(
                hub=mutating_basic_test_setup.hub,
                dataset_id=test_dataset.dataset_id,
                account_id=mutating_basic_test_setup.test_consumer_account.id,
                stage=mutating_basic_test_setup.stage,
                region=region,
            )

        permissions_after_create = mutating_basic_test_setup.core_api_client.get_dataset_permissions(
            hub=mutating_basic_test_setup.hub, dataset_id=test_dataset.dataset_id
        )
        dataset = mutating_basic_test_setup.core_api_client.get_dataset(
            hub=mutating_basic_test_setup.hub, dataset_id=test_dataset.dataset_id
        )
        for region in mutating_basic_test_setup.hub.regions:
            expected_permission = DatasetAccountPermission(
                account_id=mutating_basic_test_setup.test_consumer_account.id,
                region=region,
                stage=mutating_basic_test_setup.stage,
                sync_type=sync_type,
            )
            assert expected_permission in permissions_after_create
            assert expected_permission in dataset.permissions

        for region in mutating_basic_test_setup.hub.regions:
            wait(
                lambda: mutating_basic_test_setup.core_api_client.revoke_dataset_permission(
                    hub=mutating_basic_test_setup.hub,
                    dataset_id=test_dataset.dataset_id,
                    account_id=mutating_basic_test_setup.test_consumer_account.id,
                    stage=mutating_basic_test_setup.stage,
                    region=region,  # noqa: B023, pylint: disable=cell-var-from-loop
                )  # type: ignore
                is None,
                expected_exceptions=HttpStatusCodeNotInExpectedCodes,
                timeout_seconds=60,
                sleep_seconds=5,
                waiting_for="association of resources after resource share creation",
            )

        permissions_after_delete = mutating_basic_test_setup.core_api_client.get_dataset_permissions(
            hub=mutating_basic_test_setup.hub, dataset_id=test_dataset.dataset_id
        )
        assert len(permissions_after_delete) == 0


class TestAuditLogger:
    @staticmethod
    def get_logs_client(credentials: Dict[str, Any]) -> Any:
        return boto3.client("logs", **credentials)

    @staticmethod
    def get_request_id(response: requests.Response) -> str:
        return response.headers["x-amzn-RequestId"]

    def test_create_and_put_and_delete_dataset_should_be_found_in_auditlog(
        self,
        test_dataset_definition: TestDatasetDefinition,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        mutating_basic_test_config: MutatingBasicTestConfig,
    ) -> None:
        """Create, put and delete a dataset to generate audit log entries.

        This creates POST, GET and DELETE HTTP requests, where only POST and DELETE should appear in the log
        To test if the logs are written the request id is search in the log group
        """
        body = CoreApiClient.RequestBuilder.build_new_dataset_body(
            business_object=mutating_basic_test_setup.business_object,
            name=test_dataset_definition.dataset_name,
            layer=mutating_basic_test_setup.layer,
            confidentiality=build_confidentiality(),
            description="Created by functional test for audit logger",
            engineers=[DatasetParticipant(id=build_dataset_participant_id("someone@example.com"), idp="some")],
        )
        raw_response = mutating_basic_test_setup.http_client.raw(
            method="POST",
            path=f"/{mutating_basic_test_setup.hub.value}/datasets",
            body=body,
            expected_status_codes=[HTTPStatus.CREATED],
            min_bytes=0,
        )
        post_request_id = self.get_request_id(raw_response)
        assert {"id", "friendlyName", "businessObject", "hub"} <= set(raw_response.json().keys())
        assert raw_response.json()["id"] == test_dataset_definition.dataset_id

        self._check_dataset_exists(mutating_basic_test_setup, test_dataset_definition.dataset_id)

        body = CoreApiClient.RequestBuilder.build_update_dataset_body(
            description="Updated by functional test for audit logger",
            confidentiality=build_confidentiality(),
            contains_pii=False,
            friendly_name="Audit logger test",
            tags={},
            engineers=[DatasetParticipant(id=build_dataset_participant_id("someone.else@example.com"), idp="myidp")],
        )
        raw_response = mutating_basic_test_setup.http_client.raw(
            method="PUT",
            path=f"/{mutating_basic_test_setup.hub.value}/datasets/{test_dataset_definition.dataset_id}",
            expected_status_codes=[HTTPStatus.OK],
            min_bytes=0,
            body=body,
        )
        put_request_id = self.get_request_id(raw_response)

        raw_response = mutating_basic_test_setup.http_client.raw(
            method="DELETE",
            path=f"/{mutating_basic_test_setup.hub.value}/datasets/{test_dataset_definition.dataset_id}",
            expected_status_codes=[HTTPStatus.NO_CONTENT],
            min_bytes=0,
        )
        delete_request_id = self.get_request_id(raw_response)

        raw_response = mutating_basic_test_setup.http_client.raw(
            method="GET",
            path=f"/{mutating_basic_test_setup.hub.value}/datasets/{test_dataset_definition.dataset_id}",
            expected_status_codes=[HTTPStatus.NOT_FOUND],
            min_bytes=0,
        )
        get_request_id = self.get_request_id(raw_response)

        self._check_logs(
            post_request_id=post_request_id,
            put_request_id=put_request_id,
            delete_request_id=delete_request_id,
            get_request_id=get_request_id,
            client=self.get_logs_client(mutating_basic_test_setup.api_account_credentials),
            log_group_name=f"{mutating_basic_test_config.resource_name_prefix}cdh-audit-log",
        )

    @staticmethod
    def _check_dataset_exists(mutating_basic_test_setup: MutatingBasicTestSetup, dataset_id: DatasetId) -> None:
        response = mutating_basic_test_setup.http_client.get(
            f"/{mutating_basic_test_setup.hub.value}/datasets/{dataset_id}",
            expected_status_codes=[HTTPStatus.OK],
        )
        assert {"id", "friendlyName", "businessObject", "hub"} <= set(response.keys())
        assert response["id"] == dataset_id
        response = mutating_basic_test_setup.http_client.get_with_pagination(
            f"/{mutating_basic_test_setup.hub.value}/datasets",
            next_page_key=NEXT_PAGE_TOKEN_KEY,
            item_key="datasets",
        )
        assert any(dataset["id"] == dataset_id for dataset in response["datasets"])

    def _check_logs(
        self,
        post_request_id: str,
        put_request_id: str,
        delete_request_id: str,
        get_request_id: str,
        client: Any,
        log_group_name: str,
    ) -> None:
        assert len(client.describe_log_groups(logGroupNamePrefix=log_group_name, limit=10)["logGroups"]) == 1

        stream_page_iterator = client.get_paginator("describe_log_streams").paginate(
            logGroupName=log_group_name,
            logStreamNamePrefix=datetime.datetime.now().strftime("%Y/%m/%d"),
            orderBy="LogStreamName",
            descending=True,
        )
        log_stream_names_nested = [
            [stream["logStreamName"] for stream in item["logStreams"]] for item in stream_page_iterator
        ]
        assert len(log_stream_names_nested) > 0

        # POST, PUT and DELETE should be found in the logs
        location_in_audit_log = "request.apiRequestId"
        retry_counter = 0
        post_delete_result: Dict[str, Any] = {}
        while retry_counter <= 6 and len(post_delete_result.get("events", [])) < 3:
            post_delete_result = self.filter_logs(
                boto_logs_client=client,
                log_group_name=log_group_name,
                pattern=f'{{$.{location_in_audit_log} = "{post_request_id}" || '  # noqa: B028
                f'$.{location_in_audit_log} = "{delete_request_id}" || '
                f'$.{location_in_audit_log} = "{put_request_id}"}}',
            )
            retry_counter += 1
            time.sleep(5)
        assert len(post_delete_result["events"]) == 3

        # GET should not be found
        get_result = self.filter_logs(
            boto_logs_client=client,
            log_group_name=log_group_name,
            pattern=f'{{$.{location_in_audit_log} = "{get_request_id}"}}',  # noqa: B028
        )
        assert len(get_result["events"]) == 0

    @staticmethod
    def filter_logs(boto_logs_client: Any, log_group_name: str, pattern: str) -> Dict[str, Any]:
        return boto_logs_client.filter_log_events(  # type: ignore
            logGroupName=log_group_name,
            logStreamNamePrefix=datetime.datetime.now().strftime("%Y/%m/%d"),
            startTime=int((time.time() - 600) * 1000),  # now - 10 min
            endTime=int((time.time() + 600) * 1000),  # now + 10 min
            filterPattern=pattern,
            limit=10,
            interleaved=True,
        )


class TestBusinessObjects:
    def test_get_single_business_object(self, mutating_basic_test_setup: MutatingBasicTestSetup) -> None:
        hub_business_object = mutating_basic_test_setup.core_api_client.get_hub_business_object(
            hub=mutating_basic_test_setup.hub, business_object=mutating_basic_test_setup.business_object
        )
        assert hub_business_object == HubBusinessObject.get_default_hub_business_object(
            hub=mutating_basic_test_setup.hub, business_object=mutating_basic_test_setup.business_object
        )

    def test_get_all_business_objects(self, mutating_basic_test_setup: MutatingBasicTestSetup) -> None:
        hub = mutating_basic_test_setup.hub
        hub_business_objects = mutating_basic_test_setup.core_api_client.get_hub_business_objects(hub)
        expected_hub_business_objects = [
            HubBusinessObject.get_default_hub_business_object(hub=hub, business_object=business_object)
            for business_object in list(BusinessObject)
        ]
        assert sorted(
            expected_hub_business_objects, key=lambda hbo: hbo.business_object.value  # type:ignore
        ) == sorted(
            hub_business_objects, key=lambda hbo: hbo.business_object.value  # type:ignore
        )


class TestResources:
    def test_create_and_delete_s3(
        self, test_dataset: TestDataset, mutating_basic_test_setup: MutatingBasicTestSetup
    ) -> None:
        create_first_bucket = mutating_basic_test_setup.core_api_client.create_s3_resource(
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            hub=mutating_basic_test_setup.hub,
        )

        assert create_first_bucket.type is ResourceType.s3
        assert create_first_bucket.dataset_id == test_dataset.dataset_id
        assert create_first_bucket.hub is mutating_basic_test_setup.hub
        assert create_first_bucket.stage is mutating_basic_test_setup.stage
        assert create_first_bucket.region is mutating_basic_test_setup.region
        first_bucket = mutating_basic_test_setup.core_api_client.get_s3_resource(
            hub=mutating_basic_test_setup.hub,
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
        )
        assert first_bucket == create_first_bucket
        first_bucket_by_name = mutating_basic_test_setup.core_api_client.get_s3_resource_by_bucket_name(
            create_first_bucket.name
        )
        assert first_bucket_by_name == create_first_bucket

        mutating_basic_test_setup.core_api_client.delete_resource(
            hub=mutating_basic_test_setup.hub,
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            resource_type=ResourceType.s3,
        )

        response = mutating_basic_test_setup.http_client.get(
            "/".join(
                [
                    f"/{mutating_basic_test_setup.hub.value}",
                    "resources",
                    ResourceType.s3.value,
                    test_dataset.dataset_id,
                    mutating_basic_test_setup.stage.value,
                    mutating_basic_test_setup.region.value,
                ]
            ),
            expected_status_codes=[HTTPStatus.NOT_FOUND],
        )
        assert response["Code"] == "NotFoundError"
        response_bucket_name = mutating_basic_test_setup.http_client.get(
            f"/resources/{ResourceType.s3.value}",
            params={"bucketName": create_first_bucket.name},
            expected_status_codes=[HTTPStatus.NOT_FOUND],
        )
        assert response_bucket_name["Code"] == "NotFoundError"

    def test_resource_filtered(
        self,
        test_dataset: TestDataset,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        resources_to_clean_up: ResourceList,
    ) -> None:
        region, other_region = random.sample(list(mutating_basic_test_setup.regions), 2)
        region_resource = mutating_basic_test_setup.core_api_client.create_s3_resource(
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=region,
            hub=mutating_basic_test_setup.hub,
        )
        resources_to_clean_up.append(region_resource)

        other_region_resource = mutating_basic_test_setup.core_api_client.create_s3_resource(
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=other_region,
            hub=mutating_basic_test_setup.hub,
        )
        resources_to_clean_up.append(other_region_resource)

        region_resources = mutating_basic_test_setup.core_api_client.get_resources(
            hub=mutating_basic_test_setup.hub, region=region
        )
        other_region_resources = mutating_basic_test_setup.core_api_client.get_resources(
            hub=mutating_basic_test_setup.hub, region=other_region
        )
        assert all(resource.region == region for resource in region_resources)
        assert any(resource == region_resource for resource in region_resources)
        assert all(resource.region == other_region for resource in other_region_resources)
        assert any(resource == other_region_resource for resource in other_region_resources)

    @pytest.mark.parametrize("sync_type", [SyncType.resource_link, SyncType.lake_formation])
    def test_create_and_delete_glue_sync(
        self,
        test_dataset: TestDataset,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        resources_to_clean_up: ResourceList,
        sync_type: SyncType,
    ) -> None:
        bucket = mutating_basic_test_setup.core_api_client.create_s3_resource(
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            hub=mutating_basic_test_setup.hub,
        )
        resources_to_clean_up.append(bucket)

        post_response = mutating_basic_test_setup.core_api_client.create_glue_sync(
            hub=mutating_basic_test_setup.hub,
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            sync_type=sync_type,
        )

        assert post_response.type is ResourceType.glue_sync
        assert post_response.hub is mutating_basic_test_setup.hub
        assert post_response.stage is mutating_basic_test_setup.stage
        assert post_response.region is mutating_basic_test_setup.region

        get_response = mutating_basic_test_setup.core_api_client.get_glue_resource(
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
            hub=mutating_basic_test_setup.hub,
        )
        assert get_response == post_response

        wait(
            lambda: mutating_basic_test_setup.core_api_client.delete_resource(
                dataset_id=test_dataset.dataset_id,
                stage=mutating_basic_test_setup.stage,
                region=mutating_basic_test_setup.region,
                hub=mutating_basic_test_setup.hub,
                resource_type=ResourceType.glue_sync,
            )  # type: ignore
            is None,
            expected_exceptions=HttpStatusCodeNotInExpectedCodes,
            timeout_seconds=60,
            sleep_seconds=5,
            waiting_for="association of resources after resource share creation",
        )

        glue_sync = mutating_basic_test_setup.core_api_client.create_glue_sync(
            hub=mutating_basic_test_setup.hub,
            dataset_id=test_dataset.dataset_id,
            stage=mutating_basic_test_setup.stage,
            region=mutating_basic_test_setup.region,
        )
        resources_to_clean_up.append(glue_sync)

    def test_get_resources_pagination(
        self,
        mutating_basic_test_setup: MutatingBasicTestSetup,
        mutating_basic_test_config: MutatingBasicTestConfig,
        resources_to_clean_up: ResourceList,
    ) -> None:
        test_datasets = [TestDataset(mutating_basic_test_setup, mutating_basic_test_config) for _ in range(3)]
        created_resources = []
        for dataset in test_datasets:
            for region in mutating_basic_test_setup.regions:
                resource = mutating_basic_test_setup.core_api_client.create_s3_resource(
                    hub=mutating_basic_test_setup.hub,
                    dataset_id=dataset.dataset_id,
                    stage=mutating_basic_test_setup.stage,
                    region=region,
                )
                created_resources.append(resource)
                resources_to_clean_up.append(resource)

        first_response = mutating_basic_test_setup.http_client.raw(
            "GET", f"/{mutating_basic_test_setup.hub.value}/resources", expected_status_codes=[HTTPStatus.OK]
        )
        next_page_token = first_response.headers[NEXT_PAGE_TOKEN_KEY]
        assert next_page_token

        second_response = mutating_basic_test_setup.http_client.raw(
            "GET",
            f"/{mutating_basic_test_setup.hub.value}/resources",
            params={NEXT_PAGE_TOKEN_KEY: next_page_token},
            expected_status_codes=[HTTPStatus.OK],
        )
        assert all(
            first_resource != second_resource
            for first_resource in first_response.json()["resources"]
            for second_resource in second_response.json()["resources"]
        )
        all_resources = mutating_basic_test_setup.core_api_client.get_resources(hub=mutating_basic_test_setup.hub)
        assert all(resource in all_resources for resource in created_resources)
