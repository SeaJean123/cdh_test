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
from dataclasses import replace
from http import HTTPStatus
from random import randint
from typing import Optional
from unittest.mock import Mock
from unittest.mock import patch

import cdh_core_api.endpoints
import pytest
from cdh_core_api.bodies.resources import NewGlueSyncBody
from cdh_core_api.bodies.resources import NewS3BucketBody
from cdh_core_api.catalog.base_test import build_last_evaluated_key
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.config_test import build_config
from cdh_core_api.endpoints import resources
from cdh_core_api.endpoints.resources import TypedResourcePath
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.glue_resource_manager import GlueResourceManager
from cdh_core_api.services.glue_resource_manager import GlueSyncAlreadyExists
from cdh_core_api.services.pagination_service import NextPageTokenContext
from cdh_core_api.services.pagination_service import PaginationService
from cdh_core_api.services.resource_payload_builder import ResourcePayloadBuilder
from cdh_core_api.services.resource_validator import ResourceValidator
from cdh_core_api.services.s3_resource_manager import S3ResourceManager
from cdh_core_api.services.sns_publisher import EntityType
from cdh_core_api.services.sns_publisher import Operation
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from cdh_core_api.validation.common_paths import HubPath

from cdh_core.config.authorization_api_test import build_auth_api
from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.account_store_test import build_account_store
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.entities.resource import ResourcesPayload
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class TestGetResources:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.stage = build_stage()
        self.region = build_region(self.hub.partition)
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.pagination_service = Mock(PaginationService)
        self.pagination_service.decode_token.return_value = None

    def test_all_resources(self) -> None:
        dataset = build_dataset(hub=self.hub)
        s3_resource = build_s3_resource(dataset=dataset)
        glue_sync_resource = build_glue_sync_resource(dataset=dataset)
        self.visible_data_loader.get_resources.return_value = ([s3_resource, glue_sync_resource], None)
        response = resources.get_resources(
            path=HubPath(hub=self.hub),
            query=resources.ResourcesQuerySchema(),
            visible_data_loader=self.visible_data_loader,
            config=build_config(),
            pagination_service=self.pagination_service,
        )

        assert response.body == ResourcesPayload.from_resources([s3_resource, glue_sync_resource])
        self.visible_data_loader.get_resources.assert_called_once()

    def test_with_filters(self) -> None:
        self.visible_data_loader.get_resources.return_value = ([], None)
        dataset = build_dataset(hub=self.hub)
        account_id = build_account_id()
        dataset_id = dataset.id
        query_schema = resources.ResourcesQuerySchema(
            stage=self.stage,
            region=self.region,
            resourceAccountId=account_id,
            datasetId=dataset_id,
        )
        config = build_config()
        resources.get_resources(
            path=HubPath(hub=self.hub),
            query=query_schema,
            visible_data_loader=self.visible_data_loader,
            config=config,
            pagination_service=self.pagination_service,
        )
        self.visible_data_loader.get_resources.assert_called_once_with(
            stage=self.stage,
            hub=self.hub,
            region=self.region,
            resource_account=account_id,
            dataset_id=dataset_id,
            limit=config.result_page_size,
            last_evaluated_key=None,
        )

    def test_return_next_page_token(self) -> None:
        last_evaluated_key = build_last_evaluated_key()
        self.visible_data_loader.get_resources.return_value = ([], last_evaluated_key)
        encrypted_token = Builder.build_random_string()
        self.pagination_service.issue_token.return_value = encrypted_token

        response = resources.get_resources(
            path=HubPath(hub=self.hub),
            query=resources.ResourcesQuerySchema(),
            visible_data_loader=self.visible_data_loader,
            config=build_config(),
            pagination_service=self.pagination_service,
        )

        assert response.headers["nextPageToken"] == encrypted_token
        self.pagination_service.issue_token.assert_called_once_with(
            last_evaluated_key=last_evaluated_key,
            context=NextPageTokenContext.RESOURCES,
        )

    def test_with_next_page_token_in_query(self) -> None:
        self.visible_data_loader.get_resources.return_value = ([], None)
        next_page_token = Builder.build_random_string()
        last_evaluated_key = build_last_evaluated_key()
        self.pagination_service.decode_token.return_value = last_evaluated_key
        page_size = randint(1, 10)

        resources.get_resources(
            path=HubPath(hub=self.hub),
            query=resources.ResourcesQuerySchema(nextPageToken=next_page_token),
            visible_data_loader=self.visible_data_loader,
            config=build_config(result_page_size=page_size),
            pagination_service=self.pagination_service,
        )

        assert self.visible_data_loader.get_resources.call_args.kwargs["last_evaluated_key"] == last_evaluated_key
        assert self.visible_data_loader.get_resources.call_args.kwargs["limit"] == page_size
        self.pagination_service.decode_token.assert_called_once_with(
            next_page_token=next_page_token, context=NextPageTokenContext.RESOURCES
        )


class TestGetResource:
    def setup_method(self) -> None:
        self.visible_data_loader = Mock()
        self.resource_payload_builder = Mock()

    @pytest.mark.parametrize("resource_type", ResourceType)
    def test_get_resource(self, resource_type: ResourceType) -> None:
        resource = build_resource(resource_type=resource_type)
        self.visible_data_loader.get_resource.return_value = resource
        response = resources.get_resource(
            path=TypedResourcePath(
                hub=resource.hub,
                type=resource_type,
                datasetId=resource.dataset_id,
                stage=resource.stage,
                region=resource.region,
            ),
            visible_data_loader=self.visible_data_loader,
            resource_payload_builder=self.resource_payload_builder,
        )
        assert response.body == self.resource_payload_builder.return_value
        self.visible_data_loader.get_resource.assert_called_once_with(
            resource_type=resource_type, dataset_id=resource.dataset_id, stage=resource.stage, region=resource.region
        )
        self.resource_payload_builder.assert_called_once_with(resource)

    @pytest.mark.parametrize("resource_type", ResourceType)
    def test_get_resource_non_existent(self, resource_type: ResourceType) -> None:
        self.visible_data_loader.get_resource.side_effect = ResourceNotFound(build_dataset_id(), "my_resource")

        with pytest.raises(NotFoundError):
            resources.get_resource(
                path=TypedResourcePath(
                    hub=build_hub(),
                    type=resource_type,
                    datasetId=build_dataset_id(),
                    stage=build_stage(),
                    region=build_region(),
                ),
                visible_data_loader=self.visible_data_loader,
                resource_payload_builder=self.resource_payload_builder,
            )

    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    @pytest.mark.parametrize("resource_type", ResourceType)
    def test_wrong_hub(
        self, resource_type: ResourceType, mock_config_file: ConfigFile  # pylint: disable=unused-argument
    ) -> None:
        resource = build_resource(resource_type=resource_type)
        self.visible_data_loader.get_resource.return_value = resource
        other_hub = Builder.get_random_element(Hub, exclude=[resource.hub])

        with pytest.raises(NotFoundError):
            resources.get_resource(
                path=TypedResourcePath(
                    hub=other_hub,
                    type=resource_type,
                    datasetId=resource.dataset_id,
                    stage=resource.stage,
                    region=resource.region,
                ),
                visible_data_loader=self.visible_data_loader,
                resource_payload_builder=self.resource_payload_builder,
            )

    def test_get_from_bucket_name(self) -> None:
        bucket_name = Builder.build_random_string()
        mock_resource = Mock()
        mock_response = Mock()
        self.visible_data_loader.get_resource_from_bucket_name.return_value = mock_resource
        self.resource_payload_builder.return_value = mock_response

        response = resources.get_resource_by_bucket_name(
            query=resources.ResourceByBucketNameQuerySchema(bucketName=bucket_name),
            resource_payload_builder=self.resource_payload_builder,
            visible_data_loader=self.visible_data_loader,
        )

        self.visible_data_loader.get_resource_from_bucket_name.assert_called_once_with(bucket_name)
        self.resource_payload_builder.assert_called_once_with(mock_resource)
        assert response.body == mock_response


class TestCreateS3Bucket:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.stage = build_stage()
        self.region = build_region()
        self.dataset = build_dataset(hub=self.hub)
        self.glue_sync_account = build_resource_account(hub=self.hub, stage=self.stage, stage_priority=0)
        self.default_account = build_resource_account(hub=self.hub, stage=self.stage, stage_priority=1)
        self.config = build_config(account_store=build_account_store([self.default_account, self.glue_sync_account]))
        self.user = Builder.build_random_string()
        self.requester_identity = build_requester_identity(user=self.user)
        self.s3_resource_manager = Mock(spec=S3ResourceManager)
        self.expected_resource = build_s3_resource(dataset=self.dataset, creator_user_id=self.user)
        self.s3_resource_manager.create_bucket.return_value = self.expected_resource

        self.visible_data_loader = Mock(spec=VisibleDataLoader)
        self.sns_publisher = Mock(SnsPublisher)
        self.authorization_api = Mock(spec=AuthorizationApi)
        self.resource_validator = Mock(spec=ResourceValidator)
        self.resource_validator.check_dataset_visible.return_value = self.dataset
        self.resource_payload_builder = Mock(ResourcePayloadBuilder)

    def create_bucket(
        self,
        body: NewS3BucketBody,
        hub: Optional[Hub] = None,
    ) -> JsonResponse:
        return cdh_core_api.endpoints.resources.create_s3_bucket(
            config=self.config,
            requester_identity=self.requester_identity,
            path=HubPath(hub if hub else self.hub),
            body=body,
            s3_resource_manager=self.s3_resource_manager,
            sns_publisher=self.sns_publisher,
            authorization_api=self.authorization_api,
            resource_validator=self.resource_validator,
            resource_payload_builder=self.resource_payload_builder,
        )

    @pytest.mark.parametrize("set_owner_account_id", [False, True])
    @pytest.mark.parametrize("get_user_from_auth_api", [False, True])
    def test_create_s3_bucket(self, get_user_from_auth_api: bool, set_owner_account_id: bool) -> None:
        self.resource_validator.determine_account_for_new_resource.return_value = self.default_account
        if get_user_from_auth_api:
            self.authorization_api.get_user_id.return_value = self.user
            self.requester_identity = replace(self.requester_identity, user=Builder.build_random_string())
        else:
            self.authorization_api.get_user_id.return_value = None
        owner_account_id = build_account_id() if set_owner_account_id else self.requester_identity.account_id
        body = NewS3BucketBody(
            datasetId=self.dataset.id,
            stage=self.stage,
            region=self.region,
            ownerAccountId=owner_account_id if set_owner_account_id else None,
        )

        response = self.create_bucket(body)

        self.s3_resource_manager.create_bucket.assert_called_once_with(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            resource_account=self.default_account,
            user=self.user,
            owner_account_id=owner_account_id,
        )
        assert response.body == self.resource_payload_builder.return_value
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.RESOURCE,
            operation=Operation.CREATE,
            payload=self.expected_resource.to_payload(),
        )
        self.resource_validator.check_may_create_resource.assert_called_once_with(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            resource_type=ResourceType.s3,
            owner_account_id=owner_account_id,
        )
        self.resource_payload_builder.assert_called_once_with(self.expected_resource)

    def test_create_bucket_without_authorization_api(self) -> None:
        self.config = replace(self.config, authorization_api_params=build_auth_api(use_authorization=False))
        response = self.create_bucket(NewS3BucketBody(datasetId=self.dataset.id, stage=self.stage, region=self.region))

        assert response.body == self.resource_payload_builder.return_value
        self.resource_payload_builder.assert_called_once_with(self.expected_resource)
        self.authorization_api.assert_not_called()

    def test_dataset_not_visible(self) -> None:
        self.resource_validator.check_dataset_visible.side_effect = NotFoundError(DatasetNotFound(self.dataset.id))
        body = NewS3BucketBody(datasetId=self.dataset.id, stage=self.stage, region=self.region)
        with pytest.raises(NotFoundError):
            self.create_bucket(body)

    def test_requester_must_be_authorized(self) -> None:
        self.resource_validator.check_may_create_resource.side_effect = ForbiddenError()
        body = NewS3BucketBody(datasetId=self.dataset.id, stage=self.stage, region=self.region)
        with pytest.raises(ForbiddenError):
            self.create_bucket(body)

    def test_create_s3_bucket_with_existing_glue_sync(self) -> None:
        self.authorization_api.get_user_id.return_value = None
        self.resource_validator.determine_account_for_new_resource.return_value = self.glue_sync_account

        owner_account_id = self.requester_identity.account_id
        body = NewS3BucketBody(
            datasetId=self.dataset.id,
            stage=self.stage,
            region=self.region,
        )

        self.create_bucket(body)

        self.s3_resource_manager.create_bucket.assert_called_once_with(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            resource_account=self.glue_sync_account,
            user=self.user,
            owner_account_id=owner_account_id,
        )

    def test_bucket_already_exists(self) -> None:
        self.s3_resource_manager.create_bucket.side_effect = ConflictError()
        body = NewS3BucketBody(datasetId=self.dataset.id, stage=self.stage, region=self.region)
        with pytest.raises(ConflictError):
            self.create_bucket(body)


class TestCreateGlueSync:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.dataset = build_dataset(hub=self.hub)
        self.stage = build_stage()
        self.region = build_region(self.hub.partition)
        self.resource_account = build_resource_account()
        self.user = Builder.build_random_string()
        self.requester_identity = build_requester_identity()
        self.body = NewGlueSyncBody(datasetId=self.dataset.id, stage=self.stage, region=self.region)
        self.expected_resource = build_glue_sync_resource(dataset=self.dataset, creator_user_id=self.user)

        self.manager = Mock(GlueResourceManager)
        self.manager.create_glue_sync.return_value = self.expected_resource
        self.dataset_permissions_manager = Mock(DatasetPermissionsManager)
        self.sns_publisher = Mock(SnsPublisher)
        self.authorization_api = Mock(AuthorizationApi)
        self.resource_validator = Mock(ResourceValidator)
        self.resource_validator.check_dataset_visible.return_value = self.dataset
        self.resource_validator.determine_account_for_new_resource.return_value = self.resource_account

    def create_glue_sync(
        self,
        body: Optional[NewGlueSyncBody] = None,
    ) -> JsonResponse:
        return resources.create_glue_sync(
            requester_identity=self.requester_identity,
            path=HubPath(self.hub),
            body=body or self.body,
            glue_resource_manager=self.manager,
            config=build_config(),
            sns_publisher=self.sns_publisher,
            authorization_api=self.authorization_api,
            dataset_permissions_manager=self.dataset_permissions_manager,
            resource_validator=self.resource_validator,
        )

    @patch.object(resources, "get_user")
    @pytest.mark.parametrize("set_owner_account_id", [False, True])
    def test_create_glue_sync(self, get_user: Mock, set_owner_account_id: bool) -> None:
        user = Builder.build_random_string()
        get_user.return_value = user
        owner_account_id = build_account_id() if set_owner_account_id else self.requester_identity.account_id
        body = NewGlueSyncBody(
            datasetId=self.dataset.id,
            stage=self.stage,
            region=self.region,
            ownerAccountId=owner_account_id if set_owner_account_id else None,
        )
        response = self.create_glue_sync(body=body)

        assert response.body == self.expected_resource.to_payload()
        assert response.status_code == HTTPStatus.CREATED.value
        self.resource_validator.check_dataset_visible.assert_called_once_with(hub=self.hub, dataset_id=self.dataset.id)
        self.resource_validator.check_may_create_resource.assert_called_once_with(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            resource_type=ResourceType.glue_sync,
            owner_account_id=owner_account_id,
        )
        self.resource_validator.check_glue_sync_resource_requirements.assert_called_once_with(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            owner_account_id=owner_account_id,
            sync_type=body.syncType,
            partition=self.hub.partition,
        )
        self.resource_validator.determine_account_for_new_resource.assert_called_once_with(
            dataset=self.dataset,
            hub=self.hub,
            stage=self.stage,
            region=self.region,
            resource_type=ResourceType.glue_sync,
        )
        self.manager.create_glue_sync.assert_called_once_with(
            dataset=self.dataset,
            body=body,
            resource_account=self.resource_account,
            owner_account_id=owner_account_id,
            user=user,
        )
        self.dataset_permissions_manager.create_missing_resource_links.assert_called_once_with(
            self.dataset, self.stage, self.region
        )
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.RESOURCE,
            operation=Operation.CREATE,
            payload=self.expected_resource.to_payload(),
        )

    def test_dataset_not_visible(self) -> None:
        self.resource_validator.check_dataset_visible.side_effect = NotFoundError(DatasetNotFound(self.dataset.id))
        with pytest.raises(NotFoundError):
            self.create_glue_sync(body=self.body)

    def test_requester_not_authorized(self) -> None:
        self.resource_validator.check_may_create_resource.side_effect = ForbiddenError()
        with pytest.raises(ForbiddenError):
            self.create_glue_sync(body=self.body)

    def test_check_glue_sync_resource_requirements_fails(self) -> None:
        self.resource_validator.check_glue_sync_resource_requirements.side_effect = ForbiddenError()
        with pytest.raises(ForbiddenError):
            self.create_glue_sync(body=self.body)

    def test_glue_sync_already_exists(self) -> None:
        self.manager.create_glue_sync.side_effect = GlueSyncAlreadyExists(self.dataset.id, self.stage, self.region)
        with pytest.raises(ConflictError):
            self.create_glue_sync(body=self.body)


class TestDeleteS3Bucket:
    def setup_method(self) -> None:
        self.region = build_region()
        self.hub = build_hub()
        self.stage = build_stage()
        self.dataset = build_dataset(hub=self.hub, permissions=frozenset())
        self.s3_resource = build_s3_resource(dataset=self.dataset, stage=self.stage, region=self.region)
        self.resource_path = resources.ResourcePath(
            hub=self.hub, datasetId=self.dataset.id, stage=self.stage, region=self.region
        )
        self.s3_resource_manager = Mock(S3ResourceManager)
        self.sns_publisher = Mock(SnsPublisher)
        self.resource_validator = Mock(ResourceValidator)
        self.resource_validator.check_dataset_visible.return_value = self.dataset
        self.resource_validator.check_may_delete_s3_resource.return_value = self.s3_resource

    def delete_resource(self) -> JsonResponse:
        return resources.delete_s3_resource(
            path=self.resource_path,
            s3_resource_manager=self.s3_resource_manager,
            sns_publisher=self.sns_publisher,
            resource_validator=self.resource_validator,
        )

    def test_delete_successful(self) -> None:
        response = self.delete_resource()
        assert response.body is None

        self.s3_resource_manager.delete_bucket.assert_called_once_with(s3_resource=self.s3_resource)
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.RESOURCE,
            operation=Operation.DELETE,
            payload=self.s3_resource.to_payload(),
        )

    def test_dataset_not_visible(self) -> None:
        self.resource_validator.check_dataset_visible.side_effect = NotFoundError(DatasetNotFound(self.dataset.id))

        with pytest.raises(NotFoundError):
            self.delete_resource()

        self.s3_resource_manager.delete_bucket.assert_not_called()

    def test_requester_must_be_authorized(self) -> None:
        self.resource_validator.check_may_delete_s3_resource.side_effect = ForbiddenError()

        with pytest.raises(ForbiddenError):
            self.delete_resource()

        self.s3_resource_manager.delete_bucket.assert_not_called()


class TestDeleteGlueSync:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.stage = build_stage()
        self.region = build_region()
        self.dataset = build_dataset(hub=self.hub)
        self.glue_sync_resource = build_glue_sync_resource(dataset=self.dataset, stage=self.stage, region=self.region)
        self.resource_path = resources.ResourcePath(
            hub=self.hub, stage=self.stage, region=self.region, datasetId=self.dataset.id
        )
        self.glue_resource_manager = Mock(GlueResourceManager)
        self.sns_publisher = Mock(SnsPublisher)
        self.dataset_permissions_manager = Mock(DatasetPermissionsManager)
        self.resource_validator = Mock(ResourceValidator)
        self.resource_validator.check_dataset_visible.return_value = self.dataset
        self.resource_validator.check_may_delete_glue_sync_resource.return_value = self.glue_sync_resource

    def delete_resource(
        self,
    ) -> JsonResponse:
        return resources.delete_glue_sync_resource(
            path=self.resource_path,
            glue_resource_manager=self.glue_resource_manager,
            sns_publisher=self.sns_publisher,
            dataset_permissions_manager=self.dataset_permissions_manager,
            resource_validator=self.resource_validator,
        )

    def test_delete_successful(self) -> None:
        response = self.delete_resource()

        assert response.body is None
        self.resource_validator.check_dataset_visible.assert_called_once_with(hub=self.hub, dataset_id=self.dataset.id)
        self.resource_validator.check_may_delete_glue_sync_resource.assert_called_once_with(
            dataset=self.dataset, stage=self.stage, region=self.region
        )
        self.resource_validator.check_glue_sync_resource_deletion_requirements.assert_called_once_with(
            self.glue_sync_resource, self.dataset
        )
        self.glue_resource_manager.delete_glue_sync.assert_called_once_with(self.glue_sync_resource)
        self.dataset_permissions_manager.delete_metadata_syncs_for_glue_sync.assert_called_once_with(
            self.glue_sync_resource, self.dataset
        )
        self.sns_publisher.publish.assert_called_once_with(
            entity_type=EntityType.RESOURCE,
            operation=Operation.DELETE,
            payload=self.glue_sync_resource.to_payload(),
        )

    def test_dataset_not_visible(self) -> None:
        self.resource_validator.check_dataset_visible.side_effect = NotFoundError()

        with pytest.raises(NotFoundError):
            self.delete_resource()

        self.glue_resource_manager.delete_glue_sync.assert_not_called()

    def test_requester_must_be_authorized(self) -> None:
        self.resource_validator.check_may_delete_glue_sync_resource.side_effect = ForbiddenError()

        with pytest.raises(ForbiddenError):
            self.delete_resource()

        self.glue_resource_manager.delete_glue_sync.assert_not_called()
