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
from typing import Optional
from unittest.mock import Mock

import pytest
from cdh_core_api.catalog.accounts_table import GenericAccountsTable
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.filter_packages_table import FilterPackagesTable
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.metadata_role_assumer import AssumableAccountSpec
from cdh_core_api.services.resource_validator import ResourceValidator
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.account_store_test import build_account_store
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.filter_package_test import build_filter_package
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.entities.resource import Resource
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_sync_type
from cdh_core.enums.environment import Environment
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_resource_type
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import BadRequestError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder
from cdh_core_dev_tools.testing.utils import UnusableMock


class ResourceValidatorTestCase:
    def setup_method(self) -> None:
        self.config = build_config()
        self.accounts_table = Mock(GenericAccountsTable)
        self.resources_table = Mock(ResourcesTable)
        self.authorizer = Mock(Authorizer)
        self.requester_identity = build_requester_identity()
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.filter_packages_table = Mock(FilterPackagesTable)

        self.hub = build_hub()
        self.dataset = build_dataset(hub=self.hub)
        self.stage = build_stage()
        self.region = build_region()
        self.owner_account_id = build_account_id()
        self.resource_type = build_resource_type()
        self.partition = Partition("aws")

        self.resource_validator = ResourceValidator(
            accounts_table=self.accounts_table,
            config=self.config,
            resources_table=self.resources_table,
            authorizer=self.authorizer,
            requester_identity=self.requester_identity,
            visible_data_loader=self.visible_data_loader,
            assumable_account_spec_cls=AssumableAccountSpec,
            filter_packages_table=self.filter_packages_table,
        )


class TestCheckDatasetVisible(ResourceValidatorTestCase):
    def test_visible(self) -> None:
        self.visible_data_loader.get_dataset.return_value = self.dataset

        dataset = self.resource_validator.check_dataset_visible(hub=self.dataset.hub, dataset_id=self.dataset.id)

        assert dataset == self.dataset

    def test_dataset_does_not_exist(self) -> None:
        self.visible_data_loader.get_dataset.side_effect = DatasetNotFound(self.dataset.id)

        with pytest.raises(NotFoundError):
            self.resource_validator.check_dataset_visible(hub=self.dataset.hub, dataset_id=self.dataset.id)

    def test_wrong_hub(self) -> None:
        other_hub = Builder.get_random_element(Hub, exclude=[self.hub])
        self.visible_data_loader.get_dataset.return_value = replace(self.dataset, hub=other_hub)

        with pytest.raises(NotFoundError):
            self.resource_validator.check_dataset_visible(hub=self.dataset.hub, dataset_id=self.dataset.id)


class TestCheckMayCreateResource(ResourceValidatorTestCase):
    def test_authorized(self) -> None:
        self.resource_validator.check_may_create_resource(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            owner_account_id=self.owner_account_id,
            resource_type=self.resource_type,
        )

        self.authorizer.check_requester_may_create_resource.assert_called_once_with(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            resource_type=self.resource_type,
            owner_account_id=self.owner_account_id,
        )

    def test_unauthorized(self) -> None:
        self.authorizer.check_requester_may_create_resource.side_effect = ForbiddenError

        with pytest.raises(ForbiddenError):
            self.resource_validator.check_may_create_resource(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                owner_account_id=self.owner_account_id,
                resource_type=self.resource_type,
            )


class TestCheckGlueSyncResourceRequirements(ResourceValidatorTestCase):
    def test_forbidden_for_account_without_metadata_role_support(self) -> None:
        self.accounts_table.get.return_value = build_account(account_type=AccountType.technical)

        with pytest.raises(ForbiddenError):
            self.resource_validator.check_glue_sync_resource_requirements(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                owner_account_id=self.owner_account_id,
                sync_type=build_sync_type(),
                partition=self.partition,
            )

    def test_forbidden_for_legacy_sync_type(self) -> None:
        with pytest.raises(ForbiddenError):
            self.resource_validator.check_glue_sync_resource_requirements(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                owner_account_id=self.owner_account_id,
                sync_type=SyncType.glue_sync,
                partition=self.partition,
            )

    def test_forbidden_for_sync_type_lake_formation_in_non_test_env(self) -> None:
        environment = next(env for env in list(Environment) if not env.is_test_environment)

        resource_validator = ResourceValidator(
            accounts_table=self.accounts_table,
            config=build_config(environment=environment),
            resources_table=UnusableMock(),
            authorizer=UnusableMock(),
            requester_identity=UnusableMock(),
            visible_data_loader=UnusableMock(),
            assumable_account_spec_cls=AssumableAccountSpec,
            filter_packages_table=self.filter_packages_table,
        )

        with pytest.raises(ForbiddenError):
            resource_validator.check_glue_sync_resource_requirements(
                dataset=self.dataset,
                stage=self.stage,
                region=self.region,
                owner_account_id=self.owner_account_id,
                sync_type=SyncType.lake_formation,
                partition=self.partition,
            )

    @pytest.mark.parametrize(
        "requested_sync_type, permission_sync_type",
        [
            (SyncType.lake_formation, SyncType.resource_link),
            (SyncType.lake_formation, SyncType.glue_sync),
            (SyncType.lake_formation, SyncType.lake_formation),
            (SyncType.resource_link, SyncType.lake_formation),
        ],
    )
    def test_forbidden_for_conflicting_sync_types(
        self, requested_sync_type: SyncType, permission_sync_type: SyncType
    ) -> None:
        environment = next(env for env in list(Environment) if env.is_test_environment)
        conflicting_permission = build_dataset_account_permission(
            stage=self.stage, region=self.region, sync_type=permission_sync_type
        )
        dataset = build_dataset(permissions=frozenset([conflicting_permission]))

        resource_validator = ResourceValidator(
            accounts_table=self.accounts_table,
            config=build_config(environment=environment),
            resources_table=UnusableMock(),
            authorizer=UnusableMock(),
            requester_identity=UnusableMock(),
            visible_data_loader=UnusableMock(),
            assumable_account_spec_cls=AssumableAccountSpec,
            filter_packages_table=self.filter_packages_table,
        )

        with pytest.raises(ForbiddenError):
            resource_validator.check_glue_sync_resource_requirements(
                dataset=dataset,
                stage=self.stage,
                region=self.region,
                owner_account_id=self.owner_account_id,
                sync_type=requested_sync_type,
                partition=self.partition,
            )

    @pytest.mark.parametrize(
        "requested_sync_type, permission_sync_type",
        [
            (SyncType.resource_link, SyncType.resource_link),
            (SyncType.resource_link, SyncType.glue_sync),
            (SyncType.lake_formation, None),
            (SyncType.resource_link, None),
            (None, None),
        ],
    )
    def test_allowed_cases(
        self, requested_sync_type: Optional[SyncType], permission_sync_type: Optional[SyncType]
    ) -> None:
        environment = next(env for env in list(Environment) if env.is_test_environment)
        existing_permissions = (
            [build_dataset_account_permission(stage=self.stage, region=self.region, sync_type=permission_sync_type)]
            if permission_sync_type
            else []
        )
        dataset = build_dataset(permissions=frozenset(existing_permissions))

        resource_validator = ResourceValidator(
            accounts_table=self.accounts_table,
            config=build_config(environment=environment),
            resources_table=UnusableMock(),
            authorizer=UnusableMock(),
            requester_identity=UnusableMock(),
            visible_data_loader=UnusableMock(),
            assumable_account_spec_cls=AssumableAccountSpec,
            filter_packages_table=self.filter_packages_table,
        )

        resource_validator.check_glue_sync_resource_requirements(
            dataset=dataset,
            stage=self.stage,
            region=self.region,
            owner_account_id=self.owner_account_id,
            sync_type=requested_sync_type,
            partition=self.partition,
        )


class TestDetermineAccountForNewResource(ResourceValidatorTestCase):
    def setup_method(self) -> None:
        super().setup_method()

        self.environment = build_environment()
        self.existing_resource_account = build_resource_account(
            self.hub, self.stage, stage_priority=0, environment=self.environment
        )
        self.default_account = build_resource_account(
            self.hub, self.stage, stage_priority=1, environment=self.environment
        )
        self.config = build_config(
            account_store=build_account_store([self.existing_resource_account, self.default_account]),
            environment=self.environment,
        )

        self.resource_validator = ResourceValidator(
            accounts_table=self.accounts_table,
            config=self.config,
            resources_table=self.resources_table,
            authorizer=self.authorizer,
            requester_identity=self.requester_identity,
            visible_data_loader=self.visible_data_loader,
            assumable_account_spec_cls=AssumableAccountSpec,
            filter_packages_table=self.filter_packages_table,
        )

    def test_new_s3_no_existing_glue_sync_resource(self) -> None:
        self.resources_table.get_glue_sync.side_effect = ResourceNotFound(
            self.dataset.id, Builder.build_random_string()
        )

        result = self.resource_validator.determine_account_for_new_resource(
            dataset=self.dataset,
            hub=self.hub,
            stage=self.stage,
            region=self.region,
            resource_type=ResourceType.s3,
        )

        assert result == self.default_account
        self.resources_table.get_glue_sync.assert_called_once_with(
            dataset_id=self.dataset.id, region=self.region, stage=self.stage
        )

    def test_new_s3_no_glue_sync_default_resource_account_not_found(self) -> None:
        self.resources_table.get_glue_sync.side_effect = ResourceNotFound(
            self.dataset.id, Builder.build_random_string()
        )

        with pytest.raises(BadRequestError):
            self.resource_validator.determine_account_for_new_resource(
                dataset=self.dataset,
                hub=self.hub,
                stage=Builder.get_random_element(to_choose_from=list(Stage), exclude=[self.stage]),
                region=self.region,
                resource_type=ResourceType.s3,
            )

    def test_new_s3_with_existing_glue_sync_resource(self) -> None:
        self.resources_table.get_glue_sync.return_value = build_resource(
            resource_type=ResourceType.glue_sync, resource_account_id=self.existing_resource_account.id
        )

        with pytest.raises(ForbiddenError):
            self.resource_validator.determine_account_for_new_resource(
                dataset=self.dataset,
                hub=self.hub,
                stage=self.stage,
                region=self.region,
                resource_type=ResourceType.s3,
            )

        self.resources_table.get_glue_sync.assert_called_once_with(
            dataset_id=self.dataset.id, region=self.region, stage=self.stage
        )

    def test_new_glue_sync_with_existing_s3_resource(self) -> None:
        self.resources_table.get_s3.return_value = build_resource(
            resource_type=ResourceType.s3, resource_account_id=self.existing_resource_account.id
        )

        result = self.resource_validator.determine_account_for_new_resource(
            dataset=self.dataset,
            hub=self.hub,
            stage=self.stage,
            region=self.region,
            resource_type=ResourceType.glue_sync,
        )
        assert result == self.existing_resource_account

        self.resources_table.get_s3.assert_called_once_with(
            dataset_id=self.dataset.id, region=self.region, stage=self.stage
        )

    def test_new_glue_sync_no_existing_s3_resource(self) -> None:
        self.resources_table.get_s3.side_effect = ResourceNotFound(self.dataset.id, Builder.build_random_string())

        with pytest.raises(ForbiddenError):
            self.resource_validator.determine_account_for_new_resource(
                dataset=self.dataset,
                hub=self.hub,
                stage=self.stage,
                region=self.region,
                resource_type=ResourceType.glue_sync,
            )

        self.resources_table.get_s3.assert_called_once_with(
            dataset_id=self.dataset.id, region=self.region, stage=self.stage
        )


class TestCheckMayDeleteResource(ResourceValidatorTestCase):
    def setup_method(self) -> None:
        super().setup_method()
        self.resource = build_resource(
            resource_type=self.resource_type,
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
            owner_account_id=self.owner_account_id,
        )
        self.visible_data_loader.get_resource.return_value = self.resource
        self.dataset = replace(self.dataset, permissions=frozenset())

    def check_may_delete(self) -> Resource:
        if self.resource.type is ResourceType.s3:
            return self.resource_validator.check_may_delete_s3_resource(
                dataset=self.dataset,
                stage=self.resource.stage,
                region=self.resource.region,
            )
        return self.resource_validator.check_may_delete_glue_sync_resource(
            dataset=self.dataset,
            stage=self.resource.stage,
            region=self.resource.region,
        )

    def test_authorized(self) -> None:
        if self.resource_type is ResourceType.s3:
            self.resources_table.get_glue_sync.side_effect = ResourceNotFound(
                self.dataset.id, Builder.build_random_string()
            )

        resource = self.check_may_delete()

        assert resource == self.resource

    def test_unauthorized_fails(self) -> None:
        self.authorizer.check_requester_may_delete_resource.side_effect = ForbiddenError
        with pytest.raises(ForbiddenError):
            self.check_may_delete()

    def test_resource_does_not_exist(self) -> None:
        self.visible_data_loader.get_resource.side_effect = ResourceNotFound(self.dataset.id, "something")
        with pytest.raises(NotFoundError):
            self.check_may_delete()

    def test_dataset_account_permission_present(self) -> None:
        s3_resource = build_s3_resource(dataset=self.dataset)
        self.resource = s3_resource
        self.visible_data_loader.get_resource.return_value = s3_resource
        permission = build_dataset_account_permission(stage=s3_resource.stage, region=s3_resource.region)
        self.dataset = replace(self.dataset, permissions=frozenset({permission}))

        with pytest.raises(ForbiddenError):
            self.check_may_delete()

    def test_delete_s3_with_existing_glue_sync_resource(self) -> None:
        self.resources_table.get_glue_sync.return_value = build_glue_sync_resource(dataset=self.dataset)
        s3_resource = build_s3_resource(dataset=self.dataset)
        self.resource = s3_resource
        self.visible_data_loader.get_resource.return_value = s3_resource
        with pytest.raises(ForbiddenError):
            self.check_may_delete()


class TestCheckGlueSyncResourceDeletionRequirements(ResourceValidatorTestCase):
    def test_forbidden_for_lake_formation_sync_type_with_existing_permissions(self) -> None:
        glue_resource = build_glue_sync_resource(sync_type=SyncType.lake_formation)
        permissions = [build_dataset_account_permission(region=glue_resource.region, stage=glue_resource.stage)]
        dataset = build_dataset(permissions=frozenset(permissions))
        self.filter_packages_table.list.return_value = []

        with pytest.raises(ForbiddenError):
            self.resource_validator.check_glue_sync_resource_deletion_requirements(
                glue_resource=glue_resource,
                dataset=dataset,
            )

    def test_forbidden_for_lake_formation_sync_type_with_existing_filter_packages(self) -> None:
        glue_resource = build_glue_sync_resource(sync_type=SyncType.lake_formation)
        dataset = build_dataset(permissions=frozenset())
        filter_package = build_filter_package()
        self.filter_packages_table.list.return_value = [filter_package]

        with pytest.raises(ForbiddenError):
            self.resource_validator.check_glue_sync_resource_deletion_requirements(
                glue_resource=glue_resource, dataset=dataset
            )

    def test_allowed_for_lake_formation_sync_type_without_permissions_or_filter_packages(self) -> None:
        glue_resource = build_glue_sync_resource(sync_type=SyncType.lake_formation)
        dataset = build_dataset(permissions=frozenset())
        self.filter_packages_table.list.return_value = []

        self.resource_validator.check_glue_sync_resource_deletion_requirements(
            glue_resource=glue_resource,
            dataset=dataset,
        )

    @pytest.mark.parametrize("sync_type", [SyncType.glue_sync, SyncType.resource_link])
    @pytest.mark.parametrize("permissions_exist", [True, False])
    def test_allowed_for_other_sync_types(self, sync_type: SyncType, permissions_exist: bool) -> None:
        glue_resource = build_glue_sync_resource(sync_type=sync_type)
        permissions = (
            [build_dataset_account_permission(region=glue_resource.region, stage=glue_resource.stage)]
            if permissions_exist
            else []
        )
        dataset = build_dataset(permissions=frozenset(permissions))

        self.resource_validator.check_glue_sync_resource_deletion_requirements(
            glue_resource=glue_resource,
            dataset=dataset,
        )
