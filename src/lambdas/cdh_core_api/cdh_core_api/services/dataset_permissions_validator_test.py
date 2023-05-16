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
import inspect
from dataclasses import replace
from functools import wraps
from typing import Any
from typing import Callable
from typing import Optional
from typing import Type
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from cdh_core_api.bodies.datasets import DatasetAccountPermissionBody
from cdh_core_api.bodies.datasets import DatasetAccountPermissionPostBody
from cdh_core_api.catalog.accounts_table import AccountNotFound
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.generic_types import GenericAccount
from cdh_core_api.generic_types import GenericS3Resource
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.dataset_permissions_validator import DatasetPermissionsValidator
from cdh_core_api.services.dataset_permissions_validator import NON_SHAREABLE_ACCOUNT_TYPES
from cdh_core_api.services.dataset_permissions_validator import ValidatedDatasetAccessPermission
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.entities.accounts import Account
from cdh_core.entities.accounts_test import build_account
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_account_permission
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_sync_type
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import ConflictError
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class DatasetPermissionsValidatorTestCase:
    def setup_method(self) -> None:
        self.stage = build_stage()
        self.region = build_region(partition=Partition("aws"))
        self.hub = build_hub(partition=Partition("aws"))
        self.account = build_account(hub=self.hub, account_type=AccountType.usecase)
        self.dataset = build_dataset(hub=self.hub)
        self.s3_resource = build_s3_resource(dataset=self.dataset, region=self.region, stage=self.stage)
        self.glue_resource = build_glue_sync_resource(
            dataset=self.dataset,
            region=self.region,
            stage=self.stage,
            sync_type=Builder.get_random_element(
                list(SyncType),
                exclude={SyncType.lake_formation},
            ),
        )
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.accounts_table = Mock(AccountsTable)
        self.accounts_table.get.return_value = self.account
        self.authorizer = Mock(Authorizer)
        self.resources_table = Mock(ResourcesTable)
        self.resources_table.list_glue_sync.return_value = []
        self.dataset_permissions_validator = DatasetPermissionsValidator(
            authorizer=self.authorizer,
            accounts_table=self.accounts_table,
            resources_table=self.resources_table,
            visible_data_loader=self.visible_data_loader,
        )
        self.body = DatasetAccountPermissionBody(self.account.id, self.region, self.stage)
        self.post_body = DatasetAccountPermissionPostBody(self.account.id, self.region, self.stage)


def patch_fetches(testcase: Callable[..., None]) -> Callable[..., None]:
    @wraps(testcase)
    def patched_testcase(self: DatasetPermissionsValidatorTestCase, *args: Any, **kwargs: Any) -> None:
        with patch("cdh_core_api.services.dataset_permissions_validator.fetch_dataset", return_value=self.dataset):
            with patch(
                "cdh_core_api.services.dataset_permissions_validator.DatasetPermissionsValidator.fetch_s3_resource",
                return_value=self.s3_resource,
            ):
                with patch(
                    "cdh_core_api.services.dataset_permissions_validator.DatasetPermissionsValidator"
                    ".fetch_glue_resource",
                    return_value=self.glue_resource,
                ):
                    testcase(self, *args, **kwargs)

    return patched_testcase


def patch_fetches_in_class(
    test_class: Type[DatasetPermissionsValidatorTestCase],
) -> Type[DatasetPermissionsValidatorTestCase]:
    for name, function in inspect.getmembers(test_class, inspect.isfunction):
        if name.startswith("test") and not name.startswith("test_fetch"):
            setattr(test_class, name, patch_fetches(function))

    return test_class


@patch_fetches_in_class
class TestDatasetPermissionsValidatorGrant(DatasetPermissionsValidatorTestCase):
    def test_validate_request_call(self) -> None:
        with patch(
            "cdh_core_api.services.dataset_permissions_validator."
            "DatasetPermissionsValidator.get_validated_dataset_access_permission"
        ) as build_permission:
            self.dataset_permissions_validator.validate_dataset_access_request(
                hub=self.hub, dataset_id=self.dataset.id, body=self.post_body
            )

            build_permission.assert_called_once_with(
                hub=self.hub,
                dataset_id=self.dataset.id,
                account_id=self.body.accountId,
                region=self.region,
                stage=self.stage,
                sync_type=None,
            )

    @pytest.mark.parametrize(
        "requested_sync_type, provider_sync_type",
        [
            (SyncType.glue_sync, None),
            (SyncType.glue_sync, SyncType.glue_sync),
            (SyncType.glue_sync, SyncType.resource_link),
            (SyncType.resource_link, None),
            (SyncType.resource_link, SyncType.glue_sync),
            (SyncType.resource_link, SyncType.resource_link),
            (SyncType.lake_formation, SyncType.lake_formation),
            (None, SyncType.glue_sync),
            (None, SyncType.resource_link),
            (None, SyncType.lake_formation),
        ],
    )
    def test_allowed_cases(
        self, requested_sync_type: Optional[SyncType], provider_sync_type: Optional[SyncType]
    ) -> None:
        if provider_sync_type:
            glue_resource = build_glue_sync_resource(sync_type=provider_sync_type)
            self.dataset_permissions_validator.fetch_glue_resource = Mock(return_value=glue_resource)  # type: ignore
        else:
            self.dataset_permissions_validator.fetch_glue_resource = Mock(side_effect=NotFoundError())  # type: ignore

        validated_permission: ValidatedDatasetAccessPermission[
            Account, S3Resource
        ] = self.dataset_permissions_validator.get_validated_dataset_access_permission(
            hub=self.hub,
            dataset_id=self.dataset.id,
            region=self.region,
            stage=self.stage,
            account_id=self.account.id,
            sync_type=requested_sync_type,
        )
        assert validated_permission.dataset == self.dataset
        assert validated_permission.account == self.account
        assert validated_permission.s3_resource == self.s3_resource
        expected_sync_type = requested_sync_type or (
            provider_sync_type if provider_sync_type is SyncType.lake_formation else SyncType.resource_link
        )
        assert validated_permission.permission == DatasetAccountPermission(
            account_id=self.account.id,
            region=self.region,
            stage=self.stage,
            sync_type=expected_sync_type,
        )

    @pytest.mark.parametrize(
        "requested_sync_type, provider_sync_type",
        [
            (SyncType.lake_formation, SyncType.resource_link),
            (SyncType.lake_formation, SyncType.glue_sync),
            (SyncType.lake_formation, None),
            (SyncType.resource_link, SyncType.lake_formation),
            (SyncType.glue_sync, SyncType.lake_formation),
        ],
    )
    def test_forbidden_cases(
        self, requested_sync_type: Optional[SyncType], provider_sync_type: Optional[SyncType]
    ) -> None:
        if provider_sync_type:
            glue_resource = build_glue_sync_resource(sync_type=provider_sync_type)
            self.dataset_permissions_validator.fetch_glue_resource = Mock(return_value=glue_resource)  # type: ignore
        else:
            self.dataset_permissions_validator.fetch_glue_resource = Mock(side_effect=NotFoundError())  # type: ignore

        with pytest.raises(ForbiddenError):
            self.dataset_permissions_validator.get_validated_dataset_access_permission(
                hub=self.hub,
                dataset_id=self.dataset.id,
                region=self.region,
                stage=self.stage,
                account_id=self.account.id,
                sync_type=requested_sync_type,
            )

    def test_fetch_resource_call(self) -> None:
        with patch("cdh_core_api.services.dataset_permissions_validator.fetch_dataset", return_value=self.dataset):
            with patch(
                "cdh_core_api.services.dataset_permissions_validator.DatasetPermissionsValidator.fetch_s3_resource",
                return_value=self.s3_resource,
            ) as fetch_s3_resource:
                with patch(
                    "cdh_core_api.services.dataset_permissions_validator.DatasetPermissionsValidator"
                    ".fetch_glue_resource",
                    return_value=self.glue_resource,
                ) as fetch_glue_resource:
                    self.dataset_permissions_validator.validate_dataset_access_request(
                        hub=self.hub, dataset_id=self.dataset.id, body=self.post_body
                    )

                    fetch_s3_resource.assert_called_once_with(
                        dataset_id=self.dataset.id,
                        hub=self.hub,
                        stage=self.stage,
                        region=self.region,
                    )

                    fetch_glue_resource.assert_called_once_with(
                        dataset_id=self.dataset.id,
                        hub=self.hub,
                        stage=self.stage,
                        region=self.region,
                    )

    def test_fetch_dataset_call(self) -> None:
        with patch(
            "cdh_core_api.services.dataset_permissions_validator.fetch_dataset", return_value=self.dataset
        ) as fetch_dataset:
            with patch(
                "cdh_core_api.services.dataset_permissions_validator.DatasetPermissionsValidator.fetch_s3_resource",
                return_value=self.s3_resource,
            ):
                with patch(
                    "cdh_core_api.services.dataset_permissions_validator.DatasetPermissionsValidator"
                    ".fetch_glue_resource",
                    return_value=self.glue_resource,
                ):
                    self.dataset_permissions_validator.validate_dataset_access_request(
                        hub=self.hub, dataset_id=self.dataset.id, body=self.post_body
                    )
                    fetch_dataset.assert_called_once_with(
                        hub=self.hub, dataset_id=self.dataset.id, visible_data_loader=self.visible_data_loader
                    )

    def test_authorization_check(self) -> None:
        self.dataset_permissions_validator.validate_dataset_access_request(
            hub=self.hub, dataset_id=self.dataset.id, body=self.post_body
        )

        self.authorizer.check_requester_may_manage_dataset_access.assert_called_once_with(
            self.s3_resource.owner_account_id
        )

    @pytest.mark.parametrize("account_type", sorted(NON_SHAREABLE_ACCOUNT_TYPES, key=lambda t: t.value))  # type: ignore
    def test_infeasible_target_account(self, account_type: AccountType) -> None:
        new_account = build_account(account_type=account_type)
        assert all(p.account_id != new_account.id for p in self.dataset.permissions)
        self.accounts_table.get.return_value = new_account
        with pytest.raises(ForbiddenError):
            self.dataset_permissions_validator.validate_dataset_access_request(
                hub=self.hub,
                dataset_id=self.dataset.id,
                body=DatasetAccountPermissionPostBody(new_account.id, self.region, self.stage),
            )

    def test_unknown_target_account_fails(self) -> None:
        unknown_account_id = build_account_id()
        self.accounts_table.get.side_effect = AccountNotFound(unknown_account_id)
        with pytest.raises(ForbiddenError):
            self.dataset_permissions_validator.validate_dataset_access_request(
                hub=self.hub,
                dataset_id=self.dataset.id,
                body=DatasetAccountPermissionPostBody(unknown_account_id, self.region, self.stage),
            )

    def test_already_existing_permission(self) -> None:
        dataset = replace(
            self.dataset,
            permissions=frozenset(
                {build_dataset_account_permission(self.account.id, self.region, self.stage, build_sync_type())}
            ),
        )
        with patch("cdh_core_api.services.dataset_permissions_validator.fetch_dataset", return_value=dataset):
            with pytest.raises(ConflictError):
                self.dataset_permissions_validator.validate_dataset_access_request(
                    hub=self.hub, dataset_id=dataset.id, body=self.post_body
                )

    def test_conflicting_resource(self) -> None:
        resource = build_s3_resource(
            dataset=self.dataset, stage=self.stage, region=self.region, owner_account_id=self.account.id
        )
        self.resources_table.list_glue_sync.return_value = [resource]

        with pytest.raises(ConflictError):
            self.dataset_permissions_validator.validate_dataset_access_request(
                hub=self.hub, dataset_id=self.dataset.id, body=self.post_body
            )


@patch_fetches_in_class
class TestDatasetPermissionsValidatorRevoke(DatasetPermissionsValidatorTestCase):
    def setup_method(self) -> None:
        super().setup_method()
        self.existing_permission = build_dataset_account_permission(
            region=self.region, stage=self.stage, account_id=self.account.id
        )
        self.dataset = build_dataset(hub=self.hub, permissions=frozenset({self.existing_permission}))

    def test_successful_revoke(self) -> None:
        validated_permission: ValidatedDatasetAccessPermission[
            Account, S3Resource
        ] = self.dataset_permissions_validator.validate_revoke(hub=self.hub, dataset_id=self.dataset.id, body=self.body)

        assert validated_permission.dataset == self.dataset
        assert validated_permission.account == self.account
        assert validated_permission.s3_resource == self.s3_resource
        assert validated_permission.permission == self.existing_permission

    def test_authorizer_call(self) -> None:
        self.dataset_permissions_validator.validate_revoke(hub=self.hub, dataset_id=self.dataset.id, body=self.body)

        self.authorizer.check_requester_may_manage_dataset_access.assert_called_once_with(
            self.s3_resource.owner_account_id
        )

    def test_missing_permission(self) -> None:
        other_body = DatasetAccountPermissionBody(build_account_id(), self.region, self.stage)

        with pytest.raises(ConflictError):
            self.dataset_permissions_validator.validate_revoke(
                hub=self.hub, dataset_id=self.dataset.id, body=other_body
            )

    def test_fetch_resource_call(self) -> None:
        with patch("cdh_core_api.services.dataset_permissions_validator.fetch_dataset", return_value=self.dataset):
            with patch(
                "cdh_core_api.services.dataset_permissions_validator.fetch_resource", return_value=self.s3_resource
            ) as fetch_resource:
                self.dataset_permissions_validator.validate_revoke(
                    hub=self.hub, dataset_id=self.dataset.id, body=self.body
                )

                fetch_resource.assert_called_once_with(
                    hub=self.hub,
                    dataset_id=self.dataset.id,
                    resource_type=self.s3_resource.type,
                    stage=self.stage,
                    region=self.region,
                    visible_data_loader=self.visible_data_loader,
                )

    def test_fetch_dataset_call(self) -> None:
        with patch(
            "cdh_core_api.services.dataset_permissions_validator.fetch_dataset", return_value=self.dataset
        ) as fetch_dataset:
            with patch(
                "cdh_core_api.services.dataset_permissions_validator.fetch_resource", return_value=self.s3_resource
            ):
                self.dataset_permissions_validator.validate_revoke(
                    hub=self.hub, dataset_id=self.dataset.id, body=self.body
                )
                fetch_dataset.assert_called_once_with(
                    hub=self.hub, dataset_id=self.dataset.id, visible_data_loader=self.visible_data_loader
                )

    def test_unknown_target_account_fails(self) -> None:
        unknown_account_id = build_account_id()
        self.accounts_table.get.side_effect = AccountNotFound(unknown_account_id)
        with pytest.raises(ForbiddenError):
            self.dataset_permissions_validator.validate_revoke(
                hub=self.hub,
                dataset_id=self.dataset.id,
                body=DatasetAccountPermissionBody(unknown_account_id, self.region, self.stage),
            )


def build_validated_dataset_access_permission(
    dataset: Optional[Dataset] = None,
    account: Optional[GenericAccount] = None,
    s3_resource: Optional[GenericS3Resource] = None,
    stage: Optional[Stage] = None,
    region: Optional[Region] = None,
    sync_type: Optional[SyncType] = None,
) -> ValidatedDatasetAccessPermission[Account, S3Resource]:
    used_dataset = dataset or build_dataset()
    used_account = account or build_account()
    used_stage = stage or build_stage()
    used_region = region or build_region()
    used_resource = s3_resource or build_s3_resource(dataset=used_dataset, stage=used_stage, region=used_region)
    return ValidatedDatasetAccessPermission[Account, S3Resource](
        dataset=used_dataset,
        account=used_account,
        s3_resource=used_resource,
        permission=build_dataset_account_permission(
            account_id=used_account.id,
            stage=used_stage,
            region=used_region,
            sync_type=sync_type or build_sync_type(),
        ),
    )
