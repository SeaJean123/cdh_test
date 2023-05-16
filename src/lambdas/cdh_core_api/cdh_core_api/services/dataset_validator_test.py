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
import random
from dataclasses import replace
from typing import Optional
from unittest.mock import Mock

import pytest
from cdh_core_api.bodies.datasets_test import build_new_dataset_body
from cdh_core_api.bodies.datasets_test import build_update_dataset_body
from cdh_core_api.catalog.datasets_table import DatasetNotFound
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.config_test import build_config
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.dataset_validator import DatasetValidator
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from cdh_core_api.validation.datasets_test import build_deletable_source_identifier
from cdh_core_api.validation.datasets_test import build_deletable_support_group
from marshmallow import ValidationError

from cdh_core.entities.dataset import DatasetLineage
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.dataset_test import build_dataset_id
from cdh_core.entities.dataset_test import build_dataset_tags
from cdh_core.entities.dataset_test import build_external_link
from cdh_core.entities.request_test import build_requester_identity
from cdh_core.entities.resource_test import build_glue_sync_resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.dataset_properties import ExternalLinkType
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.dataset_properties_test import build_dataset_purpose
from cdh_core.enums.dataset_properties_test import build_ingest_frequency
from cdh_core.enums.dataset_properties_test import build_retention_period
from cdh_core.enums.dataset_properties_test import build_support_level
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.exceptions.http import ForbiddenError
from cdh_core.exceptions.http import NotFoundError
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


class DatasetValidatorTestCase:
    @pytest.fixture(autouse=True)
    def service_setup(self, time_travel: None) -> None:  # pylint: disable=unused-argument
        self.authorization_api = Mock(AuthorizationApi)
        self.authorizer = Mock(Authorizer)
        self.config = build_config()
        self.user = Builder.build_random_string()
        self.requester_identity = build_requester_identity(user=self.user)
        self.resources_table = Mock(ResourcesTable)
        self.visible_data_loader = Mock(VisibleDataLoader)
        self.dataset_validator = DatasetValidator(
            authorization_api=self.authorization_api,
            authorizer=self.authorizer,
            config=self.config,
            requester_identity=self.requester_identity,
            resources_table=self.resources_table,
            visible_data_loader=self.visible_data_loader,
        )


class TestValidateNewDataset(DatasetValidatorTestCase):
    @pytest.fixture(autouse=True)
    def extend_setup(self, service_setup: None) -> None:  # pylint: disable=unused-argument
        self.visible_data_loader.get_datasets_cross_hub.return_value = [build_dataset()]
        self.authorization_api.get_user_id.return_value = None
        layer = Builder.get_random_element(to_choose_from=list(Layer), exclude={Layer.sem})
        self.expected_dataset = build_dataset(
            creator_user_id=self.user,
            external_links=[],
            hub_visibility=set(),
            layer=layer,
            lineage=DatasetLineage(set()),
            permissions=frozenset(),
            owner_account_id=build_account_id(),
            status=DatasetStatus.initial_value(),
        )
        self.body = build_new_dataset_body(self.expected_dataset)

    def _validate_successful(self) -> None:
        dataset = self.dataset_validator.validate_new_dataset_body(
            body=self.body,
            hub=self.expected_dataset.hub,
        )
        assert dataset == self.expected_dataset
        self.authorizer.check_requester_may_create_dataset.assert_called_once_with(
            hub=self.expected_dataset.hub,
            business_object=self.expected_dataset.business_object,
            layer=self.expected_dataset.layer,
            owner_account_id=self.expected_dataset.owner_account_id,
        )
        self.visible_data_loader.get_datasets_cross_hub.assert_called_once_with(
            list(self.body.upstreamLineage or set())
        )

    def test_create_valid(self) -> None:
        self._validate_successful()

    def test_create_requester_is_owner_if_not_set_in_body(self) -> None:
        self.body = replace(self.body, ownerAccountId=None)
        self.expected_dataset = replace(self.expected_dataset, owner_account_id=self.requester_identity.account_id)
        self._validate_successful()

    @pytest.mark.parametrize(
        "contains_pii, confidentiality, expected_preview_available",
        [
            (True, Confidentiality.secret, False),
            (True, Confidentiality.public, False),
            (False, Confidentiality.secret, False),
            (False, Confidentiality.public, True),
        ],
    )
    def test_create_preview_available_is_default_if_not_set_in_body(
        self, contains_pii: bool, confidentiality: Confidentiality, expected_preview_available: bool
    ) -> None:
        self.body = replace(self.body, previewAvailable=None, containsPii=contains_pii, confidentiality=confidentiality)
        self.expected_dataset = replace(
            self.expected_dataset,
            preview_available=expected_preview_available,
            contains_pii=contains_pii,
            confidentiality=confidentiality,
        )
        self._validate_successful()

    def test_create_get_user_from_auth_api(self) -> None:
        user = Builder.build_random_string()
        self.expected_dataset = replace(self.expected_dataset, creator_user_id=user)
        self.authorization_api.get_user_id.return_value = user
        self._validate_successful()

    def test_create_dataset_with_lineage_valid(self) -> None:
        existing_datasets = [build_dataset() for _ in range(3)]
        self.expected_dataset = replace(
            self.expected_dataset, lineage=DatasetLineage({dataset.id for dataset in existing_datasets})
        )
        self.visible_data_loader.get_datasets_cross_hub.return_value = existing_datasets
        self.body = build_new_dataset_body(self.expected_dataset)

        self._validate_successful()

    def test_create_dataset_unauthorized_invalid(self) -> None:
        self.authorizer.check_requester_may_create_dataset.side_effect = ForbiddenError(build_account_id())

        with pytest.raises(ForbiddenError):
            self.dataset_validator.validate_new_dataset_body(
                hub=Mock(),
                body=build_new_dataset_body(),
            )

    def test_create_dataset_verify_lineage_invalid(self) -> None:
        dataset = build_dataset(lineage=DatasetLineage({build_dataset_id()}))
        self.visible_data_loader.get_datasets_cross_hub.return_value = []

        with pytest.raises(ValidationError):
            self.dataset_validator.validate_new_dataset_body(
                hub=Mock(),
                body=build_new_dataset_body(dataset=dataset),
            )


class TestValidateDeletion(DatasetValidatorTestCase):
    @pytest.fixture(autouse=True)
    def extend_setup(self, service_setup: None) -> None:  # pylint: disable=unused-argument
        self.dataset = build_dataset()
        self.hub = self.dataset.hub
        self.resources_table.list.return_value = []
        self.visible_data_loader.get_dataset.return_value = self.dataset

    def test_delete_non_empty_dataset(self) -> None:
        self.resources_table.list.return_value = [
            build_s3_resource(dataset=self.dataset),
            build_glue_sync_resource(dataset=self.dataset),
        ]
        with pytest.raises(ForbiddenError):
            self.dataset_validator.validate_deletion(self.dataset.id, self.hub)

    def test_delete_nonexisting_dataset(self) -> None:
        self.visible_data_loader.get_dataset.side_effect = DatasetNotFound(self.dataset.id)

        with pytest.raises(NotFoundError):
            self.dataset_validator.validate_deletion(self.dataset.id, self.hub)

    def test_delete_dataset_wrong_hub_raises(self) -> None:
        other_hub = Builder.get_random_element(Hub, exclude=[self.dataset.hub])
        self.visible_data_loader.get_dataset.return_value = self.dataset

        with pytest.raises(NotFoundError):
            self.dataset_validator.validate_deletion(self.dataset.id, other_hub)

    def test_unauthorized_to_delete(self) -> None:
        self.visible_data_loader.get_dataset.return_value = self.dataset
        self.authorizer.check_requester_may_delete_dataset.side_effect = ForbiddenError
        with pytest.raises(ForbiddenError):
            self.dataset_validator.validate_deletion(self.dataset.id, self.hub)


class TestValidateUpdateBody(DatasetValidatorTestCase):
    @pytest.fixture(autouse=True)
    def extend_setup(self, service_setup: None) -> None:  # pylint: disable=unused-argument
        self.external_links = [
            ExternalLink(url=Builder.build_random_string(), name=Builder.build_random_string(), type=link_type)
            for link_type in ExternalLinkType
        ]
        self.dataset = build_dataset(external_links=self.external_links)
        self.visible_data_loader.get_datasets_cross_hub.return_value = [self.dataset, build_dataset()]
        self.body = build_update_dataset_body()

    def _assert_update_dataset_body_valid(self, hub: Optional[Hub] = None) -> None:
        dataset = self.dataset_validator.validate_update_dataset_body(
            dataset_id=self.dataset.id,
            body=self.body,
            hub=hub or self.dataset.hub,
        )
        assert dataset == self.dataset
        self.authorizer.check_requester_may_update_dataset.assert_called_once_with(dataset=self.dataset)
        self.visible_data_loader.get_datasets_cross_hub.assert_called_once_with(
            list((self.body.upstreamLineage or set()).union({self.dataset.id}))
        )

    def test_empty_body_valid(self) -> None:
        self._assert_update_dataset_body_valid()

    def test_update_all_simple_dataset_fields(self) -> None:
        self.body = build_update_dataset_body(
            confidentiality=build_confidentiality(),
            contains_pii=not self.dataset.contains_pii,
            description=Builder.build_random_string(),
            documentation=Builder.build_random_string(length=1000),
            external_links=[build_external_link() for _ in range(3)],
            friendly_name=Builder.build_random_string(),
            hub_visibility={build_hub() for _ in range(3)},
            ingest_frequency=build_ingest_frequency(),
            labels={Builder.build_random_string()},
            purpose={build_dataset_purpose()},
            retention_period=build_retention_period(),
            source_identifier=build_deletable_source_identifier(),
            status=random.choice([status for status in DatasetStatus if status is not DatasetStatus.RELEASED]),
            support_group=build_deletable_support_group(),
            support_level=build_support_level(),
            tags=build_dataset_tags(),
        )

        self._assert_update_dataset_body_valid()

    def test_unauthorized_to_update_dataset(self) -> None:
        self.authorizer.check_requester_may_update_dataset.side_effect = ForbiddenError()

        with pytest.raises(ForbiddenError):
            self._assert_update_dataset_body_valid()

    @pytest.mark.parametrize("status", (status for status in DatasetStatus if status is not DatasetStatus.RELEASED))
    def test_unauthorized_to_release_dataset_may_change_to_other_status(self, status: DatasetStatus) -> None:
        self.authorizer.check_requester_may_release_dataset.side_effect = ForbiddenError()
        self.body = build_update_dataset_body(status=status)

        self._assert_update_dataset_body_valid()

    def test_unauthorized_to_release_dataset_may_not_release(self) -> None:
        self.authorizer.check_requester_may_release_dataset.side_effect = ForbiddenError()

        self.dataset = build_dataset(
            status=Builder.get_random_element(list(DatasetStatus), exclude={DatasetStatus.RELEASED})
        )
        self.visible_data_loader.get_datasets_cross_hub.return_value = [self.dataset]
        self.body = build_update_dataset_body(status=DatasetStatus.RELEASED)

        with pytest.raises(ForbiddenError):
            self._assert_update_dataset_body_valid()

    def test_not_found(self) -> None:
        self.visible_data_loader.get_datasets_cross_hub.return_value = []
        with pytest.raises(NotFoundError):
            self._assert_update_dataset_body_valid()

    def test_wrong_hub(self) -> None:
        wrong_hub = Builder.get_random_element(list(Hub), exclude={self.dataset.hub})
        with pytest.raises(NotFoundError):
            self._assert_update_dataset_body_valid(hub=wrong_hub)

    def test_update_dataset_with_lineage_invalid(self) -> None:
        self.body = build_update_dataset_body(upstream_lineage={build_dataset_id()})
        with pytest.raises(ValidationError):
            self._assert_update_dataset_body_valid()

    def test_update_dataset_with_lineage_success(self) -> None:
        existing_datasets = [build_dataset() for _ in range(3)]
        self.visible_data_loader.get_datasets_cross_hub.return_value = existing_datasets + [self.dataset]
        self.body = build_update_dataset_body(upstream_lineage={dataset.id for dataset in existing_datasets})

        self._assert_update_dataset_body_valid()

    @pytest.mark.parametrize("attribute", ["documentation", "source_identifier", "support_group"])
    def test_reset_optional_string_attribute(self, attribute: str) -> None:
        self.body = build_update_dataset_body(**{attribute: ""})  # type: ignore

        self._assert_update_dataset_body_valid()
