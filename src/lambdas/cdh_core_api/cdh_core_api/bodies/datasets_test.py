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
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

import pytest
from cdh_core_api.api.validation import SchemaValidator
from cdh_core_api.bodies.datasets import DatasetAccountPermissionBody
from cdh_core_api.bodies.datasets import DatasetAccountPermissionPostBody
from cdh_core_api.bodies.datasets import DatasetParticipantBodyPart
from cdh_core_api.bodies.datasets import ExternalLinkBody
from cdh_core_api.bodies.datasets import NewDatasetBody
from cdh_core_api.bodies.datasets import UpdateDatasetBody
from cdh_core_api.config import ValidationContext
from cdh_core_api.config_test import build_config
from cdh_core_api.validation.datasets import DeletableSourceIdentifier
from cdh_core_api.validation.datasets import DeletableSupportGroup
from marshmallow import ValidationError

from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import DatasetTags
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset import SupportGroup
from cdh_core.entities.dataset_participants import DatasetParticipant
from cdh_core.entities.dataset_participants import DatasetParticipantId
from cdh_core.entities.dataset_participants_test import build_dataset_participant
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetPurpose
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.dataset_properties import IngestFrequency
from cdh_core.enums.dataset_properties import RetentionPeriod
from cdh_core.enums.dataset_properties import SupportLevel
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.dataset_properties_test import build_external_link_type
from cdh_core.enums.dataset_properties_test import build_layer
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def build_new_dataset_body(
    dataset: Optional[Dataset] = None,
    engineers: Optional[List[DatasetParticipant]] = None,
    stewards: Optional[List[DatasetParticipant]] = None,
) -> NewDatasetBody:
    engineers = engineers if engineers is not None else [build_dataset_participant() for _ in range(3)]
    dataset = dataset or build_dataset()
    return NewDatasetBody(
        name=dataset.name,
        businessObject=dataset.business_object,
        containsPii=dataset.contains_pii,
        description=dataset.description,
        documentation=dataset.documentation,
        engineers=cast(List[DatasetParticipantBodyPart], engineers),
        friendlyName=dataset.friendly_name,
        ingestFrequency=dataset.ingest_frequency,
        labels=list(dataset.labels),
        layer=dataset.layer,
        previewAvailable=dataset.preview_available,
        purpose=list(dataset.purpose),
        retentionPeriod=dataset.retention_period,
        sourceIdentifier=dataset.source_identifier,
        stewards=cast(List[DatasetParticipantBodyPart], stewards) if stewards is not None else None,
        supportLevel=dataset.support_level,
        tags=DatasetTags(dataset.tags.copy()),
        ownerAccountId=dataset.owner_account_id,
        confidentiality=dataset.confidentiality,
        externalLinks=[
            ExternalLinkBody(type=link.type, url=link.url, name=link.name) for link in dataset.external_links
        ],
        hubVisibility=dataset.hub_visibility,
        upstreamLineage=dataset.lineage.upstream,
        supportGroup=dataset.support_group,
    )


def build_update_dataset_body(
    confidentiality: Optional[Confidentiality] = None,
    contains_pii: Optional[bool] = None,
    description: Optional[str] = None,
    documentation: Optional[str] = None,
    engineers: Optional[List[DatasetParticipant]] = None,
    external_links: Optional[List[ExternalLink]] = None,
    friendly_name: Optional[str] = None,
    hub_visibility: Optional[Set[Hub]] = None,
    ingest_frequency: Optional[IngestFrequency] = None,
    labels: Optional[Set[str]] = None,
    upstream_lineage: Optional[Set[DatasetId]] = None,
    preview_available: Optional[bool] = None,
    purpose: Optional[Set[DatasetPurpose]] = None,
    quality_score: Optional[int] = None,
    retention_period: Optional[RetentionPeriod] = None,
    source_identifier: Optional[DeletableSourceIdentifier] = None,
    support_group: Optional[DeletableSupportGroup] = None,
    status: Optional[DatasetStatus] = None,
    stewards: Optional[List[DatasetParticipant]] = None,
    support_level: Optional[SupportLevel] = None,
    tags: Optional[DatasetTags] = None,
) -> UpdateDatasetBody:
    return UpdateDatasetBody(
        confidentiality=confidentiality,
        containsPii=contains_pii,
        description=description,
        documentation=documentation,
        externalLinks=None
        if external_links is None
        else [ExternalLinkBody(type=link.type, url=link.url, name=link.name) for link in external_links],
        engineers=cast(List[DatasetParticipantBodyPart], engineers) if engineers is not None else None,
        friendlyName=friendly_name,
        hubVisibility=hub_visibility,
        ingestFrequency=ingest_frequency,
        labels=None if labels is None else list(labels),
        upstreamLineage=upstream_lineage,
        previewAvailable=preview_available,
        purpose=None if purpose is None else list(purpose),
        qualityScore=quality_score,
        retentionPeriod=retention_period,
        supportGroup=support_group,
        sourceIdentifier=source_identifier,
        status=status,
        stewards=cast(List[DatasetParticipantBodyPart], stewards) if stewards is not None else None,
        supportLevel=support_level,
        tags=tags,
    )


def build_dataset_account_permission_body(
    account_id: Optional[AccountId] = None, region: Optional[Region] = None, stage: Optional[Stage] = None
) -> DatasetAccountPermissionBody:
    return DatasetAccountPermissionBody(
        accountId=account_id or build_account_id(),
        region=region or build_region(),
        stage=stage or build_stage(),
    )


def build_dataset_account_permission_post_body(
    account_id: Optional[AccountId] = None,
    region: Optional[Region] = None,
    stage: Optional[Stage] = None,
    sync_type: Optional[SyncType] = None,
) -> DatasetAccountPermissionPostBody:
    return DatasetAccountPermissionPostBody(
        accountId=account_id or build_account_id(),
        region=region or build_region(),
        stage=stage or build_stage(),
        syncType=sync_type,
    )


class _TestDatasetBody:
    plain_body: Dict[str, Any]
    body: Union[NewDatasetBody, UpdateDatasetBody]
    validator: SchemaValidator[Union[NewDatasetBody, UpdateDatasetBody]]

    def _assert_valid(self) -> None:
        assert self.validator(self.plain_body) == self.body

    def test_valid(self) -> None:
        self._assert_valid()

    def test_invalid_tags(self) -> None:
        invalid_key = "InvalidKey"
        invalid_value = "invalid.value"
        self.plain_body["tags"] = [{invalid_key: invalid_value}]

        with pytest.raises(ValidationError):
            self.validator(self.plain_body)

    def test_invalid_email(self) -> None:
        invalid_email = "NotAnEmail"
        self.plain_body["engineers"] = [{"id": invalid_email, "idp": "some"}]

        with pytest.raises(ValidationError):
            self.validator(self.plain_body)

    def test_invalid_source_identifier(self) -> None:
        invalid_source_identifier = 42
        self.plain_body["sourceIdentifier"] = invalid_source_identifier

        with pytest.raises(ValidationError):
            self.validator(self.plain_body)

    def test_invalid_support_group(self) -> None:
        invalid_support_group = 42
        self.plain_body["supportGroup"] = invalid_support_group

        with pytest.raises(ValidationError):
            self.validator(self.plain_body)


class TestNewDatasetBody(_TestDatasetBody):
    def setup_method(self) -> None:
        business_object = build_business_object()
        confidentiality = build_confidentiality()
        link_type = build_external_link_type()
        environment = build_environment()
        hub = random.choice(list(Hub.get_hubs(environment=environment)))
        layer = build_layer()
        preview_available = Builder.get_random_bool()
        self.plain_body = {
            "name": "docstest",
            "businessObject": business_object.value,
            "containsPii": False,
            "confidentiality": confidentiality.value,
            "description": "functional test test data",
            "engineers": [{"id": "cdh-all@example.com", "idp": "example"}],
            "externalLinks": [{"type": link_type.value, "url": "https://mydocurl.com"}],
            "friendlyName": "functional test test data",
            "hubVisibility": [hub.value],
            "layer": layer.value,
            "previewAvailable": preview_available,
            "supportGroup": "some:group",
            "tags": {"mytesttag": "mytesttagvalue"},
        }
        self.body = NewDatasetBody(
            name="docstest",
            businessObject=business_object,
            containsPii=False,
            confidentiality=confidentiality,
            description="functional test test data",
            engineers=[DatasetParticipantBodyPart(id=DatasetParticipantId("cdh-all@example.com"), idp="example")],
            externalLinks=[ExternalLinkBody(type=link_type, url="https://mydocurl.com")],
            friendlyName="functional test test data",
            hubVisibility={hub},
            layer=layer,
            previewAvailable=preview_available,
            supportGroup=SupportGroup("some:group"),
            tags=DatasetTags({"mytesttag": "mytesttagvalue"}),
        )
        self.validator = SchemaValidator(
            NewDatasetBody,
            context=ValidationContext(config=build_config(environment=environment), current_hub=build_hub()),
        )


class TestUpdateDatasetBody(_TestDatasetBody):
    def setup_method(self) -> None:
        self.plain_body = {}
        self.body = UpdateDatasetBody()
        self.validator = SchemaValidator(
            UpdateDatasetBody, context=ValidationContext(config=build_config(), current_hub=build_hub())
        )

    def test_invalid_quality_score(self) -> None:
        invalid_quality_score = 101
        self.plain_body["qualityScore"] = invalid_quality_score

        with pytest.raises(ValidationError):
            self.validator(self.plain_body)

    def test_reset_optional_strings_validation(self) -> None:
        self.plain_body = {"documentation": "", "sourceIdentifier": "", "supportGroup": ""}
        self.body = replace(
            self.body,
            documentation="",
            sourceIdentifier="",
            supportGroup="",
        )

        self._assert_valid()
