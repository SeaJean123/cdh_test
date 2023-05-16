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
from dataclasses import fields
from dataclasses import replace
from datetime import datetime
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional
from typing import Set

import pytest

from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset import DatasetAccountPermission
from cdh_core.entities.dataset import DatasetAccountPermissionAction
from cdh_core.entities.dataset import DatasetId
from cdh_core.entities.dataset import DatasetLineage
from cdh_core.entities.dataset import DatasetTags
from cdh_core.entities.dataset import ExternalLink
from cdh_core.entities.dataset import ResponseDataset
from cdh_core.entities.dataset import ResponseDatasets
from cdh_core.entities.dataset import SourceIdentifier
from cdh_core.entities.dataset import SupportGroup
from cdh_core.entities.dataset_participants import DatasetParticipants
from cdh_core.entities.dataset_participants_test import build_dataset_participants
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetPurpose
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.dataset_properties import IngestFrequency
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties import RetentionPeriod
from cdh_core.enums.dataset_properties import SupportLevel
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_business_object
from cdh_core.enums.dataset_properties_test import build_confidentiality
from cdh_core.enums.dataset_properties_test import build_dataset_purpose
from cdh_core.enums.dataset_properties_test import build_dataset_status
from cdh_core.enums.dataset_properties_test import build_external_link_type
from cdh_core.enums.dataset_properties_test import build_ingest_frequency
from cdh_core.enums.dataset_properties_test import build_layer
from cdh_core.enums.dataset_properties_test import build_retention_period
from cdh_core.enums.dataset_properties_test import build_support_level
from cdh_core.enums.dataset_properties_test import build_sync_type
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def build_dataset_account_permission(
    account_id: Optional[AccountId] = None,
    region: Optional[Region] = None,
    stage: Optional[Stage] = None,
    sync_type: Optional[SyncType] = None,
) -> DatasetAccountPermission:
    return DatasetAccountPermission(
        account_id=account_id or build_account_id(),
        region=region or build_region(),
        stage=stage or build_stage(),
        sync_type=sync_type or build_sync_type(),
    )


def build_dataset_tags(tags: Optional[Dict[str, str]] = None) -> DatasetTags:
    return DatasetTags(
        tags
        if tags is not None
        else {f"key_{Builder.build_random_string()}": f"value_{Builder.build_random_string()}" for _ in range(5)},
    )


def build_dataset_id(hub: Optional[Hub] = None) -> DatasetId:
    return Dataset.build_id(
        business_object=build_business_object(),
        name=Builder.build_random_string(),
        layer=build_layer(),
        hub=hub or Hub.default(),
    )


def build_external_link() -> ExternalLink:
    return ExternalLink(
        url=Builder.build_random_string(),
        name=Builder.build_random_string(),
        type=build_external_link_type(),
    )


def build_dataset_lineage(upstream_lineage: Optional[Set[DatasetId]] = None) -> DatasetLineage:
    return DatasetLineage(upstream=upstream_lineage or set())


def build_source_identifier(source_identifier: Optional[str] = None) -> SourceIdentifier:
    return SourceIdentifier(source_identifier or Builder.build_random_string())


def build_support_group(support_group: Optional[str] = None) -> SupportGroup:
    return SupportGroup(support_group or Builder.build_random_string())


def build_dataset(
    business_object: Optional[BusinessObject] = None,
    hub: Optional[Hub] = None,
    contains_pii: Optional[bool] = None,
    creator_user_id: Optional[str] = None,
    creation_date: Optional[datetime] = None,
    confidentiality: Optional[Confidentiality] = None,
    description: Optional[str] = None,
    documentation: Optional[str] = None,
    external_links: Optional[List[ExternalLink]] = None,
    friendly_name: Optional[str] = None,
    hub_visibility: Optional[Set[Hub]] = None,
    ingest_frequency: Optional[IngestFrequency] = None,
    labels: Optional[Set[str]] = None,
    layer: Optional[Layer] = None,
    lineage: Optional[DatasetLineage] = None,
    name: Optional[str] = None,
    permissions: Optional[FrozenSet[DatasetAccountPermission]] = None,
    preview_available: Optional[bool] = None,
    retention_period: Optional[RetentionPeriod] = None,
    source_identifier: Optional[SourceIdentifier] = None,
    support_level: Optional[SupportLevel] = None,
    tags: Optional[DatasetTags] = None,
    update_date: Optional[datetime] = None,
    owner_account_id: Optional[AccountId] = None,
    support_group: Optional[SupportGroup] = None,
    status: Optional[DatasetStatus] = None,
    purpose: Optional[Set[DatasetPurpose]] = None,
    quality_score: Optional[int] = None,
) -> Dataset:
    if business_object is None:
        business_object = build_business_object()
    if hub is None:
        hub = Hub.default()
    if name is None:
        name = Builder.build_random_string()
    if layer is None:
        layer = build_layer()
    return Dataset(
        id=Dataset.build_id(business_object, name, layer, hub),
        business_object=business_object,
        hub=hub,
        confidentiality=confidentiality or build_confidentiality(),
        contains_pii=contains_pii if contains_pii is not None else Builder.get_random_bool(),
        creator_user_id=creator_user_id or Builder.build_random_string(),
        creation_date=creation_date or datetime.now(),
        description=description or "something",
        documentation=documentation or "I describe a Dataset.\nEven with multiple lines.",
        external_links=external_links if external_links is not None else [build_external_link() for _ in range(3)],
        friendly_name=friendly_name or "My Dataset",
        hub_visibility=(hub_visibility if hub_visibility is not None else {build_hub() for _ in range(3)}),
        ingest_frequency=ingest_frequency or build_ingest_frequency(),
        labels=labels if labels is not None else {Builder.build_random_string() for _ in range(3)},
        lineage=lineage or build_dataset_lineage(),
        layer=layer,
        name=name,
        permissions=(
            permissions
            if permissions is not None
            else frozenset({build_dataset_account_permission() for _ in range(3)})
        ),
        preview_available=preview_available if preview_available is not None else Builder.get_random_bool(),
        retention_period=retention_period or build_retention_period(),
        source_identifier=source_identifier or build_source_identifier(),
        support_level=support_level or build_support_level(),
        tags=tags if tags is not None else build_dataset_tags(),
        update_date=update_date or datetime.now(),
        owner_account_id=owner_account_id or build_account_id(),
        support_group=support_group,
        status=status or build_dataset_status(),
        purpose=purpose if purpose is not None else {build_dataset_purpose()},
        quality_score=quality_score,
    )


def build_response_dataset(dataset_participants: Optional[DatasetParticipants] = None) -> ResponseDataset:
    return ResponseDataset.from_dataset(
        build_dataset(), dataset_participants=dataset_participants or build_dataset_participants()
    )


@pytest.mark.usefixtures("mock_config_file")
class TestDataset:
    def test_build_id_default_hub(self) -> None:
        hub = Hub("global")
        business_object = build_business_object()
        name = Builder.build_random_string()
        layer = build_layer()
        assert Dataset.build_id(business_object, name, layer, hub) == DatasetId(
            f"{business_object.value}_{name}_{layer.value}"
        )

    # pylint: disable=unused-argument
    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_build_id_non_default_hub(self, mock_config_file: ConfigFile) -> None:
        hub = Hub("cn")
        business_object = build_business_object()
        name = Builder.build_random_string()
        layer = build_layer()
        assert Dataset.build_id(business_object, name, layer, hub) == DatasetId(
            f"{hub.value}_{business_object.value}_{name}_{layer.value}"
        )

    def test_to_plain_dict(self) -> None:
        account_id = build_account_id()
        region = build_region()
        stage = build_stage()
        sync_type = build_sync_type()
        dataset = build_dataset(
            permissions=frozenset(
                {
                    build_dataset_account_permission(
                        account_id=account_id, region=region, stage=stage, sync_type=sync_type
                    ),
                }
            ),
            lineage=build_dataset_lineage({build_dataset_id(), build_dataset_id()}),
        )
        dataset_json = dataset.to_plain_dict()
        assert Dataset.from_dict(dataset_json) == dataset
        assert dataset_json["creationDate"] != dataset_json["updateDate"]
        assert json.dumps(dataset_json)
        assert dataset_json["permissions"] == [
            {"accountId": account_id, "region": region.value, "stage": stage.value, "syncType": sync_type.value},
        ]

    def test_filter_permissions(self) -> None:
        account_id = build_account_id()
        region, second_region = Builder.choose_without_repetition(Region, 2)
        stage, second_stage, third_stage = Builder.choose_without_repetition(Stage, 3)
        sync_type = build_sync_type()
        first_permission = build_dataset_account_permission(
            account_id=account_id, region=region, stage=stage, sync_type=sync_type
        )
        second_permission = build_dataset_account_permission(
            account_id=account_id, region=region, stage=second_stage, sync_type=sync_type
        )
        third_permission = build_dataset_account_permission(
            account_id=account_id, region=second_region, stage=third_stage, sync_type=sync_type
        )
        dataset = build_dataset(
            permissions=frozenset(
                {
                    first_permission,
                    second_permission,
                    third_permission,
                    build_dataset_account_permission(region=region, stage=third_stage),
                }
            )
        )

        assert dataset.filter_permissions(account_id=account_id, region=region) == frozenset(
            {first_permission, second_permission}
        )
        assert dataset.filter_permissions(stage=stage, sync_type=sync_type) == frozenset({first_permission})
        assert dataset.filter_permissions(region=second_region, stage=stage) == frozenset()

    @pytest.mark.parametrize("prefix", ["", "cdhxtest"])
    @pytest.mark.parametrize("default_hub", [True, False])
    @pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
    def test_build_cdh_bucket_name(self, prefix: str, default_hub: bool, mock_config_file: ConfigFile) -> None:
        hub = Hub.default() if default_hub else Builder.get_random_element(list(Hub), exclude={Hub.default()})
        business_object = build_business_object()
        layer = build_layer()
        dataset = build_dataset(hub=hub, business_object=business_object, layer=layer)
        dataset = replace(dataset, hub=build_hub(), business_object=build_business_object(), layer=build_layer())

        hub_infix = "" if default_hub else f"{hub.value}-"
        expected_bucket_name_prefix = f"{prefix}cdh-{hub_infix}{business_object.value}-{dataset.name}-{layer.value}-"

        bucket_name = dataset.build_cdh_bucket_name(prefix)

        assert bucket_name[:-4] == expected_bucket_name_prefix

    def test_get_account_ids_with_read_access(self) -> None:
        stage, other_stage = Builder.choose_without_repetition(Stage, 2)
        region, other_region = Builder.choose_without_repetition(Region, 2)
        account_ids = [build_account_id(), build_account_id()]

        dataset = build_dataset(
            permissions=frozenset(
                {
                    build_dataset_account_permission(account_id=account_ids[0], region=region, stage=stage),
                    build_dataset_account_permission(account_id=account_ids[1], region=region, stage=stage),
                    build_dataset_account_permission(region=other_region, stage=stage),
                    build_dataset_account_permission(region=region, stage=other_stage),
                }
            )
        )

        assert dataset.get_account_ids_with_read_access(stage, region) == frozenset(account_ids)


class TestDatasetAccountPermissionAction:
    def test_inverse_defined(self) -> None:
        for action in DatasetAccountPermissionAction:
            assert isinstance(action.inverse, DatasetAccountPermissionAction)

    def test_inverse_add(self) -> None:
        assert DatasetAccountPermissionAction.add.inverse is DatasetAccountPermissionAction.remove

    def test_inverse_remove(self) -> None:
        assert DatasetAccountPermissionAction.remove.inverse is DatasetAccountPermissionAction.add


class TestResponseDataset:
    def test_response_dataset(self) -> None:
        dataset = build_dataset()
        dataset_participants = build_dataset_participants()
        response_dataset = ResponseDataset.from_dataset(
            dataset=dataset,
            dataset_participants=dataset_participants,
        )

        assert response_dataset.stewards == dataset_participants.stewards
        assert response_dataset.engineers == dataset_participants.engineers
        for field in fields(Dataset):
            assert getattr(dataset, field.name) == getattr(response_dataset, field.name)


class TestResponseDatasets:
    def test_to_json(self) -> None:
        dataset = build_dataset()
        response_dataset = ResponseDataset.from_dataset(
            dataset=dataset, dataset_participants=build_dataset_participants()
        )
        response_datasets = ResponseDatasets(datasets=[response_dataset])
        json_str = response_datasets.to_json()
        assert json.loads(json_str)
