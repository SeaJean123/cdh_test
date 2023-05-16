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
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from typing import Union

import pytest

from cdh_core.dataclasses_json_cdh.dataclasses_json_cdh import DataClassJsonCDHMixin
from cdh_core.dates import date_output
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_kms_key_arn
from cdh_core.entities.dataset import Dataset
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.glue_database import DatabaseName
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import GlueSyncResourcePayload
from cdh_core.entities.resource import ResourcePayload
from cdh_core.entities.resource import ResourcesPayload
from cdh_core.entities.resource import S3Resource
from cdh_core.entities.resource import S3ResourcePayload
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.dataset_properties_test import build_sync_type
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_resource_type
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id


def build_resource(
    resource_type: Optional[ResourceType] = None,
    dataset: Optional[Dataset] = None,
    arn: Optional[Arn] = None,
    creator_user_id: Optional[str] = None,
    creation_date: Optional[datetime] = None,
    region: Optional[Region] = None,
    resource_account_id: Optional[AccountId] = None,
    stage: Optional[Stage] = None,
    sns_topic_arn: Optional[Arn] = None,
    kms_key_arn: Optional[Arn] = None,
    owner_account_id: Optional[AccountId] = None,
) -> Union[GlueSyncResource, S3Resource]:
    if not resource_type:
        resource_type = build_resource_type()
    if resource_type == ResourceType.glue_sync:
        return build_glue_sync_resource(
            dataset=dataset,
            arn=arn,
            creator_user_id=creator_user_id,
            creation_date=creation_date,
            region=region,
            resource_account_id=resource_account_id,
            stage=stage,
            owner_account_id=owner_account_id,
        )
    if resource_type == ResourceType.s3:
        return build_s3_resource(
            dataset=dataset,
            arn=arn,
            creator_user_id=creator_user_id,
            creation_date=creation_date,
            region=region,
            resource_account_id=resource_account_id,
            stage=stage,
            sns_topic_arn=sns_topic_arn,
            kms_key_arn=kms_key_arn,
            owner_account_id=owner_account_id,
        )
    raise ValueError(f"Resource type {resource_type} is not a valid type.")


def build_s3_resource(
    dataset: Optional[Dataset] = None,
    arn: Optional[Arn] = None,
    creator_user_id: Optional[str] = None,
    creation_date: Optional[datetime] = None,
    region: Optional[Region] = None,
    resource_account_id: Optional[AccountId] = None,
    stage: Optional[Stage] = None,
    sns_topic_arn: Optional[Arn] = None,
    kms_key_arn: Optional[Arn] = None,
    owner_account_id: Optional[AccountId] = None,
) -> S3Resource:
    dataset = dataset or build_dataset()
    region = region or build_region()
    resource_account_id = resource_account_id or build_account_id()
    resource_name = dataset.build_cdh_bucket_name()
    return S3Resource(
        dataset_id=dataset.id,
        resource_account_id=resource_account_id,
        hub=dataset.hub,
        arn=arn or build_arn(service="s3", account_id=resource_account_id, region=region, resource=resource_name),
        creator_user_id=creator_user_id or "USER_IDENTITY",
        creation_date=creation_date or datetime.now(),
        region=region,
        stage=stage or build_stage(),
        sns_topic_arn=sns_topic_arn or build_arn(service="sns", account_id=resource_account_id, resource=resource_name),
        kms_key_arn=kms_key_arn or build_kms_key_arn(),
        update_date=creation_date or datetime.now(),
        owner_account_id=owner_account_id or build_account_id(),
    )


def build_glue_sync_resource(
    dataset: Optional[Dataset] = None,
    arn: Optional[Arn] = None,
    creator_user_id: Optional[str] = None,
    creation_date: Optional[datetime] = None,
    region: Optional[Region] = None,
    resource_account_id: Optional[AccountId] = None,
    stage: Optional[Stage] = None,
    owner_account_id: Optional[AccountId] = None,
    database_name: Optional[DatabaseName] = None,
    sync_type: Optional[SyncType] = None,
) -> GlueSyncResource:
    dataset = dataset or build_dataset()
    region = region or build_region()
    stage = stage or build_stage()
    resource_account_id = resource_account_id or build_account_id()
    resource_name = dataset.build_cdh_bucket_name()
    return GlueSyncResource(
        dataset_id=dataset.id,
        hub=dataset.hub,
        resource_account_id=resource_account_id,
        arn=arn
        or build_arn(service="glue-sync", account_id=resource_account_id, region=region, resource=resource_name),
        creation_date=creation_date or datetime.now(),
        creator_user_id=creator_user_id,
        region=region,
        stage=stage,
        update_date=creation_date or datetime.now(),
        owner_account_id=owner_account_id or build_account_id(),
        database_name=database_name or DatabaseName(dataset.id),
        sync_type=sync_type or build_sync_type(),
    )


S3ResourceAttributes = S3ResourcePayload.S3ResourceAttributes


def test_s3_payload_to_plain() -> None:
    bucket = build_s3_resource()
    assert bucket.update_date is not None  # for mypy
    expected_dict = {
        "datasetId": bucket.dataset_id,
        "hub": bucket.hub.value,
        "resourceAccountId": str(bucket.resource_account_id),
        "arn": str(bucket.arn),
        "name": bucket.name,
        "creationDate": date_output(bucket.creation_date),
        "creatorUserId": bucket.creator_user_id,
        "region": bucket.region.value,
        "stage": bucket.stage.value,
        "type": "s3",
        "attributes": {
            "snsTopicArn": str(bucket.sns_topic_arn),
            "kmsKeyArn": str(bucket.kms_key_arn),
        },
        "updateDate": date_output(bucket.update_date),
        "ownerAccountId": str(bucket.owner_account_id),
    }
    assert bucket.to_payload().to_plain_dict() == expected_dict


def test_glue_sync_payload_to_plain() -> None:
    glue_sync = build_glue_sync_resource()
    assert glue_sync.update_date is not None  # for mypy
    expected_dict = {
        "datasetId": glue_sync.dataset_id,
        "hub": glue_sync.hub.value,
        "resourceAccountId": str(glue_sync.resource_account_id),
        "arn": str(glue_sync.arn),
        "name": glue_sync.name,
        "creationDate": date_output(glue_sync.creation_date),
        "creatorUserId": glue_sync.creator_user_id,
        "region": glue_sync.region.value,
        "stage": glue_sync.stage.value,
        "type": "glue-sync",
        "attributes": {"syncType": glue_sync.sync_type.value},
        "updateDate": date_output(glue_sync.update_date),
        "ownerAccountId": str(glue_sync.owner_account_id),
    }
    assert glue_sync.to_payload().to_plain_dict() == expected_dict


def test_s3_payload_from_plain() -> None:
    payload = build_s3_resource().to_payload()
    assert S3ResourcePayload.from_dict(payload.to_plain_dict()) == payload
    assert S3ResourcePayload.from_json(payload.to_json()) == payload


def test_glue_sync_payload_from_plain() -> None:
    payload = build_glue_sync_resource().to_payload()
    assert GlueSyncResourcePayload.from_dict(payload.to_plain_dict()) == payload
    assert GlueSyncResourcePayload.from_json(payload.to_json()) == payload


@pytest.mark.parametrize("resource_type", sorted(ResourceType, key=str))
def test_generic_payload_from_plain(resource_type: ResourceType) -> None:
    payload = build_resource(resource_type=resource_type).to_payload()
    assert ResourcePayload.from_dict(payload.to_plain_dict()) == payload
    assert ResourcePayload.from_json(payload.to_json()) == payload


class TestResourcesPayload:
    def setup_method(self) -> None:
        self.bucket_1 = build_s3_resource()
        self.bucket_2 = build_s3_resource()
        self.payload = ResourcesPayload(resources=[self.bucket_1.to_payload(), self.bucket_2.to_payload()])

    def test_resources_payload_from_resources(self) -> None:
        assert ResourcesPayload.from_resources(resources=[self.bucket_1, self.bucket_2]) == self.payload
        bucket_1 = build_s3_resource()
        bucket_2 = build_s3_resource()
        payload = ResourcesPayload.from_resources([bucket_1, bucket_2])

        assert payload.resources == [bucket_1.to_payload(), bucket_2.to_payload()]

    def test_resources_payload_to_plain(self) -> None:
        assert self.payload.to_plain_dict() == {
            "resources": [
                self.bucket_1.to_payload().to_plain_dict(),
                self.bucket_2.to_payload().to_plain_dict(),
            ]
        }

    def test_resources_payload_from_plain(self) -> None:
        plain = {
            "resources": [
                self.bucket_1.to_payload().to_plain_dict(),
                self.bucket_2.to_payload().to_plain_dict(),
            ]
        }
        assert ResourcesPayload.from_dict(plain) == self.payload
        assert ResourcesPayload.from_json(json.dumps(plain)) == self.payload


def test_registration_only_at_base_class() -> None:
    with pytest.raises(RuntimeError):
        S3ResourcePayload.register_for_resource_type(ResourceType.glue_sync)


def test_register_payload_class() -> None:
    @ResourcePayload.register_for_resource_type(ResourceType.glue_sync, True)
    @dataclass(frozen=True)
    class FooPayload(ResourcePayload):
        @dataclass(frozen=True)
        class FooAttributes(DataClassJsonCDHMixin):
            foo: str  # pylint: disable=disallowed-name

        attributes: FooAttributes

    foo_dict = {
        "datasetId": "bi_yczsjeab_raw",
        "hub": build_hub().value,
        "resourceAccountId": "258613343851",
        "arn": "arn:aws:foo:::bar",
        "creationDate": "2021-09-13T07:53:39.264078+00:00",
        "creatorUserId": "USER_IDENTITY",
        "region": build_region().value,
        "stage": build_stage().value,
        "updateDate": "2021-09-13T07:53:39.264606+00:00",
        "ownerAccountId": "646456501325",
        "type": "glue-sync",
        "name": "foo",
        "attributes": {"foo": "bar"},
    }
    assert FooPayload.from_dict(foo_dict) == ResourcePayload.from_dict(foo_dict)
    assert FooPayload.from_dict(foo_dict).type is ResourceType.glue_sync
    assert ResourcePayload.from_dict(foo_dict).attributes == FooPayload.FooAttributes(foo="bar")
