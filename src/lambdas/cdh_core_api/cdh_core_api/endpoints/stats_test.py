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
from typing import Any
from typing import Dict
from typing import Optional
from typing import TYPE_CHECKING
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from cdh_core_api.catalog.resource_table import ResourceNotFound
from cdh_core_api.endpoints.stats import get_stats_of_s3
from cdh_core_api.endpoints.stats import S3_STATS_SCHEMA
from cdh_core_api.endpoints.stats import StatsPath
from cdh_core_api.services.visible_data_loader import VisibleDataLoader

from cdh_core.aws_clients.cloudwatch_client import CloudwatchClient
from cdh_core.aws_clients.cloudwatch_client import MetricResult
from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.entities.dataset_test import build_dataset
from cdh_core.entities.resource import Resource
from cdh_core.entities.resource_test import build_s3_resource
from cdh_core.entities.response import JsonResponse
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_region
from cdh_core.enums.hubs import Hub
from cdh_core.enums.hubs_test import build_hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.enums.resource_properties_test import build_stage
from cdh_core.exceptions.http import NotFoundError
from cdh_core.exceptions.http import TooManyRequestsError
from cdh_core_dev_tools.testing.builder import Builder

if TYPE_CHECKING:
    from botocore.exceptions import _ClientErrorResponseTypeDef
else:
    _ClientErrorResponseTypeDef = Dict[str, Any]


class TestResourceStats:
    def setup_method(self) -> None:
        self.hub = build_hub()
        self.dataset = build_dataset(hub=self.hub)
        self.stage = build_stage()
        self.region = build_region()
        self.s3_resource = build_s3_resource(
            dataset=self.dataset,
            stage=self.stage,
            region=self.region,
        )

        self.aws = Mock(AwsClientFactory)
        self.cloudwatch_client = Mock(CloudwatchClient)
        self.aws.cloudwatch_client.return_value = self.cloudwatch_client

        def fake_get_s3_resource(
            resource_type: ResourceType, dataset_id: str, stage: Stage, region: Region
        ) -> Resource:
            if (
                dataset_id == self.dataset.id
                and stage == self.stage
                and region == self.region
                and resource_type == ResourceType.s3
            ):
                return self.s3_resource
            raise ResourceNotFound(dataset_id, "")

        self.visible_data_loader = Mock(VisibleDataLoader)
        self.visible_data_loader.get_resource.side_effect = fake_get_s3_resource

    def _get_stats_of_s3(self, hub: Optional[Hub] = None) -> JsonResponse:
        return get_stats_of_s3(
            path=StatsPath(hub=hub or self.hub, datasetId=self.dataset.id, stage=self.stage, region=self.region),
            aws=self.aws,
            visible_data_loader=self.visible_data_loader,
        )

    def test_basic_metrics(self) -> None:
        values = [float(random.randint(1, 100)) for _ in range(3)]
        time_stamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=1000)
        self.cloudwatch_client.last_value_of_metrics.return_value = [
            MetricResult(
                metric_id="numberOfMessagesPublished",
                label="numberOfMessagesPublished",
                values=values,
                timestamps=[time_stamp],
            ),
            MetricResult(metric_id="x", label="x", values=[], timestamps=[]),
        ] + [
            MetricResult(metric_id=label, label=label, values=[], timestamps=[])
            for label in S3_STATS_SCHEMA.properties.keys()
            if label != "numberOfMessagesPublished"
        ]

        result = self._get_stats_of_s3().body

        assert result.keys() == S3_STATS_SCHEMA.properties.keys()  # type: ignore
        assert result == {
            k: values[0] if k == "numberOfMessagesPublished" else None for k in S3_STATS_SCHEMA.properties.keys()
        }
        requested_metrics = self.cloudwatch_client.last_value_of_metrics.call_args_list[0].kwargs["requested_metrics"]
        assert all(metric.metric_id[0].islower() for metric in requested_metrics)

    def test_get_stats_of_s3_non_existent(self) -> None:
        self.visible_data_loader.get_resource.side_effect = ResourceNotFound(self.dataset.id, "")
        with pytest.raises(NotFoundError):
            self._get_stats_of_s3()

    def test_get_stats_of_s3_wrong_hub(self) -> None:
        other_hub = Builder.get_random_element(Hub, exclude=[self.hub])
        with pytest.raises(NotFoundError):
            self._get_stats_of_s3(hub=other_hub)

    def test_handle_throttling_to_return_400_error(self) -> None:
        error_response: _ClientErrorResponseTypeDef = {
            "Error": {"Code": "Throttling", "Message": "Request was throttled."},
        }
        client_error = ClientError(error_response=error_response, operation_name="foo")
        with patch("cdh_core_api.endpoints.stats.gather_metrics", side_effect=client_error):
            with pytest.raises(TooManyRequestsError):
                self._get_stats_of_s3()
