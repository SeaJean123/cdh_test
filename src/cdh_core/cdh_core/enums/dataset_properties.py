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
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from cdh_core.config.config_file_loader import ConfigFileLoader


# pylint: disable=invalid-name
class Layer(Enum):
    """Level of processing applied to the dataset."""

    raw = "raw"
    src = "src"
    pre = "pre"
    sem = "sem"

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        if self is Layer.raw:
            return "Raw"
        if self is Layer.src:
            return "Source"
        if self is Layer.pre:
            return "Prepared"
        if self is Layer.sem:
            return "Semantic"
        raise ValueError(f"This enum value is not supported: {self}")


class BusinessObjectMixin(Enum):
    """Provides the logic for the Enum BusinessObject."""

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for a business object."""
        return ConfigFileLoader.get_config().business_object.instances[self.name].friendly_name


if TYPE_CHECKING:
    BusinessObject = BusinessObjectMixin
else:
    BusinessObject = Enum(
        "BusinessObject",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().business_object.instances.items()},
        type=BusinessObjectMixin,
        module=__name__,
    )
BusinessObject.__doc__ = "Clusters datasets by their origin and/or the nature of the data they contain"


class SyncType(Enum):
    """Type of synchronisation used for the glue database."""

    glue_sync = "glue-sync"
    resource_link = "resource-link"
    lake_formation = "lake-formation"

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        if self is SyncType.glue_sync:
            return "Glue Sync"
        if self is SyncType.resource_link:
            return "Resource Link"
        if self is SyncType.lake_formation:
            return "Lake Formation"
        raise ValueError(f"This enum value is not supported: {self}")


class DatasetStatus(Enum):
    """Set by the dataset's owner to convey whether the dataset is ready for consumption by others."""

    PLANNED = "planned"
    DEVELOPMENT = "development"
    RELEASED = "released"
    DEPRECATED = "deprecated"

    @classmethod
    def initial_value(cls) -> "DatasetStatus":
        """Get the initial value set for newly created datasets."""
        return cls.PLANNED

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        return self.value.capitalize()


class DatasetPurposeMixin(Enum):
    """Provides the logic for the Enum DatasetPurpose."""

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for a dataset purpose."""
        return ConfigFileLoader.get_config().dataset_purpose.instances[self.name].friendly_name


if TYPE_CHECKING:
    DatasetPurpose = DatasetPurposeMixin
else:
    DatasetPurpose = Enum(
        "DatasetPurpose",
        {instance: entry.value for instance, entry in ConfigFileLoader.get_config().dataset_purpose.instances.items()},
        type=DatasetPurposeMixin,
        module=__name__,
    )
DatasetPurpose.__doc__ = (
    "Specifies how a dataset is intended and/or allowed to be used, e.g. with respect to data privacy regulations"
)


class IngestFrequency(Enum):
    """Can be used to specify what kind of data ingest can be expected for a dataset.

    Note that this is non-inferred information. The provider of a dataset is responsible for maintaining this
    information manually.
    """

    undefined = "undefined"
    streaming = "streaming"
    hourly = "hourly"
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"
    ad_hoc = "ad-hoc"

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        return self.value.title()  # pylint: disable=no-member


class RetentionPeriod(Enum):
    """Can be used to specify how long data is typically available in a dataset.

    The provider of a dataset is responsible for maintaining this information manually and ensuring the desired
    retention is actually implemented. In particular, there is no platform-side automatism removing data based on this
    attribute.
    """

    undefined = "undefined"
    seven_days = "seven-days"
    fourteen_days = "fourteen-days"
    thirty_days = "thirty-days"
    three_months = "three-months"
    six_months = "six-months"
    one_year = "one-year"
    ten_years = "ten-years"

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        name = {
            RetentionPeriod.undefined: "Undefined",
            RetentionPeriod.seven_days: "7 days at least",
            RetentionPeriod.fourteen_days: "14 days at least",
            RetentionPeriod.thirty_days: "30 days at least",
            RetentionPeriod.three_months: "Three months at least",
            RetentionPeriod.six_months: "Six months at least",
            RetentionPeriod.one_year: "One year at least",
            RetentionPeriod.ten_years: "Ten years at least",
        }.get(self)
        if not name:
            raise ValueError(f"This enum value is not supported: {self}")
        return name


class SupportLevel(Enum):
    """Can be used to specify how data providers will support a dataset."""

    undefined = "undefined"
    none = "none"
    best_effort = "best-effort"
    business_hours = "business-hours"
    twenty_four_seven = "twenty-four-seven"

    @property
    def friendly_name(self) -> str:
        """Return a human friendly name."""
        if self is SupportLevel.undefined:
            return "Undefined"
        if self is SupportLevel.none:
            return "None"
        if self is SupportLevel.best_effort:
            return "Best Effort"
        if self is SupportLevel.business_hours:
            return "Business Hours"
        if self is SupportLevel.twenty_four_seven:
            return "24/7"
        raise ValueError(f"This enum value is not supported: {self}")


class ExternalLinkTypeMixin(Enum):
    """Provides the logic for the Enum ExternalLinkType."""

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for an external link type."""
        return ConfigFileLoader.get_config().dataset_external_link_type.instances[self.name].friendly_name


if TYPE_CHECKING:
    ExternalLinkType = ExternalLinkTypeMixin
else:
    ExternalLinkType = Enum(
        "DatasetExternalLinkType",
        {
            instance: entry.value
            for instance, entry in ConfigFileLoader.get_config().dataset_external_link_type.instances.items()
        },
        type=ExternalLinkTypeMixin,
        module=__name__,
    )
ExternalLinkType.__doc__ = "Specifies the type of content found at a given URL."


class Confidentiality(Enum):
    """Describes the level of protection in place for a given dataset."""

    public = "public"
    not_public = "not-public"
    confidential = "confidential"
    strictly_confidential = "strictly-confidential"
    secret = "secret"

    @property
    def friendly_name(self) -> str:
        """Return the human friendly name for a confidentiality level."""
        if self is Confidentiality.public:
            return "Public"
        if self is Confidentiality.not_public:
            return "Not Public"
        if self is Confidentiality.confidential:
            return "Confidential"
        if self is Confidentiality.strictly_confidential:
            return "Strictly Confidential"
        if self is Confidentiality.secret:
            return "Secret"
        raise ValueError(f"This enum value is not supported: {self}")
