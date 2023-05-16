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

import os
from dataclasses import dataclass
from typing import Collection
from typing import Dict
from typing import FrozenSet
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple

from cdh_core.aws_clients.sfn_client import StateMachineExecutionStatus
from cdh_core.config.authorization_api import AuthApi
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import AccountRoleType
from cdh_core.entities.accounts import SecurityAccount
from cdh_core.entities.arn import Arn
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts import AccountType
from cdh_core.enums.accounts import Affiliation
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.dataset_properties import Confidentiality
from cdh_core.enums.dataset_properties import DatasetPurpose
from cdh_core.enums.dataset_properties import DatasetStatus
from cdh_core.enums.dataset_properties import ExternalLinkType
from cdh_core.enums.dataset_properties import IngestFrequency
from cdh_core.enums.dataset_properties import Layer
from cdh_core.enums.dataset_properties import RetentionPeriod
from cdh_core.enums.dataset_properties import SupportLevel
from cdh_core.enums.dataset_properties import SyncType
from cdh_core.enums.environment import Environment
from cdh_core.enums.hubs import Hub
from cdh_core.enums.resource_properties import ResourceType
from cdh_core.enums.resource_properties import Stage
from cdh_core.primitives.account_id import AccountId


@dataclass(frozen=True)
class Config:
    """Contains the central configuration for the API."""

    lambda_account_id: AccountId
    environment: Environment
    hubs: Collection[Hub]
    notification_topics: FrozenSet[Arn]
    prefix: str
    disabled: bool
    account_store: AccountStore
    authorization_api_params: AuthApi
    encryption_key: str
    result_page_size: int

    ENUMS_TO_EXPOSE = [
        AccountType,
        Affiliation,
        BusinessObject,
        Confidentiality,
        ExternalLinkType,
        DatasetPurpose,
        DatasetStatus,
        Environment,
        IngestFrequency,
        Layer,
        Partition,
        Region,
        ResourceType,
        RetentionPeriod,
        AccountRoleType,
        Stage,
        StateMachineExecutionStatus,
        SupportLevel,
        SyncType,
    ]

    @classmethod
    def from_environment_and_context(cls, lambda_context: LambdaContext, account_store: AccountStore) -> Config:
        """
        Create a new config based on the environment variables and the lambda context.

        How to use DISABLE_CORE_API when deploying changes that require a downtime:
         - Use the AWS console to set this value to true. The Core API will only respond with 503 now.
         - Run your migrations.
         - Deploy the new code (this will automatically unset the variable again).
        """
        env = Environment(os.environ["ENVIRONMENT"])
        return Config(
            lambda_account_id=Arn(lambda_context.invoked_function_arn).account_id,
            environment=env,
            hubs=Hub.get_hubs(environment=env),
            notification_topics=frozenset(
                {Arn(os.environ["DATASET_NOTIFICATION_TOPIC"]), Arn(os.environ["NOTIFICATION_TOPIC"])}
            ),
            prefix=os.environ["RESOURCE_NAME_PREFIX"],
            disabled=os.environ.get("DISABLE_CORE_API", "").lower() == "true",
            account_store=account_store,
            authorization_api_params=AuthApi(
                auth_url=os.environ["AUTHORIZATION_API_URL"],
                cookie_name=os.environ["AUTHORIZATION_API_COOKIE_NAME"],
                users_url=os.environ["USERS_API_URL"],
            ),
            encryption_key=os.environ["ENCRYPTION_KEY_NAME"],
            result_page_size=int(os.environ["RESULT_PAGE_SIZE"]),
        )

    @property
    def stages(self) -> Set[Stage]:
        """Return a list of possible stages."""
        return {account.stage for account in self.account_store.query_resource_accounts(environments=self.environment)}

    @property
    def using_authorization_api(self) -> bool:
        """Return true if the authorization API should be used."""
        return self.authorization_api_params.active

    @property
    def functional_tests_user_role_name(self) -> str:
        """Return the name of the functional tests role in tests accounts."""
        return f"{self.prefix}cdh-core-functional-tests"

    def get_assumable_role_arns(self) -> Dict[Tuple[AccountId, Optional[AccountPurpose]], Optional[Arn]]:
        """Get a mapping of all role ARNs assumable by the Core API Lambda."""
        role_arns: Dict[Tuple[AccountId, Optional[AccountPurpose]], Optional[Arn]] = {}
        resource_role_arns: Dict[Tuple[AccountId, Optional[AccountPurpose]], Optional[Arn]] = {
            (account.id, account.purpose): account.get_assumable_role_arn_for_core_api(self.prefix)
            for account in self.account_store.query_resource_accounts(environments=self.environment)
        }
        role_arns.update(resource_role_arns)
        security_arns: Mapping[Tuple[AccountId, Optional[AccountPurpose]], Optional[Arn]] = {
            (account.id, account.purpose): account.get_assumable_role_arn_for_core_api(environment=self.environment)
            for account in self.account_store.query_accounts(
                account_purposes=AccountPurpose("security"), environments=frozenset(Environment)
            )
            if isinstance(account, SecurityAccount)
        }
        role_arns.update(security_arns)
        role_arns[
            (self.lambda_account_id, AccountPurpose("api"))
        ] = None  # In our api account we do not need to assume a role.
        return role_arns

    @staticmethod
    def get_athena_result_bucket_name(resource_account_id: AccountId, region: Region) -> str:
        """Return a generated s3 bucket name for the athena results."""
        return f"aws-athena-query-results-{resource_account_id}-{region.value}"

    @staticmethod
    def get_athena_output_location(resource_account_id: AccountId, region: Region, workgroup: str) -> str:
        """Return the s3 URL for an athena result bucket."""
        return f"s3://{Config.get_athena_result_bucket_name(resource_account_id, region)}/{workgroup}"

    def get_lake_formation_registration_role_arn(self, resource_account_id: AccountId, region: Region) -> Arn:
        """Return the arn of the role with which data locations are registered to lake formation."""
        role_name = f"{self.prefix}cdh-lakeformation-registration"
        return Arn(f"arn:{region.partition.value}:iam::{resource_account_id}:role/{role_name}")

    def get_s3_attribute_extractor_lambda_role_arn(self, resource_account_id: AccountId, region: Region) -> Arn:
        """Return the role arn of the s3 attribute extractor lambda."""
        role_name = f"{self.prefix}s3-attribute-extractor-lambda-{region.value}"
        return Arn(f"arn:{region.partition.value}:iam::{resource_account_id}:role/{role_name}")

    def get_s3_attribute_extractor_topic_arn(self, resource_account_id: AccountId, region: Region) -> Arn:
        """Return the arn of the sns topic that triggers the s3 attribute extractor lambda."""
        topic_name = f"{self.prefix}s3-attribute-extractor-s3-trigger-{region.value}"
        return Arn(f"arn:{region.partition.value}:sns:{region.value}:{resource_account_id}:{topic_name}")


@dataclass(frozen=True)
class ValidationContext:
    """Defines the context within which requests are validated."""

    config: Config
    current_hub: Optional[Hub]
