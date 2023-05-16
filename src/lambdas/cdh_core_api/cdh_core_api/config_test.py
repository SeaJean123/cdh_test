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
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from unittest.mock import Mock

from cdh_core_api.config import Config
from cdh_core_api.config import ValidationContext

from cdh_core.config.authorization_api import AuthApi
from cdh_core.config.authorization_api_test import build_auth_api
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.account_store_test import build_account_store
from cdh_core.entities.accounts_test import build_resource_account
from cdh_core.entities.accounts_test import build_security_account
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.environment import Environment
from cdh_core.enums.environment_test import build_environment
from cdh_core.enums.hubs import Hub
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder


def build_config(
    prefix: Optional[str] = None,
    lambda_account_id: Optional[AccountId] = None,
    environment: Optional[Environment] = None,
    disabled: bool = False,
    partition: Optional[Partition] = None,
    account_store: Optional[AccountStore] = None,
    use_authorization: bool = True,
    encryption_key: Optional[str] = None,
    result_page_size: int = 42,
) -> Config:
    region: Region = Region.preferred(partition or build_partition())
    dataset_topic_arn = build_arn(
        service="sns",
        resource="cdh-dataset-notification-topic",
        account_id=lambda_account_id or build_account_id(),
        region=region,
    )
    environment = environment or build_environment()
    return Config(
        lambda_account_id=lambda_account_id or build_account_id(),
        environment=environment,
        hubs=Hub.get_hubs(environment=environment),
        authorization_api_params=build_auth_api(use_authorization),
        prefix=prefix if prefix is not None else Builder.build_resource_name_prefix(),
        disabled=disabled,
        notification_topics=frozenset({dataset_topic_arn}),
        account_store=account_store if account_store else build_account_store(),
        encryption_key=encryption_key or Builder.build_random_string(),
        result_page_size=result_page_size,
    )


def build_validation_context(config: Optional[Config] = None, current_hub: Optional[Hub] = None) -> ValidationContext:
    config = build_config() if config is None else config
    return ValidationContext(config=config, current_hub=current_hub)


class TestConfig:
    def test_load_config_from_environment_and_context(self, monkeypatch: Any) -> None:
        invoked_function_arn = build_arn("lambda", account_id=build_account_id())
        prefix = Builder.build_resource_name_prefix()
        dataset_notification_topic = build_arn("sns")
        notification_topic = build_arn("sns")
        roles_sync_queue_url = Builder.build_random_url()
        authorization_url = Builder.build_random_url()
        authorization_cookie_name = "auth_cookie"
        account_store = build_account_store()
        env = build_environment()
        users_url = Builder.build_random_url()
        encryption_key = "encryption-key"
        result_page_size = 42
        monkeypatch.setenv("ENVIRONMENT", env.value)
        monkeypatch.setenv("RESOURCE_NAME_PREFIX", prefix)
        monkeypatch.setenv("DATASET_NOTIFICATION_TOPIC", str(dataset_notification_topic))
        monkeypatch.setenv("NOTIFICATION_TOPIC", str(notification_topic))
        monkeypatch.setenv("ROLES_SYNC_QUEUE_URL", roles_sync_queue_url)
        monkeypatch.setenv("AUTHORIZATION_API_URL", authorization_url)
        monkeypatch.setenv("AUTHORIZATION_API_COOKIE_NAME", authorization_cookie_name)
        monkeypatch.setenv("USERS_API_URL", users_url)
        monkeypatch.setenv("ENCRYPTION_KEY_NAME", encryption_key)
        monkeypatch.setenv("RESULT_PAGE_SIZE", str(result_page_size))

        config = Config.from_environment_and_context(
            Mock(invoked_function_arn=str(invoked_function_arn)), account_store
        )
        assert config == Config(
            lambda_account_id=invoked_function_arn.account_id,
            environment=env,
            hubs=Hub.get_hubs(environment=env),
            prefix=prefix,
            disabled=False,
            notification_topics=frozenset({dataset_notification_topic, notification_topic}),
            authorization_api_params=AuthApi(
                auth_url=authorization_url,
                cookie_name=authorization_cookie_name,
                users_url=users_url,
            ),
            account_store=account_store,
            encryption_key=encryption_key,
            result_page_size=result_page_size,
        )

    def test_disable_core_api(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("ENVIRONMENT", build_environment().value)
        monkeypatch.setenv("RESOURCE_NAME_PREFIX", "")
        monkeypatch.setenv("DISABLE_CORE_API", "TRUE")
        monkeypatch.setenv("DATASET_NOTIFICATION_TOPIC", str(build_arn("sns")))
        monkeypatch.setenv("NOTIFICATION_TOPIC", str(build_arn("sns")))
        monkeypatch.setenv("AUTHORIZATION_API_URL", "")
        monkeypatch.setenv("AUTHORIZATION_API_COOKIE_NAME", "")
        monkeypatch.setenv("USERS_API_URL", "")
        monkeypatch.setenv("ENCRYPTION_KEY_NAME", "")
        monkeypatch.setenv("RESULT_PAGE_SIZE", "0")

        config = Config.from_environment_and_context(
            Mock(invoked_function_arn=str(build_arn("lambda"))), account_store=build_account_store()
        )

        assert config.disabled is True

    def test_get_assumable_role_arns(self) -> None:
        prefix = Builder.build_resource_name_prefix()
        environment, other_environment = Builder.choose_without_repetition(set(Environment), 2)
        resource_accounts = [build_resource_account(environment=environment) for _ in range(3)]
        resource_accounts_other_env = [build_resource_account(environment=other_environment) for _ in range(3)]
        security_accounts = [build_security_account(environment=environment) for _ in range(3)]

        account_store = build_account_store([*resource_accounts, *resource_accounts_other_env, *security_accounts])
        config = build_config(environment=environment, prefix=prefix, account_store=account_store)

        expected_role_arns: Dict[Tuple[AccountId, Optional[AccountPurpose]], Optional[Arn]] = {
            (config.lambda_account_id, AccountPurpose("api")): None,
            **{
                (account.id, AccountPurpose("security")): account.get_assumable_role_arn_for_core_api(
                    environment=environment
                )
                for account in security_accounts
            },
            **{
                (account.id, AccountPurpose("resources")): account.get_assumable_role_arn_for_core_api(prefix)
                for account in resource_accounts
            },
        }
        assert config.get_assumable_role_arns() == expected_role_arns
