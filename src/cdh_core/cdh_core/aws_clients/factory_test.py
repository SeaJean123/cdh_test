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
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Mapping
from typing import Optional
from typing import Tuple
from unittest.mock import Mock
from unittest.mock import patch

import boto3
import pytest
from botocore.config import Config

from cdh_core.aws_clients import factory
from cdh_core.aws_clients.factory import AssumeRoleSessionProvider
from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.aws_clients.factory import BotocoreSessionWrapper
from cdh_core.aws_clients.factory import UnknownAccountError
from cdh_core.aws_clients.glue_client import GlueClient
from cdh_core.aws_clients.iam_client import IamClient
from cdh_core.aws_clients.s3_client import S3Client
from cdh_core.config.config_file import ConfigFile
from cdh_core.config.config_file_test import CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS
from cdh_core.entities.arn import Arn
from cdh_core.entities.arn_test import build_arn
from cdh_core.entities.arn_test import build_role_arn
from cdh_core.entities.credential import Credential
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.accounts_test import build_account_purpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_partition
from cdh_core.enums.aws_test import build_region
from cdh_core.primitives.account_id import AccountId
from cdh_core.primitives.account_id_test import build_account_id
from cdh_core_dev_tools.testing.builder import Builder

_ADMIN = build_account_id()
_ADMIN_PURPOSE = random.choice([build_account_purpose(), None])
_USER = build_account_id()
_USER_PURPOSE = random.choice([build_account_purpose(), None])
_ROLE_ARNS: Mapping[Tuple[AccountId, Optional[AccountPurpose]], Optional[Arn]] = {
    (_ADMIN, _ADMIN_PURPOSE): build_arn("iam", "admin", account_id=_ADMIN),
    (_USER, _USER_PURPOSE): build_arn("iam", "user", account_id=_USER),
}


# pylint: disable=unused-argument
@pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
@patch.object(factory, "BotocoreSessionWrapper")
class TestAssumeRoleProvider:
    def test_unknown_account_raises(self, session_wrapper_class: Mock, mock_config_file: ConfigFile) -> None:
        assume_role_session_provider = AssumeRoleSessionProvider(role_arns=_ROLE_ARNS)

        with pytest.raises(UnknownAccountError):
            assume_role_session_provider.get_session(build_account_id(), build_account_purpose())

    def test_get_target_role(self, session_wrapper_class: Mock, mock_config_file: ConfigFile) -> None:
        base_session = Mock()
        session_with_role = Mock()
        base_session.assume_role.return_value = session_with_role
        session_wrapper_class.side_effect = [base_session, Exception("No further sessions should be called")]
        session_name = Builder.build_random_string()
        region = build_region()
        proxies = {"http": "my_proxy"}
        assume_role_session_provider = AssumeRoleSessionProvider(
            role_arns=_ROLE_ARNS,
            region_for_default_session=region,
            proxies_per_region={region: proxies},
            additional_config={"read_timeout": 42},
            assume_role_session_name=session_name,
        )

        assert (
            assume_role_session_provider.get_session(_ADMIN, _ADMIN_PURPOSE, duration=1234)
            == session_with_role.boto3_session
        )
        config = session_wrapper_class.call_args.kwargs["config"]
        assert config.read_timeout == 42
        assert config.region_name == region.value
        assert config.proxies == proxies
        base_session.assume_role.assert_called_once_with(
            role_arn=_ROLE_ARNS[(_ADMIN, _ADMIN_PURPOSE)], session_name=session_name, duration=1234
        )

    def test_get_target_role_via_base_role(self, session_wrapper_class: Mock, mock_config_file: ConfigFile) -> None:
        base_session = Mock()
        session_with_base_role = Mock()
        session_with_target_role = Mock()
        base_session.assume_role.return_value = session_with_base_role
        session_with_base_role.assume_role.return_value = session_with_target_role
        session_wrapper_class.side_effect = [base_session, Exception("No further sessions should be called")]
        base_session_name = Builder.build_random_string()
        target_session_name = Builder.build_random_string()
        base_role_arn = build_role_arn()
        assume_role_session_provider = AssumeRoleSessionProvider(
            role_arns=_ROLE_ARNS, base_role_arn=base_role_arn, assume_role_session_name=base_session_name
        )

        assert (
            assume_role_session_provider.get_session(
                _ADMIN, _ADMIN_PURPOSE, duration=1234, session_name=target_session_name
            )
            == session_with_target_role.boto3_session
        )
        base_session.assume_role.assert_called_once_with(role_arn=base_role_arn, session_name=base_session_name)
        session_with_base_role.assume_role.assert_called_once_with(
            role_arn=_ROLE_ARNS[(_ADMIN, _ADMIN_PURPOSE)], session_name=target_session_name, duration=1234
        )

    def test_get_default_session(self, session_wrapper_class: Mock, mock_config_file: ConfigFile) -> None:
        base_session = Mock()
        session_wrapper_class.side_effect = [base_session, Exception("No further sessions should be called")]
        assume_role_session_provider = AssumeRoleSessionProvider(role_arns={(_ADMIN, _ADMIN_PURPOSE): None})

        assert assume_role_session_provider.get_session(_ADMIN, _ADMIN_PURPOSE) == base_session.boto3_session

    def test_get_base_role(self, session_wrapper_class: Mock, mock_config_file: ConfigFile) -> None:
        base_session = Mock()
        base_session_with_role = Mock()
        base_session.assume_role.return_value = base_session_with_role
        session_wrapper_class.side_effect = [base_session, Exception("No further sessions should be called")]
        session_name = Builder.build_random_string()
        base_role_arn = build_role_arn()
        assume_role_session_provider = AssumeRoleSessionProvider(
            role_arns={(_ADMIN, _ADMIN_PURPOSE): None},
            base_role_arn=base_role_arn,
            assume_role_session_name=session_name,
        )

        assert assume_role_session_provider.get_session(_ADMIN, _ADMIN_PURPOSE) == base_session_with_role.boto3_session
        base_session.assume_role.assert_called_once_with(role_arn=base_role_arn, session_name=session_name)

    def test_get_with_credentials(self, session_wrapper_class: Mock, mock_config_file: ConfigFile) -> None:
        for partition in Partition:  # temporary workaround for #64 if fixed use pytest.mark.parametrize
            default_session = Mock()
            session_with_credentials = Mock()
            session_wrapper_class.side_effect = [
                default_session,
                session_with_credentials,
                Exception("No further sessions should be called"),
            ]
            credential = Credential(
                access_key_id=Builder.build_random_string(),
                secret_access_key=Builder.build_random_string(),
                partition=partition,
            )
            proxies = {"http": "my_proxy"}

            assume_role_session_provider = AssumeRoleSessionProvider(
                role_arns={(_ADMIN, _ADMIN_PURPOSE): None},
                credentials={_ADMIN: credential},
                proxies_per_region={credential.region: proxies},
                additional_config={"read_timeout": 42},
            )
            assert (
                assume_role_session_provider.get_session(_ADMIN, _ADMIN_PURPOSE)
                == session_with_credentials.boto3_session
            )
            first_call, second_call = session_wrapper_class.call_args_list[-2], session_wrapper_class.call_args_list[-1]
            assert first_call.kwargs["config"].read_timeout == 42
            assert first_call.kwargs["config"].region_name is None
            assert first_call.kwargs.get("credentials") is None
            assert second_call.kwargs["config"].read_timeout == 42
            assert second_call.kwargs["config"].region_name == credential.region.value
            assert second_call.kwargs["config"].proxies == proxies
            assert second_call.kwargs["credentials"] == credential

    def test_get_target_role_via_credentials(self, session_wrapper_class: Mock, mock_config_file: ConfigFile) -> None:
        for partition in Partition:  # temporary workaround for #64 if fixed use pytest.mark.parametrize
            default_session = Mock()
            session_with_credentials = Mock()
            session_with_credentials_assumed_role = Mock()
            session_with_credentials.assume_role.return_value = session_with_credentials_assumed_role
            session_wrapper_class.side_effect = [
                default_session,
                session_with_credentials,
                Exception("No further sessions should be called"),
            ]
            credential = Credential(
                access_key_id=Builder.build_random_string(),
                secret_access_key=Builder.build_random_string(),
                partition=partition,
            )
            session_name = Builder.build_random_string()

            assume_role_session_provider = AssumeRoleSessionProvider(
                role_arns=_ROLE_ARNS, credentials={_ADMIN: credential}, assume_role_session_name=session_name
            )

            first_call, second_call = session_wrapper_class.call_args_list[-2], session_wrapper_class.call_args_list[-1]
            assert first_call.kwargs["config"].region_name is None
            assert first_call.kwargs.get("credentials") is None
            assert second_call.kwargs["config"].region_name == credential.region.value
            assert second_call.kwargs["credentials"] == credential
            assert (
                assume_role_session_provider.get_session(_ADMIN, _ADMIN_PURPOSE, duration=1234)
                == session_with_credentials_assumed_role.boto3_session
            )
            session_with_credentials.assume_role.assert_called_once_with(
                role_arn=_ROLE_ARNS[(_ADMIN, _ADMIN_PURPOSE)], session_name=session_name, duration=1234
            )


# pylint: disable=unused-argument, protected-access
@pytest.mark.usefixtures("mock_iam", "mock_s3")
@pytest.mark.parametrize("mock_config_file", [CONFIG_FILE_MULTIPLE_PARTITIONS_ENVIRONMENTS_HUBS], indirect=True)
class TestAwsClientFactory:
    @pytest.fixture(autouse=True)
    def service_setup(self, mock_config_file: ConfigFile) -> None:  # pylint: disable=unused-argument
        assume_role_session_provider = Mock(spec=AssumeRoleSessionProvider)
        assume_role_session_provider.get_session.return_value = boto3.session.Session()
        self.factory = AwsClientFactory(assume_role_session_provider)
        self.partition = build_partition()
        self.region = build_region(self.partition)

    def test_cache_clients(self) -> None:
        client = self.factory.s3_client(_USER, _USER_PURPOSE, self.region)
        assert self.factory.s3_client(_USER, _USER_PURPOSE, self.region) is client

    def test_iam_client(self) -> None:
        assert isinstance(self.factory.iam_client(_USER, _USER_PURPOSE, self.partition), IamClient)
        assert isinstance(self.factory.s3_client(_USER, _USER_PURPOSE, self.region), S3Client)

    def test_glue_client(self) -> None:
        assert isinstance(self.factory.glue_client(_USER, _USER_PURPOSE, self.region), GlueClient)

    def test_client_service_is_correct(self) -> None:
        client = self.factory.s3_client(_USER, _USER_PURPOSE, self.region)
        assert "s3." in client._client.meta.endpoint_url

    def test_return_different_clients_for_different_credentials(self) -> None:
        client = self.factory.s3_client(_USER, _USER_PURPOSE, self.region)
        assert self.factory.s3_client(_ADMIN, _ADMIN_PURPOSE, self.region) is not client

    def test_client_region_is_correct(self) -> None:
        for region in Region:
            assert self.factory.s3_client(_USER, _USER_PURPOSE, region)._client.meta.region_name == region.value

    @pytest.mark.parametrize(
        "account_id,account_purpose,session",
        [
            (AccountId("111111111111"), AccountPurpose("api"), None),
            (AccountId("111111111111"), None, None),
            (None, None, Mock(boto3.session.Session)),
        ],
    )
    def test_create_client_successful(
        self,
        account_id: Optional[AccountId],
        account_purpose: Optional[AccountPurpose],
        session: Optional[boto3.session.Session],
    ) -> None:
        self.factory.create_client(
            service="s3", region=self.region, account_id=account_id, account_purpose=account_purpose, session=session
        )

    @pytest.mark.parametrize(
        "account_id,account_purpose,session",
        [
            (None, None, None),
            (None, AccountPurpose("api"), None),
            (AccountId("111111111111"), None, Mock(boto3.session.Session)),
            (None, AccountPurpose("api"), Mock(boto3.session.Session)),
            (AccountId("111111111111"), AccountPurpose("api"), Mock(boto3.session.Session)),
        ],
    )
    def test_create_client_fails_if_forbidden_parameter_combination(
        self,
        account_id: Optional[AccountId],
        account_purpose: Optional[AccountPurpose],
        session: Optional[boto3.session.Session],
    ) -> None:
        expected_error_message = (
            "Please specifier either the account_id with an optional account_purpose or the session."
        )
        with pytest.raises(Exception) as exc_info:
            self.factory.create_client(
                service="s3",
                region=self.region,
                account_id=account_id,
                account_purpose=account_purpose,
                session=session,
            )
        assert exc_info
        assert expected_error_message in str(exc_info.value)


@patch("botocore.credentials.AssumeRoleCredentialFetcher._create_client")
class TestBotocoreSessionWrapper:
    def setup_method(self) -> None:
        self._botocore_session_wrapper = BotocoreSessionWrapper(Config())

    def test_assume_role(self, credential_fetcher_client_method: Mock) -> None:
        sts_client = Mock()
        credential_fetcher_client_method.return_value = sts_client
        sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "1",
                "SecretAccessKey": "2",
                "SessionToken": "3",
                "Expiration": datetime.now(tz=timezone.utc) + timedelta(hours=1),
            }
        }
        role_arn = build_role_arn()

        result_session = self._botocore_session_wrapper.assume_role(role_arn, "session", 1200)

        credentials = result_session._session.get_credentials()  # pylint: disable=protected-access
        assert credentials.access_key == "1"
        assert credentials.secret_key == "2"
        assert credentials.token == "3"
        sts_client.assume_role.assert_called_with(
            RoleArn=str(role_arn), RoleSessionName="session", DurationSeconds=1200
        )
