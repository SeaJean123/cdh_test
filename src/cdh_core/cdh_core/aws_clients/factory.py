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

from functools import lru_cache
from typing import Any
from typing import cast
from typing import Dict
from typing import Literal
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypedDict
from typing import TypeVar
from typing import Union

import boto3
import botocore.session
from botocore.config import Config
from botocore.credentials import AssumeRoleCredentialFetcher
from botocore.credentials import CredentialProvider
from botocore.credentials import CredentialResolver
from botocore.credentials import DeferredRefreshableCredentials

from cdh_core.aws_clients.athena_client import AthenaClient
from cdh_core.aws_clients.cloudwatch_client import CloudwatchClient
from cdh_core.aws_clients.events_client import EventsClient
from cdh_core.aws_clients.glue_client import GlueClient
from cdh_core.aws_clients.iam_client import IamClient
from cdh_core.aws_clients.kms_client import KmsClient
from cdh_core.aws_clients.lakeformation_client import LakeFormationClient
from cdh_core.aws_clients.lambda_client import LambdaClient
from cdh_core.aws_clients.logs_client import LogsClient
from cdh_core.aws_clients.ram_client import RamClient
from cdh_core.aws_clients.s3_client import S3Client
from cdh_core.aws_clients.ses_client import SesClient
from cdh_core.aws_clients.sfn_client import SfnClient
from cdh_core.aws_clients.sns_client import SnsClient
from cdh_core.aws_clients.sqs_client import SqsClient
from cdh_core.aws_clients.ssm_client import SsmClient
from cdh_core.entities.arn import Arn
from cdh_core.entities.credential import Credential
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id import AccountId

T = TypeVar(  # pylint: disable=invalid-name
    "T",
    bound=Union[
        AthenaClient,
        CloudwatchClient,
        EventsClient,
        GlueClient,
        IamClient,
        KmsClient,
        LakeFormationClient,
        LambdaClient,
        LogsClient,
        RamClient,
        S3Client,
        SesClient,
        SfnClient,
        SnsClient,
        SqsClient,
        SsmClient,
    ],
)


class BotocoreSessionWrapper:
    """Abstracts the Botocore sessions handling."""

    def __init__(self, config: Config, credentials: Optional[Credential] = None):
        self._config = config
        self._credentials = credentials
        self._session = botocore.session.Session()
        self._session.set_default_client_config(config)
        if credentials:
            self._session.set_credentials(
                access_key=credentials.access_key_id, secret_key=credentials.secret_access_key
            )

    def assume_role(self, role_arn: Arn, session_name: str = "session", duration: int = 3600) -> BotocoreSessionWrapper:
        """Assume the given role arn."""
        fetcher = AssumeRoleCredentialFetcher(
            client_creator=self._session.create_client,
            source_credentials=self._session.get_credentials(),
            role_arn=str(role_arn),
            extra_args={"DurationSeconds": duration, "RoleSessionName": session_name},
        )
        fetcher.fetch_credentials()
        role_session = BotocoreSessionWrapper(self._config)
        role_session.register_credential_provider(CredentialResolver([AssumeRoleProvider(fetcher)]))
        return role_session

    def register_credential_provider(self, component: CredentialResolver) -> None:
        """Set the CredentialResolver at the current session."""
        self._session.register_component("credential_provider", component)

    @property
    def boto3_session(self) -> boto3.session.Session:
        """Return a new boto3 session."""
        return boto3.session.Session(botocore_session=self._session)


class AssumeRoleProvider(CredentialProvider):
    """Provides roles via the CredentialFetcher."""

    def __init__(self, fetcher: AssumeRoleCredentialFetcher, session: Optional[botocore.session.Session] = None):
        super().__init__(session=session)
        self._fetcher = fetcher

    def load(self) -> DeferredRefreshableCredentials:
        """Load the credentials from their source & sets them on the object."""
        return DeferredRefreshableCredentials(self._fetcher.fetch_credentials, "assume-role")


class AssumeRoleSessionProvider:
    """Provides assumable Boto3 sessions."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        role_arns: Mapping[Tuple[AccountId, Optional[AccountPurpose]], Optional[Arn]],
        credentials: Optional[Dict[AccountId, Credential]] = None,
        proxies_per_region: Optional[Dict[Region, Dict[str, str]]] = None,
        additional_config: Optional[Dict[str, Any]] = None,
        base_role_arn: Optional[Arn] = None,
        assume_role_session_name: Optional[str] = None,
        region_for_default_session: Optional[Region] = None,
    ):
        self._additional_config = additional_config or {}
        self._proxies_per_region = proxies_per_region or {}
        self._assume_role_session_name = assume_role_session_name or "core-api"
        self._base_session = BotocoreSessionWrapper(
            config=Config(
                region_name=region_for_default_session.value if region_for_default_session else None,
                # botocore stub defines proxies as not being Optional but it is. If explicitly set to {} then
                # pre-determined proxy settings (eg. defined in HTTPS_PROXY env) are not used.
                proxies=self._proxies_per_region.get(region_for_default_session)
                if region_for_default_session
                else None,
                **self._additional_config,
            )
        )
        if base_role_arn:
            self._base_session = self._base_session.assume_role(
                role_arn=base_role_arn, session_name=self._assume_role_session_name
            )
        self._sessions_with_credentials = {
            account_id: BotocoreSessionWrapper(
                config=Config(
                    region_name=credential.region.value,
                    proxies=self._proxies_per_region.get(credential.region),
                    **self._additional_config,
                ),
                credentials=credential,
            )
            for account_id, credential in (credentials or {}).items()
        }
        self._role_arns = role_arns

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def get_session(
        self,
        account_id: AccountId,
        account_purpose: Optional[AccountPurpose],
        duration: int = 3600,
        session_name: Optional[str] = None,
    ) -> boto3.session.Session:
        """Create a Boto3 session for a given account, specified by its id and purpose, and duration."""
        return self.get_session_wrapped(account_id, account_purpose, duration, session_name).boto3_session

    @lru_cache()  # noqa: B019 # service instantiated only once per lambda runtime
    def get_session_wrapped(
        self,
        account_id: AccountId,
        account_purpose: Optional[AccountPurpose],
        duration: int = 3600,
        session_name: Optional[str] = None,
    ) -> BotocoreSessionWrapper:
        """Return a BotocoreSessionWrapper for the account with given id and purpose."""
        session = self._sessions_with_credentials.get(account_id, self._base_session)
        if (account_id, account_purpose) not in self._role_arns:
            raise UnknownAccountError(
                f"Cannot create a session for account {account_id!r} with purpose "
                f"{(account_purpose.value if account_purpose else account_purpose)!r} because no role was specified. "
                f"To create a session using the current role, assign `None` to your current account."
            )
        if role_arn := self._role_arns.get((account_id, account_purpose)):
            session = session.assume_role(
                role_arn=role_arn,
                session_name=session_name or self._assume_role_session_name,
                duration=duration,
            )
        return session


class _RetryDict(TypedDict, total=False):
    total_max_attempts: int
    max_attempts: int
    mode: Literal["legacy", "standard", "adaptive"]


class AwsClientFactory:
    """Creates and caches AWS clients."""

    def __init__(
        self,
        assume_role_session_provider: AssumeRoleSessionProvider,
        proxies_per_region: Optional[Dict[Any, Dict[str, str]]] = None,
        boto_read_timeout: Optional[int] = None,
    ):
        self._assume_role_session_provider = assume_role_session_provider
        self._clients: Dict[Tuple[str, str, Optional[AccountPurpose], Region], Any] = {}
        self._proxies_per_region = proxies_per_region or {}
        self._boto_read_timeout = boto_read_timeout or 10

    def athena_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region
    ) -> AthenaClient:
        """Create a athena client."""
        return self._get_client(
            service="athena",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=AthenaClient,
        )

    def cloudwatch_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region
    ) -> CloudwatchClient:
        """Create a cloudwatch client."""
        return self._get_client(
            service="cloudwatch",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=CloudwatchClient,
        )

    def events_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region
    ) -> EventsClient:
        """Create a events client."""
        return self._get_client(
            service="events",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=EventsClient,
        )

    def glue_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region
    ) -> GlueClient:
        """Create a glue client."""
        return self._get_client(
            service="glue",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=GlueClient,
        )

    def iam_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], partition: Partition
    ) -> IamClient:
        """Create an IAM client."""
        return self._get_client(
            service="iam",
            account_id=account_id,
            account_purpose=account_purpose,
            region=Region.preferred(partition),
            client_class=IamClient,
        )

    def kms_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> KmsClient:
        """Create a KMS client."""
        return self._get_client(
            service="kms", account_id=account_id, account_purpose=account_purpose, region=region, client_class=KmsClient
        )

    def lake_formation_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region
    ) -> LakeFormationClient:
        """Create a lake formation client."""
        return self._get_client(
            service="lakeformation",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=LakeFormationClient,
        )

    def lambda_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region
    ) -> LambdaClient:
        """Create a lambda client."""
        return self._get_client(
            service="lambda",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=LambdaClient,
        )

    def logs_client(
        self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region
    ) -> LogsClient:
        """Create a logs client."""
        return self._get_client(
            service="logs",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=LogsClient,
        )

    def ram_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> RamClient:
        """Create a RAM client."""
        return self._get_client(
            service="ram", account_id=account_id, account_purpose=account_purpose, region=region, client_class=RamClient
        )

    def s3_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> S3Client:
        """Create a S3 client."""
        return self._get_client(
            service="s3", account_id=account_id, account_purpose=account_purpose, region=region, client_class=S3Client
        )

    def ses_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> SesClient:
        """Create a SES client."""
        return self._get_client(
            service="ses", account_id=account_id, account_purpose=account_purpose, region=region, client_class=SesClient
        )

    def sfn_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> SfnClient:
        """Create a SFN client."""
        return self._get_client(
            service="stepfunctions",
            account_id=account_id,
            account_purpose=account_purpose,
            region=region,
            client_class=SfnClient,
        )

    def sns_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> SnsClient:
        """Create a SNS client."""
        return self._get_client(
            service="sns", account_id=account_id, account_purpose=account_purpose, region=region, client_class=SnsClient
        )

    def sqs_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> SqsClient:
        """Create a SQS client."""
        return self._get_client(
            service="sqs", account_id=account_id, account_purpose=account_purpose, region=region, client_class=SqsClient
        )

    def ssm_client(self, account_id: AccountId, account_purpose: Optional[AccountPurpose], region: Region) -> SsmClient:
        """Create a SSM client."""
        return self._get_client(
            service="ssm", account_id=account_id, account_purpose=account_purpose, region=region, client_class=SsmClient
        )

    def _get_client(  # pylint: disable=too-many-arguments
        self,
        service: str,
        account_id: AccountId,
        account_purpose: Optional[AccountPurpose],
        region: Region,
        client_class: Type[T],
    ) -> T:
        key = (service, account_id, account_purpose, region)
        if key not in self._clients:
            if issubclass(client_class, IamClient):
                self._clients[key] = client_class(
                    self.create_client(
                        service=service, region=region, account_id=account_id, account_purpose=account_purpose
                    ),
                    account_id=account_id,
                    partition=region.partition,
                )
            elif issubclass(client_class, GlueClient):
                self._clients[key] = client_class(
                    self.create_client(
                        service=service, region=region, account_id=account_id, account_purpose=account_purpose
                    )
                )
            else:
                self._clients[key] = client_class(  # type: ignore
                    self.create_client(
                        service=service, region=region, account_id=account_id, account_purpose=account_purpose
                    )
                )
        return cast(T, self._clients[key])

    def create_client(  # pylint: disable=too-many-arguments
        self,
        service: str,
        region: Region,
        account_id: Optional[AccountId] = None,
        account_purpose: Optional[AccountPurpose] = None,
        session: Optional[boto3.session.Session] = None,
    ) -> Any:
        """Create the boto3 client for a given service.

        One can either specify the account_id (together with an optional account_purpose) in which the session should
        be created or pass a boto3 session.
        """
        if ((account_id or account_purpose) and session) or (account_id is None and session is None):
            raise ValueError("Please specifier either the account_id with an optional account_purpose or the session.")

        session = session or self._assume_role_session_provider.get_session(account_id, account_purpose)  # type: ignore
        # we allow more retries for the iam client since throttling is a real issue especially with the
        # ManageRolePolicy operation. Standard retry mode includes exponential backoff which does only really comes into
        # effect on later retries
        retry_config = _RetryDict(mode="standard", total_max_attempts=6 if service in ["iam", "cloudwatch"] else 4)
        return session.client(  # type: ignore
            service_name=service,
            region_name=region.value,
            config=Config(
                connect_timeout=4,
                read_timeout=self._boto_read_timeout,
                retries=retry_config,
                proxies=self._proxies_per_region.get(region),
            ),
        )


class UnknownAccountError(Exception):
    """Signals the used account id is not within the known role arns."""
