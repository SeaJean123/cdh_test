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
# pylint: disable=unnecessary-lambda
import dataclasses
import datetime
import logging
import os
from contextlib import suppress
from typing import Any
from typing import Callable
from typing import Dict
from typing import get_origin
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

from aws_xray_sdk.core import xray_recorder
from cdh_core_api.api.openapi_spec.openapi import Handler
from cdh_core_api.api.openapi_spec.openapi import OpenApiSpecCollector
from cdh_core_api.api.route_collection import AnyHandler
from cdh_core_api.api.router import Router
from cdh_core_api.api.validation import SchemaValidator
from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.bodies.resources import NewGlueSyncBody
from cdh_core_api.catalog.accounts_table import AccountsTable
from cdh_core_api.catalog.datasets_table import DatasetsTable
from cdh_core_api.catalog.filter_packages_table import FilterPackagesTable
from cdh_core_api.catalog.resource_table import ResourcesTable
from cdh_core_api.config import Config
from cdh_core_api.config import ValidationContext
from cdh_core_api.jwt_helper import extract_jwt_user_id
from cdh_core_api.jwt_helper import get_jwt
from cdh_core_api.services.account_environment_verifier import AccountEnvironmentVerifier
from cdh_core_api.services.account_id_verifier import AccountIdVerifier
from cdh_core_api.services.account_manager import AccountManager
from cdh_core_api.services.account_validator import AccountValidator
from cdh_core_api.services.api_info_manager import ApiInfoManager
from cdh_core_api.services.authorization_api import AuthorizationApi
from cdh_core_api.services.authorizer import Authorizer
from cdh_core_api.services.data_explorer import DataExplorerSync
from cdh_core_api.services.dataset_manager import DatasetManager
from cdh_core_api.services.dataset_participants_manager import DatasetParticipantsManager
from cdh_core_api.services.dataset_permissions_manager import DatasetPermissionsManager
from cdh_core_api.services.dataset_permissions_validator import DatasetPermissionsValidator
from cdh_core_api.services.dataset_validator import DatasetValidator
from cdh_core_api.services.encryption_service import EncryptionService
from cdh_core_api.services.full_vision_check import FullVisionCheck
from cdh_core_api.services.glue_resource_manager import GlueResourceManager
from cdh_core_api.services.kms_service import KmsService
from cdh_core_api.services.lake_formation_service import LakeFormationService
from cdh_core_api.services.lock_service import LockService
from cdh_core_api.services.metadata_role_assumer import AssumableAccountSpec
from cdh_core_api.services.metadata_role_assumer import MetadataRoleAssumer
from cdh_core_api.services.pagination_service import PaginationService
from cdh_core_api.services.phone_book import PhoneBook
from cdh_core_api.services.resource_link import ResourceLink
from cdh_core_api.services.resource_payload_builder import ResourcePayloadBuilder
from cdh_core_api.services.resource_validator import ResourceValidator
from cdh_core_api.services.response_account_builder import ResponseAccountBuilder
from cdh_core_api.services.response_dataset_builder import ResponseDatasetBuilder
from cdh_core_api.services.s3_bucket_manager import S3BucketManager
from cdh_core_api.services.s3_resource_manager import S3ResourceManager
from cdh_core_api.services.sns_publisher import SnsPublisher
from cdh_core_api.services.sns_topic_manager import SnsTopicManager
from cdh_core_api.services.users_api import UsersApi
from cdh_core_api.services.visibility_check import VisibilityCheck
from cdh_core_api.services.visible_data_loader import VisibleDataLoader
from cdh_core_api.validation.base import validate_hub
from cryptography.fernet import Fernet

from cdh_core.aws_clients.cloudwatch_log_writer import CloudwatchLogWriter
from cdh_core.aws_clients.factory import AssumeRoleSessionProvider
from cdh_core.aws_clients.factory import AwsClientFactory
from cdh_core.config.config_file_loader import ConfigFileLoader
from cdh_core.entities.account_store import AccountStore
from cdh_core.entities.accounts import Account
from cdh_core.entities.lambda_context import LambdaContext
from cdh_core.entities.request import Request
from cdh_core.entities.request import RequesterIdentity
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.enums.http import HttpVerb
from cdh_core.log.log_safe import log_safe
from cdh_core.log.logger import configure_logging
from cdh_core.manager.dependency_manager import DependencyManager
from cdh_core.services.external_api import ApiSessionBuilder
from cdh_core.services.external_api import ExternalApiSession
from cdh_core.services.external_api import get_retry_config
from cdh_core.services.external_api import UnusableSession

__all__ = ["coreapi", "openapi"]

LOG = logging.getLogger(__name__)


class Application:
    """This class contains the AWS lambda entry and represents the API."""

    def __init__(self, openapi_collector: OpenApiSpecCollector):
        self._dependency_manager = DependencyManager()
        self.dependency = self._dependency_manager.register
        self._router = Router(set(ConfigFileLoader.get_config().stage_by_origin.instances), self._dependency_manager)
        self._configured = False
        self._registered_dependencies = False
        self._openapi = openapi_collector

    def _configure(self, context: LambdaContext) -> None:
        if self._configured:
            return
        configure_logging(__package__)
        logging.getLogger("cdh_core_utils").setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        self._register_dependencies(context)

        self._dependency_manager.validate_dependencies()
        deps = self._dependency_manager.build_forever_dependencies()
        self._router.set_audit_logger(
            CloudwatchLogWriter(
                deps["aws"].logs_client(
                    account_id=deps["config"].lambda_account_id,
                    account_purpose=AccountPurpose("api"),
                    region=Region(os.environ["AWS_REGION"]),
                ),
                f"{os.environ.get('RESOURCE_NAME_PREFIX', '')}cdh-audit-log",
                datetime.datetime.now().strftime("%Y/%m/%d"),
            )
        )
        self._configured = True

    def _register_dependencies(self, context: LambdaContext) -> None:
        if self._registered_dependencies:
            return
        self.dependency("context", DependencyManager.TimeToLive.FOREVER)(lambda: context)
        self.dependency("current_hub", DependencyManager.TimeToLive.PER_REQUEST)(
            lambda config, request: validate_hub(ValidationContext(config, None), request.path_params["hub"])
            if "hub" in request.path_params
            else None
        )
        self.dependency("validation_context", DependencyManager.TimeToLive.PER_REQUEST)(
            lambda config, current_hub: ValidationContext(config, current_hub)
        )
        self.dependency("body", DependencyManager.TimeToLive.PER_REQUEST)(
            lambda validation_context, request, body_annotation: SchemaValidator(
                body_annotation, context=validation_context
            )(request.body)
            if body_annotation
            else None
        )
        self.dependency("path", DependencyManager.TimeToLive.PER_REQUEST)(
            lambda validation_context, request, path_annotation: SchemaValidator(
                path_annotation, context=validation_context
            )(request.path_params)
            if path_annotation
            else None
        )

        Query = TypeVar("Query")

        @self.dependency("query", DependencyManager.TimeToLive.PER_REQUEST)
        def build_query(validation_context: Any, request: Request, query_annotation: Type[Query]) -> Optional[Query]:
            def _get_query_params() -> Dict[str, Union[str, List[str]]]:
                query_params: Dict[str, Union[str, List[str]]] = dict(request.query_params or {})
                assert dataclasses.is_dataclass(query_annotation)
                for dataclass_field in dataclasses.fields(query_annotation):
                    with suppress(KeyError):
                        list_values = request.query_params_multi_value[dataclass_field.name]
                        if get_origin(dataclass_field.type) is list or len(list_values) > 1:
                            query_params[dataclass_field.name] = list_values
                return query_params

            return (
                SchemaValidator(query_annotation, context=validation_context)(_get_query_params())
                if query_annotation
                else None
            )

        self._registered_dependencies = True
        return

    def route(self, path: str, methods: List[str], force: bool = False) -> Callable[[Handler], Handler]:
        """Register a new route."""
        assert len(methods) == 1
        http_verb = HttpVerb[methods[0].upper()]

        def decorator(handler: Handler) -> Handler:
            # Apply both decorators to register the route with Chalice and our OpenApiSpecCollector.
            self._router.route(path, http_verb, force)(handler)
            self._openapi.route(path, http_verb, force)(handler)
            return handler

        return decorator

    def handle_request(self, event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
        """Handle the AWS lambda request."""
        xray_recorder.begin_subsegment("configure and build_forever_dependencies")
        self._configure(context=context)
        deps = self._dependency_manager.build_forever_dependencies()
        deps["lock_service"].set_request_id(request_id=context.aws_request_id)
        xray_recorder.end_subsegment()
        response = self._router.handle_request(event, context, deps["config"])
        if deps["lock_service"].lock_count != 0:
            LOG.error(
                f"Lock service holds still {deps['lock_service'].lock_count} "
                f"locks for request id {context.aws_request_id}"
            )
        return response

    def get_route(self, path: str, method: HttpVerb) -> AnyHandler:
        """Return the route handler for the path/method combination."""
        return self._router.get_route(path=path, method=method)


openapi = OpenApiSpecCollector()
coreapi: Application = Application(openapi)
coreapi.dependency("account_store", DependencyManager.TimeToLive.FOREVER)(lambda: AccountStore())
coreapi.dependency("config", DependencyManager.TimeToLive.FOREVER)(
    lambda account_store, context: Config.from_environment_and_context(context, account_store)
)
coreapi.dependency("assume_role_session_provider", DependencyManager.TimeToLive.FOREVER)(
    lambda config: AssumeRoleSessionProvider(
        role_arns=config.get_assumable_role_arns(),
    )
)
coreapi.dependency("aws", DependencyManager.TimeToLive.FOREVER)(
    lambda assume_role_session_provider: AwsClientFactory(assume_role_session_provider=assume_role_session_provider)
)
coreapi.dependency("metadata_role_assumer", DependencyManager.TimeToLive.FOREVER)(
    lambda assume_role_session_provider, account_store, config: MetadataRoleAssumer(
        assume_role_session_provider, account_store, AssumableAccountSpec, config
    )
)
coreapi.dependency("account_environment_verifier", DependencyManager.TimeToLive.FOREVER)(
    lambda metadata_role_assumer: AccountEnvironmentVerifier(metadata_role_assumer)
)
coreapi.dependency("api_info_manager", DependencyManager.TimeToLive.FOREVER)(
    lambda config: ApiInfoManager(config, openapi)
)
coreapi.dependency("s3_bucket_manager", DependencyManager.TimeToLive.FOREVER)(
    lambda config, aws: S3BucketManager(config, aws)
)
coreapi.dependency("lock_service", DependencyManager.TimeToLive.FOREVER)(lambda config: LockService(config))
coreapi.dependency("accounts_table", DependencyManager.TimeToLive.FOREVER)(lambda config: AccountsTable(config.prefix))
coreapi.dependency("filter_packages_table", DependencyManager.TimeToLive.FOREVER)(
    lambda config: FilterPackagesTable(config.prefix)
)
coreapi.dependency("resources_table", DependencyManager.TimeToLive.FOREVER)(
    lambda config: ResourcesTable(config.prefix)
)
coreapi.dependency("resource_link", DependencyManager.TimeToLive.FOREVER)(
    lambda aws, accounts_table, metadata_role_assumer: ResourceLink(aws, accounts_table, metadata_role_assumer)
)
coreapi.dependency("lake_formation_service", DependencyManager.TimeToLive.FOREVER)(
    lambda aws, config: LakeFormationService(aws, config)
)
coreapi.dependency("kms_service", DependencyManager.TimeToLive.FOREVER)(
    lambda aws, config, lock_service: KmsService(config, aws, lock_service)
)
coreapi.dependency("datasets_table", DependencyManager.TimeToLive.FOREVER)(lambda config: DatasetsTable(config.prefix))
coreapi.dependency("account_id_verifier", DependencyManager.TimeToLive.FOREVER)(
    lambda config, aws: AccountIdVerifier(config, aws)
)
coreapi.dependency("response_account_builder", DependencyManager.TimeToLive.FOREVER)(lambda: ResponseAccountBuilder())
coreapi.dependency("phone_book", DependencyManager.TimeToLive.FOREVER)(lambda config: PhoneBook(config))
coreapi.dependency("authorization_api_session", DependencyManager.TimeToLive.FOREVER)(
    lambda config: ExternalApiSession(
        request_session_factory=ApiSessionBuilder(
            api_name="authorization-api",
            retry_config=get_retry_config(
                retries=2, backoff_factor=0.2, retry_on_post=True  # The auth API only has non-mutating POST endpoints
            ),
        ).get_session
        if config.using_authorization_api
        else UnusableSession,
        api_url=config.authorization_api_params.auth_url,
        timeout=(0.4, 1.0),
    )
)
coreapi.dependency("jwt", DependencyManager.TimeToLive.PER_REQUEST)(lambda request, config: get_jwt(request, config))
coreapi.dependency("requester_identity", DependencyManager.TimeToLive.PER_REQUEST)(
    lambda request, jwt: RequesterIdentity(
        arn=request.requester_arn, user=request.user, jwt_user_id=extract_jwt_user_id(jwt)
    )
)
coreapi.dependency("authorization_api", DependencyManager.TimeToLive.PER_REQUEST)(
    lambda authorization_api_session, request, jwt: AuthorizationApi(
        authorization_api_session, request.requester_arn, jwt
    )
)
coreapi.dependency("full_vision_check", DependencyManager.TimeToLive.FOREVER)(
    lambda config, phone_book: FullVisionCheck(config, phone_book)
)
coreapi.dependency("visibility_check", DependencyManager.TimeToLive.PER_REQUEST)(
    lambda authorization_api, request, full_vision_check, config: VisibilityCheck(
        full_vision_check=full_vision_check,
        authorization_api=authorization_api,
        requester=request.requester_arn,
        config=config,
    )
)

coreapi.dependency("sns_client", DependencyManager.TimeToLive.FOREVER)(
    lambda config, aws: aws.sns_client(
        account_id=config.lambda_account_id,
        account_purpose=AccountPurpose("api"),
        region=Region(os.environ["AWS_REGION"]),
    )
)
coreapi.dependency("ssm_client", DependencyManager.TimeToLive.FOREVER)(
    lambda config, aws: aws.ssm_client(
        account_id=config.lambda_account_id,
        account_purpose=AccountPurpose("api"),
        region=Region(os.environ["AWS_REGION"]),
    )
)
coreapi.dependency("sns_publisher", DependencyManager.TimeToLive.PER_REQUEST)(
    lambda config, sns_client, requester_identity: SnsPublisher(
        sns_client=sns_client, topic_arns=config.notification_topics, requester_identity=requester_identity
    )
)

coreapi.dependency("users_api_session", DependencyManager.TimeToLive.FOREVER)(
    lambda config: ExternalApiSession(
        request_session_factory=ApiSessionBuilder(
            api_name="users-api",
            retry_config=get_retry_config(
                retries=4,
                backoff_factor=0.2,
                retry_on_post=True,  # The only users API POST endpoint we currently use is idempotent
            ),
        ).get_session,
        api_url=config.authorization_api_params.users_url,
        timeout=(0.4, 2.0),
    )
)

coreapi.dependency("users_api", DependencyManager.TimeToLive.FOREVER)(
    lambda users_api_session: UsersApi(session=users_api_session)
)


@coreapi.dependency("visible_data_loader", DependencyManager.TimeToLive.PER_REQUEST)
def _build_visible_data_loader(
    accounts_table: AccountsTable,
    resources_table: ResourcesTable,
    datasets_table: DatasetsTable,
    filter_packages_table: FilterPackagesTable,
    visibility_check: VisibilityCheck,
) -> VisibleDataLoader[Account, S3Resource, GlueSyncResource]:
    return VisibleDataLoader(
        accounts_table=accounts_table,
        resources_table=resources_table,
        datasets_table=datasets_table,
        filter_packages_table=filter_packages_table,
        visibility_check=visibility_check,
    )


@coreapi.dependency("account_manager", DependencyManager.TimeToLive.PER_REQUEST)
def _build_account_manager(
    accounts_table: AccountsTable,
    lock_service: LockService,
    dataset_permissions_manager: DatasetPermissionsManager[Account, S3Resource, GlueSyncResource],
) -> AccountManager[Account, UpdateAccountBody]:
    return AccountManager(
        accounts_table=accounts_table,
        lock_service=lock_service,
        dataset_permissions_manager=dataset_permissions_manager,
    )


@coreapi.dependency("account_validator", DependencyManager.TimeToLive.PER_REQUEST)
def _build_account_validator(  # pylint: disable=too-many-arguments
    account_environment_verifier: AccountEnvironmentVerifier[Account, UpdateAccountBody],
    account_id_verifier: AccountIdVerifier,
    accounts_table: AccountsTable,
    datasets_table: DatasetsTable,
    resources_table: ResourcesTable,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> AccountValidator[Account, UpdateAccountBody]:
    return AccountValidator(
        account_environment_verifier=account_environment_verifier,
        account_id_verifier=account_id_verifier,
        accounts_table=accounts_table,
        assumable_account_spec_cls=AssumableAccountSpec,
        datasets_table=datasets_table,
        resources_table=resources_table,
        visible_data_loader=visible_data_loader,
    )


coreapi.dependency("response_dataset_builder", DependencyManager.TimeToLive.PER_REQUEST)(
    lambda config, authorization_api, phone_book: ResponseDatasetBuilder(
        config=config,
        authorization_api=authorization_api,
        phone_book=phone_book,
    )
)
coreapi.dependency("encryption_service", DependencyManager.TimeToLive.FOREVER)(
    lambda config, ssm_client: EncryptionService(config, ssm_client, Fernet)
)
coreapi.dependency("pagination_service", DependencyManager.TimeToLive.FOREVER)(
    lambda encryption_service: PaginationService(encryption_service)
)
coreapi.dependency("sns_topic_manager", DependencyManager.TimeToLive.FOREVER)(
    lambda config, aws: SnsTopicManager(config, aws)
)


@coreapi.dependency("data_explorer_sync", DependencyManager.TimeToLive.FOREVER)
def _build_data_explorer_sync(  # pylint: disable=too-many-arguments
    config: Config,
    datasets_table: DatasetsTable,
    resources_table: ResourcesTable,
    aws: AwsClientFactory,
    lock_service: LockService,
    kms_service: KmsService,
) -> DataExplorerSync:
    return DataExplorerSync(
        config=config,
        resources_table=resources_table,
        datasets_table=datasets_table,
        clients=aws,
        lock_service=lock_service,
        kms_service=kms_service,
    )


@coreapi.dependency("dataset_manager", DependencyManager.TimeToLive.FOREVER)
def _build_dataset_manager(
    datasets_table: DatasetsTable,
    resources_table: ResourcesTable,
    lock_service: LockService,
    data_explorer_sync: DataExplorerSync,
) -> DatasetManager:
    return DatasetManager(
        datasets_table=datasets_table,
        lock_service=lock_service,
        resources_table=resources_table,
        data_explorer_sync=data_explorer_sync,
    )


coreapi.dependency("resource_payload_builder", DependencyManager.TimeToLive.FOREVER)(lambda: ResourcePayloadBuilder())


@coreapi.dependency("s3_resource_manager", DependencyManager.TimeToLive.FOREVER)
def _build_s3_resource_manager(  # pylint: disable=too-many-arguments
    config: Config,
    resources_table: ResourcesTable,
    datasets_table: DatasetsTable,
    s3_bucket_manager: S3BucketManager,
    sns_topic_manager: SnsTopicManager,
    lock_service: LockService,
    kms_service: KmsService,
    data_explorer_sync: DataExplorerSync,
) -> S3ResourceManager[S3Resource]:
    return S3ResourceManager(
        resources_table=resources_table,
        datasets_table=datasets_table,
        config=config,
        s3_bucket_manager=s3_bucket_manager,
        sns_topic_manager=sns_topic_manager,
        lock_service=lock_service,
        kms_service=kms_service,
        s3_resource_type=S3Resource,
        data_explorer_sync=data_explorer_sync,
    )


@coreapi.dependency("authorizer", DependencyManager.TimeToLive.PER_REQUEST)
def _build_authorizer(
    request: Request,
    config: Config,
    accounts_table: AccountsTable,
    authorization_api: AuthorizationApi,
    phone_book: PhoneBook,
) -> Authorizer[Account]:
    return Authorizer(
        config=config,
        requester_arn=request.requester_arn,
        auth_api=authorization_api,
        phone_book=phone_book,
        accounts_table=accounts_table,
    )


@coreapi.dependency("dataset_validator", DependencyManager.TimeToLive.PER_REQUEST)
def _build_dataset_validator(  # pylint: disable=too-many-arguments
    authorizer: Authorizer[Account],
    authorization_api: AuthorizationApi,
    config: Config,
    requester_identity: RequesterIdentity,
    resources_table: ResourcesTable,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> DatasetValidator:
    return DatasetValidator(
        authorization_api=authorization_api,
        authorizer=authorizer,
        config=config,
        requester_identity=requester_identity,
        resources_table=resources_table,
        visible_data_loader=visible_data_loader,
    )


coreapi.dependency("dataset_participants_manager", DependencyManager.TimeToLive.PER_REQUEST)(
    lambda authorization_api, config, sns_publisher, users_api: DatasetParticipantsManager(
        authorization_api=authorization_api,
        config=config,
        sns_publisher=sns_publisher,
        users_api=users_api,
    )
)


@coreapi.dependency("resource_validator", DependencyManager.TimeToLive.PER_REQUEST)
def _build_resource_validator(  # pylint: disable=too-many-arguments
    accounts_table: AccountsTable,
    config: Config,
    resources_table: ResourcesTable,
    authorizer: Authorizer[Account],
    requester_identity: RequesterIdentity,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
    filter_packages_table: FilterPackagesTable,
) -> ResourceValidator:
    return ResourceValidator(
        accounts_table=accounts_table,
        assumable_account_spec_cls=AssumableAccountSpec,
        config=config,
        resources_table=resources_table,
        authorizer=authorizer,
        requester_identity=requester_identity,
        visible_data_loader=visible_data_loader,
        filter_packages_table=filter_packages_table,
    )


@coreapi.dependency("dataset_permissions_validator", DependencyManager.TimeToLive.PER_REQUEST)
def _build_dataset_permissions_validator(
    authorizer: Authorizer[Account],
    accounts_table: AccountsTable,
    resources_table: ResourcesTable,
    visible_data_loader: VisibleDataLoader[Account, S3Resource, GlueSyncResource],
) -> DatasetPermissionsValidator:
    return DatasetPermissionsValidator(
        authorizer=authorizer,
        accounts_table=accounts_table,
        resources_table=resources_table,
        visible_data_loader=visible_data_loader,
    )


@coreapi.dependency("dataset_permissions_manager", DependencyManager.TimeToLive.PER_REQUEST)
def _build_dataset_permissions_manager(  # pylint: disable=too-many-arguments
    config: Config,
    datasets_table: DatasetsTable,
    lock_service: LockService,
    sns_publisher: SnsPublisher,
    s3_resource_manager: S3ResourceManager[S3Resource],
    lake_formation_service: LakeFormationService,
    resource_link: ResourceLink,
    accounts_table: AccountsTable,
    resources_table: ResourcesTable,
) -> DatasetPermissionsManager[Account, S3Resource, GlueSyncResource]:
    return DatasetPermissionsManager(
        config=config,
        datasets_table=datasets_table,
        lock_service=lock_service,
        s3_resource_manager=s3_resource_manager,
        lake_formation_service=lake_formation_service,
        sns_publisher=sns_publisher,
        accounts_table=accounts_table,
        resources_table=resources_table,
        resource_link=resource_link,
    )


@coreapi.dependency("glue_resource_manager", DependencyManager.TimeToLive.FOREVER)
def _build_glue_resource_manager(  # pylint: disable=too-many-arguments
    aws: AwsClientFactory,
    resources_table: ResourcesTable,
    config: Config,
    lake_formation_service: LakeFormationService,
    lock_service: LockService,
    resource_link: ResourceLink,
    data_explorer_sync: DataExplorerSync,
) -> GlueResourceManager[GlueSyncResource, NewGlueSyncBody]:
    return GlueResourceManager(
        aws=aws,
        config=config,
        lake_formation_service=lake_formation_service,
        lock_service=lock_service,
        resource_link=resource_link,
        resources_table=resources_table,
        glue_sync_resource_type=GlueSyncResource,
        data_explorer_sync=data_explorer_sync,
    )


entry_point = log_safe()(coreapi.handle_request)
