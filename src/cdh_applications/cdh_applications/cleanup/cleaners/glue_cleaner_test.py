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
# pylint: disable=protected-access
from logging import getLogger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from unittest.mock import Mock

import boto3
import pytest

from cdh_applications.cleanup.cleaners.glue_cleaner import GlueCleaner
from cdh_applications.cleanup.cleanup_utils_test import PREFIX
from cdh_core.aws_clients.glue_resource_policy import GlueResourcePolicy
from cdh_core.aws_clients.policy import PolicyDocument
from cdh_core.aws_clients.policy_test import build_policy_statement
from cdh_core.entities.accounts import BaseAccount
from cdh_core.entities.accounts_test import build_base_account
from cdh_core.entities.arn_test import build_arn
from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.enums.aws_test import build_partition
from cdh_core_dev_tools.testing.builder import Builder


class GlueCleanerTestCase:
    @pytest.fixture()
    def partition(self) -> Partition:
        return build_partition()

    @pytest.fixture()
    def region(self, partition: Partition) -> str:
        regions: List[str] = [region.value for region in list(Region) if region.partition == partition]
        return Builder.get_random_element(
            regions,
            exclude={"us-east-1"},
        )

    @pytest.fixture()
    def account(self) -> BaseAccount:
        return build_base_account()

    @pytest.fixture()
    def principal(self, partition: Partition, account: BaseAccount) -> str:
        return f"arn:{partition.value}:iam::{account.id}:root"

    def mocked_glue_resource_policy_boto_response(
        self, glue_resource_policy: Optional[GlueResourcePolicy] = None
    ) -> Dict[str, Any]:
        if not glue_resource_policy:
            glue_resource_policy = GlueResourcePolicy(
                document=PolicyDocument.create_glue_resource_policy([build_policy_statement()]), policy_hash="hash"
            )
        return {
            "PolicyInJson": glue_resource_policy.to_boto(),
            "PolicyHash": glue_resource_policy.policy_hash,
        }


@pytest.mark.usefixtures("mock_glue")
@pytest.mark.usefixtures("mock_s3")
class TestGlueCleanerCrawler(GlueCleanerTestCase):
    def test_dont_clean_if_clean_filter_returns_false(
        self, region: str, account: BaseAccount, partition: Partition
    ) -> None:
        prefixed_crawler_name = f"{PREFIX}my_crawler_to_reset"
        crawler_names = ["some_crawler", "another_crawler", prefixed_crawler_name]
        clean_filter = Mock(return_value=False)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        for crawler_name in crawler_names:
            glue_cleaner._client.create_crawler(Name=crawler_name, Role="some_role", Targets={"S3Targets": []})
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response()
        )
        glue_cleaner._client.update_crawler = Mock()  # type: ignore

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("crawler", prefixed_crawler_name, getLogger())
        glue_cleaner._client.update_crawler.assert_not_called()

    def test_reset_crawler_sources_with_prefix(self, region: str, account: BaseAccount, partition: Partition) -> None:
        prefixed_crawler_name = f"{PREFIX}my_crawler_to_reset"
        crawler_names = ["some_crawler", "another_crawler", prefixed_crawler_name]
        clean_filter = Mock(return_value=True)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        for crawler_name in crawler_names:
            glue_cleaner._client.create_crawler(Name=crawler_name, Role="some_role", Targets={"S3Targets": []})
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response()
        )
        glue_cleaner._client.update_crawler = Mock()  # type: ignore

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("crawler", prefixed_crawler_name, getLogger())
        assert glue_cleaner._client.get_crawler(Name=prefixed_crawler_name)["Crawler"]["Name"] == prefixed_crawler_name
        glue_cleaner._client.update_crawler.assert_called_once_with(
            Name=prefixed_crawler_name, Targets={"S3Targets": [{"Path": "reset-by-crawler", "Exclusions": ["*"]}]}
        )


@pytest.mark.usefixtures("mock_glue")
@pytest.mark.usefixtures("mock_s3")
class TestGlueCleanerCacheBucket(GlueCleanerTestCase):
    def test_dont_clean_if_clean_filter_returns_false(
        self, region: str, account: BaseAccount, partition: Partition
    ) -> None:
        clean_filter = Mock(return_value=False)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response()
        )
        cache_bucket_name = f"{PREFIX}cdh-core-glue-sync-cache-"
        glue_cleaner._s3_resource.create_bucket(
            Bucket=cache_bucket_name, CreateBucketConfiguration={"LocationConstraint": region}  # type: ignore
        )
        glue_cleaner._s3_resource.Bucket(cache_bucket_name).put_object(Key="test-key", Body="test-body")
        glue_cleaner._cache_bucket_name = cache_bucket_name

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("database_cache", cache_bucket_name, getLogger())
        assert [obj.key for obj in glue_cleaner._s3_resource.Bucket(cache_bucket_name).objects.all()] == ["test-key"]

    def test_clean_valid_cache_bucket(self, region: str, account: BaseAccount, partition: Partition) -> None:
        clean_filter = Mock(return_value=True)
        cache_bucket_name = f"{PREFIX}cdh-core-glue-sync-cache-"
        some_other_bucket_name = "some-other-bucket-name"
        s3_resource = boto3.resource("s3", region_name=region)
        s3_resource.create_bucket(
            Bucket=cache_bucket_name, CreateBucketConfiguration={"LocationConstraint": region}  # type: ignore
        )
        s3_resource.create_bucket(
            Bucket=some_other_bucket_name, CreateBucketConfiguration={"LocationConstraint": region}  # type: ignore
        )
        s3_resource.Bucket(cache_bucket_name).put_object(Key="test-key", Body="test-body")
        s3_resource.Bucket(some_other_bucket_name).put_object(Key="some-other-bucket-test-key", Body="test-body")
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response()
        )

        glue_cleaner.clean()

        assert len([obj.key for obj in glue_cleaner._s3_resource.Bucket(cache_bucket_name).objects.all()]) == 0
        assert [obj.key for obj in glue_cleaner._s3_resource.Bucket(some_other_bucket_name).objects.all()] == [
            "some-other-bucket-test-key"
        ]


@pytest.mark.usefixtures("mock_glue")
@pytest.mark.usefixtures("mock_s3")
class TestGlueCleanerDatabases(GlueCleanerTestCase):
    def test_dont_delete_database_if_clean_filter_returns_false(
        self, region: str, account: BaseAccount, partition: Partition
    ) -> None:
        database_name = PREFIX + "vehicle_ftest1234_raw"
        clean_filter = Mock(return_value=False)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response()
        )
        glue_cleaner._client.delete_database = Mock()  # type: ignore
        glue_cleaner._client.create_database(
            CatalogId=Builder.build_random_string(), DatabaseInput={"Name": database_name}
        )

        glue_cleaner.clean()

        glue_cleaner._client.delete_database.assert_not_called()

    def test_only_delete_valid_database(self, region: str, account: BaseAccount, partition: Partition) -> None:
        to_be_deleted_database_name = PREFIX + "vehicle_ftest1234_raw"
        other_prefix = Builder.build_resource_name_prefix()
        other_prefix_database_name = other_prefix + "vehicle_ftest1234_raw"
        clean_filter = Mock(return_value=True)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response()
        )
        glue_cleaner._client.create_database(
            CatalogId=Builder.build_random_string(), DatabaseInput={"Name": to_be_deleted_database_name}
        )
        glue_cleaner._client.create_database(
            CatalogId=Builder.build_random_string(), DatabaseInput={"Name": other_prefix_database_name}
        )

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("database", to_be_deleted_database_name, getLogger())
        assert [database["Name"] for database in glue_cleaner._client.get_databases()["DatabaseList"]] == [
            other_prefix_database_name
        ]


@pytest.mark.usefixtures("mock_glue")
@pytest.mark.usefixtures("mock_s3")
class TestGlueCleanerResourcePolicy(GlueCleanerTestCase):
    def build_protection_statement(
        self, databases: List[str], principal: str, region: Region, partition: Partition, account_id: str
    ) -> Dict[str, Any]:
        return GlueResourcePolicy.create_resource_link_protect_policy_statement(
            principal=principal,
            resources={
                build_arn(
                    service="glue",
                    resource="database/" + database,
                    region=region,
                    partition=partition,
                    account_id=account_id,
                )
                for database in databases
            },
        )

    def test_no_resource_policy(self, region: str, account: BaseAccount, partition: Partition) -> None:
        clean_filter = Mock(return_value=True)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            side_effect=glue_cleaner._client.exceptions.EntityNotFoundException(
                error_response={"Error": {"Code": "EntityNotFoundException"}}, operation_name="some operation"
            )
        )
        glue_cleaner._client.put_resource_policy = Mock()  # type: ignore
        glue_cleaner._client.delete_resource_policy = Mock()  # type: ignore

        glue_cleaner.clean()

        glue_cleaner._client.put_resource_policy.assert_not_called()
        glue_cleaner._client.delete_resource_policy.assert_not_called()

    def test_resource_policy_without_protection_statement(
        self, region: str, account: BaseAccount, partition: Partition
    ) -> None:
        clean_filter = Mock(return_value=True)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )

        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response()
        )
        glue_cleaner._client.put_resource_policy = Mock()  # type: ignore
        glue_cleaner._client.delete_resource_policy = Mock()  # type: ignore

        glue_cleaner.clean()

        glue_cleaner._client.put_resource_policy.assert_not_called()
        glue_cleaner._client.delete_resource_policy.assert_not_called()

    @pytest.mark.parametrize("include_other_statements", [False, True])
    def test_resource_policy_protection_statement_no_prefix_match(
        self, include_other_statements: bool, region: str, principal: str, partition: Partition, account: BaseAccount
    ) -> None:
        clean_filter = Mock(return_value=True)
        statements = [
            self.build_protection_statement(
                [Builder.build_random_string() for _ in range(3)],
                principal=principal,
                region=Region.preferred(partition),
                partition=partition,
                account_id=str(account.id),
            )
        ]
        if include_other_statements:
            statements.extend([build_policy_statement() for _ in range(3)])
        document = PolicyDocument.create_glue_resource_policy(statements)
        glue_resource_policy = GlueResourcePolicy(document=document, policy_hash="hash")
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response(glue_resource_policy)
        )
        glue_cleaner._client.put_resource_policy = Mock()  # type: ignore
        glue_cleaner._client.delete_resource_policy = Mock()  # type: ignore

        glue_cleaner.clean()

        glue_cleaner._client.put_resource_policy.assert_not_called()
        glue_cleaner._client.delete_resource_policy.assert_not_called()

    @pytest.mark.parametrize("include_other_statements", [False, True])
    def test_resource_policy_clean_filter_returns_false(
        self, include_other_statements: bool, region: str, principal: str, partition: Partition, account: BaseAccount
    ) -> None:
        database_name = PREFIX + Builder.build_random_string()
        clean_filter = Mock(return_value=False)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        statements = [
            self.build_protection_statement(
                [database_name],
                principal=principal,
                region=Region.preferred(partition),
                partition=partition,
                account_id=str(account.id),
            )
        ]
        if include_other_statements:
            statements.extend([build_policy_statement() for _ in range(3)])
        document = PolicyDocument.create_glue_resource_policy(statements)
        glue_resource_policy = GlueResourcePolicy(document=document, policy_hash="hash")
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response(glue_resource_policy)
        )
        glue_cleaner._client.put_resource_policy = Mock()  # type: ignore
        glue_cleaner._client.delete_resource_policy = Mock()  # type: ignore

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("database-protection", database_name, getLogger())
        glue_cleaner._client.put_resource_policy.assert_not_called()
        glue_cleaner._client.delete_resource_policy.assert_not_called()

    @pytest.mark.parametrize("include_other_statements", [False, True])
    def test_resource_policy_keep_other_protection_statement(
        self, include_other_statements: bool, principal: str, region: str, partition: Partition, account: BaseAccount
    ) -> None:
        clean_filter = Mock(return_value=True)
        database_name = PREFIX + Builder.build_random_string()
        other_databases = [Builder.build_random_string() for _ in range(3)]
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        protection_statement = self.build_protection_statement(
            [database_name] + other_databases,
            principal=principal,
            region=Region(region),
            partition=partition,
            account_id=str(account.id),
        )
        other_statements = [build_policy_statement() for _ in range(3)] if include_other_statements else []
        document = PolicyDocument.create_glue_resource_policy(other_statements + [protection_statement])
        policy_hash = Builder.build_random_string()
        glue_resource_policy = GlueResourcePolicy(document=document, policy_hash=policy_hash)
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response(glue_resource_policy)
        )
        glue_cleaner._client.put_resource_policy = Mock()  # type: ignore
        glue_cleaner._client.delete_resource_policy = Mock()  # type: ignore
        expected_statements = other_statements + [
            self.build_protection_statement(
                other_databases,
                principal=principal,
                region=Region(region),
                partition=partition,
                account_id=str(account.id),
            )
        ]
        expected_policy_document = PolicyDocument.create_glue_resource_policy(expected_statements)

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("database-protection", database_name, getLogger())
        glue_cleaner._client.put_resource_policy.assert_called_once_with(
            PolicyInJson=expected_policy_document.encode(),
            PolicyHashCondition=policy_hash,
        )
        glue_cleaner._client.delete_resource_policy.assert_not_called()

    def test_resource_policy_delete_statement_keep_policy(
        self, principal: str, region: str, partition: Partition, account: BaseAccount
    ) -> None:
        clean_filter = Mock(return_value=True)
        database_name = PREFIX + Builder.build_random_string()
        protection_statement = self.build_protection_statement(
            [database_name], principal=principal, region=Region(region), partition=partition, account_id=str(account.id)
        )
        other_statements = [build_policy_statement() for _ in range(3)]
        document = PolicyDocument.create_glue_resource_policy(other_statements + [protection_statement])
        policy_hash = Builder.build_random_string()
        glue_resource_policy = GlueResourcePolicy(document=document, policy_hash=policy_hash)
        expected_policy_document = PolicyDocument.create_glue_resource_policy(other_statements)
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response(glue_resource_policy)
        )
        glue_cleaner._client.put_resource_policy = Mock()  # type: ignore
        glue_cleaner._client.delete_resource_policy = Mock()  # type: ignore

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("database-protection", database_name, getLogger())
        glue_cleaner._client.put_resource_policy.assert_called_once_with(
            PolicyInJson=expected_policy_document.encode(),
            PolicyHashCondition=policy_hash,
        )
        glue_cleaner._client.delete_resource_policy.assert_not_called()

    def test_resource_policy_delete_policy(
        self, principal: str, region: str, partition: Partition, account: BaseAccount
    ) -> None:
        clean_filter = Mock(return_value=True)
        database_name = PREFIX + Builder.build_random_string()
        glue_cleaner = GlueCleaner(
            region=region,
            account=account,
            prefix=PREFIX,
            clean_filter=clean_filter,
            partition=partition,
            log=getLogger(),
            credentials={},
        )
        protection_statement = self.build_protection_statement(
            [database_name], principal=principal, region=Region(region), partition=partition, account_id=str(account.id)
        )
        document = PolicyDocument.create_glue_resource_policy([protection_statement])
        policy_hash = Builder.build_random_string()
        glue_resource_policy = GlueResourcePolicy(document=document, policy_hash=policy_hash)
        glue_cleaner._client.get_resource_policy = Mock(  # type: ignore
            return_value=self.mocked_glue_resource_policy_boto_response(glue_resource_policy)
        )
        glue_cleaner._client.put_resource_policy = Mock()  # type: ignore
        glue_cleaner._client.delete_resource_policy = Mock()  # type: ignore

        glue_cleaner.clean()

        clean_filter.assert_called_once_with("database-protection", database_name, getLogger())
        glue_cleaner._client.put_resource_policy.assert_not_called()
        glue_cleaner._client.delete_resource_policy.assert_called_once_with(PolicyHashCondition=policy_hash)
