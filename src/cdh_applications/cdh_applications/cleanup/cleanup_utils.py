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
# pylint: disable=duplicate-code
import logging
import re
from itertools import islice
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import TypeVar

import boto3
from botocore.exceptions import ClientError

from cdh_core.enums.aws import Partition
from cdh_core.enums.dataset_properties import BusinessObject
from cdh_core.enums.hubs import Hub

T = TypeVar("T")

LOG = logging.getLogger()

FORBIDDEN_PREFIXES = (
    {"cdh"}  # S3 buckets, Dynamo Tables, SNS topics
    | {bo.value for bo in BusinessObject}  # Glue databases, RAM shares in default hub
    | {hub.value for hub in Hub}  # Glue databases, RAM shares in non-default hub
)


def assume_cleanup_role(account_id: str, prefix: str, partition: Partition) -> Dict[str, Any]:
    """Assume the role for the cdh_cleanup."""
    role_name = f"arn:{partition.value}:iam::{account_id}:role/{prefix}cdh-core-cleanup"
    thread_safe_session = boto3.session.Session()
    client = thread_safe_session.client("sts")
    try:
        response = client.assume_role(RoleArn=role_name, RoleSessionName="cdh_cleanup")
    except ClientError as error:
        if error.response["Error"]["Code"] == "AccessDenied":
            raise RuntimeError(
                f"Could not switch role to {role_name}. Did you forget to switch to an admin role?" f"Error: {error}"
            ) from error
        raise

    return {
        "aws_access_key_id": response["Credentials"]["AccessKeyId"],
        "aws_secret_access_key": response["Credentials"]["SecretAccessKey"],
        "aws_session_token": response["Credentials"]["SessionToken"],
    }


def has_prefix(name: str, prefix: str) -> bool:
    """Check whether the given name has a prefix."""
    if not _is_legal_prefix(prefix):
        return False

    return name.startswith(prefix)


def _is_legal_prefix(prefix: str) -> bool:
    if not prefix:
        return False
    if prefix in FORBIDDEN_PREFIXES:
        return False
    forbidden_first_blocks = {f"{p}{separator}" for p in FORBIDDEN_PREFIXES for separator in ["-", "_"]}
    if any(prefix.startswith(first_block) for first_block in forbidden_first_blocks):
        return False
    return True


def has_resource_prefix(name: str, prefix: str) -> bool:
    """Check whether the given name matches the resource pattern."""
    if not has_prefix(name, prefix):
        return False

    hubs = [f"{hub.value}-" for hub in Hub if hub is not Hub.default()] + [""]
    business_objects = [bo.value for bo in BusinessObject]
    resource_pattern = rf"^{prefix}cdh-({'|'.join(hubs)})({'|'.join(business_objects)})-"
    if re.match(resource_pattern, name):
        return True
    return False


def chunked(iterator: Iterator[T], size: int) -> Iterator[List[T]]:
    """Chunk the given iterator into smaller pieces."""
    return iter(lambda: list(islice(iterator, 0, size)), [])


def always_delete_filter(resource_type: str, identifier: str, logger: logging.Logger = LOG) -> bool:
    """Set the given resource to always delete."""
    logger.info("Deleting %s %s", resource_type, identifier)
    return True


def never_delete_filter(resource_type: str, identifier: str, logger: logging.Logger = LOG) -> bool:
    """Prevent deletion of resource."""
    logger.info("Would delete %s %s", resource_type, identifier)
    return False


def ask_user_filter(
    resource_type: str, identifier: str, logger: logging.Logger = LOG  # pylint: disable=unused-argument
) -> bool:
    """Ask user confirmation for deletion."""
    return input(f"Delete {resource_type} {identifier}? (answer y/N)") == "y"
