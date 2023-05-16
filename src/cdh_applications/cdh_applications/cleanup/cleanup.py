#!/usr/bin/env python3
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
import argparse
import logging
import os
import sys
from typing import Any
from typing import Callable
from typing import List
from typing import Tuple

import boto3

from cdh_applications.cleanup.cleaner_factory import CleanerFactory
from cdh_applications.cleanup.cleanup_utils import _is_legal_prefix
from cdh_applications.cleanup.cleanup_utils import always_delete_filter
from cdh_applications.cleanup.cleanup_utils import ask_user_filter
from cdh_applications.cleanup.cleanup_utils import assume_cleanup_role
from cdh_applications.cleanup.cleanup_utils import FORBIDDEN_PREFIXES
from cdh_applications.cleanup.cleanup_utils import never_delete_filter
from cdh_core.entities.account_store import AccountStore
from cdh_core.enums.accounts import AccountPurpose
from cdh_core.enums.aws import Region
from cdh_core.enums.environment import Environment

root_logger = logging.getLogger()


class IllegalPrefixException(Exception):
    """Exception for an illegal prefix."""


def parse_arguments(
    account_store: AccountStore,
) -> Tuple[Callable[[str, str, Any], bool], Any]:
    """Parse the arguments for the cdh_cleanup prefix script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", type=str, default="INFO", help="Set Python log level.")
    parser.add_argument("--force", help="Delete all found resources without prompting.", action="store_true")
    parser.add_argument("--dry-run", help="Only list found resources, do not delete them.", action="store_true")
    parser.add_argument("--account-id", help="Account id of the target account", type=str)
    parser.add_argument("--account-purpose", help="Account purpose of the target account", type=str, required=False)
    args = parser.parse_args()

    logging.basicConfig(level=args.log.upper())
    if args.dry_run:
        clean_filter = never_delete_filter
    elif args.force:
        clean_filter = always_delete_filter
    else:
        clean_filter = ask_user_filter

    filtered_account_purpose = AccountPurpose(args.account_purpose) if args.account_purpose else None
    if args.account_id:
        accounts = account_store.query_accounts(
            environments=frozenset(Environment), account_ids=args.account_id, account_purposes=filtered_account_purpose
        )
    else:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        accounts = account_store.query_accounts(
            environments=frozenset(Environment), account_ids=account_id, account_purposes=filtered_account_purpose
        )
        for account in accounts:
            if not account.environment.is_test_environment:
                root_logger.error(f"Cleanup must only be used for test environments: {account}")
                sys.exit(1)

    return clean_filter, accounts


def get_account_regions(account: Any) -> List[str]:
    """Get regions for a specific account."""
    regions = [region.value for region in Region if region.partition == account.partition]

    assert regions, f"Cannot determine supported regions of account {account.id}"

    return regions


def main() -> None:
    """Start the cdh_cleanup process."""
    prefix = os.environ["RESOURCE_NAME_PREFIX"].lower()
    if not _is_legal_prefix(prefix):
        raise IllegalPrefixException(
            f"Illegal prefix in RESOURCE_NAME_PREFIX env! Prefix cannot be one of {FORBIDDEN_PREFIXES}"
        )

    account_store = AccountStore()
    clean_filter, accounts = parse_arguments(account_store=account_store)

    for account in accounts:
        log = logging.getLogger(f"{__name__}_{account.id}_{account.purpose}")
        log.info(f"Cleanup of account {account.id} requested.")

        partition = account.partition
        regions = get_account_regions(account)

        credentials = assume_cleanup_role(account.id, prefix, partition)

        cleaners = CleanerFactory(
            account=account,
            prefix=prefix,
            clean_filter=clean_filter,
            regions=regions,
            partition=partition,
            log=log,
            credentials=credentials,
        ).get_cleaners()

        for cleaner in cleaners:
            cleaner.clean()


if __name__ == "__main__":
    main()
