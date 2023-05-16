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
import argparse
import os
from typing import Iterator
from typing import Set
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_iam import IAMClient
    from mypy_boto3_lakeformation.type_defs import DataLakeSettingsTypeDef
else:
    IAMClient = object
    DataLakeSettingsTypeDef = object


class DataLakeSettings:
    """Wrapper for the current data lake settings to track changes."""

    def __init__(self, settings: DataLakeSettingsTypeDef):
        self._settings = settings
        self._changed = False

    def update_admins_if_needed(self, new_admins: Set[str], iam_client: IAMClient) -> None:
        """Update admins and check if they were changed.

        Remove a current admin role if it no longer exists.
        """
        current_admins = set(admin["DataLakePrincipalIdentifier"] for admin in self._settings["DataLakeAdmins"])
        updated_admins = self._filter_admins_by_existence(current_admins | new_admins, iam_client)
        if non_existent_new_admins := [admin for admin in new_admins if admin not in updated_admins]:
            raise ValueError(f"Some of the provided admins do not exist: {non_existent_new_admins}")
        if current_admins != updated_admins:
            self._settings["DataLakeAdmins"] = [
                {"DataLakePrincipalIdentifier": admin} for admin in sorted(updated_admins)
            ]
            self._changed = True

    @staticmethod
    def _filter_admins_by_existence(admins: Set[str], iam_client: IAMClient) -> Set[str]:
        def _get_role_names_iterator() -> Iterator[str]:
            paginator = iam_client.get_paginator("list_roles")
            page_iterator = paginator.paginate()
            for page in page_iterator:
                yield from [role["RoleName"] for role in page["Roles"]]

        return set(admin for admin in admins if admin.split("/")[-1] in _get_role_names_iterator())

    def update_cross_account_version_if_needed(self) -> None:
        """Update cross account version and check if it was changed."""
        if not self._settings.get("Parameters", {}).get("CROSS_ACCOUNT_VERSION") == "3":
            self._settings["Parameters"] = self._settings.get("Parameters", {}) | {"CROSS_ACCOUNT_VERSION": "3"}
            self._changed = True

    @property
    def changed(self) -> bool:
        """Get information whether the settings were changed since initialization."""
        return self._changed

    @property
    def settings(self) -> DataLakeSettingsTypeDef:
        """Get updated settings."""
        return self._settings


def set_data_lake_settings() -> None:
    """Check if data lake settings need to be updated, and if so, update them."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin", type=str, action="append")
    parser.add_argument("--region", type=str)
    parser.add_argument("--assume-role", type=str, required=False)
    parser.add_argument("--credentials-access-key", type=str, required=False)
    parser.add_argument("--credentials-secret-key", type=str, required=False)
    args = parser.parse_args()

    credentials = {}
    if role := args.assume_role:
        if args.credentials_access_key and args.credentials_secret_key:
            sts_client = boto3.client(
                "sts",
                region_name=args.region,
                aws_access_key_id=os.environ[args.credentials_access_key],
                aws_secret_access_key=os.environ[args.credentials_secret_key],
            )
        else:
            sts_client = boto3.client(
                "sts",
                region_name=args.region,
            )
        assume_response = sts_client.assume_role(RoleArn=role, RoleSessionName="set-data-lake-settings")

        credentials = {
            "aws_access_key_id": assume_response["Credentials"]["AccessKeyId"],
            "aws_secret_access_key": assume_response["Credentials"]["SecretAccessKey"],
            "aws_session_token": assume_response["Credentials"]["SessionToken"],
        }
    lake_formation_client = boto3.client("lakeformation", region_name=args.region, **credentials)
    iam_client = boto3.client("iam", region_name=args.region, **credentials)

    data_lake_settings = DataLakeSettings(lake_formation_client.get_data_lake_settings()["DataLakeSettings"])

    data_lake_settings.update_admins_if_needed(new_admins=set(args.admin), iam_client=iam_client)
    data_lake_settings.update_cross_account_version_if_needed()

    if data_lake_settings.changed:
        lake_formation_client.put_data_lake_settings(DataLakeSettings=data_lake_settings.settings)
