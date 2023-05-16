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
from typing import Optional

from cdh_core.enums.aws import Partition
from cdh_core.enums.aws import Region
from cdh_core.primitives.account_id import AccountId


class Arn:
    """Amazon Resource Name, a unique identifier for AWS resources.

    https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
    """

    _arn_string: str
    partition: Partition
    service: str
    region: str  # Note: We don't use the Region-class here, because some ARNs contain '' or '*' as region.
    account_id: AccountId
    identifier: str

    def __init__(self, arn_string: str):
        self._arn_string = arn_string
        components = arn_string.split(":")
        try:
            self.service = components[2]
        except IndexError:
            raise MalformedArnException(  # pylint: disable=raise-missing-from
                f"{arn_string} does not have the correct number of ':'"
            )
        arn_length = {7, 8} if self.service in ["lambda", "states"] else {6}
        if not len(components) in arn_length:
            raise MalformedArnException(f"{arn_string} does not have the correct number of ':'")
        partition = components[1]
        try:
            self.partition = Partition(partition)
        except ValueError:
            raise MalformedArnException(  # pylint: disable=raise-missing-from
                f"{arn_string} has an invalid partition {partition!r}, must be one of {[p.value for p in Partition]}."
            )
        self.region = components[3]
        self.account_id = AccountId(components[4])
        self.identifier = ":".join(components[5:])

    def __str__(self) -> str:
        """Represent ARN by its string."""
        return self._arn_string

    def __eq__(self, other: object) -> bool:
        """Compare ARNs by their strings."""
        if isinstance(other, Arn):
            return self._arn_string == other._arn_string
        return NotImplemented

    def __hash__(self) -> int:
        """Hash ARN by its string."""
        return hash(self._arn_string)

    def __repr__(self) -> str:
        """Represent ARN by its string."""
        return f"Arn({self._arn_string})"

    def get_assumed_role_name(self) -> str:
        """For STS ARNs of type assumed-role, get name of that role."""
        if self.service == "sts":
            components = self.identifier.split("/")
            if len(components) == 3 and components[0] == "assumed-role":
                return components[1]
        raise ValueError(f"{self} is not an assumed role ARN")

    @classmethod
    def get_role_arn(cls, partition: Partition, account_id: AccountId, path: str, name: str) -> "Arn":
        """Build role ARN out of its components."""
        return cls(f"arn:{partition.value}:iam::{account_id}:role{path}{name}")


def build_arn_string(
    service: str, region: Optional[Region], account: AccountId, resource: str, partition: Partition
) -> str:
    """Build ARN string out of its components."""
    region_string = region.value if region else ""
    if region_string:
        assert Region(region_string).partition is partition
    return f"arn:{partition.value}:{service}:{region_string}:{account}:{resource}"


class MalformedArnException(Exception):
    """Raised when trying to create an Arn out of an invalid arn_string."""
