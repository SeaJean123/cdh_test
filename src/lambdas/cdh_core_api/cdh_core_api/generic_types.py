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
from typing import TypeVar

from cdh_core_api.bodies.accounts import UpdateAccountBody
from cdh_core_api.bodies.resources import NewGlueSyncBody

from cdh_core.entities.accounts import Account
from cdh_core.entities.resource import GlueSyncResource
from cdh_core.entities.resource import S3Resource

GenericAccount = TypeVar("GenericAccount", bound=Account)
GenericS3Resource = TypeVar("GenericS3Resource", bound=S3Resource)  # pylint: disable=invalid-name
GenericGlueSyncResource = TypeVar("GenericGlueSyncResource", bound=GlueSyncResource)
GenericUpdateAccountBody = TypeVar("GenericUpdateAccountBody", bound=UpdateAccountBody)
GenericNewGlueSyncBody = TypeVar("GenericNewGlueSyncBody", bound=NewGlueSyncBody)
