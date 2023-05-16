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
"""Contains the API endpoints and registers them automatically."""
import cdh_core_api.endpoints.accounts
import cdh_core_api.endpoints.api_info
import cdh_core_api.endpoints.business_objects
import cdh_core_api.endpoints.config
import cdh_core_api.endpoints.dataset_account_permissions
import cdh_core_api.endpoints.datasets
import cdh_core_api.endpoints.filter_packages
import cdh_core_api.endpoints.resources
import cdh_core_api.endpoints.stats
