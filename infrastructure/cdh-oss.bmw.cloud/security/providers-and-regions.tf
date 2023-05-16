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

provider "aws" {
  alias  = "primary"
  region = "eu-west-1"
}

data "aws_region" "primary" {
  provider = aws.primary
}

provider "aws" {
  alias  = "eu_west_1"
  region = "eu-west-1"
}

data "aws_region" "eu_west_1" {
  provider = aws.eu_west_1
}

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

data "aws_region" "us_east_1" {
  provider = aws.us_east_1
}
