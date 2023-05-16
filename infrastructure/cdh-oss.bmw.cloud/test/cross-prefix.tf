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

module "athena-result-bucket_eu_west_1" {
  source = "../../modules/athena-result-bucket"
  providers = {
    aws = aws.eu_west_1
  }

  count                = var.resource_name_prefix == "" ? 1 : 0
  clean_up_all_objects = true
  bucket_name          = "${local.athena_query_result_bucket_base}-eu-west-1"
}

moved {
  from = aws_s3_bucket.cdh-core-athena-query-results-eu-west-1[0]
  to   = module.athena-result-bucket_eu_west_1[0].aws_s3_bucket.athena_results
}

module "athena-result-bucket_us_east_1" {
  source = "../../modules/athena-result-bucket"
  providers = {
    aws = aws.us_east_1
  }

  count                = var.resource_name_prefix == "" ? 1 : 0
  clean_up_all_objects = true
  bucket_name          = "${local.athena_query_result_bucket_base}-us-east-1"
}

moved {
  from = aws_s3_bucket.cdh-core-athena-query-results-us-east-1[0]
  to   = module.athena-result-bucket_us_east_1[0].aws_s3_bucket.athena_results
}
