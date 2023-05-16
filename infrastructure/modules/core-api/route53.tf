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

data "aws_route53_zone" "parent_zone" {
  name = var.domain
}


resource "aws_route53_record" "domain_alias" {
  zone_id = data.aws_route53_zone.parent_zone.id
  name    = local.full_domain
  type    = "A"
  alias {
    name                   = aws_api_gateway_domain_name.apigw-custom-domain.regional_domain_name
    zone_id                = aws_api_gateway_domain_name.apigw-custom-domain.regional_zone_id
    evaluate_target_health = false
  }
}


resource "aws_route53_record" "validation" {
  for_each = {
    for dvo in aws_acm_certificate.certificate.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = data.aws_route53_zone.parent_zone.zone_id
}
