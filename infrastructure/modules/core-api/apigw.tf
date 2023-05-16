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

data "aws_iam_policy_document" "api_gw_policy" {
  dynamic "statement" {
    # include the statement only if an OrgId is provided, since otherwise creation fails
    for_each = local.enable_access_via_org_id ? [1] : []
    content {
      effect = "Allow"
      principals {
        type        = "AWS"
        identifiers = ["*"]
      }
      actions   = ["execute-api:Invoke"]
      resources = ["*"]
      condition {
        test     = "StringEquals"
        values   = var.trusted_org_ids
        variable = "aws:PrincipalOrgID"
      }
    }
  }
  statement {
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = var.trusted_accounts
    }
    actions   = ["execute-api:Invoke"]
    resources = ["*"]
  }
  statement {
    effect = "Allow"
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    actions = ["execute-api:Invoke"]
    resources = [
      local.options_arn
    ]
  }
}

resource "aws_api_gateway_rest_api" "core-api-gw" {
  name                     = "${var.resource_name_prefix}${var.api_name}"
  body                     = local.open_api_spec_template
  policy                   = data.aws_iam_policy_document.api_gw_policy.json
  minimum_compression_size = 0 # needs to be set in openapi spec, too
  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_cloudwatch_log_group" "api-execution-logs" {
  # This resource is created automatically by API Gateway.
  # We include it here so that it is managed by Terraform and to set the retention period.
  # The following name must coincide with the one used by API Gateway.
  name              = "API-Gateway-Execution-Logs_${aws_api_gateway_rest_api.core-api-gw.id}/${local.api_stage_name}"
  retention_in_days = 30
  kms_key_id        = var.kms_master_key_arn
}

resource "aws_api_gateway_documentation_version" "core-api" {
  version     = "core-api-${local.api_spec_hash}"
  rest_api_id = aws_api_gateway_rest_api.core-api-gw.id
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_deployment" "core-api-gw-stage" {
  rest_api_id       = aws_api_gateway_rest_api.core-api-gw.id
  stage_name        = ""
  stage_description = "api-spec-hash ${local.api_spec_hash}"
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_method_settings" "core-api-settings" {
  rest_api_id = aws_api_gateway_rest_api.core-api-gw.id
  stage_name  = aws_api_gateway_stage.core-api-gw-stage-v1.stage_name
  method_path = "*/*"

  settings {
    logging_level = "ERROR"
  }
}

resource "aws_cloudwatch_log_group" "core-api-gw-loggroup" {
  name              = "${var.resource_name_prefix}API-Gateway-Access-Logs_${aws_api_gateway_rest_api.core-api-gw.id}"
  retention_in_days = 30
  kms_key_id        = var.kms_master_key_arn
}

resource "aws_api_gateway_stage" "core-api-gw-stage-v1" {
  depends_on = [aws_cloudwatch_log_group.api-execution-logs, var.api_gw_cloudwatch_setting_id]

  stage_name            = local.api_stage_name
  rest_api_id           = aws_api_gateway_rest_api.core-api-gw.id
  deployment_id         = aws_api_gateway_deployment.core-api-gw-stage.id
  documentation_version = aws_api_gateway_documentation_version.core-api.version
  xray_tracing_enabled  = true
  variables = {
    region = data.aws_region.current.name
    stage  = local.api_stage_name
  }
  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.core-api-gw-loggroup.arn
    format          = "$context.identity.sourceIp $context.identity.caller $context.identity.user [$context.requestTime] $context.httpMethod $context.path $context.protocol $context.status $context.responseLatency ms ($context.integrationLatency ms) $context.responseLength $context.requestId $context.extendedRequestId $context.xrayTraceId"
  }
}

resource "aws_lambda_permission" "apigw-allow-invoke-core-api" {
  statement_id  = "${var.resource_name_prefix}AllowExecutionOfCoreApiFromAPIGW-${data.aws_region.current.name}"
  action        = "lambda:InvokeFunction"
  function_name = module.core-api-lambda.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.core-api-gw.execution_arn}/*/*/*"
  qualifier     = aws_lambda_alias.core_api_lambda_alias.name
}

resource "aws_acm_certificate" "certificate" {
  domain_name       = local.full_domain
  validation_method = "DNS"
}

resource "aws_acm_certificate_validation" "validation" {
  certificate_arn         = aws_acm_certificate.certificate.arn
  validation_record_fqdns = [for record in aws_route53_record.validation : record.fqdn]
}

resource "aws_api_gateway_domain_name" "apigw-custom-domain" {
  domain_name              = local.full_domain
  regional_certificate_arn = aws_acm_certificate_validation.validation.certificate_arn
  endpoint_configuration {
    types = ["REGIONAL"]
  }
  security_policy = "TLS_1_2"
}

resource "aws_api_gateway_base_path_mapping" "apigw-custom-domain-stage-mapping" {
  api_id      = aws_api_gateway_rest_api.core-api-gw.id
  stage_name  = aws_api_gateway_stage.core-api-gw-stage-v1.stage_name
  domain_name = aws_api_gateway_domain_name.apigw-custom-domain.domain_name
}

resource "aws_xray_sampling_rule" "api-gw" {
  count          = var.resource_name_prefix == "" ? 1 : 0
  rule_name      = "${var.api_name}-gw"
  priority       = 1000
  version        = 1
  reservoir_size = 1
  fixed_rate     = 1.0
  url_path       = "*"
  host           = "*"
  http_method    = "*"
  service_type   = "AWS::ApiGateway::*"
  service_name   = "*"
  resource_arn   = "*"
}

output "apigw-invoke-url" {
  value = "https://${aws_api_gateway_domain_name.apigw-custom-domain.domain_name}"
}

output "rest-api-id" {
  value = aws_api_gateway_rest_api.core-api-gw.id
}
