# The edge terminates and verifies client mTLS. Only this ALB security group
# can connect to the private API task's HTTPS port.
data "aws_subnet" "tenant_alb" {
  for_each = var.tenant_profile ? coalesce(try(var.tenant_gateway.alb_subnet_ids, null), toset([])) : toset([])
  id       = each.value
}

locals {
  tenant_health_check = "import ssl, urllib.request; urllib.request.urlopen('https://127.0.0.1:8080/healthz', timeout=3, context=ssl._create_unverified_context()).close()"
  tenant_environment = var.tenant_profile && var.tenant_gateway != null ? [
    { name = "TENANT_HTTP_PROFILE", value = "true" },
    { name = "TENANT_PROXY_CIDRS", value = join(",", sort([for subnet in data.aws_subnet.tenant_alb : subnet.cidr_block])) },
    { name = "AUTH_ISSUER", value = var.tenant_gateway.auth_issuer },
    { name = "TENANT_TOKEN_PROFILE", value = var.tenant_gateway.token_profile },
    { name = "TENANT_REGISTRY_SECRET_ID", value = var.tenant_gateway.registry_secret_arn },
    { name = "TENANT_POOL_CAPACITY", value = tostring(var.tenant_gateway.pool_capacity) },
    { name = "TENANT_QUEUE_AGE_SECONDS", value = tostring(var.tenant_gateway.queue_age_seconds) },
    { name = "TENANT_ARTIFACT_PREFIXES_JSON", value = jsonencode(var.tenant_gateway.artifact_prefixes) },
  ] : []
  worker_tenant_environment = var.tenant_profile && try(var.tenant_gateway.callback_registry_secret_arn != null, false) ? [
    { name = "TENANT_CALLBACK_DESTINATIONS_SECRET_ID", value = var.tenant_gateway.callback_registry_secret_arn },
  ] : []
  tenant_routes = {
    submit = {
      priority = 10
      method   = "POST"
      paths    = ["/v2/events"]
      pattern  = "^/v2/events$"
    }
    read = {
      priority = 20
      method   = "GET"
      paths    = ["/v2/stage-runs/*", "/v2/jobs/*/stages", "/v2/jobs/*/history"]
      pattern  = "^/v2/(stage-runs/[^/]+|jobs/[^/]+/(stages|history))$"
    }
  }
}

module "tenant_client_auth" {
  count  = var.tenant_profile && var.tenant_gateway != null ? 1 : 0
  source = "./modules/alb-mtls"

  name = "${var.service_name}-${var.environment}"
  ca_bundle = {
    bucket  = var.tenant_gateway.trust_store_bucket
    key     = var.tenant_gateway.ca_bundle_key
    version = var.tenant_gateway.ca_bundle_object_version
  }
  revocation_lists = {
    primary = {
      bucket  = var.tenant_gateway.trust_store_bucket
      key     = var.tenant_gateway.revocation_list_key
      version = var.tenant_gateway.revocation_list_object_version
    }
  }
}

moved {
  from = aws_lb_trust_store.tenant[0]
  to   = module.tenant_client_auth[0].aws_lb_trust_store.this
}

moved {
  from = aws_lb_trust_store_revocation.tenant[0]
  to   = module.tenant_client_auth[0].aws_lb_trust_store_revocation.this["primary"]
}

resource "aws_lb_listener_rule" "tenant" {
  for_each = var.tenant_profile ? local.tenant_routes : {}

  listener_arn = aws_lb_listener.http.arn
  priority     = each.value.priority

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this.arn
  }

  condition {
    http_request_method {
      values = [each.value.method]
    }
  }

  condition {
    path_pattern {
      values = each.value.paths
    }
  }

  depends_on = [aws_wafv2_web_acl_association.tenant]
}

resource "aws_wafv2_web_acl" "tenant" {
  count = var.tenant_profile ? 1 : 0

  name  = "${var.service_name}-${var.environment}-tenant"
  scope = "REGIONAL"

  default_action {
    block {
      custom_response {
        response_code = 404
      }
    }
  }

  # Inspect names, including empty values and duplicates. Deny requests that
  # exceed the inspection limit so a reserved header cannot sit past it.
  rule {
    name     = "reject-client-certificate-headers"
    priority = 0

    action {
      block {}
    }

    statement {
      byte_match_statement {
        positional_constraint = "STARTS_WITH"
        search_string         = "x-amzn-mtls-"

        field_to_match {
          headers {
            match_pattern {
              all {}
            }
            match_scope       = "KEY"
            oversize_handling = "MATCH"
          }
        }

        text_transformation {
          priority = 0
          type     = "LOWERCASE"
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "RejectClientCertificateHeaders"
      sampled_requests_enabled   = false
    }
  }

  # Listener wildcard rules recognize candidate routes. These regexes enforce
  # exact segments before handing the request to the application.
  dynamic "rule" {
    for_each = local.tenant_routes

    content {
      name     = "allow-${rule.key}"
      priority = rule.value.priority

      action {
        allow {}
      }

      statement {
        and_statement {
          statement {
            byte_match_statement {
              positional_constraint = "EXACTLY"
              search_string         = rule.value.method

              field_to_match {
                method {}
              }

              text_transformation {
                priority = 0
                type     = "NONE"
              }
            }
          }

          statement {
            regex_match_statement {
              regex_string = rule.value.pattern

              field_to_match {
                uri_path {}
              }

              text_transformation {
                priority = 0
                type     = "NONE"
              }
            }
          }
        }
      }

      visibility_config {
        cloudwatch_metrics_enabled = true
        metric_name                = "Allow-${rule.key}"
        sampled_requests_enabled   = false
      }
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${var.service_name}-${var.environment}-tenant"
    sampled_requests_enabled   = false
  }
}

resource "aws_wafv2_web_acl_association" "tenant" {
  count        = var.tenant_profile ? 1 : 0
  resource_arn = aws_lb.this.arn
  web_acl_arn  = aws_wafv2_web_acl.tenant[0].arn
}

resource "aws_security_group" "tenant_worker" {
  count       = var.tenant_profile ? 1 : 0
  name_prefix = "${var.service_name}-${var.environment}-tenant-worker-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = var.db_port
    to_port     = var.db_port
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.this.cidr_block]
  }
}

resource "aws_cloudwatch_event_bus" "tenant" {
  count = var.tenant_profile ? 1 : 0
  name  = "${var.service_name}-${var.environment}-tenant-output"
}

resource "aws_cloudwatch_event_bus_policy" "tenant" {
  count          = var.tenant_profile ? 1 : 0
  event_bus_name = aws_cloudwatch_event_bus.tenant[0].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "RejectNonServicePublishers"
      Effect    = "Deny"
      Principal = "*"
      Action    = "events:PutEvents"
      Resource  = aws_cloudwatch_event_bus.tenant[0].arn
      Condition = {
        ArnNotEquals = { "aws:PrincipalArn" = aws_iam_role.tenant_worker[0].arn }
      }
    }]
  })
}

resource "aws_iam_role" "tenant_worker" {
  count              = var.tenant_profile ? 1 : 0
  name               = "${var.service_name}-${var.environment}-tenant-worker"
  assume_role_policy = data.aws_iam_policy_document.assume_ecs_tasks.json
}

resource "aws_iam_role_policy" "tenant_api" {
  count = var.tenant_profile && var.tenant_gateway != null ? 1 : 0
  name  = "tenant-api"
  role  = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = [var.db_password_secret_arn, var.tenant_gateway.backend_tls_secret_arn, var.tenant_gateway.registry_secret_arn]
    }]
  })
}

resource "aws_iam_role_policy" "tenant_worker" {
  count = var.tenant_profile && var.tenant_gateway != null ? 1 : 0
  name  = "tenant-worker"
  role  = aws_iam_role.tenant_worker[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["events:PutEvents", "events:DescribeEventBus"]
        Resource = aws_cloudwatch_event_bus.tenant[0].arn
      },
      {
        Effect = "Allow"
        Action = "secretsmanager:GetSecretValue"
        Resource = distinct(concat(compact([
          var.db_password_secret_arn, var.tenant_gateway.registry_secret_arn,
          var.tenant_gateway.callback_registry_secret_arn
        ]), sort(tolist(var.tenant_gateway.callback_signing_secret_arns))))
      }
    ]
  })
}
