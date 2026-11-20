# ECS Fargate target: the same ECR image the Lambda target in
# ../serverless.yml deploys. `var.image_uri` is the shared seam (plan
# section 8) -- it must be a digest pin (repo@sha256:...), never a floating
# tag, so promotion between environments is "point at this digest," not a
# rebuild.
#
# Not applied by this template -- see README.md, and nothing here creates
# infrastructure until a service points a real backend at it.
#
# It IS validated, which it previously was not. This header used to say that
# `terraform validate` could not be run in the authoring environment and that
# the file had been hand-checked against the provider's published schema
# instead -- a hand-check that reads exactly like a machine check in the diff.
# `scripts/check_template_iac.sh` now runs `init -backend=false`, `validate`
# and `fmt -check` for real, in a COPY of this directory so the run cannot
# rewrite `.terraform.lock.hcl`, and the same script is the workflow's `iac`
# job. Running it the first time found a genuine defect one file over: the
# workflow's `serverless deploy` could never have worked.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- networking: bring your own VPC per environment (plan section 8: "each
# environment names its own account, its own VPC") ------------------------

data "aws_vpc" "this" {
  id = var.vpc_id
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  tags = {
    Tier = "private"
  }
}

# The public ALB needs public subnets; API and worker tasks remain private.
data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  tags = {
    Tier = "public"
  }
}

# --- ECS cluster + task + service -----------------------------------------

resource "aws_ecs_cluster" "this" {
  name = "${var.service_name}-${var.environment}"
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${var.service_name}-${var.environment}"
  retention_in_days = 14
}

resource "aws_ecs_task_definition" "this" {
  family                   = "${var.service_name}-${var.environment}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = var.service_name
      image     = var.image_uri # the shared seam with iac/serverless.yml
      essential = true
      portMappings = [
        { containerPort = 8080, protocol = "tcp" }
      ]
      environment = concat(var.execution_enabled ? local.execution_runtime_environment : [], local.tenant_environment, var.tenant_profile && var.tenant_gateway != null ? [
        { name = "TENANT_TLS_SECRET_ID", value = var.tenant_gateway.backend_tls_secret_arn },
        ] : [], [
        { name = "SERVICE_ENV", value = var.environment },
        { name = "PRINCIPAL_ISSUER", value = "foundry-workflow" },
        # EVENT_BUS_MODE deliberately absent -- see the note in serverless.yml:
        # SQS_QUEUE_URL, not this variable, selects the ledger
        # drain's downstream, and no composition root reads config.event_bus_mode.
        { name = "PORT", value = "8080" },
      ])
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.this.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
      healthCheck = {
        command     = ["CMD", "/opt/venv/bin/python", "-c", var.tenant_profile ? local.tenant_health_check : "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz', timeout=3).close()"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 10
      }
    }
  ])

  lifecycle {
    precondition {
      condition = !var.tenant_profile || (
        var.execution_enabled && var.tenant_gateway != null
        && var.sqs_queue_arn == "" && length(var.workflow_event_tenants) == 0
        && length(var.execution_subscriptions) == 0
        && var.workflow_event_bus_name == "" && var.workflow_event_bus_arn == ""
        && startswith(var.auth_jwks_uri, "https://")
        && can(regex("^arn:aws[^:]*:secretsmanager:[^:*]+:[0-9]{12}:secret:[^*?]+$", var.db_password_secret_arn))
      )
      error_message = "The tenant profile requires its gateway, execution worker, HTTPS JWKS and a database secret ARN, with all legacy event bus, SQS and subscription inputs empty."
    }
  }
}

resource "aws_ecs_service" "this" {
  name            = "${var.service_name}-${var.environment}"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = data.aws_subnets.private.ids
    security_groups  = [aws_security_group.service.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.this.arn
    container_name   = var.service_name
    container_port   = 8080
  }

  depends_on = [aws_lb_listener.http, aws_lb_listener_rule.tenant, aws_iam_role_policy.tenant_api]
}

# --- ALB + target group -----------------------------------------------

resource "aws_security_group" "service" {
  name_prefix = "${var.service_name}-${var.environment}-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "alb" {
  name_prefix = "${var.service_name}-${var.environment}-alb-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb" "this" {
  name                       = "${var.service_name}-${var.environment}"
  internal                   = false
  load_balancer_type         = "application"
  security_groups            = [aws_security_group.alb.id]
  subnets                    = var.tenant_profile && var.tenant_gateway != null ? var.tenant_gateway.alb_subnet_ids : data.aws_subnets.public.ids
  desync_mitigation_mode     = var.tenant_profile ? "strictest" : "defensive"
  drop_invalid_header_fields = var.tenant_profile
  enable_waf_fail_open       = false
  client_keep_alive          = var.tenant_profile ? 60 : null

  lifecycle {
    precondition {
      condition = !var.tenant_profile || (
        length(toset([for subnet in data.aws_subnet.tenant_alb : subnet.availability_zone])) >= 2
        && alltrue([for subnet in data.aws_subnet.tenant_alb : subnet.vpc_id == var.vpc_id])
      )
      error_message = "The tenant ALB requires subnets in at least two availability zones in its VPC."
    }
  }
}

resource "aws_lb_target_group" "this" {
  name        = "${var.service_name}-${var.environment}"
  port        = 8080
  protocol    = var.tenant_profile ? "HTTPS" : "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    protocol            = var.tenant_profile ? "HTTPS" : "HTTP"
    path                = "/readyz" # readiness, not liveness -- an ALB should drain, not just detect a dead process
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
    timeout             = 5
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = var.certificate_arn

  ssl_policy = var.tenant_profile ? "ELBSecurityPolicy-TLS13-1-2-2021-06" : null

  dynamic "mutual_authentication" {
    for_each = module.tenant_client_auth[*].listener_authentication
    content {
      mode                             = mutual_authentication.value.mode
      trust_store_arn                  = mutual_authentication.value.trust_store_arn
      ignore_client_certificate_expiry = mutual_authentication.value.ignore_client_certificate_expiry
    }
  }

  default_action {
    type             = var.tenant_profile ? "fixed-response" : "forward"
    target_group_arn = var.tenant_profile ? null : aws_lb_target_group.this.arn
    dynamic "fixed_response" {
      for_each = var.tenant_profile ? [1] : []
      content {
        content_type = "text/plain"
        status_code  = "404"
      }
    }
  }
}

# --- IAM --------------------------------------------------------------

data "aws_iam_policy_document" "assume_ecs_tasks" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.service_name}-${var.environment}-execution"
  assume_role_policy = data.aws_iam_policy_document.assume_ecs_tasks.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name               = "${var.service_name}-${var.environment}-task"
  assume_role_policy = data.aws_iam_policy_document.assume_ecs_tasks.json
}

resource "aws_iam_role_policy" "task_sqs" {
  count = !var.tenant_profile && var.sqs_queue_arn != "" ? 1 : 0
  name  = "outbox-publish"
  role  = aws_iam_role.task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = var.sqs_queue_arn
      }
    ]
  })
}
