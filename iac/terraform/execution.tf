# Reuse Foundry's consumer queue, DLQ, alarms and queue policy.
# Subscription keys are sorted here and compiled from the runtime catalog.
module "execution_queue" {
  source  = "git::https://github.com/ZGTR/comms-eventing.git//iac/modules/consumer_queue?ref=02a394333ec038fa4cf0ea79ffc5504b5bd79386"
  enabled = var.execution_enabled
  name    = "${var.service_name}-${var.environment}-execution"
  subscriptions = [
    for key in sort(keys(var.execution_subscriptions)) : {
      key             = key
      source_bus_name = var.execution_subscriptions[key].source_bus_name
      event_pattern   = var.execution_subscriptions[key].event_pattern
    }
  ]
  create_event_source_mapping = false
  function_timeout_seconds    = 60
}

locals {
  execution_environment = [
    { name = "AWS_REGION", value = var.aws_region },
    { name = "DB_BACKEND", value = "mysql" },
    { name = "DB_HOST", value = var.db_host },
    { name = "DB_PORT", value = tostring(var.db_port) },
    { name = "DB_NAME", value = var.db_name },
    { name = "DB_USER", value = var.db_user },
    { name = "DB_PASSWORD_SECRET_ID", value = var.db_password_secret_arn },
    { name = "AUTH_JWKS_URI", value = var.auth_jwks_uri },
    { name = "AUTH_AUDIENCE", value = var.auth_audience },
    { name = "WORKFLOW_CATALOG_PATH", value = var.workflow_catalog_path },
    { name = "WORKFLOW_EVENT_BUS", value = var.workflow_event_bus_name },
    { name = "WORKFLOW_EVENT_TENANTS", value = join(",", var.workflow_event_tenants) },
    { name = "WORKFLOW_INGRESS_QUEUE_URL", value = module.execution_queue.queue_url },
  ]
}

resource "aws_ecs_task_definition" "execution" {
  count                    = var.execution_enabled ? 1 : 0
  family                   = "${var.service_name}-${var.environment}-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn
  container_definitions = jsonencode([{
    name      = "execution"
    image     = var.image_uri
    essential = true
    command   = ["/opt/venv/bin/python", "-m", "shepherd_workflow_service.execution_main"]
    environment = concat(local.execution_environment, [
      { name = "SERVICE_ENV", value = var.environment },
      { name = "PRINCIPAL_ISSUER", value = "foundry-workflow" },
    ])
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.this.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "execution"
      }
    }
  }])
  lifecycle {
    precondition {
      condition = (
        length(var.execution_subscriptions) > 0 && length(var.workflow_event_tenants) > 0
        && var.workflow_event_bus_arn != "" && var.workflow_event_bus_name != ""
        && var.workflow_catalog_path != "" && var.db_host != ""
        && var.db_name != "" && var.db_user != "" && var.db_password_secret_arn != ""
        && var.auth_jwks_uri != "" && var.auth_audience != ""
      )
      error_message = "Execution requires subscriptions, tenants, bus, catalog, MySQL and signed auth."
    }
  }
}

resource "aws_ecs_service" "execution" {
  count           = var.execution_enabled ? 1 : 0
  name            = "${var.service_name}-${var.environment}-worker"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.execution[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"
  network_configuration {
    subnets          = data.aws_subnets.private.ids
    security_groups  = [aws_security_group.service.id]
    assign_public_ip = false
  }
}

resource "aws_iam_role_policy" "execution" {
  count = var.execution_enabled ? 1 : 0
  name  = "workflow-execution"
  role  = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["events:PutEvents", "events:DescribeEventBus"]
        Resource = var.workflow_event_bus_arn
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"]
        Resource = module.execution_queue.queue_arn
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = var.db_password_secret_arn
      },
    ]
  })
}
