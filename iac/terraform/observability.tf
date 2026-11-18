variable "execution_alarm_actions" {
  description = "SNS topic ARNs notified by execution alarms. Empty keeps alarms visible without paging."
  type        = list(string)
  default     = []
}

locals {
  execution_metric_dimensions = {
    Service     = "foundry-workflow"
    Environment = var.environment
  }
  execution_alarms = {
    outbox_age = { metric = "OldestOutboxAgeSeconds", threshold = 300, statistic = "Maximum", comparison = "GreaterThanThreshold", missing = "notBreaching" }
    recovery   = { metric = "OverdueStages", threshold = 0, statistic = "Minimum", comparison = "GreaterThanThreshold", missing = "notBreaching" }
    failures   = { metric = "WorkerCycleFailures", threshold = 0, statistic = "Sum", comparison = "GreaterThanThreshold", missing = "notBreaching" }
    heartbeat  = { metric = "WorkerHeartbeat", threshold = 1, statistic = "Sum", comparison = "LessThanThreshold", missing = "breaching" }
  }
}

resource "aws_cloudwatch_metric_alarm" "execution" {
  for_each            = var.execution_enabled ? local.execution_alarms : {}
  alarm_name          = "${var.service_name}-${var.environment}-${each.key}"
  alarm_description   = "Workflow ${each.key}; see docs/operations.md for diagnosis and recovery."
  namespace           = "Foundry/Workflow"
  metric_name         = each.value.metric
  dimensions          = local.execution_metric_dimensions
  comparison_operator = each.value.comparison
  threshold           = each.value.threshold
  statistic           = each.value.statistic
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 3
  treat_missing_data  = each.value.missing
  alarm_actions       = var.execution_alarm_actions
  ok_actions          = var.execution_alarm_actions
}

resource "aws_cloudwatch_dashboard" "execution" {
  count          = var.execution_enabled ? 1 : 0
  dashboard_name = "${var.service_name}-${var.environment}-execution"
  dashboard_body = jsonencode({
    widgets = [
      for index, group in [
        { title = "Worker health", metrics = ["WorkerHeartbeat", "WorkerCycleFailures"] },
        { title = "Delivery backlog", metrics = ["PendingOutbox", "OldestOutboxAgeSeconds"] },
        { title = "Stage state", metrics = ["RunningStages", "RetryWaitingStages", "FailedStages", "ExpiredStages", "OverdueStages"] },
        ] : {
        type = "metric", x = 0, y = index * 6, width = 24, height = 6
        properties = {
          title   = group.title, region = var.aws_region, period = 60, stat = "Maximum"
          metrics = [for name in group.metrics : ["Foundry/Workflow", name, "Service", "foundry-workflow", "Environment", var.environment]]
        }
      }
    ]
  })
}
