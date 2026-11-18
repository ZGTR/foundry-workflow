output "alb_dns_name" {
  value = aws_lb.this.dns_name
}

output "ecs_service_name" {
  value = aws_ecs_service.this.name
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "execution_service_name" {
  value = var.execution_enabled ? aws_ecs_service.execution[0].name : null
}

output "execution_task_definitions" {
  value = var.execution_enabled ? [aws_ecs_task_definition.this.arn, aws_ecs_task_definition.execution[0].arn] : []
}
