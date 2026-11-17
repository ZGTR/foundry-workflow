variable "service_name" {
  type    = string
  default = "foundry-workflow"
}

variable "environment" {
  description = "e.g. dev, staging, prod -- each environment owns its own state backend (plan section 8)"
  type        = string
}

variable "aws_region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "image_uri" {
  description = "The ECR image digest (repo@sha256:...) -- the shared seam with iac/serverless.yml. Never a floating tag."
  type        = string

  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image_uri))
    error_message = "image_uri must be a digest pin (repo@sha256:<64 hex chars>), not a floating tag."
  }
}

variable "certificate_arn" {
  description = "ACM certificate for the HTTPS listener"
  type        = string
}

variable "desired_count" {
  type    = number
  default = 2
}

variable "task_cpu" {
  type    = number
  default = 512
}

variable "task_memory" {
  type    = number
  default = 1024
}

variable "sqs_queue_arn" {
  description = "The legacy outbox relay downstream; unused for EventBridge execution."
  type        = string
  default     = ""
}

variable "execution_enabled" {
  type    = bool
  default = false
}

variable "workflow_catalog_path" {
  type    = string
  default = ""
}

variable "workflow_event_bus_name" {
  type    = string
  default = ""
}

variable "workflow_event_bus_arn" {
  type    = string
  default = ""
}

variable "workflow_event_tenants" {
  type    = list(string)
  default = []
}

variable "db_host" {
  type    = string
  default = ""
}

variable "db_port" {
  type    = number
  default = 3306
}

variable "db_name" {
  type    = string
  default = ""
}

variable "db_user" {
  type    = string
  default = ""
}

variable "db_password_secret_arn" {
  type    = string
  default = ""
}

variable "auth_jwks_uri" {
  type    = string
  default = ""
}

variable "auth_audience" {
  type    = string
  default = ""
}

variable "execution_subscriptions" {
  type = map(object({
    source_bus_name = string
    event_pattern   = string
  }))
  default = {}
}
