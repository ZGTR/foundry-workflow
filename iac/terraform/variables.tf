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

variable "tenant_profile" {
  description = "Enable the ECS HTTPS-only tenant profile on a dedicated ledger and deployment."
  type        = bool
  default     = false
}

variable "tenant_gateway" {
  description = "Tenant gateway inputs. Pin the S3 CA bundle and CRL versions and supply Secrets Manager ARNs for the workload registry and backend TLS key pair."
  type = object({
    alb_subnet_ids                 = set(string)
    trust_store_bucket             = string
    ca_bundle_key                  = string
    ca_bundle_object_version       = string
    revocation_list_key            = string
    revocation_list_object_version = string
    backend_tls_secret_arn         = string
    registry_secret_arn            = string
    auth_issuer                    = string
    token_profile                  = optional(string, "rfc9068")
    pool_capacity                  = optional(number, 16)
    queue_age_seconds              = optional(number, 60)
    artifact_prefixes              = optional(map(list(string)), {})
    callback_registry_secret_arn   = optional(string)
    callback_signing_secret_arns   = optional(set(string), [])
  })
  default = null

  validation {
    condition = var.tenant_gateway == null ? true : (
      length(var.tenant_gateway.alb_subnet_ids) >= 2
      && startswith(var.tenant_gateway.auth_issuer, "https://")
      && contains(["rfc9068", "cognito"], var.tenant_gateway.token_profile)
      && var.tenant_gateway.pool_capacity >= 1
      && floor(var.tenant_gateway.pool_capacity) == var.tenant_gateway.pool_capacity
      && var.tenant_gateway.queue_age_seconds >= 1 && var.tenant_gateway.queue_age_seconds <= 604800
      && floor(var.tenant_gateway.queue_age_seconds) == var.tenant_gateway.queue_age_seconds
      && alltrue([
        for value in [
          var.tenant_gateway.trust_store_bucket, var.tenant_gateway.ca_bundle_key,
          var.tenant_gateway.ca_bundle_object_version, var.tenant_gateway.revocation_list_key,
          var.tenant_gateway.revocation_list_object_version
        ] : trimspace(value) != "" && value != "null"
      ])
      && var.tenant_gateway.backend_tls_secret_arn != ""
      && var.tenant_gateway.registry_secret_arn != ""
      && (var.tenant_gateway.callback_registry_secret_arn == null
        ? length(var.tenant_gateway.callback_signing_secret_arns) == 0
        : var.tenant_gateway.callback_registry_secret_arn != ""
      )
      && alltrue([
        for arn in concat(compact([
          var.tenant_gateway.backend_tls_secret_arn, var.tenant_gateway.registry_secret_arn,
          var.tenant_gateway.callback_registry_secret_arn
        ]), tolist(var.tenant_gateway.callback_signing_secret_arns)) : can(regex("^arn:aws[^:]*:secretsmanager:[^:*]+:[0-9]{12}:secret:[^*?]+$", arn))
      ])
    )
    error_message = "Configure at least two ALB subnets, pinned CA and CRL versions, an HTTPS issuer, a valid token profile and capacity, and exact Secrets Manager ARNs."
  }
}
