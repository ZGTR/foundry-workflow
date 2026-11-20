terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.30.0"
    }
  }
}

variable "name" {
  type        = string
  description = "Name of the ALB client certificate trust store."
}

variable "ca_bundle" {
  description = "Versioned S3 object containing PEM client CA certificates."
  type = object({
    bucket  = string
    key     = string
    version = string
  })
  validation {
    condition = alltrue([
      for value in [var.ca_bundle.bucket, var.ca_bundle.key, var.ca_bundle.version] :
      trimspace(value) != "" && value != "null"
    ])
    error_message = "The CA bundle must refer to a specific version in a versioned S3 bucket."
  }
}

variable "revocation_lists" {
  description = "Versioned S3 objects holding issuer-signed PEM certificate revocation lists."
  type = map(object({
    bucket  = string
    key     = string
    version = string
  }))
  validation {
    condition = length(var.revocation_lists) > 0 && alltrue(flatten([
      for crl in values(var.revocation_lists) : [
        for value in [crl.bucket, crl.key, crl.version] :
        trimspace(value) != "" && value != "null"
      ]
    ]))
    error_message = "Provide at least one CRL and pin every CRL to a specific S3 object version."
  }
}

resource "aws_lb_trust_store" "this" {
  name                                     = var.name
  ca_certificates_bundle_s3_bucket         = var.ca_bundle.bucket
  ca_certificates_bundle_s3_key            = var.ca_bundle.key
  ca_certificates_bundle_s3_object_version = var.ca_bundle.version
}

resource "aws_lb_trust_store_revocation" "this" {
  for_each = var.revocation_lists

  trust_store_arn               = aws_lb_trust_store.this.arn
  revocations_s3_bucket         = each.value.bucket
  revocations_s3_key            = each.value.key
  revocations_s3_object_version = each.value.version

  lifecycle {
    create_before_destroy = true
  }
}

output "listener_authentication" {
  description = "Attributes for the HTTPS listener mutual_authentication block."

  value = {
    mode                             = "verify"
    trust_store_arn                  = aws_lb_trust_store.this.arn
    ignore_client_certificate_expiry = false
  }
  depends_on = [aws_lb_trust_store_revocation.this]
}

output "revocation_list_versions" {
  description = "Pinned CRL versions attached to the trust store."
  value       = { for issuer, resource in aws_lb_trust_store_revocation.this : issuer => resource.revocations_s3_object_version }
}
