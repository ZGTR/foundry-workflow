mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_data "aws_vpc" {
    defaults = {
      cidr_block = "10.0.0.0/16"
    }
  }
  mock_data "aws_subnets" {
    defaults = {
      ids = ["subnet-01010101010101010", "subnet-02020202020202020"]
    }
  }
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::123456789012:role/workflow-test"
    }
  }
  mock_resource "aws_lb_target_group" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/workflow-test/1111111111111111"
    }
  }
  mock_resource "aws_lb" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/workflow-test/1111111111111111"
    }
  }
  mock_resource "aws_lb_listener" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/workflow-test/1111111111111111/2222222222222222"
    }
  }
  mock_resource "aws_lb_trust_store" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:truststore/workflow-test/1111111111111111"
    }
  }
  mock_resource "aws_wafv2_web_acl" {
    defaults = {
      arn = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/workflow-test/11111111-1111-1111-1111-111111111111"
    }
  }
  mock_resource "aws_cloudwatch_event_bus" {
    defaults = {
      arn = "arn:aws:events:us-east-1:123456789012:event-bus/workflow-test-tenant-output"
    }
  }
}

variables {
  environment     = "test"
  aws_region      = "us-east-1"
  vpc_id          = "vpc-01010101010101010"
  image_uri       = "123456789012.dkr.ecr.us-east-1.amazonaws.com/workflow@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/11111111-1111-1111-1111-111111111111"
}

run "legacy_default" {
  command = plan

  assert {
    condition = (
      aws_lb_target_group.this.protocol == "HTTP"
      && aws_lb_listener.http.default_action[0].type == "forward"
      && length(aws_lb_listener_rule.tenant) == 0
      && length(aws_wafv2_web_acl.tenant) == 0
      && length(aws_cloudwatch_event_bus.tenant) == 0
    )
    error_message = "The tenant profile is opt-in."
  }
}

run "verified_private_origin" {
  command = apply

  variables {
    tenant_profile         = true
    execution_enabled      = true
    workflow_catalog_path  = "/app/contracts/catalog.v2.json"
    db_host                = "workflow.example.invalid"
    db_name                = "tenant_workflow"
    db_user                = "tenant_workflow"
    db_password_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:mysql-test"
    auth_jwks_uri          = "https://issuer.example.invalid/jwks"
    auth_audience          = "workflow"
    tenant_gateway = {
      alb_subnet_ids                 = ["subnet-01010101010101010", "subnet-02020202020202020"]
      trust_store_bucket             = "workflow-tenant-trust-store"
      ca_bundle_key                  = "ca.pem"
      ca_bundle_object_version       = "ca-version"
      revocation_list_key            = "crl.pem"
      revocation_list_object_version = "crl-version"
      backend_tls_secret_arn         = "arn:aws:secretsmanager:us-east-1:123456789012:secret:tls-test"
      registry_secret_arn            = "arn:aws:secretsmanager:us-east-1:123456789012:secret:registry-test"
      auth_issuer                    = "https://issuer.example.invalid"
      queue_age_seconds              = 45
    }
  }

  override_data {
    target = data.aws_subnet.tenant_alb["subnet-01010101010101010"]
    values = {
      availability_zone = "us-east-1a"
      cidr_block        = "10.0.1.0/24"
      vpc_id            = "vpc-01010101010101010"
    }
  }

  override_data {
    target = data.aws_subnet.tenant_alb["subnet-02020202020202020"]
    values = {
      availability_zone = "us-east-1b"
      cidr_block        = "10.0.2.0/24"
      vpc_id            = "vpc-01010101010101010"
    }
  }

  assert {
    condition = (
      aws_lb_listener.http.mutual_authentication[0].mode == "verify"
      && aws_lb_listener.http.mutual_authentication[0].ignore_client_certificate_expiry == false
      && module.tenant_client_auth[0].revocation_list_versions["primary"] == "crl-version"
      && aws_lb_listener.http.default_action[0].type == "fixed-response"
      && aws_lb_target_group.this.protocol == "HTTPS"
      && aws_lb_target_group.this.health_check[0].protocol == "HTTPS"
      && !aws_ecs_service.this.network_configuration[0].assign_public_ip
      && one(aws_security_group.service.ingress).security_groups == toset([aws_security_group.alb.id])
      && length(aws_wafv2_web_acl_association.tenant) == 1
      && !aws_lb.this.enable_waf_fail_open
      && length(aws_lb_listener_rule.tenant) == 2
    )
    error_message = "The tenant gateway requires mTLS verification, a fail-closed WAF and an HTTPS-only private origin."
  }

  assert {
    condition = (
      module.execution_queue.queue_arn == ""
      && length(aws_iam_role_policy.execution) == 0
      && length(aws_iam_role_policy.task_sqs) == 0
      && !strcontains(aws_ecs_task_definition.execution[0].container_definitions, "WORKFLOW_INGRESS_QUEUE_URL")
      && !strcontains(aws_iam_role_policy.tenant_worker[0].policy, "sqs:")
      && jsondecode(aws_cloudwatch_event_bus_policy.tenant[0].policy).Statement[0].Effect == "Deny"
      && length(aws_security_group.tenant_worker[0].ingress) == 0
    )
    error_message = "An HTTPS-only worker cannot receive shared ingress or publish to a caller-supplied bus."
  }

  assert {
    condition = alltrue([
      for definition in [aws_ecs_task_definition.this, aws_ecs_task_definition.execution[0]] :
      contains(jsondecode(definition.container_definitions)[0].environment, {
        name  = "TENANT_QUEUE_AGE_SECONDS"
        value = "45"
      })
    ])
    error_message = "Tenant API and worker must use the same configured queue-age threshold."
  }
}
