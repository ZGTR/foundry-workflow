# Mutual TLS for service APIs

This guide defines an inbound HTTPS profile for service-to-service calls. It
specifies certificate issuance, listener configuration, application authorization,
rotation and release verification. The [Terraform module](../iac/terraform/modules/alb-mtls/main.tf)
implements the AWS Application Load Balancer (ALB) profile. Services using a
different gateway can apply the same controls at their TLS termination point.
For a callback protected by mTLS, the callback receiver applies the same
profile and the originating service presents its own client certificate.

## 1. Required behavior

- Clients **must** verify the server certificate chain and service hostname.
  Gateways **must** require a client certificate, validate its chain and validity
  period, and enforce revocation. Set TLS 1.2 as the minimum and support TLS 1.3.
  [TLS 1.3](https://www.rfc-editor.org/rfc/rfc8446.html) · [Service identity](https://www.rfc-editor.org/rfc/rfc9525.html)
- Issue a separate client key pair and certificate to each workload and
  environment. Put the service hostname in the server certificate's DNS SAN.
  Issue client leaves with the client-auth extended key usage and server leaves
  with server-auth. Store private keys in the workload's secret store or
  keystore. Keep CA signing keys with the PKI operator. [X.509 PKI profile](https://www.rfc-editor.org/rfc/rfc5280.html)
- Each service **must** resolve a verified client to an active workload
  registration and check its permissions on every operation and object.
  Multi-tenant services **must** resolve tenant ownership from that registration.
  The request body, headers and resource IDs carry no authority to select a
  different tenant.
- Services using OAuth **should** require short-lived, audience-scoped access
  tokens. With RFC 8705, compare the token's `cnf.x5t#S256` to the unpadded
  base64url SHA-256 digest of the verified leaf in DER form. When a supported
  issuer cannot issue certificate-bound tokens, explicitly require that the
  verified certificate and token map independently to the same workload.
  [OAuth mTLS and certificate-bound tokens](https://www.rfc-editor.org/rfc/rfc8705.html)
- If a proxy terminates TLS, the origin **must** accept certificate metadata
  only from its trusted proxy path. Reject caller-supplied protected headers
  before injecting proxy-generated identity. The origin must reject duplicate
  identity headers, check the immediate peer, and deny alternate ingress.
  Privately route origin traffic and restrict who can attach the proxy's
  network identity or update its routes.
- Verify the identity of the origin over the backend connection when the
  network boundary requires it. AWS ALB HTTPS targets receive encrypted
  traffic, and ALB does not validate target certificates. That configuration
  relies on private routing and security-group identity for the backend
  hop. Use a gateway that verifies backend certificates when the trust
  boundary requires cryptographic backend identity.
  [AWS ALB target TLS behavior](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html#target-group-routing-configuration)

## 2. Provision the trust material

1. Get a server certificate provisioned for the service hostname. For ALB,
   place it in AWS Certificate Manager in the listener's region.
2. Have the approved PKI issue client leaf certificates. Export a PEM bundle
   of the CA certificates that should authenticate clients. Publish a current
   issuer-signed PEM revocation list for each issuing CA in the trust profile,
   including an empty list for an issuer with no revocations yet. Verify issuer,
   signature, update timestamps and next-update deadlines before publishing.
3. Upload the CA bundle and CRLs to a private, versioned S3 bucket for ALB
   trust material. Restrict object writes to the PKI or release role, and
   allow the deployment to load the pinned versions. Capture each S3 object
   version ID. Keep private keys, access tokens, and workload credentials
   in secret storage. Terraform receives object coordinates and secret
   references. [AWS ALB CA and CRL requirements](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/mutual-authentication.html)

To get the leaf thumbprint for a workload registration or token comparison,
hash the DER-encoded certificate.

```sh
openssl x509 -in workload.pem -outform DER |
  openssl dgst -sha256 -binary |
  openssl base64 -A |
  tr '+/' '-_' |
  tr -d '='
```

If the deployment owns trust-material publication, manage each public PEM
artifact with an `aws_s3_object` in the pre-provisioned versioned bucket. Use
the object's `bucket`, `key` and `version_id` as the module's `bucket`, `key`
and `version` inputs. Set `source_hash` to `filesha256()` of the reviewed PEM
so a content change publishes a new object version. If the PKI team owns
publication, pass the version IDs from its release manifest to Terraform.

## 3. Configure a listener in Terraform

Copy or vendor the [ALB mTLS module](../iac/terraform/modules/alb-mtls/main.tf)
at a reviewed revision. Pass existing public CA and CRL object versions.
Keep your listener's normal server certificate and owned route configuration.

```hcl
module "client_auth" {
  source = "./modules/alb-mtls"

  name      = "orders-staging"
  ca_bundle = var.client_ca_bundle
  revocation_lists = {
    workload_issuer = var.workload_issuer_crl
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.service.arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = var.server_certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  mutual_authentication {
    mode                             = module.client_auth.listener_authentication.mode
    trust_store_arn                  = module.client_auth.listener_authentication.trust_store_arn
    ignore_client_certificate_expiry = module.client_auth.listener_authentication.ignore_client_certificate_expiry
  }

  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "text/plain"
      status_code  = "404"
    }
  }
}
```

Each input object has `bucket`, `key`, and `version` fields. The module
requires pinned versions and at least one CRL. The listener consumes an
output that depends on the CRL attachments. Add forwarding rules for your
service's approved methods and routes. Configure the gateway header boundary
before opening those routes. On ALB, verify mode supplies the encoded
`X-Amzn-Mtls-Clientcert-Leaf` header. Foundry pairs it with WAF header
rejection and a private backend security group. [Foundry Terraform example](../iac/terraform/tenant.tf) · [AWS ALB mTLS headers](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/mutual-authentication.html)

For the existing Foundry deployment, use the tenant profile and the
`tenant_gateway` inputs listed in the [staging runbook](operations.md#tenant-ecs-staging).
Any existing Foundry trust store and CRL retain their Terraform state mapping
through the declared moves. Review the plan before applying.

## 4. Verify before directing client traffic

Use the [standalone verifier](../deploy/standalone/verify_mtls.py) against an
owned staging endpoint. It uses Python's standard library and creates a fresh
TLS connection per request. The default server trust is the system CA store.
Use `--server-ca` for a private service CA. Client-certificate refusal probes
use TLS 1.2 to capture the handshake alert before any HTTP request. Successful
requests negotiate the highest TLS version supported by both peers.

```sh
python3 deploy/standalone/verify_mtls.py \
  https://orders.staging.example.com/v2/jobs/owned/history \
  --client-cert /run/mtls/tenant-a.pem --client-key /run/mtls/tenant-a.key \
  --token-file /run/mtls/tenant-a.token \
  --untrusted-client /run/mtls/untrusted.pem /run/mtls/untrusted.key \
  --expired-client /run/mtls/expired.pem /run/mtls/expired.key \
  --revoked-client /run/mtls/revoked.pem /run/mtls/revoked.key \
  --mismatched-client /run/mtls/tenant-b.pem /run/mtls/tenant-b.key \
  --foreign-path /v2/jobs/other-tenant/history --missing-path /v2/jobs/unknown/history
```

Use a real owned resource and actual staging fixtures. Require success for
both tenants on their own resources. Verify refusal for missing, untrusted,
expired and revoked client certificates at TLS. Check a valid certificate
with the other tenant's token at the application boundary. Verify equivalent
404 responses for missing and foreign resources. Also test forged and
duplicate identity headers, origin attempts outside the gateway path,
credential rotation, and revocation of an already connected client. If the
edge closes a failed handshake without a client-auth alert, corroborate the
rejection in gateway connection logs. The script reports ambiguous failures
as failures. [AWS ALB connection logs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-connection-logs.html)

The [local integration test](../tests/integration/test_mtls_transport.py)
uses real TLS sockets, separate server and client CAs, a signed CRL, and the
Foundry API behind a reference proxy. Each tenant submits, reads, and completes
its own jobs. The test verifies the private origin's peer check and rejects
cross-tenant reads and results. It also reloads the CRL and checks revocation
on fresh connections, then verifies the application registry blocks further
requests on an already-open connection at its refresh boundary.

```sh
PYTHONPATH=.:src:vendor python -m pytest tests/integration/test_mtls_transport.py
```

The staging checks exercise the deployed gateway and its network and route
policies and measure propagation from the actual AWS revocation update.

## 5. Rotate and revoke

For routine rotation, register the replacement certificate alongside the
existing certificate under the same workload. Deploy the replacement to the
client and verify with a fresh connection. Revoke and remove the old leaf after
the overlap. For a CA rollover, attach the new CA chain and its CRL, verify
both issuing paths during the overlap, then remove the old path. Refresh
CRLs before their next-update deadline and pin each published version in IaC.

For suspected compromise, deactivate the application workload grant and
publish a new CRL version for its certificate. Apply the update and measure
time to rejection at both layers. Terminate or drain established sessions
when your risk window requires it. Keep issuer, serial number, expiry,
workload ID and policy version in restricted audit records. Exclude private
keys, full bearer tokens and certificate headers from ordinary logs. Roll
back application routing with the current revocation policy intact.
