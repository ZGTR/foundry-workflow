# Supply bucket, region, environment-specific key and lock table at init.
# Validation uses -backend=false and never reads cloud state.
terraform {
  backend "s3" {}
}
