#!/usr/bin/env bash
set -euo pipefail

# SAFE TEARDOWN SCRIPT
# Destroys Terraform-managed resources for both dev and staging.
# Usage:
#   bash infra/scripts/teardown_all.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

destroy_env() {
  local env_dir="$1"
  if [[ ! -d "$env_dir" ]]; then
    return
  fi
  echo "[teardown] env: $env_dir"
  (
    cd "$env_dir"
    if [[ -f ".terraform.lock.hcl" ]]; then
      terraform destroy -auto-approve
    else
      echo "[teardown] skipping (terraform init not run): $env_dir"
    fi
  )
}

destroy_env "$ROOT_DIR/infra/terraform/envs/staging"
destroy_env "$ROOT_DIR/infra/terraform/envs/dev"

echo "[teardown] complete"
