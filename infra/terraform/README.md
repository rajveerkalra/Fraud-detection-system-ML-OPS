# Terraform (AWS) — cost-conscious cloud demo (dry-run first)

This folder contains Terraform to deploy a **minimal, demo-friendly** version of the project on AWS while minimizing surprise costs.

For the current planning phase, use:

- `infra/AWS_DRY_RUN_PLAN.md` (architecture, costs, deployment checklist)
- `terraform plan` only (no apply)

## Why “minimal” for AWS free tier?

AWS **Free Tier** does **not** cover most managed streaming components you’d want for a “real” architecture (MSK, Managed Flink, ElastiCache). Those can become expensive if left running.

So this Terraform is structured as:

- **Always-on cheap demo** (default): deploy only the **`model-service`** on **ECS Fargate** with a **public IP** (no load balancer).
- **Optional modules**: ECR repos, S3 bucket for MLflow artifacts, etc.
- **Future upgrade path**: add MSK / Flink / Redis when you’re ready to do a short-lived “big demo” run.

## What gets created (modular default)

- ECS cluster (Fargate) for `model-service`
- EC2 host for Kafka/Flink/Redis/MLflow/Prometheus/Grafana (cost-aware demo mode)
- Security groups
- CloudWatch log group
- Optional ECR repo(s)
- Optional S3 bucket for MLflow artifacts
- Optional AWS Budget alerts

## Layout

- Module: `infra/terraform/modules/cost_safe_stack`
- Environments:
  - `infra/terraform/envs/dev`
  - `infra/terraform/envs/staging`

Each environment has `terraform.tfvars.example` for safe configuration.

## Dry-run quickstart (dev)

From `infra/terraform/envs/dev`:

```bash
terraform init
terraform plan -var-file=terraform.tfvars
```

Do not run `terraform apply` until cost/budget checks are completed.

## Tear down (when applied)

```bash
bash ../../scripts/teardown_all.sh
```

