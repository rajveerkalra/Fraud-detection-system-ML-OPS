# Terraform (AWS) — cost-conscious cloud demo

This folder contains Terraform to deploy a **minimal, demo-friendly** version of the project on AWS while minimizing surprise costs.

## Why “minimal” for AWS free tier?

AWS **Free Tier** does **not** cover most managed streaming components you’d want for a “real” architecture (MSK, Managed Flink, ElastiCache). Those can become expensive if left running.

So this Terraform is structured as:

- **Always-on cheap demo** (default): deploy only the **`model-service`** on **ECS Fargate** with a **public IP** (no load balancer).
- **Optional modules**: ECR repos, S3 bucket for MLflow artifacts, etc.
- **Future upgrade path**: add MSK / Flink / Redis when you’re ready to do a short-lived “big demo” run.

## What gets created (default)

- ECS cluster (Fargate)
- ECS task + service for `model-service` (public IP)
- Security group allowing inbound TCP 8000
- CloudWatch log group
- Optional: ECR repo(s) for container images

## Quickstart (dev)

From `infra/terraform/envs/dev`:

```bash
terraform init
terraform plan
terraform apply
```

Then use the output `model_service_public_ip`:

```bash
curl "http://<public-ip>:8000/health"
```

## Tear down (important)

```bash
terraform destroy
```

