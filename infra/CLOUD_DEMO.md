## AWS cloud demo (Free Tier aware)

This project is **free-tier conscious**, but important reality:

- **MSK / Managed Flink / ElastiCache** are **not** meaningfully covered by AWS Free Tier.
- If you deploy those and leave them on, you can get billed.

So we provide a **two-stage cloud story**:

### Stage 1 (cheap-ish, always-on demo)
Deploy only the **`model-service`** on ECS Fargate with a public IP (no ALB).

Terraform: `infra/terraform/envs/dev`

#### Rough cost (very approximate)
Depends on region and usage, but **Fargate is not Free Tier** (on most accounts). Expect on the order of:
- **$10–$30/mo** if left running continuously (tiny task), plus CloudWatch logs.

If you truly must stay near $0, keep AWS off and use the local stack + screenshots/video.

### Stage 2 (short-lived “big architecture” demo)
Spin up for **a few hours**, record a demo, then destroy:
- MSK (Kafka)
- Managed Flink
- ElastiCache Redis

This looks great in interviews, but it’s not free-tier.

## Deploy (Stage 1) steps

### Cost notes (Stage 1)

- **ECR storage**: small, usually cents–a few dollars/month depending on size.
- **CloudWatch Logs**: small for demos, but can grow with traffic.
- **ECS Fargate**: pay-per-second; not generally “free-tier”.

If you want to keep costs extremely low:

- deploy for a few hours to record a demo and then destroy
- avoid load balancers/NAT gateways (big hidden cost drivers)

### 1) Create ECR repo and get its URL
```bash
cd infra/terraform/envs/dev
terraform init
terraform apply -auto-approve -var='model_service_image=PLACEHOLDER'
terraform output ecr_model_service_repo_url
```

### 2) Build and push image to ECR
From repo root, build a container image for `model-service`:

You’ll:
- `aws ecr get-login-password | docker login ...`
- `docker build -t <ecr-url>:latest services/model-service`
- `docker push <ecr-url>:latest`

### 3) Deploy service using the pushed image
```bash
cd infra/terraform/envs/dev
terraform apply -auto-approve -var='model_service_image=<ecr-url>:latest'
```

Then check:
```bash
curl "http://<public-ip>:8000/health"
```

## Tear down
```bash
cd infra/terraform/envs/dev
terraform destroy
```

