data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

locals {
  prefix = "${var.project_name}-${var.environment}"
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    CostMode    = "cost-safe-dry-run"
  }
}

resource "aws_cloudwatch_log_group" "model_service" {
  name              = "/ecs/${local.prefix}/model-service"
  retention_in_days = 7
  tags              = local.tags
}

resource "aws_ecs_cluster" "this" {
  name = "${local.prefix}-cluster"
  tags = local.tags
}

resource "aws_security_group" "model_service" {
  name        = "${local.prefix}-model-sg"
  description = "Allow inbound HTTP for model-service"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "model-service"
    from_port   = var.model_service_port
    to_port     = var.model_service_port
    protocol    = "tcp"
    cidr_blocks = [var.model_service_ingress_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

resource "aws_iam_role" "task_execution" {
  name = "${local.prefix}-ecs-task-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_ecs_task_definition" "model_service" {
  family                   = "${local.prefix}-model-service"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.model_service_cpu
  memory                   = var.model_service_memory
  execution_role_arn       = aws_iam_role.task_execution.arn

  container_definitions = jsonencode([
    {
      name      = "model-service"
      image     = var.model_service_image
      essential = true
      portMappings = [
        {
          containerPort = var.model_service_port
          hostPort      = var.model_service_port
          protocol      = "tcp"
        }
      ]
      environment = [
        { name = "DECISION_THRESHOLD", value = "0.5" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.model_service.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
  tags = local.tags
}

resource "aws_ecs_service" "model_service" {
  name            = "${local.prefix}-model-service"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.model_service.arn
  desired_count   = var.model_service_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.model_service.id]
    assign_public_ip = true
  }

  tags = local.tags
}

resource "aws_ecr_repository" "model_service" {
  count = var.enable_ecr ? 1 : 0
  name  = "${local.prefix}/model-service"
  image_scanning_configuration {
    scan_on_push = true
  }
  tags = local.tags
}

resource "aws_s3_bucket" "mlflow_artifacts" {
  count  = var.enable_s3_mlflow_artifacts ? 1 : 0
  bucket = "${local.prefix}-${data.aws_caller_identity.current.account_id}-mlflow-artifacts"
  tags   = local.tags
}

resource "aws_security_group" "streaming_host" {
  name        = "${local.prefix}-streaming-host-sg"
  description = "Single EC2 host for Kafka/Flink/Redis/MLflow/Prometheus/Grafana"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  # No public ingress for Kafka (9092), Grafana (3000), or MLflow (5000).
  # Access Grafana/MLflow via SSH tunnel only.

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

resource "aws_instance" "streaming_host" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.streaming_instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.streaming_host.id]
  key_name                    = var.key_pair_name
  associate_public_ip_address = true

  root_block_device {
    volume_size = var.streaming_root_volume_gb
    volume_type = "gp3"
  }

  # SAFE USAGE NOTE:
  # Install Docker and run only the minimum compose services needed for the demo.
  # Keep host off except demo windows to control cost.
  user_data = <<-EOF
              #!/bin/bash
              set -euxo pipefail
              dnf update -y
              dnf install -y docker git
              systemctl enable docker
              systemctl start docker
              usermod -aG docker ec2-user
              EOF

  tags = merge(local.tags, { Name = "${local.prefix}-streaming-host" })
}

resource "aws_budgets_budget" "monthly" {
  count        = var.enable_budget_alerts && var.budget_alert_email != "" ? 1 : 0
  name         = "${local.prefix}-monthly-budget"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 25
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
