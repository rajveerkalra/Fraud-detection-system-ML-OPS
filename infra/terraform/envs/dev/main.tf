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

locals {
  name = var.project_name
}

resource "aws_cloudwatch_log_group" "model_service" {
  name              = "/ecs/${local.name}/model-service"
  retention_in_days = 7
}

resource "aws_ecs_cluster" "this" {
  name = "${local.name}-cluster"
}

resource "aws_security_group" "model_service" {
  name        = "${local.name}-model-service-sg"
  description = "Allow inbound model-service traffic"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "HTTP (model-service)"
    from_port   = var.model_service_port
    to_port     = var.model_service_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "task_execution" {
  name = "${local.name}-ecs-task-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = { Service = "ecs-tasks.amazonaws.com" }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn  = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_ecs_task_definition" "model_service" {
  family                   = "${local.name}-model-service"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.task_execution.arn

  container_definitions = jsonencode([
    {
      name      = "model-service"
      image     = var.model_service_image
      essential = true
      portMappings = [
        { containerPort = var.model_service_port, hostPort = var.model_service_port, protocol = "tcp" }
      ]
      environment = [
        # For cloud demo we typically run without MLflow/Feast; keep service health/metrics working.
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
}

resource "aws_ecs_service" "model_service" {
  name            = "${local.name}-model-service"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.model_service.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.model_service.id]
    assign_public_ip = true
  }
}

resource "aws_ecr_repository" "model_service" {
  count = var.enable_ecr ? 1 : 0
  name  = "${local.name}/model-service"
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_s3_bucket" "mlflow_artifacts" {
  count  = var.enable_s3_mlflow_artifacts ? 1 : 0
  bucket = "${local.name}-${data.aws_caller_identity.current.account_id}-mlflow-artifacts"
}

