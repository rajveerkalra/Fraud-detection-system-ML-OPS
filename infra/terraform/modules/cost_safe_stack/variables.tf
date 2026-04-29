variable "aws_region" {
  type        = string
  description = "AWS region"
}

variable "project_name" {
  type        = string
  description = "Resource name prefix"
}

variable "environment" {
  type        = string
  description = "Environment name (dev/staging)"
}

variable "model_service_image" {
  type        = string
  description = "ECR image URI for model-service"
}

variable "model_service_port" {
  type        = number
  description = "FastAPI container port"
  default     = 8000
}

variable "model_service_ingress_cidr" {
  type        = string
  description = "CIDR allowed to access model-service. Use 0.0.0.0/0 for public demo or your IP/32 for restricted access."
  default     = "0.0.0.0/0"
}

variable "model_service_cpu" {
  type        = number
  description = "Fargate CPU units (256, 512, ...)"
  default     = 256
}

variable "model_service_memory" {
  type        = number
  description = "Fargate memory MB"
  default     = 512
}

variable "model_service_desired_count" {
  type        = number
  description = "Desired ECS tasks"
  default     = 1
}

variable "streaming_instance_type" {
  type        = string
  description = "EC2 instance type for Kafka/Flink/Redis/MLflow/Prometheus/Grafana node"
  default     = "t3.small"
}

variable "streaming_root_volume_gb" {
  type        = number
  description = "Root EBS size in GB"
  default     = 30
}

variable "ssh_cidr" {
  type        = string
  description = "CIDR allowed for SSH access"
  default     = "0.0.0.0/0"
}

variable "key_pair_name" {
  type        = string
  description = "Optional EC2 key pair name"
  default     = null
}

variable "enable_ecr" {
  type        = bool
  description = "Create ECR repos"
  default     = true
}

variable "enable_s3_mlflow_artifacts" {
  type        = bool
  description = "Create S3 bucket for MLflow artifacts"
  default     = true
}

variable "enable_budget_alerts" {
  type        = bool
  description = "Create AWS Budget email alerts"
  default     = true
}

variable "monthly_budget_usd" {
  type        = string
  description = "Monthly budget in USD"
  default     = "200"
}

variable "budget_alert_email" {
  type        = string
  description = "Email for budget alerts"
  default     = ""
}
