variable "aws_region" {
  type        = string
  description = "AWS region to deploy to"
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Project name prefix for resources"
  default     = "streaming-fraud-mlops"
}

variable "model_service_image" {
  type        = string
  description = "Container image URI for model-service"
}

variable "model_service_port" {
  type        = number
  description = "Container port"
  default     = 8000
}

variable "model_service_ingress_cidr" {
  type        = string
  description = "CIDR allowed to access model-service"
  default     = "0.0.0.0/0"
}

variable "model_service_cpu" {
  type        = number
  description = "Fargate CPU units"
  default     = 512
}

variable "model_service_memory" {
  type        = number
  description = "Fargate memory in MB"
  default     = 1024
}

variable "model_service_desired_count" {
  type        = number
  description = "Number of running model-service tasks"
  default     = 1
}

variable "enable_ecr" {
  type        = bool
  description = "Create ECR repositories"
  default     = true
}

variable "enable_s3_mlflow_artifacts" {
  type        = bool
  description = "Create S3 bucket for MLflow artifacts"
  default     = true
}

variable "streaming_instance_type" {
  type        = string
  description = "EC2 type for streaming host"
  default     = "t3.small"
}

variable "streaming_root_volume_gb" {
  type        = number
  description = "Root EBS volume size"
  default     = 40
}

variable "ssh_cidr" {
  type        = string
  description = "Allowed SSH CIDR"
  default     = "0.0.0.0/0"
}

variable "key_pair_name" {
  type        = string
  description = "Optional EC2 key pair name"
  default     = null
}

variable "enable_budget_alerts" {
  type        = bool
  description = "Enable AWS budget alerts"
  default     = true
}

variable "monthly_budget_usd" {
  type        = string
  description = "Monthly budget amount in USD"
  default     = "200"
}

variable "budget_alert_email" {
  type        = string
  description = "Email address for budget alerts"
  default     = ""
}
