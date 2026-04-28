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
  description = "Container image URI for model-service (e.g. <acct>.dkr.ecr.<region>.amazonaws.com/model-service:latest)"
}

variable "model_service_port" {
  type        = number
  description = "Container port"
  default     = 8000
}

variable "enable_ecr" {
  type        = bool
  description = "Create ECR repositories for images"
  default     = true
}

variable "enable_s3_mlflow_artifacts" {
  type        = bool
  description = "Create an S3 bucket for MLflow artifacts (demo use)"
  default     = false
}

