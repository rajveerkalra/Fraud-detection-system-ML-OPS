output "aws_region" {
  value = var.aws_region
}

output "ecr_model_service_repo_url" {
  value       = module.cost_safe_stack.ecr_model_service_repo_url
  description = "ECR repository URL"
}

output "mlflow_artifacts_bucket" {
  value       = module.cost_safe_stack.mlflow_artifacts_bucket
  description = "S3 bucket name for MLflow artifacts"
}

output "streaming_host_public_ip" {
  value       = module.cost_safe_stack.streaming_host_public_ip
  description = "Public IP for the EC2 streaming host"
}
