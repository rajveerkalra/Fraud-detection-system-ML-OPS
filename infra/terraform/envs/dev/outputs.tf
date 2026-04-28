output "aws_account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "aws_region" {
  value = var.aws_region
}

output "ecr_model_service_repo_url" {
  value       = try(aws_ecr_repository.model_service[0].repository_url, null)
  description = "ECR repository URL (if enable_ecr=true)"
}

output "mlflow_artifacts_bucket" {
  value       = try(aws_s3_bucket.mlflow_artifacts[0].bucket, null)
  description = "S3 bucket name (if enable_s3_mlflow_artifacts=true)"
}

