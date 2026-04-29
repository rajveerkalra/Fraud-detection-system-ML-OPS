output "ecr_model_service_repo_url" {
  value       = try(aws_ecr_repository.model_service[0].repository_url, null)
  description = "ECR repo URL for model-service image"
}

output "streaming_host_public_ip" {
  value       = aws_instance.streaming_host.public_ip
  description = "Public IP for EC2 streaming host"
}

output "model_service_security_group_id" {
  value       = aws_security_group.model_service.id
  description = "Security group for model-service task"
}

output "streaming_host_security_group_id" {
  value       = aws_security_group.streaming_host.id
  description = "Security group for streaming host"
}

output "mlflow_artifacts_bucket" {
  value       = try(aws_s3_bucket.mlflow_artifacts[0].bucket, null)
  description = "Optional MLflow artifacts bucket"
}
