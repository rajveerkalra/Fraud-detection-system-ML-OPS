module "cost_safe_stack" {
  source = "../../modules/cost_safe_stack"

  aws_region                  = var.aws_region
  project_name                = var.project_name
  environment                 = "staging"
  model_service_image         = var.model_service_image
  model_service_port          = var.model_service_port
  model_service_ingress_cidr  = var.model_service_ingress_cidr
  model_service_cpu           = var.model_service_cpu
  model_service_memory        = var.model_service_memory
  model_service_desired_count = var.model_service_desired_count

  streaming_instance_type  = var.streaming_instance_type
  streaming_root_volume_gb = var.streaming_root_volume_gb
  ssh_cidr                 = var.ssh_cidr
  key_pair_name            = var.key_pair_name

  enable_ecr                 = var.enable_ecr
  enable_s3_mlflow_artifacts = var.enable_s3_mlflow_artifacts
  enable_budget_alerts       = var.enable_budget_alerts
  monthly_budget_usd         = var.monthly_budget_usd
  budget_alert_email         = var.budget_alert_email
}
