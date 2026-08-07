
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # Bucket is intentionally omitted here and supplied at `terraform init`
  # time via -backend-config="bucket=terraform-state-<account-id>".
  # This avoids hardcoding an account ID that breaks every time your
  # AWS Academy/Vocareum lab account rotates. See main.yml's
  # "Ensure Terraform state bucket exists" step.
  backend "s3" {
    key    = "agro-weather/terraform.tfstate"
    region = "us-east-1"
  }
}
provider "aws" {
  region = "us-east-1"
}
