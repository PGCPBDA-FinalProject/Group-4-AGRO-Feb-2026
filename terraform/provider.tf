
terraform {
  backend "s3" {
    bucket = "terraform-state-533267437108"
    key    = "agro-weather/terraform.tfstate"
    region = "us-east-1"
  }
}
provider "aws" {
  region = "us-east-1"
}
