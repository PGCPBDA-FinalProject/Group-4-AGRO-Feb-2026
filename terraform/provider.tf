terraform {
  backend "s3" {
    bucket = "agro-weather-terraform-state-654654400554"
    key    = "agro-weather/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = "us-east-1"
}
