terraform {
  backend "s3" {
  bucket = "agro-weather-terraform-state-654654400554"   # new account
  key    = "agro-weather/terraform.tfstate"
  region = "..."
}
}

provider "aws" {
  region = "us-east-1"
}
