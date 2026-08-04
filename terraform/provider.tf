terraform {
  backend "s3" {
    bucket = "agro-weather-terraform-state-339712929234"
    key    = "agro-weather/terraform.tfstate"
    region = "us-east-1"
    # Lab account has no DynamoDB lock table permissions in most cases;
    # omit dynamodb_table unless you've confirmed you can create one.
  }
}

provider "aws" {
  region = "us-east-1"
}
