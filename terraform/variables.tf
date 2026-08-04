variable "region" {
  default = "us-east-1"
}

variable "glue_role_arn" {
  default = "arn:aws:iam::381492026114:role/LabRole"
}

variable "bronze_bucket" {
  default = "agro-weather-data-lake"
}

variable "silver_bucket" {
  default = "agro-weather-data-lake2"
}

variable "gold_bucket" {
  default = "agro-weather-data-lake3"
}

variable "glue_assets_bucket" {
  default = "aws-glue-assets-381492026114-us-east-1"
}

#declare a glue job name
variable "glue_job_name" {
  default = "glue-etl-job28"
}

#declare a crawler name
variable "glue_crawler_name" {
  default = "my-etl-crawler28"
}
