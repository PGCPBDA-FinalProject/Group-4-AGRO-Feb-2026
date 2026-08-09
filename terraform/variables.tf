variable "region" {
  default = "us-east-1"
}

variable "glue_role_arn" {
  default = "arn:aws:iam::654654400554:role/LabRole"
}

variable "bronze_bucket" {
  default = "agro-weather-data-lake-fi"
}

variable "silver_bucket" {
  default = "agro-weather-data-lake-silver3-fi"
}

variable "gold_bucket" {
  default = "agro-weather-data-lake-gold3-fi"
}

variable "glue_assets_bucket" {
  default = "aws-glue-assets-654654400554-us-east-1"
}

#declare a glue job name
variable "glue_job_name" {
  default = "glue-etl-job32"
}

#declare a crawler name
variable "glue_crawler_name" {
  default = "my-etl-crawler32"
}
#declare athena results bucket
variable "athena_results_bucket" {
  default = "agro-weather-data-lake-athena-results-fi"
}

#declare athena workgroup name
variable "athena_workgroup_name" {
  default = "agro-weather-workgroup"
}

# ---------------------------------------------------------------------
# Kaggle credentials for the bronze ingestion jobs.
# Populate via TF_VAR_kaggle_username / TF_VAR_kaggle_key in CI (mapped
# from GitHub Secrets). Never commit real values here.
# ---------------------------------------------------------------------
variable "kaggle_username" {
  description = "Kaggle username used by the bronze ingestion Glue jobs"
  type        = string
  sensitive   = true
  default     = ""
}

variable "kaggle_key" {
  description = "Kaggle API key used by the bronze ingestion Glue jobs"
  type        = string
  sensitive   = true
  default     = ""
}

variable "weather_dataset_name" {
  description = "Kaggle dataset slug for the weather ingestion job"
  type        = string
  default     = "mukeshdevrath007/indian-5000-cities-weather-data"
}

variable "crop_dataset_name" {
  description = "Kaggle dataset slug for the crop yield ingestion job"
  type        = string
  default     = "zoya77/indian-historical-crop-yield-and-weather-data"
}
