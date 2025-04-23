variable "snowflake_organization_name" {
  type    = string
  default = "NTTDATAAIOTTDP"
}

variable "snowflake_account_name" {
  type    = string
  default = "NTTCOM"
}

variable "snowflake_user" {
  type    = string
  default = "TERRAFORM_USER"
}

variable "snowflake_role" {
  type    = string
  default = "SECURITYADMIN"
}

variable "snowflake_private_key" {
  type      = string
  sensitive = true
}

variable "aws_region" {
  type    = string
  default = "ap-northeast-1"
}
