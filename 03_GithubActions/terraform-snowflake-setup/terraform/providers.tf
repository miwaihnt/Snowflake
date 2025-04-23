provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.snowflake_private_key
  role              = var.snowflake_role
}

provider "snowflake" {
  alias             = "sysadmin"
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.snowflake_private_key
  role              = "SYSADMIN"
}

provider "snowflake" {
  alias             = "securityadmin"
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.snowflake_private_key
  role              = "SECURITYADMIN"
}

provider "aws" {
  region = var.aws_region
}
