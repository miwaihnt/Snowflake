terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
      version = "~> 1.0.2"
    }
  }
  required_version = ">= 1.10.0"
}
