terraform {
  backend "s3" {
    bucket       = "tf-state-backend-252219867511"
    key          = "terraform.tfstate"
    region       = "ap-northeast-1"
    encrypt      = true
    use_lockfile = true
  }
}
