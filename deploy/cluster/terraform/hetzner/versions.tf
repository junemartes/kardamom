terraform {
  required_version = ">= 1.8"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.69"
    }
  }
}

# The API token comes from the HCLOUD_TOKEN environment variable of the
# operator or the CI job. It is never a variable, so it never enters the
# plan, the state or a tfvars file.
provider "hcloud" {}
