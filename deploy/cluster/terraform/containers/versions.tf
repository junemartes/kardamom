terraform {
  required_version = ">= 1.8"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.9"
    }
  }
}

# The provider talks to the daemon of the DOCKER_HOST environment variable,
# or to the local socket when the variable is not set.
provider "docker" {}
