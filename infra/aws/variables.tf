variable "aws_region" {
  type    = string
  default = "eu-west-2"
}

variable "project_name" {
  type    = string
  default = "distributed-event-processing"
}

variable "container_image" {
  type        = string
  description = "ECR image URI/tag for the worker image."
}
