# AWS Infrastructure

This directory contains the infrastructure starting point for a production-style deployment.

The current Terraform layer creates:

- An ECR repository for application images.
- A CloudWatch log group.

The next cloud deployment phase should add the networking and managed runtime components, for example:

- VPC and private/public subnets
- ECS/Fargate services
- Managed PostgreSQL (RDS)
- Managed Kafka (Amazon MSK) or an equivalent event-streaming service
- IAM roles with least privilege
- Secrets Manager
- Application Load Balancer for the API
- CloudWatch metrics/alarms

Do not apply this configuration until AWS credentials, cost controls, networking, and the intended deployment architecture have been reviewed.
