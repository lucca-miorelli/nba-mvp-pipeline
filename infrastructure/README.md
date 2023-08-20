# AWS Resources with Terraform

This README explains the provisioning of cloud resources for this project with Terraform.

## Table of Contents

- [AWS Resources with Terraform](#aws-resources-with-terraform)
  - [Table of Contents](#table-of-contents)
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Cleaning Up](#cleaning-up)
  - [Configuration](#configuration)
  - [Resources Created](#resources-created)
    - [S3 Bucket](#s3-bucket)
    - [EC2 Instance](#ec2-instance)
    - [EC2 Instance Security Group](#ec2-instance-security-group)
    - [EC2 Instance S3 Access Policy](#ec2-instance-s3-access-policy)
    - [IAM Role and Instance Profile](#iam-role-and-instance-profile)
    - [SSH Key Pair](#ssh-key-pair)
    - [RDS Instance](#rds-instance)
    - [Lambda Function](#lambda-function)

## Prerequisites

Before getting started, ensure that you have the following:

- Terraform installed on your machine.
- Appropriate AWS credentials configured.

## Usage

To use Terraform and provision the AWS resources specified in the `main.tf` file, follow these steps:

1. Initialize the Terraform workspace:

   ```shell
   $ terraform init
   ```

2. (Optional) Modify the values of variables defined in a `.tfvars` or `.auto.tfvars` file.

3. Review the execution plan:

   ```shell
   $ terraform plan
   ```

4. Apply the changes and create the resources:

   ```shell
   $ terraform apply
   ```

5. Confirm the resource creation by typing `yes` when prompted.

Terraform will then provision the specified AWS resources based on the configuration defined in `main.tf`. 

## Cleaning Up

To destroy the created resources and clean up, follow these steps:

1. Run the destroy command:

   ```shell
   $ terraform destroy
   ```

2. Confirm the resource destruction by typing `yes` when prompted.

Terraform will then tear down the AWS resources that were provisioned.

## Configuration

You may need to adjust or customize the configuration in the `main.tf` file to match your specific needs. The file is well-commented to help you understand each resource and its properties. Uncomment or modify the resources as necessary.


## Resources Created

The following AWS resources are being created with the provided `main.tf` file:

### S3 Bucket
An S3 bucket is created using the aws_s3_bucket resource block named `nba_mvp_bucket`. The bucket name is configured using the var.s3_bucket_name variable.

### EC2 Instance

- An EC2 instance is created using the `aws_instance` resource block. The instance is configured with the specified AMI ID, instance type, key pair, instance profile, and security group ID.
  - The AMI ID is set using the `var.ami_id` variable.
  - The instance type is set using the `var.instance_type` variable.
  - The key pair is created with the name "TF_EC2_KEY" using the `aws_key_pair` resource block.
  - The instance profile is set to `aws_iam_instance_profile.ec2_instance_profile.name`.
  - The security group is set to `[aws_security_group.ec2_security_group.id]`.

### EC2 Instance Security Group

- An EC2 instance security group is created using the `aws_security_group` resource block. The security group allows inbound SSH (port 22), HTTP (port 80), and HTTPS (port 443) traffic.
  - Ingress rules are specified to allow traffic from `var.security_group_ids` to port 22 (SSH), from any IP to port 80 (HTTP), and from any IP to port 443 (HTTPS).
  - Egress rules are specified to allow outbound traffic to any IP on all ports.

### EC2 Instance S3 Access Policy

- An IAM policy named "s3_access" is created using the `aws_iam_policy` resource block. This policy allows the EC2 instance to access all S3 resources with the actions `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject`.

### IAM Role and Instance Profile

- An IAM role is created using the `aws_iam_role` resource block. The role is associated with the EC2 service and is named "ec2_s3_role".
- An IAM policy attachment is created using the `aws_iam_role_policy_attachment` resource block. This attaches the "s3_access" policy to the "ec2_s3_role" role.
- An instance profile is created using the `aws_iam_instance_profile` resource block. The instance profile is named "ec2_instance_profile" and is associated with the "ec2_s3_role" role.

### SSH Key Pair

- An RSA key pair named "TF_EC2_KEY" is generated using the `tls_private_key` and `aws_key_pair` resource blocks. The private key is saved to the local file `TF_EC2_KEY.pem`, and the public key is used as the EC2 instance's key pair.

Please note that this is just a summary of the resources being created. For more details, refer to the comments in the `main.tf` file.


### RDS Instance

- An RDS instance is created using the `aws_db_instance` resource block. The instance is configured with the specified DB engine, instance class, username, password, and security group ID.
  - The DB engine is set using the `var.db_engine` variable.
  - The instance class is set using the `var.db_instance_class` variable.
  - The username is set using the `var.db_username` variable.
  - The password is set using the `var.db_password` variable.
  - The security group is set to `[aws_security_group.rds_security_group.id]`.

### Lambda Function

- A Lambda function is created using the `aws_lambda_function` resource block. The function is configured with the specified runtime, handler, and role.
  - The runtime is set using the `var.lambda_runtime` variable.
  - The handler is set using the `var.lambda_handler` variable.
  - The role is set to `[aws_iam_role.lambda_role.arn]`.
---

