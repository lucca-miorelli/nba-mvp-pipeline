# Terraform module to create EC2 instance

# EC2 instance Security Group
resource "aws_security_group" "ec2_security_group" {
  name        = "ec2_security_group"
  description = "Allow SSH inbound traffic"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.security_group_ids
  }

  # TCP port 80 for HTTP
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # TCP port 443 for HTTPS
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Outbound HTTP to anywhere
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Outbound HTTPS to anywhere
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}


# EC2 instance S3 access policy
resource "aws_iam_policy" "s3_access" {
  name        = "s3_access"
  description = "Allows EC2 instances to access S3 buckets"
  policy = jsonencode(
    {
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject"
          ]
          Resource = [
            "*"
          ]
        }
      ]
    }
  )
}

# Create IAM instance profile
resource "aws_iam_role" "ec2_s3_role" {
  name = "ec2_s3_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

# Attach the policy to IAM role
resource "aws_iam_role_policy_attachment" "s3_role_policy" {
  policy_arn = aws_iam_policy.s3_access.arn
  role       = aws_iam_role.ec2_s3_role.name
}

# Create IAM instance profile
resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "ec2_instance_profile"
  role = aws_iam_role.ec2_s3_role.name
}

# RSA key of size 4096 bits
resource "tls_private_key" "TF_EC2_KEY" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

# create local file
resource "local_file" "TF_EC2_KEY" {
  content  = tls_private_key.TF_EC2_KEY.private_key_pem
  filename = "${path.module}/TF_EC2_KEY.pem"
}

# Create key pair
resource "aws_key_pair" "TF_EC2_KEY" {
  key_name   = "TF_EC2_KEY"
  public_key = tls_private_key.TF_EC2_KEY.public_key_openssh
}

# Create Instance
resource "aws_instance" "lk-instance" {

  ami                    = var.ami_id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.TF_EC2_KEY.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_instance_profile.name
  vpc_security_group_ids = [aws_security_group.ec2_security_group.id]

  tags = merge(var.tags, {
    Name      = var.instance_name
    Terraform = "true"
  })

}
