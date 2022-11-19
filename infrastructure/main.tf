################################################################################
#                                   PROVIDERS                                  #
################################################################################

terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~> 4.36"
    }
  }
}

provider "aws" {
  region = var.aws_region
  profile = var.aws_profile
}

################################################################################
#                                   S3 BUCKET                                  #
################################################################################

resource "aws_s3_bucket" "nba_mvp_bucket" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_acl" "nba_mvp_bucket_acl" {
  bucket = aws_s3_bucket.nba_mvp_bucket.id
  acl = "private"
}

################################################################################
#                                    IAM USER                                  #
################################################################################


# Create an IAM User
resource "aws_iam_user" "nba_mvp_iam_user" {
  name = var.iam_user_name
}

# Give IAM user programatic access
resource "aws_iam_access_key" "nba_mvp_iam_user_access_key" {
  user = aws_iam_user.nba_mvp_iam_user.name
}

# Create the inline policy
data "aws_iam_policy_document" "s3_get_put_delete_policy_document" {
  statement {
    actions = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
    ]

    resources = [
        "${aws_s3_bucket.nba_mvp_bucket.arn}/*"
    ]
  }
}

# Attach the policy to IAM user
resource "aws_iam_user_policy" "nba_mvp_iam_user_policy" {
  name = "NbaMvpPolicy-S3-Auth"
  user = aws_iam_user.nba_mvp_iam_user.name
  policy = data.aws_iam_policy_document.s3_get_put_delete_policy_document.json
}