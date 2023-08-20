#########################################################
#           IMPORT LIBRARIES AND DATA SOURCES           #
#########################################################

# Import the AWS caller identity data source to get current account info
data "aws_caller_identity" "current" {}

#########################################################
#               LAMBDA FUNCTION RESOURCE                #
#########################################################

# Define an AWS Lambda function resource
resource "aws_lambda_function" "load_db" {
  filename      = data.archive_file.load_db.output_path
  function_name = "load_db"
  role          = aws_iam_role.lambda.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300
  memory_size   = 500

  # Set environment variables for the Lambda function
  environment {
    variables = {
      DB_HOST     = var.db_host
      DB_PORT     = var.db_port
      DB_NAME     = var.db_database
      DB_USERNAME = var.db_username
      DB_PASSWORD = var.db_password
    }
  }

  source_code_hash = "${data.archive_file.load_db.output_base64sha256}}"

  # Attach AWS Lambda layers for additional libraries
  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:8",
    aws_lambda_layer_version.sqlalchemy.arn,
    aws_lambda_layer_version.psycopg2.arn
  ]
}

##########################################################
#          LAMBDA PERMISSION FOR S3 EVENT                #
##########################################################

# Create permission for S3 bucket to invoke the Lambda function
resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_db.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.bucket_name}"
}

#########################################################
#       S3 BUCKET NOTIFICATION CONFIGURATION            #
#########################################################

# Configure S3 bucket notification to invoke the Lambda function on object creation
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = var.bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.load_db.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "data/raw/players/"
  }
}

#########################################################
#                IAM ROLE FOR LAMBDA                    #
#########################################################

# Create an IAM role for the Lambda function
resource "aws_iam_role" "lambda" {
  name = "lambda_role"

  # Define the IAM role's assume role policy
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Attach policies to the IAM role

# Attach AWS Lambda basic execution policy
resource "aws_iam_role_policy_attachment" "lambda_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda.name
}

# Attach Amazon S3 read-only access policy
resource "aws_iam_role_policy_attachment" "lambda_s3_read" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
  role       = aws_iam_role.lambda.name
}

#########################################################
#            LAMBDA LAYER VERSION RESOURCES             #
#########################################################

# Create Lambda layer versions for psycopg2 and sqlalchemy

resource "aws_lambda_layer_version" "psycopg2" {
  layer_name = "psycopg2"
  compatible_runtimes = [
    "python3.9"
  ]

  filename         = data.archive_file.psycopg2.output_path
  source_code_hash = data.archive_file.psycopg2.output_base64sha256
}

resource "aws_lambda_layer_version" "sqlalchemy" {
  layer_name = "sqlalchemy"
  compatible_runtimes = [
    "python3.9"
  ]

  filename         = data.archive_file.sqlalchemy.output_path
  source_code_hash = data.archive_file.sqlalchemy.output_base64sha256
}

#########################################################
#        LAMBDA LAYER PERMISSIONS FOR ROOT ACCOUNT      #
#########################################################

# Create permissions for Lambda function to access each layer

resource "aws_lambda_layer_version_permission" "psycopg2" {
  layer_name     = aws_lambda_layer_version.psycopg2.layer_name
  version_number = aws_lambda_layer_version.psycopg2.version
  statement_id   = "AllowLambda"
  action         = "lambda:GetLayerVersion"
  principal      = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
}

resource "aws_lambda_layer_version_permission" "sqlalchemy" {
  layer_name     = aws_lambda_layer_version.sqlalchemy.layer_name
  version_number = aws_lambda_layer_version.sqlalchemy.version
  statement_id   = "AllowLambda"
  action         = "lambda:GetLayerVersion"
  principal      = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
}

#########################################################
#           ARCHIVE FILE DATA SOURCES                   #
#########################################################

# Create archive files for Lambda function code and layers

data "archive_file" "load_db" {
  type        = "zip"
  source_file  = "${path.module}/load_db/lambda_function.py"
  output_path = "${path.module}/load_db_function.zip"
}

data "archive_file" "psycopg2" {
  type        = "zip"
  source_dir  = "${path.module}/layers/psycopg2/"
  output_path = "${path.module}/layers/psycopg2.zip"
}

data "archive_file" "sqlalchemy" {
  type        = "zip"
  source_dir  = "${path.module}/layers/sqlalchemy/"
  output_path = "${path.module}/layers/sqlalchemy.zip"
}
