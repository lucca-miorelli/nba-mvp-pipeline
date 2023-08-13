# Create free-tier Postgres RDS database

# RDS instance Security Group
resource "aws_security_group" "rds_security_group" {
  name        = "rds_security_group"
  description = "Allow Postgres inbound traffic"

  # TCP port 5432 for Postgres 
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.cidr_blocks
  }
}

# RDS instance
resource "aws_db_instance" "lk_db_instance" {
  allocated_storage      = 20
  engine                 = "postgres"
  engine_version         = "14.7"
  instance_class         = "db.t3.micro"
  identifier             = var.instance_name
  username               = var.instance_username
  password               = var.instance_password
  skip_final_snapshot    = true
  storage_encrypted      = false
  publicly_accessible    = true
  apply_immediately      = true
  vpc_security_group_ids = [aws_security_group.rds_security_group.id]
}
