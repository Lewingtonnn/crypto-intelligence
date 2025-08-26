# Define the AWS provider
provider "aws" {
  region = "us-east-1"
}

# Create a security group to allow inbound traffic on the PostgreSQL port
resource "aws_security_group" "db_sg" {
  name_prefix = "db-security-group-"
  description = "Allow inbound traffic to Postgres DB"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # WARNING: This allows all IP addresses.
                               # Use your specific IP in production.(only using this for testing and development
  }
}

# Create the RDS PostgreSQL instance
resource "aws_db_instance" "crypto_pipeline_db" {
  identifier           = "crypto-pipeline-db"
  allocated_storage    = 20
  engine               = "postgres"
  instance_class       = "db.t3.micro"
  username             = "crypto_pipeline_user"
  password             = var.db_password
  db_name              = "cryptodb"
  publicly_accessible  = true
  skip_final_snapshot  = true
  vpc_security_group_ids = [aws_security_group.db_sg.id]
}

# Define a variable for the database password, keeping it secure
variable "db_password" {
  type        = string
  description = "The password for the RDS instance."
  sensitive   = true
}

# Output the database endpoint
output "db_endpoint" {
  value = aws_db_instance.crypto_pipeline_db.address
}