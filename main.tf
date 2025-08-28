# Define the AWS provider
provider "aws" {
  region = "us-east-1"
}

data "http" "my_ip" {
  url = "https://ifconfig.co/json"
}

# Fetch the ID of the default VPC
data "aws_vpc" "default" {
  default = true
}
# -------------------------------------------------------------

# Create a security group to allow inbound traffic on the PostgreSQL port
resource "aws_security_group" "db_sg" {
  name_prefix = "db-security-group-"
  description = "Allow inbound traffic to Postgres DB"
  # Use the ID of the default VPC
  vpc_id = data.aws_vpc.default.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    # Whitelist your IP specifically. This is secure.
    cidr_blocks = ["${jsondecode(data.http.my_ip.response_body).ip}/32"]
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
 #refference the vpc id above
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