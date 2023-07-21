# Configure the AWS provider
provider "aws" {
  region = "us-east-1"  # Change this to your desired AWS region
}

# Create a VPC
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
}

# Create public and private subnets for each layer
resource "aws_subnet" "web_subnet" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "app_subnet" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_subnet" "db_subnet" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = "us-east-1c"
}

# Create an Internet Gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
}

# Create a NAT Gateway
resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.web_subnet.id
}

resource "aws_eip" "nat" {
  vpc = true
}

# Create route tables and associate with subnets
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "web_subnet" {
  subnet_id      = aws_subnet.web_subnet.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "app_subnet" {
  subnet_id      = aws_subnet.app_subnet.id
  route_table_id = aws_route_table.public.id
}

# Create a security group for the web layer
resource "aws_security_group" "web_sg" {
  name_prefix = "web_sg"
  vpc_id      = aws_vpc.main.id

  # Allow incoming HTTP traffic
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Create an RDS database instance for the database tier
resource "aws_db_instance" "db_instance" {
  identifier             = "mydbinstance"
  engine                 = "mysql"
  instance_class         = "db.t2.micro"
  allocated_storage      = 20
  username               = "mydbuser"
  password               = "mydbpassword"
  multi_az               = false
  publicly_accessible    = false
  db_subnet_group_name   = "default"
  vpc_security_group_ids = [aws_security_group.web_sg.id]
}

# Create EC2 instances for web servers
resource "aws_instance" "web_instance" {
  ami           = "ami-0c55b159cbfafe1f0"  # Amazon Linux 2 AMI ID
  instance_type = "t2.micro"
  subnet_id     = aws_subnet.web_subnet.id
  vpc_security_group_ids = [aws_security_group.web_sg.id]
}

# Create an Application Load Balancer (ALB) for the application layer
resource "aws_lb" "app_lb" {
  name               = "my-app-lb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.app_subnet.id]
}

# Create a target group for the ALB
resource "aws_lb_target_group" "app_target_group" {
  name        = "my-app-target-group"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "instance"

  health_check {
    healthy_threshold   = 2
    unhealthy_threshold = 2
    timeout             = 3
    interval            = 30
    path                = "/"
  }
}

# Attach the EC2 instances to the target group
resource "aws_lb_target_group_attachment" "app_target_attachment" {
  target_group_arn = aws_lb_target_group.app_target_group.arn
  target_id        = aws_instance.web_instance.id
  port             = 8080
}
