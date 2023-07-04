output "instance_id" {
  description = "EC2 instance ID."
  value       = aws_instance.lk-instance.id
}

output "arn" {
  description = "value of the ARN of the instance."
  value       = aws_instance.lk-instance.arn
}
