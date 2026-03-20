output "instance_ids" {
  value = aws_instance.experiment[*].id
}

output "instance_ips" {
  value = aws_instance.experiment[*].public_ip
}

output "s3_results_path" {
  value = "s3://${var.s3_bucket}/overhead/"
}
