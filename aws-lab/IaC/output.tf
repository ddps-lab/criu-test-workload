output "az_a_ec2_ids" {
  value = aws_instance.az_a_ec2[*].id
}

output "az_c_ec2_ids" {
  value = aws_instance.az_c_ec2[*].id
}

output "az_a_volume_ids" {
  value = aws_ebs_volume.az_a_volume[*].id
}

output "az_c_volume_ids" {
  value = aws_ebs_volume.az_c_volume[*].id
}