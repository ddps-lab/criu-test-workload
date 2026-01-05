resource "aws_ebs_volume" "az_a_volume" {
    count = var.az_a_volume_count
    availability_zone = "${var.region}a"
    size = var.ebs_volume_size
    type = "gp3"
    tags = {
        Name = "${var.prefix}-az-a-volume"
    }
}

resource "aws_ebs_volume" "az_c_volume" {
    count = var.az_c_volume_count
    availability_zone = "${var.region}c"
    size = var.ebs_volume_size
    type = "gp3"
    tags = {
        Name = "${var.prefix}-az-c-volume"
    }
}