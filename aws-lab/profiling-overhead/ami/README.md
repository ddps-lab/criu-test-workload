# AMI Build Instructions

## Base AMI
- **OS**: Ubuntu 24.04 (ami-0a0e5d9c7acc336f1, us-east-1)
- **Kernel**: 6.7+ required (for PAGEMAP_SCAN and uffd-wp support)

## Build Procedure

1. Launch an m5.xlarge On-Demand instance (Ubuntu 24.04)
2. SSH in and run `setup.sh`:
   ```bash
   sudo bash setup.sh
   ```
3. Verify the setup:
   ```bash
   /opt/criu_workload/tools/dirty_tracker_c/dirty_tracker --help
   redis-server --version
   /opt/ycsb/bin/ycsb --help
   ```
4. Create the AMI:
   ```bash
   aws ec2 create-image --instance-id i-xxx --name criu-overhead-exp-v1
   ```
5. Terminate the source instance