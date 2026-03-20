# AWS Experiment Infrastructure: Write Profiling Overhead (§7.2.1)

## Goal

Reproducible infrastructure for running Write Profiling Overhead experiments (§7.2.1) on AWS m5.xlarge instances. Designed for ASPLOS artifact evaluation reproducibility badge.

## Structure

```
profiling-overhead/
├── terraform/
│   ├── main.tf
│   ├── variables.tf.sample
│   ├── outputs.tf
│   └── userdata.tpl
├── ami/
│   ├── setup.sh
│   └── README.md
├── scripts/
│   ├── collect_results.py
│   └── run_all.sh
└── README.md
```

## Quick Start

```bash
# 1. Configure Terraform variables
cd terraform
cp variables.tf.sample variables.tf
# Edit variables.tf: set ami_id and key_name

# 2. Run the full pipeline
cd ../scripts
bash run_all.sh
```

## Experiment Matrix

4 workloads × 3 intervals = 12 instances running in parallel:

| Workload | 1ms | 1000ms | 10000ms |
|----------|-----|--------|---------|
| Redis    | baseline, uffd-wp, sd-only | + uffd-sync | + uffd-sync |
| MatMul   | baseline, uffd-wp, sd-only | + uffd-sync | + uffd-sync |
| XGBoost  | baseline, uffd-wp, sd-only | + uffd-sync | + uffd-sync |
| MemWrite | baseline, uffd-wp, sd-only | + uffd-sync | + uffd-sync |

## Cost Estimate

- m5.xlarge: $0.192/hr
- 12 instances × ~1.5 hours = ~$3.50
- S3: negligible
- **Total: ~$4**

## Notes

1. **Kernel 6.7+** required — verify with `uname -r`
2. **Code deployment**: git clone vs S3 tarball (use S3 for private repos)
3. **YCSB Java heap**: default is sufficient for m5.xlarge (16GB RAM)
4. **Redis port conflicts**: not an issue since each instance runs independently
5. **userdata**: runs as root
6. **S3 bucket region**: should match instance region for faster uploads
