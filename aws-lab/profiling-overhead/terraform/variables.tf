variable "region" {
  default = "us-west-2"
}

variable "ami_id" {
  default = "ami-00090d9d8af0310af"
}

variable "key_name" {
  default = "mhsong-ddps-oregon"
}

variable "instance_type" {
  default = "m5.xlarge"
}

variable "s3_bucket" {
  default = "ddps-criu-experiments"
}

variable "experiments" {
  type = list(object({
    name     = string
    workload = string
    configs  = string
    extra    = string
    duration = number
    repeats  = number
  }))
  default = [
    # ── xgboost (DDPS-1052: 7M samples, 100 features, 3 threads) ──
    { name = "xgboost_1000ms", workload = "xgboost", configs = "baseline,uffd-wp-1000ms,sd-only-1000ms,uffd-sync-1000ms", extra = "--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3", duration = 1800, repeats = 5 },
    { name = "xgboost_3000ms", workload = "xgboost", configs = "baseline,uffd-wp-3000ms,sd-only-3000ms,uffd-sync-3000ms", extra = "--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3", duration = 1800, repeats = 5 },
    { name = "xgboost_5000ms", workload = "xgboost", configs = "baseline,uffd-wp-5000ms,sd-only-5000ms,uffd-sync-5000ms", extra = "--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3", duration = 1800, repeats = 5 },
    { name = "xgboost_10000ms", workload = "xgboost", configs = "baseline,uffd-wp-10000ms,sd-only-10000ms,uffd-sync-10000ms", extra = "--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3", duration = 1800, repeats = 5 },
    { name = "xgboost_60000ms", workload = "xgboost", configs = "baseline,uffd-wp-60000ms,sd-only-60000ms,uffd-sync-60000ms", extra = "--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3", duration = 1800, repeats = 5 },
    { name = "xgboost_180000ms", workload = "xgboost", configs = "baseline,uffd-wp-180000ms,sd-only-180000ms,uffd-sync-180000ms", extra = "--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3", duration = 1800, repeats = 5 },
    { name = "xgboost_300000ms", workload = "xgboost", configs = "baseline,uffd-wp-300000ms,sd-only-300000ms,uffd-sync-300000ms", extra = "--dataset synthetic --num-samples 7000000 --num-features 100 --num-threads 3", duration = 1800, repeats = 5 },

    # ── ml_training (DDPS-1052: large model, 50K dataset) ──
    { name = "ml_training_1000ms", workload = "ml_training", configs = "baseline,uffd-wp-1000ms,sd-only-1000ms,uffd-sync-1000ms", extra = "--model-size large --dataset-size 50000 --epochs 0", duration = 1800, repeats = 5 },
    { name = "ml_training_3000ms", workload = "ml_training", configs = "baseline,uffd-wp-3000ms,sd-only-3000ms,uffd-sync-3000ms", extra = "--model-size large --dataset-size 50000 --epochs 0", duration = 1800, repeats = 5 },
    { name = "ml_training_5000ms", workload = "ml_training", configs = "baseline,uffd-wp-5000ms,sd-only-5000ms,uffd-sync-5000ms", extra = "--model-size large --dataset-size 50000 --epochs 0", duration = 1800, repeats = 5 },
    { name = "ml_training_10000ms", workload = "ml_training", configs = "baseline,uffd-wp-10000ms,sd-only-10000ms,uffd-sync-10000ms", extra = "--model-size large --dataset-size 50000 --epochs 0", duration = 1800, repeats = 5 },
    { name = "ml_training_60000ms", workload = "ml_training", configs = "baseline,uffd-wp-60000ms,sd-only-60000ms,uffd-sync-60000ms", extra = "--model-size large --dataset-size 50000 --epochs 0", duration = 1800, repeats = 5 },
    { name = "ml_training_180000ms", workload = "ml_training", configs = "baseline,uffd-wp-180000ms,sd-only-180000ms,uffd-sync-180000ms", extra = "--model-size large --dataset-size 50000 --epochs 0", duration = 1800, repeats = 5 },
    { name = "ml_training_300000ms", workload = "ml_training", configs = "baseline,uffd-wp-300000ms,sd-only-300000ms,uffd-sync-300000ms", extra = "--model-size large --dataset-size 50000 --epochs 0", duration = 1800, repeats = 5 },

    # ── redis (DDPS-1052: 5M records, YCSB 4 threads) ──
    { name = "redis_1000ms", workload = "redis", configs = "baseline,uffd-wp-1000ms,sd-only-1000ms,uffd-sync-1000ms", extra = "--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "redis_3000ms", workload = "redis", configs = "baseline,uffd-wp-3000ms,sd-only-3000ms,uffd-sync-3000ms", extra = "--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "redis_5000ms", workload = "redis", configs = "baseline,uffd-wp-5000ms,sd-only-5000ms,uffd-sync-5000ms", extra = "--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "redis_10000ms", workload = "redis", configs = "baseline,uffd-wp-10000ms,sd-only-10000ms,uffd-sync-10000ms", extra = "--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "redis_60000ms", workload = "redis", configs = "baseline,uffd-wp-60000ms,sd-only-60000ms,uffd-sync-60000ms", extra = "--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "redis_180000ms", workload = "redis", configs = "baseline,uffd-wp-180000ms,sd-only-180000ms,uffd-sync-180000ms", extra = "--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "redis_300000ms", workload = "redis", configs = "baseline,uffd-wp-300000ms,sd-only-300000ms,uffd-sync-300000ms", extra = "--ycsb-workload a --ycsb-home /opt/ycsb --record-count 5000000 --ycsb-threads 4", duration = 1800, repeats = 5 },

    # ── memcached (DDPS-1052: 11GB slab, 8.5M records, YCSB 4 threads) ──
    { name = "memcached_1000ms", workload = "memcached", configs = "baseline,uffd-wp-1000ms,sd-only-1000ms,uffd-sync-1000ms", extra = "--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "memcached_3000ms", workload = "memcached", configs = "baseline,uffd-wp-3000ms,sd-only-3000ms,uffd-sync-3000ms", extra = "--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "memcached_5000ms", workload = "memcached", configs = "baseline,uffd-wp-5000ms,sd-only-5000ms,uffd-sync-5000ms", extra = "--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "memcached_10000ms", workload = "memcached", configs = "baseline,uffd-wp-10000ms,sd-only-10000ms,uffd-sync-10000ms", extra = "--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "memcached_60000ms", workload = "memcached", configs = "baseline,uffd-wp-60000ms,sd-only-60000ms,uffd-sync-60000ms", extra = "--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "memcached_180000ms", workload = "memcached", configs = "baseline,uffd-wp-180000ms,sd-only-180000ms,uffd-sync-180000ms", extra = "--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4", duration = 1800, repeats = 5 },
    { name = "memcached_300000ms", workload = "memcached", configs = "baseline,uffd-wp-300000ms,sd-only-300000ms,uffd-sync-300000ms", extra = "--memory-mb 11264 --ycsb-workload a --ycsb-home /opt/ycsb --record-count 8500000 --ycsb-threads 4", duration = 1800, repeats = 5 },

    # ── dataproc (DDPS-1052: 1.5M rows, 60 cols) ──
    { name = "dataproc_1000ms", workload = "dataproc", configs = "baseline,uffd-wp-1000ms,sd-only-1000ms,uffd-sync-1000ms", extra = "--num-rows 1500000 --num-cols 60", duration = 1800, repeats = 5 },
    { name = "dataproc_3000ms", workload = "dataproc", configs = "baseline,uffd-wp-3000ms,sd-only-3000ms,uffd-sync-3000ms", extra = "--num-rows 1500000 --num-cols 60", duration = 1800, repeats = 5 },
    { name = "dataproc_5000ms", workload = "dataproc", configs = "baseline,uffd-wp-5000ms,sd-only-5000ms,uffd-sync-5000ms", extra = "--num-rows 1500000 --num-cols 60", duration = 1800, repeats = 5 },
    { name = "dataproc_10000ms", workload = "dataproc", configs = "baseline,uffd-wp-10000ms,sd-only-10000ms,uffd-sync-10000ms", extra = "--num-rows 1500000 --num-cols 60", duration = 1800, repeats = 5 },
    { name = "dataproc_60000ms", workload = "dataproc", configs = "baseline,uffd-wp-60000ms,sd-only-60000ms,uffd-sync-60000ms", extra = "--num-rows 1500000 --num-cols 60", duration = 1800, repeats = 5 },
    { name = "dataproc_180000ms", workload = "dataproc", configs = "baseline,uffd-wp-180000ms,sd-only-180000ms,uffd-sync-180000ms", extra = "--num-rows 1500000 --num-cols 60", duration = 1800, repeats = 5 },
    { name = "dataproc_300000ms", workload = "dataproc", configs = "baseline,uffd-wp-300000ms,sd-only-300000ms,uffd-sync-300000ms", extra = "--num-rows 1500000 --num-cols 60", duration = 1800, repeats = 5 },
  ]
}
