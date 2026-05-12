#!/bin/bash
# build_all.sh — build + push every workload-specific image. Each entry
# is a Dockerfile in this directory plus a matching ECR repository.
#
# Usage:
#   cd criu_workload && ./images/build_all.sh
#
# Repository naming: criu-kubevirt-test/criu-workload-<name>:latest
# (e.g. criu-workload-matmul, criu-workload-xgboost). Paper YAML
# references these in paper_workloads.yaml's `image:` field.
#
# Cost note: each ECR repository is essentially free (storage is by GB,
# small images cost cents/month). Pruning the criu-workload base means
# pulling 150–600 MB per pod instead of 1.6 GB.
set -euo pipefail

REGISTRY="${REGISTRY:-786382940258.dkr.ecr.us-west-2.amazonaws.com/criu-kubevirt-test}"
REGION="${REGION:-us-west-2}"

THIS_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD_ROOT="$(dirname "${THIS_DIR}")"

cd "${WORKLOAD_ROOT}"

# Ensure ECR login. Idempotent.
aws ecr get-login-password --region "${REGION}" 2>/dev/null \
    | docker login --username AWS --password-stdin "${REGISTRY%/*}" >/dev/null

for entry in matmul dataproc ml-training xgboost redis memcached; do
    repo="criu-workload-${entry}"
    full="${REGISTRY}/${repo}:latest"

    # Idempotently create ECR repo.
    aws ecr describe-repositories --repository-names "criu-kubevirt-test/${repo}" --region "${REGION}" >/dev/null 2>&1 \
        || aws ecr create-repository --repository-name "criu-kubevirt-test/${repo}" --region "${REGION}" \
            --tags Key=Project,Value=criu-kubevirt-test >/dev/null

    echo "[build] ${entry} → ${full}"
    docker build -f "images/${entry}.Dockerfile" -t "${full}" . >/dev/null
    docker push "${full}" >/dev/null

    # Print size.
    sleep 1
    sz=$(aws ecr describe-images --repository-name "criu-kubevirt-test/${repo}" --region "${REGION}" \
        --image-ids imageTag=latest --query 'imageDetails[0].imageSizeInBytes' --output text 2>/dev/null || echo 0)
    printf "  size: %.0f MB (compressed)\n" "$(echo "${sz}" | awk '{print $1/1024/1024}')"
done

echo "[build] done"
