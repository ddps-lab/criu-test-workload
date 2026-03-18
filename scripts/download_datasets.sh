#!/bin/bash
# Download external datasets for CRIU workload experiments
#
# Usage:
#   ./scripts/download_datasets.sh                    # Download all datasets
#   ./scripts/download_datasets.sh --dataset higgs     # Download specific dataset
#   ./scripts/download_datasets.sh --dataset covtype
#   ./scripts/download_datasets.sh --output-dir /data  # Custom output directory
#   ./scripts/download_datasets.sh --check             # Check what's downloaded
#
# Datasets:
#   higgs   - Higgs boson classification (7.5 GB compressed → ~2.6 GB CSV)
#             Source: UCI ML Repository
#             Used by: xgboost_standalone.py --dataset higgs --dataset-path <path>
#
#   covtype - Forest Covertype classification (76 MB)
#             Source: UCI ML Repository / scikit-learn
#             Used by: xgboost_standalone.py --dataset covtype --dataset-path <path>
#             Note: Also available via scikit-learn (auto-download), this is optional

set -e

DEFAULT_OUTPUT_DIR="/data"
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
DATASET="all"
CHECK_ONLY=false

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}[OK]${NC} $1"; }
fail() { echo -e "  ${RED}[MISSING]${NC} $1"; }
warn() { echo -e "  ${YELLOW}[INFO]${NC} $1"; }

# ──────────────────────────────────────────────────
# Parse arguments
# ──────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)
            DATASET="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --check)
            CHECK_ONLY=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--dataset higgs|covtype|all] [--output-dir DIR] [--check]"
            echo ""
            echo "Datasets:"
            echo "  higgs    Higgs boson (7.5 GB compressed, ~2.6 GB CSV)"
            echo "  covtype  Forest Covertype (76 MB)"
            echo "  all      Download all datasets (default)"
            echo ""
            echo "Options:"
            echo "  --output-dir DIR  Output directory (default: /data)"
            echo "  --check           Check what's already downloaded"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# ──────────────────────────────────────────────────
# Check mode
# ──────────────────────────────────────────────────
check_datasets() {
    echo "=== Dataset Status ==="
    echo "Output directory: $OUTPUT_DIR"
    echo ""

    # Higgs
    if [ -f "$OUTPUT_DIR/HIGGS.csv" ]; then
        size=$(du -h "$OUTPUT_DIR/HIGGS.csv" | cut -f1)
        lines=$(wc -l < "$OUTPUT_DIR/HIGGS.csv")
        ok "Higgs: $OUTPUT_DIR/HIGGS.csv ($size, $lines lines)"
    elif [ -f "$OUTPUT_DIR/HIGGS.csv.gz" ]; then
        warn "Higgs: compressed file exists, needs decompression"
        warn "  Run: gunzip $OUTPUT_DIR/HIGGS.csv.gz"
    else
        fail "Higgs: not found at $OUTPUT_DIR/HIGGS.csv"
    fi

    # Covtype
    if [ -f "$OUTPUT_DIR/covtype.data" ] || [ -f "$OUTPUT_DIR/covtype.csv" ]; then
        local covfile
        [ -f "$OUTPUT_DIR/covtype.data" ] && covfile="$OUTPUT_DIR/covtype.data" || covfile="$OUTPUT_DIR/covtype.csv"
        size=$(du -h "$covfile" | cut -f1)
        lines=$(wc -l < "$covfile")
        ok "Covtype: $covfile ($size, $lines lines)"
    else
        warn "Covtype: not at $OUTPUT_DIR (can also load via scikit-learn)"
    fi

    echo ""
    echo "Usage examples:"
    echo "  python3 workloads/xgboost_standalone.py --dataset higgs --dataset-path $OUTPUT_DIR/HIGGS.csv --duration 300"
    echo "  python3 workloads/xgboost_standalone.py --dataset covtype --dataset-path $OUTPUT_DIR/covtype.data --duration 300"
}

if [ "$CHECK_ONLY" = true ]; then
    check_datasets
    exit 0
fi

# ──────────────────────────────────────────────────
# Create output directory
# ──────────────────────────────────────────────────
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Creating output directory: $OUTPUT_DIR"
    sudo mkdir -p "$OUTPUT_DIR"
    sudo chown "$(whoami):$(whoami)" "$OUTPUT_DIR"
fi

# ──────────────────────────────────────────────────
# Download Higgs
# ──────────────────────────────────────────────────
download_higgs() {
    local outfile="$OUTPUT_DIR/HIGGS.csv"

    if [ -f "$outfile" ]; then
        echo "Higgs dataset already exists at $outfile"
        return 0
    fi

    echo "=== Downloading Higgs Dataset ==="
    echo "Source: UCI ML Repository"
    echo "Size: ~7.5 GB compressed → ~2.6 GB CSV"
    echo "Samples: ~11M, Features: 28, Binary classification"
    echo ""

    local url="https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz"
    local gzfile="$OUTPUT_DIR/HIGGS.csv.gz"

    echo "Downloading compressed file..."
    wget --progress=bar:force -O "$gzfile" "$url"

    echo "Decompressing..."
    gunzip "$gzfile"

    local lines=$(wc -l < "$outfile")
    local size=$(du -h "$outfile" | cut -f1)
    ok "Higgs dataset: $outfile ($size, $lines lines)"
}

# ──────────────────────────────────────────────────
# Download Covtype
# ──────────────────────────────────────────────────
download_covtype() {
    local outfile="$OUTPUT_DIR/covtype.data"

    if [ -f "$outfile" ] || [ -f "$OUTPUT_DIR/covtype.csv" ]; then
        echo "Covtype dataset already exists"
        return 0
    fi

    echo "=== Downloading Covtype Dataset ==="
    echo "Source: UCI ML Repository"
    echo "Size: ~76 MB"
    echo "Samples: ~581K, Features: 54, 7-class classification"
    echo "Note: Also loadable via scikit-learn (auto-download)"
    echo ""

    local url="https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz"
    local gzfile="$OUTPUT_DIR/covtype.data.gz"

    echo "Downloading..."
    wget --progress=bar:force -O "$gzfile" "$url"

    echo "Decompressing..."
    gunzip "$gzfile"

    local lines=$(wc -l < "$outfile")
    local size=$(du -h "$outfile" | cut -f1)
    ok "Covtype dataset: $outfile ($size, $lines lines)"
}

# ──────────────────────────────────────────────────
# Execute
# ──────────────────────────────────────────────────
case "$DATASET" in
    higgs)
        download_higgs
        ;;
    covtype)
        download_covtype
        ;;
    all)
        download_higgs
        echo ""
        download_covtype
        ;;
    *)
        echo "Unknown dataset: $DATASET"
        echo "Available: higgs, covtype, all"
        exit 1
        ;;
esac

echo ""
echo "=== Download Complete ==="
check_datasets
