#!/usr/bin/env python3
"""S3 Range-GET per-connection throughput benchmark.

Isolates per-connection S3 throughput R(N) from CRIU's prefetch pipeline —
each worker uses its own requests.Session (HTTP keep-alive + TLS reuse,
matching libcurl connection-pool semantics in obstor_xfer).

Usage:
    bench_s3_range.py --bucket mhsong-criu-checkpoints \
        --key memcached-16gb-compressed/pages-1.img \
        --region us-west-2 \
        --range-mb 4 --duration 30 --workers 1,2,4,8,12,16,20,24,32 \
        --output /tmp/bench_s3.json

Authenticates via boto3 to mint a pre-signed URL once per sweep (so raw
curl/requests can hit S3 without re-signing every request).
"""
import argparse
import json
import random
import socket
import subprocess
import threading
import time
from urllib.parse import urlparse


def presign(bucket, key, region, expires=7200):
    """Use aws CLI to pre-sign the object (avoids needing boto3 installed)."""
    out = subprocess.run(
        ["aws", "s3", "presign", f"s3://{bucket}/{key}",
         "--expires-in", str(expires), "--region", region],
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


def object_size(bucket, key, region):
    out = subprocess.run(
        ["aws", "s3api", "head-object", "--bucket", bucket, "--key", key,
         "--region", region, "--query", "ContentLength", "--output", "text"],
        capture_output=True, text=True, check=True,
    )
    return int(out.stdout.strip())


def _do_range_get(session, url, off, range_bytes):
    """Single Range-GET, fully drain the body. Returns bytes received."""
    headers = {"Range": f"bytes={off}-{off + range_bytes - 1}"}
    resp = session.get(url, headers=headers, stream=True, timeout=30)
    got = 0
    try:
        for chunk in resp.iter_content(chunk_size=65536):
            got += len(chunk)
    finally:
        resp.close()
    return got


def worker(idx, url, obj_size, range_bytes, duration, warmup_per_worker,
           start_barrier, results):
    # Each worker owns its own Session -> own connection pool -> own TCP
    # connection reused across range GETs. Matches libcurl CURL_EASY handle
    # semantics in CRIU.
    #
    # Warmup runs INSIDE the worker so the connection it measures on is
    # already post-TCP-slow-start / TLS-handshake. Buggy earlier: a separate
    # warmup thread's connection died at join() and measurement threads
    # started cold.
    #
    # All workers hit start_barrier.wait() before the measurement window so
    # they begin simultaneously on warm connections (mimics CRIU's steady-
    # state prefetch regime).
    results[idx] = {"bytes": 0, "reqs": 0, "error": None,
                    "warm_reqs": 0, "warm_bytes": 0}
    try:
        import requests
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=1, pool_maxsize=4, max_retries=0,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        if obj_size <= range_bytes:
            raise ValueError(f"object {obj_size} B smaller than range {range_bytes} B")

        max_start = obj_size - range_bytes
        off = random.randint(0, max_start) & ~0xFFFFF  # 1MB-aligned start

        # Phase 1: warmup (not counted in throughput)
        warm_end = time.time() + warmup_per_worker
        warm_bytes = 0
        warm_reqs = 0
        while time.time() < warm_end:
            if off + range_bytes > obj_size:
                off = 0
            warm_bytes += _do_range_get(session, url, off, range_bytes)
            warm_reqs += 1
            off += range_bytes
        results[idx]["warm_bytes"] = warm_bytes
        results[idx]["warm_reqs"] = warm_reqs

        # Synchronise: all workers wait here until everyone finished warmup.
        start_barrier.wait()

        # Phase 2: measurement (counted)
        end = time.time() + duration
        bytes_got = 0
        reqs = 0
        while time.time() < end:
            if off + range_bytes > obj_size:
                off = 0
            bytes_got += _do_range_get(session, url, off, range_bytes)
            reqs += 1
            off += range_bytes

        results[idx]["bytes"] = bytes_got
        results[idx]["reqs"] = reqs
    except Exception as e:
        results[idx]["error"] = f"{type(e).__name__}: {e}"
        print(f"  worker {idx} error: {results[idx]['error']}", flush=True)


def run_sweep(url, obj_size, range_bytes, duration, n_list, warmup):
    rows = []
    for N in n_list:
        print(f"[N={N}] warm {warmup}s (per-worker, parallel) + measure {duration}s ...",
              flush=True)
        results = {}
        threads = []
        # Barrier: N+1 = N workers + main thread (main releases after all
        # workers finish warmup — but actually simpler: just barrier of N,
        # all workers sync on it).
        barrier = threading.Barrier(N)

        for i in range(N):
            t = threading.Thread(target=worker,
                                 args=(i, url, obj_size, range_bytes, duration,
                                       warmup, barrier, results))
            t.start()
            threads.append(t)

        # Wait for all workers to either reach barrier or error out. We can't
        # time the measurement window from main thread accurately, so we
        # trust each worker's own `duration` and aggregate results.
        for t in threads:
            t.join()

        # Measurement duration is `duration` per-worker (they all started
        # simultaneously after barrier).
        actual = duration

        total_bytes = sum(r["bytes"] for r in results.values())
        total_reqs = sum(r["reqs"] for r in results.values())
        warm_bytes = sum(r.get("warm_bytes", 0) for r in results.values())
        warm_reqs = sum(r.get("warm_reqs", 0) for r in results.values())
        errors = [r["error"] for r in results.values() if r.get("error")]

        per_worker_wall = total_bytes / (N * actual) / 1e6  # MB/s
        agg_wall = total_bytes / actual / 1e6  # MB/s
        nic_pct_10g = agg_wall / (10_000 / 8) * 100

        row = {
            "N": N, "duration_s": actual,
            "total_bytes": total_bytes, "total_reqs": total_reqs,
            "warm_bytes": warm_bytes, "warm_reqs": warm_reqs,
            "per_worker_MBps": round(per_worker_wall, 1),
            "aggregate_MBps": round(agg_wall, 1),
            "pct_of_10Gbps": round(nic_pct_10g, 1),
            "errors": errors,
        }
        rows.append(row)
        print(f"  → per-worker {row['per_worker_MBps']:.1f} MB/s, "
              f"aggregate {row['aggregate_MBps']:.1f} MB/s "
              f"({row['pct_of_10Gbps']:.0f}% of 10 Gbps)"
              + (f"  [{len(errors)} worker errors]" if errors else ""))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--range-mb", type=int, default=4)
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--workers", default="1,2,4,8,12,16,20,24,32")
    ap.add_argument("--output", default="/tmp/bench_s3.json")
    ap.add_argument("--label", default="",
                    help="Free-form label (e.g. instance type) saved with results")
    args = ap.parse_args()

    n_list = [int(x) for x in args.workers.split(",") if x.strip()]
    range_bytes = args.range_mb * 1024 * 1024

    print(f"Target: s3://{args.bucket}/{args.key} (region={args.region})")
    sz = object_size(args.bucket, args.key, args.region)
    print(f"Object size: {sz:,} bytes ({sz/1e9:.2f} GB)")
    url = presign(args.bucket, args.key, args.region)

    # Probe the actual endpoint so we note which S3 PoP we hit
    host = urlparse(url).hostname
    try:
        addrs = {a[-1][0] for a in socket.getaddrinfo(host, 443)}
    except Exception:
        addrs = set()
    print(f"Endpoint: {host} → {sorted(addrs)}")

    rows = run_sweep(url, sz, range_bytes, args.duration, n_list, args.warmup)

    out = {
        "label": args.label,
        "bucket": args.bucket,
        "key": args.key,
        "region": args.region,
        "object_size": sz,
        "range_mb": args.range_mb,
        "duration_s": args.duration,
        "endpoint_host": host,
        "endpoint_ips": sorted(addrs),
        "rows": rows,
    }
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
