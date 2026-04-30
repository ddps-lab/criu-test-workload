"""CloudFront edge cache warmer — pagemap-aware.

Warms a destination region's CloudFront edge cache for a CRIU dump prefix by
fetching every IOV that the restore-side lazy-pages daemon will eventually
request. The key insight is that CRIU's page-pipe splits each pagemap entry
to at most ~4 MB at dump time and the lazy restore path never issues a fetch
that crosses a pagemap entry boundary, so warming each (offset, length)
tuple we derive from pagemap-*.img yields Range-cache entries that every
subsequent fetch is either an exact match or a strict subset of. CloudFront
serves strict subsets from the cached Range (see empirical validation in
ddps-1126-issue.md §2).

Event schema (handler mode):
    {
        "distribution_domain": "dxxxx.cloudfront.net",
        "bucket":              "mhsong-criu-checkpoints",
        "prefix":              "memcached-16gb",
        "concurrency":         16,        # optional, default 16
        "mode":                "run"      # "run" | "dispatch", default "run"
    }

In "dispatch" mode the warmer itself splits IOVs into N shards and invokes
the same Lambda asynchronously N times with a "keys" override — used to
scale beyond a single function's NIC cap.

Returns (run mode):
    {
        "distribution_domain": ..., "prefix": ..., "pages_ids": {...},
        "iov_count": N,
        "meta_count": N,           # # non-pages whole-object entries
        "total_bytes": N,
        "wall_ms": N,
        "hit_count": N, "miss_count": N,
        "pop_counts": {...},
        "errors": [...],
        "sample_fetches": [...],   # first 20 per-fetch results for debugging
    }
"""

import json
import logging
import os
import struct
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import urllib3

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

PAGE_SIZE = 4096
PAGEMAP_MAGIC = 1443381285  # from criu/lib/pycriu/images/magic.py

# Shared pool reused across threads. One TCP stream per host + keep-alive
# dramatically reduces cold-warming overhead vs. new connections per GET.
_http = urllib3.PoolManager(
    num_pools=4,
    maxsize=128,
    retries=urllib3.Retry(total=2, backoff_factor=0.3),
    timeout=urllib3.Timeout(connect=10.0, read=900.0),
)
_s3 = boto3.client("s3")
_lambda = boto3.client("lambda")


# ----- pagemap.img parser -------------------------------------------------

def _read_varint(buf, off):
    v = 0
    shift = 0
    while True:
        b = buf[off]
        off += 1
        v |= (b & 0x7F) << shift
        if not (b & 0x80):
            return v, off
        shift += 7


def parse_pagemap(data: bytes):
    """Return (pages_id, [(vaddr, nr_pages, in_parent, flags), ...]).

    CRIU v1.1 image layout: u32 common_magic, u32 specific_magic, then a
    series of length-prefixed protobuf messages. The first is pagemap_head
    (just pages_id), the rest are pagemap_entry.
    """
    off = 0
    _common, specific = struct.unpack_from("<II", data, off)
    off += 8
    if specific != PAGEMAP_MAGIC:
        raise ValueError(f"not a pagemap image (magic=0x{specific:08x})")

    (sz,) = struct.unpack_from("<I", data, off)
    off += 4
    head = data[off:off + sz]
    off += sz
    pages_id = None
    h_off = 0
    while h_off < len(head):
        tag, h_off = _read_varint(head, h_off)
        fld, wire = tag >> 3, tag & 7
        if wire != 0:
            raise ValueError(f"unexpected wire {wire} in head")
        val, h_off = _read_varint(head, h_off)
        if fld == 1:
            pages_id = val
    if pages_id is None:
        raise ValueError("pagemap head missing pages_id")

    entries = []
    while off < len(data):
        (sz,) = struct.unpack_from("<I", data, off)
        off += 4
        msg = data[off:off + sz]
        off += sz
        vaddr = None
        nr_pages = None
        in_parent = False
        flags = None
        m_off = 0
        while m_off < len(msg):
            tag, m_off = _read_varint(msg, m_off)
            fld, wire = tag >> 3, tag & 7
            if wire != 0:
                raise ValueError(f"unexpected wire {wire} in entry")
            val, m_off = _read_varint(msg, m_off)
            if fld == 1:
                vaddr = val
            elif fld == 2:
                nr_pages = val
            elif fld == 3:
                in_parent = bool(val)
            elif fld == 4:
                flags = val
        entries.append((vaddr, nr_pages, in_parent, flags))
    return pages_id, entries


# ----- plan ---------------------------------------------------------------

def _list_prefix(bucket: str, prefix: str):
    """Enumerate every key under s3://bucket/prefix/."""
    keys = []
    token = None
    norm = prefix.rstrip("/") + "/"
    while True:
        kw = {"Bucket": bucket, "Prefix": norm}
        if token:
            kw["ContinuationToken"] = token
        resp = _s3.list_objects_v2(**kw)
        for it in resp.get("Contents", []):
            keys.append((it["Key"], it["Size"]))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def build_plan(bucket: str, prefix: str):
    """Return (iov_fetches, meta_fetches).

    iov_fetches = [(key, offset, length)]  — Range GETs inside pages-*.img
    meta_fetches = [(key, size)]           — whole-object GETs (small files)
    """
    all_keys = _list_prefix(bucket, prefix)
    by_base = {k.rsplit("/", 1)[-1]: (k, s) for k, s in all_keys}
    # find pagemap files
    pagemap_keys = sorted(
        (k, s) for k, s in all_keys
        if k.rsplit("/", 1)[-1].startswith("pagemap-")
    )
    iov_fetches = []
    pages_ids_seen = {}
    for pm_key, _ in pagemap_keys:
        body = _s3.get_object(Bucket=bucket, Key=pm_key)["Body"].read()
        pages_id, entries = parse_pagemap(body)
        pages_basename = f"pages-{pages_id}.img"
        norm = prefix.rstrip("/") + "/"
        pages_key = norm + pages_basename
        if pages_basename not in by_base:
            raise ValueError(f"{pages_key} referenced by {pm_key} missing from prefix")
        offset = 0
        for _, nr_pages, in_parent, _ in entries:
            length = nr_pages * PAGE_SIZE
            if in_parent:
                continue
            iov_fetches.append((pages_key, offset, length))
            offset += length
        pages_ids_seen.setdefault(pages_id, 0)
        pages_ids_seen[pages_id] += len(entries)

    # Meta fetches: everything that isn't a pages-*.img (including the
    # pagemap files themselves, which need to be cached for the restore-side
    # daemon to bootstrap). We use whole-object GETs because each is small.
    meta_fetches = [
        (k, s) for k, s in all_keys
        if not k.rsplit("/", 1)[-1].startswith("pages-")
    ]
    return iov_fetches, meta_fetches, pages_ids_seen


# ----- fetch primitives ---------------------------------------------------

def _warm_range(distribution: str, key: str, offset: int, length: int):
    url = f"https://{distribution}/{key}"
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
    start = time.monotonic()
    try:
        resp = _http.request("GET", url, headers=headers,
                             preload_content=False, redirect=False)
    except Exception as e:
        return {"key": key, "offset": offset, "length": length,
                "status": 0, "error": str(e),
                "elapsed_ms": (time.monotonic() - start) * 1000.0}
    got = 0
    try:
        for chunk in resp.stream(64 * 1024):
            got += len(chunk)
    finally:
        resp.release_conn()
    return {
        "key": key,
        "offset": offset,
        "length": length,
        "status": resp.status,
        "x_cache": resp.headers.get("X-Cache"),
        "x_amz_cf_pop": resp.headers.get("X-Amz-Cf-Pop"),
        "bytes": got,
        "elapsed_ms": round((time.monotonic() - start) * 1000.0, 2),
    }


def _warm_whole(distribution: str, key: str):
    url = f"https://{distribution}/{key}"
    start = time.monotonic()
    try:
        resp = _http.request("GET", url, preload_content=False, redirect=False)
    except Exception as e:
        return {"key": key, "status": 0, "error": str(e),
                "elapsed_ms": (time.monotonic() - start) * 1000.0}
    got = 0
    try:
        for chunk in resp.stream(64 * 1024):
            got += len(chunk)
    finally:
        resp.release_conn()
    return {
        "key": key,
        "status": resp.status,
        "x_cache": resp.headers.get("X-Cache"),
        "x_amz_cf_pop": resp.headers.get("X-Amz-Cf-Pop"),
        "bytes": got,
        "elapsed_ms": round((time.monotonic() - start) * 1000.0, 2),
    }


# ----- main runners -------------------------------------------------------

def _run(distribution: str, iov_fetches, meta_fetches, concurrency: int):
    results = []
    errors = []
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = []
        for key, size in meta_fetches:
            futs.append(pool.submit(_warm_whole, distribution, key))
        for key, off, length in iov_fetches:
            futs.append(pool.submit(_warm_range, distribution, key, off, length))
        for fut in as_completed(futs):
            r = fut.result()
            if r.get("error") or (r.get("status", 0) >= 400):
                errors.append(r)
            else:
                results.append(r)
    wall_ms = (time.monotonic() - t0) * 1000.0

    hits = sum(1 for r in results if (r.get("x_cache") or "").startswith("Hit"))
    miss = sum(1 for r in results if (r.get("x_cache") or "").startswith("Miss"))
    pops = Counter(r.get("x_amz_cf_pop") for r in results if r.get("x_amz_cf_pop"))
    total_bytes = sum(r.get("bytes", 0) for r in results)

    sample = sorted(
        results, key=lambda r: r.get("elapsed_ms", 0), reverse=True
    )[:20]

    return {
        "distribution_domain": distribution,
        "iov_count": len(iov_fetches),
        "meta_count": len(meta_fetches),
        "total_bytes": total_bytes,
        "wall_ms": round(wall_ms, 2),
        "hit_count": hits,
        "miss_count": miss,
        "pop_counts": dict(pops),
        "error_count": len(errors),
        "errors": errors[:20],
        "sample_fetches": sample,
    }


def _dispatch(fn_name, region, distribution, bucket, prefix,
              iov_fetches, meta_fetches, shard_count, concurrency):
    """Fan-out across N child Lambdas by splitting the IOV list."""
    shards = [[] for _ in range(shard_count)]
    for i, iov in enumerate(iov_fetches):
        shards[i % shard_count].append(iov)

    def _invoke(idx, shard):
        payload = {
            "distribution_domain": distribution,
            "bucket": bucket,
            "prefix": prefix,
            "concurrency": concurrency,
            "mode": "run",
            "iov_override": shard,
            # Only the first shard warms metadata files; rest skip to avoid
            # redundant GETs.
            "meta_override": meta_fetches if idx == 0 else [],
        }
        resp = _lambda.invoke(
            FunctionName=fn_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode(),
        )
        return json.loads(resp["Payload"].read())

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=shard_count) as pool:
        child_results = list(pool.map(
            lambda p: _invoke(*p), enumerate(shards)
        ))
    wall_ms = (time.monotonic() - t0) * 1000.0

    total_hit = sum(r.get("hit_count", 0) for r in child_results)
    total_miss = sum(r.get("miss_count", 0) for r in child_results)
    total_bytes = sum(r.get("total_bytes", 0) for r in child_results)
    pop_union = Counter()
    for r in child_results:
        for k, v in (r.get("pop_counts") or {}).items():
            pop_union[k] += v

    return {
        "distribution_domain": distribution,
        "mode": "dispatch",
        "shard_count": shard_count,
        "iov_count": len(iov_fetches),
        "meta_count": len(meta_fetches),
        "total_bytes": total_bytes,
        "wall_ms": round(wall_ms, 2),
        "hit_count": total_hit,
        "miss_count": total_miss,
        "pop_counts": dict(pop_union),
        "child_summaries": [
            {k: v for k, v in r.items() if k not in ("sample_fetches", "errors")}
            for r in child_results
        ],
    }


def lambda_handler(event, _context):
    distribution = event["distribution_domain"]
    bucket = event.get("bucket")
    prefix = event.get("prefix")
    concurrency = int(event.get("concurrency", 16))
    mode = event.get("mode", "run")

    iov_override = event.get("iov_override")
    meta_override = event.get("meta_override")
    if iov_override is not None:
        iov_fetches = [tuple(x) for x in iov_override]
        meta_fetches = [tuple(x) for x in (meta_override or [])]
        pages_ids = None
    else:
        if not bucket or not prefix:
            raise ValueError("bucket + prefix required unless iov_override provided")
        iov_fetches, meta_fetches, pages_ids = build_plan(bucket, prefix)

    log.info("plan: iov=%d meta=%d mode=%s concurrency=%d",
             len(iov_fetches), len(meta_fetches), mode, concurrency)

    if mode == "dispatch":
        fn_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or event.get("fn_name")
        region = os.environ.get("AWS_REGION")
        shard_count = int(event.get("shard_count", 4))
        result = _dispatch(fn_name, region, distribution, bucket, prefix,
                           iov_fetches, meta_fetches, shard_count, concurrency)
    else:
        result = _run(distribution, iov_fetches, meta_fetches, concurrency)

    if pages_ids is not None:
        result["pages_ids"] = pages_ids
    result["prefix"] = prefix
    log.info("summary: hit=%d miss=%d bytes=%d wall_ms=%s pops=%s",
             result.get("hit_count"), result.get("miss_count"),
             result.get("total_bytes"), result.get("wall_ms"),
             result.get("pop_counts"))
    return result


# ----- local debug entrypoint ---------------------------------------------

if __name__ == "__main__":
    import sys
    ev = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {
        "distribution_domain": "deytbsxznbpj1.cloudfront.net",
        "bucket": "mhsong-criu-checkpoints",
        "prefix": "matmul",
        "concurrency": 16,
    }
    print(json.dumps(lambda_handler(ev, None), indent=2, default=str))
