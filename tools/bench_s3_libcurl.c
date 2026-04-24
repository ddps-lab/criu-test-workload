/*
 * bench_s3_libcurl.c — S3 Range-GET throughput benchmark in C/libcurl.
 *
 * Uses the same stack as CRIU's obstor_xfer: pthread workers, each with
 * its own CURL easy handle, reusing one TCP/TLS connection via keep-alive
 * for many sequential Range GETs. Apples-to-apples with CRIU's per-ctx
 * behaviour so R(N) here should match CRIU's sweep numbers.
 *
 * Compile:  gcc -O2 -o bench_s3_libcurl bench_s3_libcurl.c -lcurl -lpthread
 *
 * Usage:    bench_s3_libcurl <url> <obj_size> <range_bytes> <duration_s>
 *                             <warmup_s> <N>
 *
 * Emits one JSON line on stdout with the aggregate result.
 */
#include <curl/curl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
	int idx;
	const char *url;
	long obj_size;
	long range_bytes;
	double duration;
	double warmup;
	pthread_barrier_t *barrier;

	/* Results */
	long warm_bytes;
	long warm_reqs;
	long total_bytes;
	long total_reqs;
	char *error;
} worker_ctx_t;

static size_t drop_cb(char *ptr, size_t sz, size_t nm, void *ud)
{
	long *p = (long *)ud;
	*p += sz * nm;
	return sz * nm;
}

static double now_sec(void)
{
	struct timespec t;
	clock_gettime(CLOCK_MONOTONIC, &t);
	return t.tv_sec + t.tv_nsec / 1e9;
}

static void *worker(void *arg)
{
	worker_ctx_t *c = arg;
	CURL *curl = curl_easy_init();
	if (!curl) {
		c->error = strdup("curl_easy_init failed");
		return NULL;
	}

	long body_bytes = 0;
	/* Options cloned from criu-s3 object-storage.c :: set_fixed_curl_options
	 * so this bench exercises exactly the same libcurl config as CRIU's
	 * restore path (HTTP/1.1 keep-alive, forbid_reuse=0, fresh_connect=0,
	 * TCP keepalive tuned identical, no progress callbacks). Matching auth
	 * is skipped here — the caller passes an already-presigned URL, but
	 * the data-plane (connection reuse + range GET) is identical. */
	curl_easy_setopt(curl, CURLOPT_URL, c->url);
	curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, drop_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body_bytes);
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L);
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L);
	curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
	curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
	curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
	curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);

	long max_start = c->obj_size - c->range_bytes;
	/* Per-worker unique starting offset (1 MB aligned, disjoint regions) */
	unsigned long seed = (unsigned long)c->idx * 2654435761UL + (unsigned long)now_sec();
	long off = ((long)(seed % (unsigned long)max_start)) & ~((long)0xFFFFF);

	char range[64];

	/* Phase 1: warmup (not counted) */
	double warm_end = now_sec() + c->warmup;
	while (now_sec() < warm_end) {
		if (off + c->range_bytes > c->obj_size)
			off = 0;
		snprintf(range, sizeof range, "%ld-%ld", off,
			 off + c->range_bytes - 1);
		curl_easy_setopt(curl, CURLOPT_RANGE, range);
		body_bytes = 0;
		CURLcode res = curl_easy_perform(curl);
		if (res != CURLE_OK) {
			c->error = strdup(curl_easy_strerror(res));
			goto cleanup;
		}
		c->warm_bytes += body_bytes;
		c->warm_reqs++;
		off += c->range_bytes;
	}

	/* Barrier: all N workers start measurement simultaneously */
	pthread_barrier_wait(c->barrier);

	/* Phase 2: measurement */
	double end = now_sec() + c->duration;
	while (now_sec() < end) {
		if (off + c->range_bytes > c->obj_size)
			off = 0;
		snprintf(range, sizeof range, "%ld-%ld", off,
			 off + c->range_bytes - 1);
		curl_easy_setopt(curl, CURLOPT_RANGE, range);
		body_bytes = 0;
		CURLcode res = curl_easy_perform(curl);
		if (res != CURLE_OK) {
			c->error = strdup(curl_easy_strerror(res));
			break;
		}
		c->total_bytes += body_bytes;
		c->total_reqs++;
		off += c->range_bytes;
	}

cleanup:
	curl_easy_cleanup(curl);
	return NULL;
}

int main(int argc, char **argv)
{
	if (argc < 7) {
		fprintf(stderr,
			"Usage: %s <url> <obj_size> <range_bytes> "
			"<duration_s> <warmup_s> <N>\n",
			argv[0]);
		return 1;
	}
	const char *url = argv[1];
	long obj_size = atol(argv[2]);
	long range_bytes = atol(argv[3]);
	double duration = atof(argv[4]);
	double warmup = atof(argv[5]);
	int N = atoi(argv[6]);

	if (obj_size <= range_bytes) {
		fprintf(stderr, "ERROR: object %ld B <= range %ld B\n",
			obj_size, range_bytes);
		return 1;
	}
	if (N < 1) {
		fprintf(stderr, "ERROR: N must be >= 1\n");
		return 1;
	}

	curl_global_init(CURL_GLOBAL_DEFAULT);

	pthread_barrier_t barrier;
	pthread_barrier_init(&barrier, NULL, N);

	pthread_t *threads = calloc(N, sizeof(pthread_t));
	worker_ctx_t *ctxs = calloc(N, sizeof(worker_ctx_t));

	fprintf(stderr, "N=%d warmup=%.1fs + measure=%.1fs ...\n",
		N, warmup, duration);
	for (int i = 0; i < N; i++) {
		ctxs[i].idx = i;
		ctxs[i].url = url;
		ctxs[i].obj_size = obj_size;
		ctxs[i].range_bytes = range_bytes;
		ctxs[i].duration = duration;
		ctxs[i].warmup = warmup;
		ctxs[i].barrier = &barrier;
		pthread_create(&threads[i], NULL, worker, &ctxs[i]);
	}

	for (int i = 0; i < N; i++)
		pthread_join(threads[i], NULL);

	long total_bytes = 0, total_reqs = 0, warm_bytes = 0, warm_reqs = 0;
	int errs = 0;
	for (int i = 0; i < N; i++) {
		total_bytes += ctxs[i].total_bytes;
		total_reqs += ctxs[i].total_reqs;
		warm_bytes += ctxs[i].warm_bytes;
		warm_reqs += ctxs[i].warm_reqs;
		if (ctxs[i].error)
			errs++;
	}

	double per_worker_mbps = total_bytes / (N * duration) / 1e6;
	double agg_mbps = total_bytes / duration / 1e6;

	printf("{\"N\":%d,\"duration_s\":%.1f,\"total_bytes\":%ld,"
	       "\"total_reqs\":%ld,\"warm_bytes\":%ld,\"warm_reqs\":%ld,"
	       "\"per_worker_MBps\":%.1f,\"aggregate_MBps\":%.1f,"
	       "\"pct_of_10Gbps\":%.1f,\"errors\":%d}\n",
	       N, duration, total_bytes, total_reqs, warm_bytes, warm_reqs,
	       per_worker_mbps, agg_mbps, agg_mbps / 1250.0 * 100, errs);

	for (int i = 0; i < N; i++)
		free(ctxs[i].error);
	free(threads);
	free(ctxs);
	pthread_barrier_destroy(&barrier);
	curl_global_cleanup();
	return 0;
}
