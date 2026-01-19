/*
 * Fast Dirty Page Tracker using PAGEMAP_SCAN ioctl
 *
 * High-performance C implementation for tracking dirty pages using
 * the PAGEMAP_SCAN ioctl (Linux 6.7+) with soft-dirty fallback.
 *
 * Compatible with Python dirty_tracker output format.
 *
 * Usage:
 *   ./dirty_tracker -p PID -i 100 -d 10 -o output.json
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <getopt.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <linux/userfaultfd.h>

/* PAGEMAP_SCAN definitions (kernel 6.7+) */
#ifndef PAGEMAP_SCAN

#define PAGEMAP_SCAN _IOWR('f', 16, struct pm_scan_arg)

/* Flags for PAGEMAP_SCAN */
#define PM_SCAN_WP_MATCHING  (1 << 0)
#define PM_SCAN_CHECK_WPASYNC (1 << 1)

/* Page flags */
#define PAGE_IS_WPALLOWED    (1 << 0)
#define PAGE_IS_WRITTEN      (1 << 1)
#define PAGE_IS_FILE         (1 << 2)
#define PAGE_IS_PRESENT      (1 << 3)
#define PAGE_IS_SWAPPED      (1 << 4)
#define PAGE_IS_PFNZERO      (1 << 5)
#define PAGE_IS_HUGE         (1 << 6)
#define PAGE_IS_SOFT_DIRTY   (1 << 7)

struct page_region {
    uint64_t start;
    uint64_t end;
    uint64_t categories;
};

struct pm_scan_arg {
    uint64_t size;
    uint64_t flags;
    uint64_t start;
    uint64_t end;
    uint64_t walk_end;
    uint64_t vec;
    uint64_t vec_len;
    uint64_t max_pages;
    uint64_t category_inverted;
    uint64_t category_mask;
    uint64_t category_anyof_mask;
    uint64_t return_mask;
};

#endif /* PAGEMAP_SCAN */

/* Soft-dirty bit in pagemap */
#define PM_SOFT_DIRTY  (1ULL << 55)
#define PM_PRESENT     (1ULL << 63)
#define PM_SWAPPED     (1ULL << 62)

#define PAGE_SIZE 4096
#define MAX_VMAS 4096
#define MAX_REGIONS 65536
#define MAX_SAMPLES 10000

/* VMA types */
typedef enum {
    VMA_HEAP,
    VMA_STACK,
    VMA_ANONYMOUS,
    VMA_CODE,
    VMA_DATA,
    VMA_VDSO,
    VMA_UNKNOWN
} vma_type_t;

/* VMA info */
typedef struct {
    uint64_t start;
    uint64_t end;
    char perms[8];
    char pathname[256];
    vma_type_t type;
} vma_info_t;

/* Dirty page info */
typedef struct {
    uint64_t addr;
    vma_type_t vma_type;
    char perms[8];
    char pathname[256];
} dirty_page_t;

/* Sample */
typedef struct {
    double timestamp_ms;
    dirty_page_t *pages;
    int page_count;
    int pid;
} sample_t;

/* Tracker state */
typedef struct {
    int pid;
    int interval_ms;
    int pagemap_fd;
    int clear_refs_fd;
    bool use_pagemap_scan;

    vma_info_t vmas[MAX_VMAS];
    int vma_count;

    struct page_region regions[MAX_REGIONS];

    sample_t samples[MAX_SAMPLES];
    int sample_count;

    struct timespec start_time;

    /* Statistics */
    int total_dirty_pages;
    uint64_t *unique_addrs;
    int unique_count;
    int unique_capacity;
} tracker_t;

static volatile sig_atomic_t stop_flag = 0;

static void signal_handler(int sig) {
    (void)sig;
    stop_flag = 1;
}

static double get_elapsed_ms(struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - start->tv_sec) * 1000.0 +
           (now.tv_nsec - start->tv_nsec) / 1000000.0;
}

static vma_type_t classify_vma(const char *pathname, const char *perms) {
    if (strcmp(pathname, "[heap]") == 0) return VMA_HEAP;
    if (strcmp(pathname, "[stack]") == 0) return VMA_STACK;
    if (strcmp(pathname, "[vdso]") == 0 ||
        strcmp(pathname, "[vvar]") == 0 ||
        strcmp(pathname, "[vsyscall]") == 0) return VMA_VDSO;
    if (pathname[0] == '/') {
        if (strchr(perms, 'x')) return VMA_CODE;
        return VMA_DATA;
    }
    if (pathname[0] == '\0') return VMA_ANONYMOUS;
    return VMA_UNKNOWN;
}

static const char *vma_type_str(vma_type_t type) {
    switch (type) {
        case VMA_HEAP: return "heap";
        case VMA_STACK: return "stack";
        case VMA_ANONYMOUS: return "anonymous";
        case VMA_CODE: return "code";
        case VMA_DATA: return "data";
        case VMA_VDSO: return "vdso";
        default: return "unknown";
    }
}

static int parse_maps(tracker_t *t) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/maps", t->pid);

    FILE *f = fopen(path, "r");
    if (!f) return -1;

    t->vma_count = 0;
    char line[512];

    while (fgets(line, sizeof(line), f) && t->vma_count < MAX_VMAS) {
        vma_info_t *vma = &t->vmas[t->vma_count];

        uint64_t start, end;
        char perms[8] = {0};
        uint64_t offset;
        int major, minor;
        uint64_t inode;
        char pathname[256] = {0};

        int n = sscanf(line, "%lx-%lx %7s %lx %d:%d %lu %255s",
                       &start, &end, perms, &offset, &major, &minor, &inode, pathname);

        if (n < 5) continue;

        vma->start = start;
        vma->end = end;
        strncpy(vma->perms, perms, sizeof(vma->perms) - 1);
        strncpy(vma->pathname, pathname, sizeof(vma->pathname) - 1);
        vma->type = classify_vma(pathname, perms);

        t->vma_count++;
    }

    fclose(f);
    return 0;
}

static vma_info_t *find_vma(tracker_t *t, uint64_t addr) {
    for (int i = 0; i < t->vma_count; i++) {
        if (addr >= t->vmas[i].start && addr < t->vmas[i].end) {
            return &t->vmas[i];
        }
    }
    return NULL;
}

static bool check_pagemap_scan_support(int fd) {
    struct pm_scan_arg args = {
        .size = sizeof(args),
        .flags = 0,
        .start = 0,
        .end = PAGE_SIZE,
        .vec = 0,
        .vec_len = 0,
        .max_pages = 0,
        .category_inverted = 0,
        .category_mask = 0,
        .category_anyof_mask = PAGE_IS_PRESENT,
        .return_mask = PAGE_IS_SOFT_DIRTY,
    };

    int ret = ioctl(fd, PAGEMAP_SCAN, &args);
    return (ret == 0 || (ret == -1 && errno != ENOTTY && errno != EINVAL));
}

static int tracker_init(tracker_t *t, int pid, int interval_ms) {
    memset(t, 0, sizeof(*t));

    t->pid = pid;
    t->interval_ms = interval_ms;
    t->pagemap_fd = -1;
    t->clear_refs_fd = -1;

    /* Open pagemap */
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/pagemap", pid);
    t->pagemap_fd = open(path, O_RDONLY);
    if (t->pagemap_fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", path, strerror(errno));
        return -1;
    }

    /* Open clear_refs */
    snprintf(path, sizeof(path), "/proc/%d/clear_refs", pid);
    t->clear_refs_fd = open(path, O_WRONLY);
    if (t->clear_refs_fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", path, strerror(errno));
        close(t->pagemap_fd);
        return -1;
    }

    /* Check PAGEMAP_SCAN support */
    t->use_pagemap_scan = check_pagemap_scan_support(t->pagemap_fd);
    fprintf(stderr, "PAGEMAP_SCAN: %s\n", t->use_pagemap_scan ? "supported" : "not supported (using soft-dirty fallback)");

    /* Initialize unique address tracking */
    t->unique_capacity = 65536;
    t->unique_addrs = malloc(t->unique_capacity * sizeof(uint64_t));
    if (!t->unique_addrs) {
        close(t->pagemap_fd);
        close(t->clear_refs_fd);
        return -1;
    }

    return 0;
}

static void tracker_cleanup(tracker_t *t) {
    if (t->pagemap_fd >= 0) close(t->pagemap_fd);
    if (t->clear_refs_fd >= 0) close(t->clear_refs_fd);

    for (int i = 0; i < t->sample_count; i++) {
        free(t->samples[i].pages);
    }

    free(t->unique_addrs);
}

static void clear_soft_dirty(tracker_t *t) {
    lseek(t->clear_refs_fd, 0, SEEK_SET);
    write(t->clear_refs_fd, "4", 1);
}

static void add_unique_addr(tracker_t *t, uint64_t addr) {
    /* Simple linear search (could be optimized with hash set) */
    for (int i = 0; i < t->unique_count; i++) {
        if (t->unique_addrs[i] == addr) return;
    }

    if (t->unique_count >= t->unique_capacity) {
        t->unique_capacity *= 2;
        t->unique_addrs = realloc(t->unique_addrs, t->unique_capacity * sizeof(uint64_t));
    }

    t->unique_addrs[t->unique_count++] = addr;
}

static int read_dirty_pages_pagemap_scan(tracker_t *t, sample_t *sample) {
    /* Allocate temporary buffer for pages */
    int capacity = 4096;
    sample->pages = malloc(capacity * sizeof(dirty_page_t));
    if (!sample->pages) return -1;
    sample->page_count = 0;

    /* Scan each writable VMA separately */
    for (int v = 0; v < t->vma_count; v++) {
        vma_info_t *vma = &t->vmas[v];

        /* Skip non-writable VMAs */
        if (!strchr(vma->perms, 'w')) continue;

        struct pm_scan_arg args = {
            .size = sizeof(args),
            .flags = 0,
            .start = vma->start,
            .end = vma->end,
            .vec = (uint64_t)t->regions,
            .vec_len = MAX_REGIONS,
            .max_pages = 0,
            .category_inverted = PAGE_IS_PFNZERO | PAGE_IS_FILE,
            .category_mask = PAGE_IS_PFNZERO | PAGE_IS_FILE,
            .category_anyof_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED,
            .return_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED | PAGE_IS_SOFT_DIRTY,
        };

        long ret = ioctl(t->pagemap_fd, PAGEMAP_SCAN, &args);
        if (ret < 0) {
            /* PAGEMAP_SCAN failed, skip this VMA */
            continue;
        }

        /* Process returned regions */
        for (long i = 0; i < ret; i++) {
            if (!(t->regions[i].categories & PAGE_IS_SOFT_DIRTY)) continue;

            for (uint64_t addr = t->regions[i].start;
                 addr < t->regions[i].end;
                 addr += PAGE_SIZE) {

                /* Grow buffer if needed */
                if (sample->page_count >= capacity) {
                    capacity *= 2;
                    sample->pages = realloc(sample->pages, capacity * sizeof(dirty_page_t));
                    if (!sample->pages) return -1;
                }

                dirty_page_t *page = &sample->pages[sample->page_count++];
                page->addr = addr;
                page->vma_type = vma->type;
                strncpy(page->perms, vma->perms, sizeof(page->perms) - 1);
                strncpy(page->pathname, vma->pathname, sizeof(page->pathname) - 1);

                add_unique_addr(t, addr);
            }
        }
    }

    return 0;
}

static int read_dirty_pages_soft_dirty(tracker_t *t, sample_t *sample) {
    /* Allocate temporary buffer */
    int capacity = 4096;
    sample->pages = malloc(capacity * sizeof(dirty_page_t));
    if (!sample->pages) return -1;

    sample->page_count = 0;

    for (int v = 0; v < t->vma_count; v++) {
        vma_info_t *vma = &t->vmas[v];

        /* Skip non-writable VMAs */
        if (!strchr(vma->perms, 'w')) continue;

        uint64_t start_page = vma->start / PAGE_SIZE;
        uint64_t num_pages = (vma->end - vma->start) / PAGE_SIZE;
        off_t offset = start_page * sizeof(uint64_t);

        /* Read pagemap entries */
        size_t buf_size = num_pages * sizeof(uint64_t);
        uint64_t *buf = malloc(buf_size);
        if (!buf) continue;

        ssize_t n = pread(t->pagemap_fd, buf, buf_size, offset);
        if (n <= 0) {
            free(buf);
            continue;
        }

        size_t entries = n / sizeof(uint64_t);

        for (size_t i = 0; i < entries; i++) {
            if (buf[i] & PM_SOFT_DIRTY) {
                /* Grow buffer if needed */
                if (sample->page_count >= capacity) {
                    capacity *= 2;
                    sample->pages = realloc(sample->pages, capacity * sizeof(dirty_page_t));
                }

                dirty_page_t *page = &sample->pages[sample->page_count++];
                page->addr = vma->start + i * PAGE_SIZE;
                page->vma_type = vma->type;
                strncpy(page->perms, vma->perms, sizeof(page->perms) - 1);
                strncpy(page->pathname, vma->pathname, sizeof(page->pathname) - 1);

                add_unique_addr(t, page->addr);
            }
        }

        free(buf);
    }

    return 0;
}

static int collect_sample(tracker_t *t) {
    if (t->sample_count >= MAX_SAMPLES) return -1;

    /* Parse maps */
    if (parse_maps(t) < 0) return -1;

    sample_t *sample = &t->samples[t->sample_count];
    sample->timestamp_ms = get_elapsed_ms(&t->start_time);
    sample->pid = t->pid;

    int ret;
    if (t->use_pagemap_scan) {
        ret = read_dirty_pages_pagemap_scan(t, sample);
    } else {
        ret = read_dirty_pages_soft_dirty(t, sample);
    }

    if (ret < 0) return ret;

    t->total_dirty_pages += sample->page_count;
    t->sample_count++;

    /* Clear soft-dirty bits */
    clear_soft_dirty(t);

    return 0;
}

static void write_json_output(tracker_t *t, const char *workload, const char *output_file) {
    FILE *f = output_file ? fopen(output_file, "w") : stdout;
    if (!f) {
        fprintf(stderr, "Failed to open output file: %s\n", strerror(errno));
        return;
    }

    fprintf(f, "{\n");
    fprintf(f, "  \"workload\": \"%s\",\n", workload);
    fprintf(f, "  \"root_pid\": %d,\n", t->pid);
    fprintf(f, "  \"track_children\": false,\n");
    fprintf(f, "  \"tracking_duration_ms\": %.3f,\n",
            t->sample_count > 0 ? t->samples[t->sample_count - 1].timestamp_ms : 0.0);
    fprintf(f, "  \"page_size\": %d,\n", PAGE_SIZE);
    fprintf(f, "  \"pagemap_scan_used\": %s,\n", t->use_pagemap_scan ? "true" : "false");

    /* Samples */
    fprintf(f, "  \"samples\": [\n");
    for (int s = 0; s < t->sample_count; s++) {
        sample_t *sample = &t->samples[s];
        fprintf(f, "    {\n");
        fprintf(f, "      \"timestamp_ms\": %.3f,\n", sample->timestamp_ms);
        fprintf(f, "      \"dirty_pages\": [\n");

        for (int p = 0; p < sample->page_count; p++) {
            dirty_page_t *page = &sample->pages[p];
            fprintf(f, "        {\"addr\": \"0x%lx\", \"vma_type\": \"%s\", \"vma_perms\": \"%s\", \"pathname\": \"%s\", \"size\": %d}%s\n",
                    page->addr, vma_type_str(page->vma_type), page->perms, page->pathname, PAGE_SIZE,
                    p < sample->page_count - 1 ? "," : "");
        }

        fprintf(f, "      ],\n");
        fprintf(f, "      \"delta_dirty_count\": %d,\n", sample->page_count);
        fprintf(f, "      \"pids_tracked\": [%d]\n", sample->pid);
        fprintf(f, "    }%s\n", s < t->sample_count - 1 ? "," : "");
    }
    fprintf(f, "  ],\n");

    /* Summary */
    fprintf(f, "  \"summary\": {\n");
    fprintf(f, "    \"total_unique_pages\": %d,\n", t->unique_count);
    fprintf(f, "    \"total_dirty_events\": %d,\n", t->total_dirty_pages);
    fprintf(f, "    \"total_dirty_size_bytes\": %d,\n", t->total_dirty_pages * PAGE_SIZE);
    fprintf(f, "    \"sample_count\": %d,\n", t->sample_count);
    fprintf(f, "    \"interval_ms\": %d\n", t->interval_ms);
    fprintf(f, "  }\n");

    fprintf(f, "}\n");

    if (output_file) fclose(f);
}

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s -p PID [options]\n", prog);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  -p, --pid PID        Process ID to track (required)\n");
    fprintf(stderr, "  -i, --interval MS    Sampling interval in milliseconds (default: 100)\n");
    fprintf(stderr, "  -d, --duration SEC   Tracking duration in seconds (default: 10)\n");
    fprintf(stderr, "  -o, --output FILE    Output JSON file (default: stdout)\n");
    fprintf(stderr, "  -w, --workload NAME  Workload name (default: unknown)\n");
    fprintf(stderr, "  -h, --help           Show this help\n");
}

int main(int argc, char *argv[]) {
    int pid = 0;
    int interval_ms = 100;
    int duration_sec = 10;
    const char *output_file = NULL;
    const char *workload = "unknown";

    static struct option long_options[] = {
        {"pid", required_argument, 0, 'p'},
        {"interval", required_argument, 0, 'i'},
        {"duration", required_argument, 0, 'd'},
        {"output", required_argument, 0, 'o'},
        {"workload", required_argument, 0, 'w'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "p:i:d:o:w:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'p': pid = atoi(optarg); break;
            case 'i': interval_ms = atoi(optarg); break;
            case 'd': duration_sec = atoi(optarg); break;
            case 'o': output_file = optarg; break;
            case 'w': workload = optarg; break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (pid <= 0) {
        fprintf(stderr, "Error: --pid is required\n");
        print_usage(argv[0]);
        return 1;
    }

    /* Setup signal handler */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /* Initialize tracker */
    tracker_t tracker;
    if (tracker_init(&tracker, pid, interval_ms) < 0) {
        return 1;
    }

    fprintf(stderr, "Tracking PID %d for %d seconds (interval=%dms)\n",
            pid, duration_sec, interval_ms);

    /* Clear soft-dirty initially */
    clear_soft_dirty(&tracker);

    /* Start tracking */
    clock_gettime(CLOCK_MONOTONIC, &tracker.start_time);
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += duration_sec;

    int sample_count = 0;

    while (!stop_flag) {
        struct timespec now, iter_start;
        clock_gettime(CLOCK_MONOTONIC, &iter_start);
        clock_gettime(CLOCK_MONOTONIC, &now);

        if (now.tv_sec > deadline.tv_sec ||
            (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec)) {
            break;
        }

        if (collect_sample(&tracker) < 0) {
            fprintf(stderr, "Failed to collect sample\n");
            break;
        }

        sample_count++;
        if (sample_count % 10 == 0) {
            fprintf(stderr, "Sample %d: %d dirty pages\n",
                    sample_count, tracker.samples[tracker.sample_count - 1].page_count);
        }

        /* Sleep for remaining interval */
        struct timespec iter_end;
        clock_gettime(CLOCK_MONOTONIC, &iter_end);

        long elapsed_ns = (iter_end.tv_sec - iter_start.tv_sec) * 1000000000L +
                          (iter_end.tv_nsec - iter_start.tv_nsec);
        long target_ns = interval_ms * 1000000L;
        long sleep_ns = target_ns - elapsed_ns;

        if (sleep_ns > 0) {
            struct timespec sleep_time = {
                .tv_sec = sleep_ns / 1000000000L,
                .tv_nsec = sleep_ns % 1000000000L
            };
            nanosleep(&sleep_time, NULL);
        }
    }

    fprintf(stderr, "Stopped tracking (total %d samples)\n", tracker.sample_count);

    /* Write output */
    write_json_output(&tracker, workload, output_file);

    if (output_file) {
        fprintf(stderr, "Output written to %s\n", output_file);
    }

    tracker_cleanup(&tracker);
    return 0;
}
