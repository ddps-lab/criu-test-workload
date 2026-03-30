/*
 * Fast Dirty Page Tracker using PAGEMAP_SCAN ioctl
 *
 * High-performance C implementation for tracking dirty pages using
 * the PAGEMAP_SCAN ioctl (Linux 6.7+) with soft-dirty fallback.
 *
 * Supports child process tracking: automatically discovers and tracks
 * descendant processes of the root PID.
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
#include <sys/ptrace.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <dirent.h>
#include <linux/userfaultfd.h>
#include <pthread.h>
#include <poll.h>

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

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif
#define MAX_VMAS 4096
#define MAX_REGIONS 65536
#define MAX_PROCESSES 64
#define UNIQUE_HASH_SIZE 65537  /* prime, for unique address hash table */

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

/* Sync mode dirty page collection buffer */
typedef struct {
    uint64_t *addrs;
    size_t count;
    size_t capacity;
    pthread_mutex_t lock;
} sync_dirty_set_t;

/* Per-process tracking state */
typedef struct {
    pid_t pid;
    int pagemap_fd;
    int clear_refs_fd;
    bool is_alive;

    /* uffd-wp state */
    long target_uffd;       /* userfaultfd fd number inside the target process (-1 if not set) */
    bool wp_active;         /* true if WP mode is actually working */
    bool wp_initialized;    /* set after initial WP probe/setup */

    /* uffd-sync mode state */
    int tracker_uffd_fd;            /* uffd fd copied to tracker via pidfd_getfd (-1 if not set) */
    sync_dirty_set_t sync_dirty;    /* dirty page collection for sync mode */
    pthread_t sync_handler_thread;  /* fault handler thread */
    volatile bool sync_handler_running; /* handler thread is active */
    volatile bool stop_sync_handler;    /* signal to stop handler thread */
    volatile bool pause_sync_handler;   /* pause handler during collect_sample re-protect */

    /* Per-process VMAs */
    vma_info_t *vmas;
    int vma_count;
    int vma_capacity;

    /* Registered VMA tracking for re-registration of new VMAs */
    uint64_t *registered_vma_starts;
    uint64_t *registered_vma_ends;
    int registered_vma_count;
    int registered_vma_capacity;

    /* Whether this process supports PAGEMAP_SCAN */
    bool use_pagemap_scan;
} process_tracker_t;

/* Per-VMA dirty page summary (lightweight, for timeline) */
typedef struct {
    uint64_t start;
    uint64_t end;
    int dirty_pages;
    int total_pages;
    char perms[8];
    vma_type_t vma_type;
} vma_dirty_summary_t;

/* Sample */
typedef struct {
    double timestamp_ms;
    dirty_page_t *pages;    /* Primary channel (WP or soft-dirty depending on mode) */
    int page_count;
    int page_capacity;      /* Current allocation capacity for pages */
    dirty_page_t *sd_pages; /* Soft-dirty channel (dual-channel mode only) */
    int sd_page_count;
    int sd_page_capacity;   /* Current allocation capacity for sd_pages */
    pid_t *pids_tracked;    /* Array of PIDs tracked in this sample */
    int pids_tracked_count;
    /* Memory usage (aggregate across all tracked processes) */
    long rss_bytes;             /* Resident Set Size from /proc/pid/statm */
    long writable_vma_bytes;    /* Sum of writable VMA sizes from /proc/pid/maps */
    /* Per-VMA dirty summary (all writable VMAs, including dirty=0) */
    vma_dirty_summary_t *vma_summaries;
    int vma_summary_count;
    int vma_summary_capacity;
} sample_t;

/* Unique address hash node */
typedef struct unique_node {
    uint64_t addr;
    struct unique_node *next;
} unique_node_t;

/* Timeline entry (lightweight, for dirty_rate_timeline output) */
typedef struct {
    double timestamp_ms;
    double rate_pages_per_sec;
    int cumulative_pages;
    int processes_tracked;
    vma_dirty_summary_t *vma_summaries;  /* owned copy (pointer transfer from sample) */
    int vma_summary_count;
} timeline_entry_t;

/* Tracker state */
typedef struct {
    pid_t root_pid;
    int interval_ms;
    bool no_clear;          /* If true, don't clear dirty bits after scan */
    bool track_children;    /* Track descendant processes (default: true) */

    /* Process management */
    process_tracker_t *processes[MAX_PROCESSES];
    int process_count;

    /* PID tracking */
    pid_t exclude_pids[64];
    int exclude_pid_count;
    pid_t known_pids[MAX_PROCESSES * 2];  /* All PIDs ever seen */
    int known_pid_count;

    /* Shared scan buffers */
    struct page_region regions[MAX_REGIONS];
    struct page_region sd_regions[MAX_REGIONS];

    /* Current sample (reused each iteration, flushed+freed after each) */
    sample_t current_sample;
    int sample_count;            /* total samples collected (monotonic) */
    double prev_timestamp_ms;    /* for rate calculation */
    struct timespec start_time;

    /* Aggregate statistics */
    int total_dirty_pages;
    unique_node_t *unique_hash[UNIQUE_HASH_SIZE];
    int unique_count;

    /* VMA type counters (indexed by vma_type_t) */
    int vma_type_counts[7];
    int vma_type_sizes[7];

    /* Dual-channel mode */
    bool dual_channel;     /* collect both WP and soft-dirty simultaneously */
    bool sd_clear;         /* clear soft-dirty after each dual-channel scan */

    /* OoH comparison modes */
    bool sd_only;          /* --sd-only: soft-dirty clear+read only, no uffd (OoH /proc) */
    bool uffd_sync;        /* --uffd-sync: userfaultfd synchronous WP mode (OoH ufd) */

    /* Verbosity */
    bool verbose;          /* -v: print per-sample progress and VMA re-register messages */

    /* Streaming output */
    FILE *output_fp;            /* opened at start, written incrementally */
    bool no_output;             /* -Q: scan+track but don't store/write page data */
    int samples_written;        /* number of samples already flushed to file */

    /* Incremental summary stats */
    double sum_rate;            /* running sum of per-sample dirty rates */
    int rate_count;             /* number of positive-rate samples */
    double peak_rate;           /* max rate seen */
    int cumulative_dirty;       /* running total dirty page count */

    /* Timeline (lightweight entries for dirty_rate_timeline) */
    timeline_entry_t *timeline;
    int timeline_count;
    int timeline_capacity;
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

/* ===== Per-process functions ===== */

static int parse_maps_for_process(process_tracker_t *pt) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/maps", pt->pid);

    FILE *f = fopen(path, "r");
    if (!f) return -1;

    pt->vma_count = 0;
    char line[512];

    while (fgets(line, sizeof(line), f) && pt->vma_count < pt->vma_capacity) {
        vma_info_t *vma = &pt->vmas[pt->vma_count];

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

        pt->vma_count++;
    }

    fclose(f);
    return 0;
}

/**
 * Read RSS (Resident Set Size) from /proc/{pid}/statm.
 * Returns RSS in bytes, or 0 on failure.
 */
static long read_rss_bytes(pid_t pid) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/statm", pid);

    FILE *f = fopen(path, "r");
    if (!f) return 0;

    long size_pages, rss_pages;
    if (fscanf(f, "%ld %ld", &size_pages, &rss_pages) != 2) {
        fclose(f);
        return 0;
    }
    fclose(f);

    return rss_pages * PAGE_SIZE;
}

/**
 * Calculate total writable VMA size for a process.
 * Called after parse_maps_for_process().
 */
static long calc_writable_vma_bytes(process_tracker_t *pt) {
    long total = 0;
    for (int i = 0; i < pt->vma_count; i++) {
        if (strchr(pt->vmas[i].perms, 'w')) {
            total += (long)(pt->vmas[i].end - pt->vmas[i].start);
        }
    }
    return total;
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

/**
 * Initialize a per-process tracker for the given PID.
 * Opens pagemap/clear_refs, allocates VMA buffer.
 * Returns NULL on failure (caller should skip this PID).
 */
static process_tracker_t *process_tracker_init(pid_t pid) {
    process_tracker_t *pt = calloc(1, sizeof(process_tracker_t));
    if (!pt) return NULL;

    pt->pid = pid;
    pt->pagemap_fd = -1;
    pt->clear_refs_fd = -1;
    pt->target_uffd = -1;
    pt->tracker_uffd_fd = -1;
    pt->is_alive = true;
    pthread_mutex_init(&pt->sync_dirty.lock, NULL);

    /* Allocate VMA buffer */
    pt->vma_capacity = MAX_VMAS;
    pt->vmas = malloc(pt->vma_capacity * sizeof(vma_info_t));
    if (!pt->vmas) {
        free(pt);
        return NULL;
    }

    /* Open pagemap */
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/pagemap", pid);
    pt->pagemap_fd = open(path, O_RDONLY);
    if (pt->pagemap_fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", path, strerror(errno));
        free(pt->vmas);
        free(pt);
        return NULL;
    }

    /* Open clear_refs (optional — not used in WP mode) */
    snprintf(path, sizeof(path), "/proc/%d/clear_refs", pid);
    pt->clear_refs_fd = open(path, O_WRONLY);
    /* Not fatal if this fails */

    /* Check PAGEMAP_SCAN support */
    pt->use_pagemap_scan = check_pagemap_scan_support(pt->pagemap_fd);

    return pt;
}

/* Forward declarations */
static int cleanup_userfaultfd_wp_for_process(process_tracker_t *pt);

static void process_tracker_cleanup(process_tracker_t *pt) {
    if (!pt) return;

    /* Stop sync handler thread if running */
    if (pt->sync_handler_running) {
        pt->stop_sync_handler = true;
        pthread_join(pt->sync_handler_thread, NULL);
        pt->sync_handler_running = false;
    }

    /* Clean up uffd-wp in target process */
    if (pt->wp_active && pt->target_uffd >= 0) {
        cleanup_userfaultfd_wp_for_process(pt);
    }

    /* Clean up tracker-side uffd fd */
    if (pt->tracker_uffd_fd >= 0) {
        close(pt->tracker_uffd_fd);
        pt->tracker_uffd_fd = -1;
    }

    /* Clean up sync dirty set */
    free(pt->sync_dirty.addrs);
    pthread_mutex_destroy(&pt->sync_dirty.lock);

    if (pt->pagemap_fd >= 0) close(pt->pagemap_fd);
    if (pt->clear_refs_fd >= 0) close(pt->clear_refs_fd);
    free(pt->vmas);
    free(pt->registered_vma_starts);
    free(pt->registered_vma_ends);
    free(pt);
}

static void clear_soft_dirty_for_process(process_tracker_t *pt) {
    if (pt->clear_refs_fd >= 0) {
        lseek(pt->clear_refs_fd, 0, SEEK_SET);
        if (write(pt->clear_refs_fd, "4", 1) < 0) {
            fprintf(stderr, "Warning: clear_refs write failed for pid %d: %s\n",
                    pt->pid, strerror(errno));
        }
    }
}

/* ===== Tracker-level init/cleanup ===== */

static int tracker_init(tracker_t *t, pid_t root_pid, int interval_ms,
                        bool no_clear, bool dual_channel, bool sd_clear,
                        bool track_children, bool sd_only, bool uffd_sync,
                        bool no_output, bool verbose) {
    memset(t, 0, sizeof(*t));

    t->root_pid = root_pid;
    t->interval_ms = interval_ms;
    t->no_clear = no_clear;
    t->dual_channel = dual_channel;
    t->sd_clear = sd_clear;
    t->track_children = track_children;
    t->sd_only = sd_only;
    t->uffd_sync = uffd_sync;
    t->no_output = no_output;
    t->verbose = verbose;

    /* unique_hash is zeroed by memset above */

    /* Initialize timeline */
    t->timeline_capacity = 1024;
    t->timeline = malloc(t->timeline_capacity * sizeof(timeline_entry_t));
    if (!t->timeline) return -1;

    /* Initialize root process tracker */
    process_tracker_t *root_pt = process_tracker_init(root_pid);
    if (!root_pt) {
        free(t->timeline);
        return -1;
    }

    t->processes[0] = root_pt;
    t->process_count = 1;
    t->known_pids[0] = root_pid;
    t->known_pid_count = 1;

    fprintf(stderr, "PAGEMAP_SCAN: %s\n",
            root_pt->use_pagemap_scan ? "supported" : "not supported (using soft-dirty fallback)");

    return 0;
}

static void tracker_cleanup(tracker_t *t) {
    /* Clean up all process trackers */
    for (int i = 0; i < t->process_count; i++) {
        process_tracker_cleanup(t->processes[i]);
        t->processes[i] = NULL;
    }
    t->process_count = 0;

    /* Free current_sample if not yet flushed */
    free(t->current_sample.pages);
    free(t->current_sample.sd_pages);
    free(t->current_sample.pids_tracked);
    free(t->current_sample.vma_summaries);

    /* Free unique address hash table */
    for (int i = 0; i < UNIQUE_HASH_SIZE; i++) {
        unique_node_t *node = t->unique_hash[i];
        while (node) {
            unique_node_t *next = node->next;
            free(node);
            node = next;
        }
    }

    /* Free timeline (including per-entry VMA summaries) */
    for (int i = 0; i < t->timeline_count; i++) {
        free(t->timeline[i].vma_summaries);
    }
    free(t->timeline);
}

/*
 * ==========================================================================
 * Ptrace-based userfaultfd-wp injection
 *
 * To use PM_SCAN_WP_MATCHING for atomic dirty page tracking, the target
 * process's VMAs need userfaultfd write-protection (VM_UFFD_WP) enabled.
 * Since userfaultfd can only register VMAs from within the owning process,
 * we inject the necessary syscalls via ptrace (similar to CRIU's compel).
 *
 * Flow:
 *   1. ptrace SEIZE + INTERRUPT the target
 *   2. Save registers + instruction at RIP
 *   3. Poke 'syscall' instruction at RIP
 *   4. Inject mmap() → scratch page in target's address space
 *   5. Inject userfaultfd() → fd in target
 *   6. Write uffdio_api struct to scratch page, inject ioctl(UFFDIO_API)
 *   7. For each writable VMA: write uffdio_register, inject ioctl(UFFDIO_REGISTER)
 *   8. Inject munmap() to free scratch page
 *   9. Restore original state, detach
 * ==========================================================================
 */

/* Guard for older kernel headers */
#ifndef UFFD_FEATURE_WP_ASYNC
#define UFFD_FEATURE_WP_ASYNC (1 << 15)
#endif

/**
 * Inject a single syscall into a stopped (ptraced) process.
 *
 * Assumes a 'syscall' instruction (0x0F 0x05) has already been poked at
 * saved_rip. Sets up registers, single-steps, returns result via *result.
 *
 * Returns 0 on success, -1 on failure.
 */
static int inject_syscall(pid_t pid, uint64_t saved_rip,
                          long nr, long a1, long a2, long a3,
                          long a4, long a5, long a6,
                          long *result)
{
    struct user_regs_struct regs;
    if (ptrace(PTRACE_GETREGS, pid, 0, &regs) < 0) return -1;

    regs.rip = saved_rip;
    regs.rax = nr;
    regs.rdi = a1;
    regs.rsi = a2;
    regs.rdx = a3;
    regs.r10 = a4;
    regs.r8  = a5;
    regs.r9  = a6;

    if (ptrace(PTRACE_SETREGS, pid, 0, &regs) < 0) return -1;
    if (ptrace(PTRACE_SINGLESTEP, pid, 0, 0) < 0) return -1;

    int status;
    if (waitpid(pid, &status, 0) < 0) return -1;

    if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP) {
        fprintf(stderr, "inject_syscall: unexpected stop (status=0x%x)\n", status);
        return -1;
    }

    if (ptrace(PTRACE_GETREGS, pid, 0, &regs) < 0) return -1;
    *result = (long)regs.rax;

    return 0;
}

/**
 * Write data to a target process's memory via PTRACE_POKETEXT.
 * Must be called while target is stopped.
 */
static int write_to_target(pid_t pid, uint64_t addr, const void *data, size_t len)
{
    const uint8_t *src = (const uint8_t *)data;
    size_t i;

    for (i = 0; i + sizeof(long) <= len; i += sizeof(long)) {
        long val;
        memcpy(&val, src + i, sizeof(long));
        if (ptrace(PTRACE_POKETEXT, pid, addr + i, val) < 0) return -1;
    }

    /* Handle trailing bytes (partial word) */
    if (i < len) {
        errno = 0;
        long val = ptrace(PTRACE_PEEKTEXT, pid, addr + i, 0);
        if (errno) return -1;
        memcpy(&val, src + i, len - i);
        if (ptrace(PTRACE_POKETEXT, pid, addr + i, val) < 0) return -1;
    }

    return 0;
}

#define MAX_THREADS 512

/**
 * Get all thread IDs for a given process.
 * Returns the number of threads found (up to max).
 */
static int get_all_threads(pid_t pid, pid_t *out, int max)
{
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/task", pid);
    DIR *dir = opendir(path);
    if (!dir) return 0;
    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL && count < max) {
        pid_t tid = (pid_t)atoi(entry->d_name);
        if (tid > 0)
            out[count++] = tid;
    }
    closedir(dir);
    return count;
}

/**
 * Freeze all threads of a process except skip_tid (the main thread being
 * injected on). Each thread is PTRACE_SEIZE'd + INTERRUPT'd + waitpid'd.
 * Failures on individual threads are silently skipped (thread may have exited).
 * Returns number of threads frozen; out_tids[0..out_count-1] filled for thaw.
 */
static int freeze_other_threads(pid_t pid, pid_t skip_tid,
                                pid_t *out_tids, int *out_count)
{
    pid_t threads[MAX_THREADS];
    int n = get_all_threads(pid, threads, MAX_THREADS);
    int frozen = 0;
    for (int i = 0; i < n; i++) {
        if (threads[i] == skip_tid) continue;
        if (ptrace(PTRACE_SEIZE, threads[i], 0, 0) < 0) continue;
        if (ptrace(PTRACE_INTERRUPT, threads[i], 0, 0) < 0) {
            ptrace(PTRACE_DETACH, threads[i], 0, 0);
            continue;
        }
        int status;
        if (waitpid(threads[i], &status, __WALL) < 0) {
            ptrace(PTRACE_DETACH, threads[i], 0, 0);
            continue;
        }
        out_tids[frozen++] = threads[i];
    }
    *out_count = frozen;
    return frozen;
}

/**
 * Thaw (PTRACE_DETACH) all previously frozen threads.
 */
static void thaw_other_threads(pid_t *tids, int count)
{
    for (int i = 0; i < count; i++) {
        ptrace(PTRACE_DETACH, tids[i], 0, 0);
    }
}

/**
 * Setup userfaultfd write-protection on a process via ptrace injection.
 *
 * This enables PM_SCAN_WP_MATCHING to work by registering each writable VMA
 * with userfaultfd in UFFDIO_REGISTER_MODE_WP + UFFD_FEATURE_WP_ASYNC.
 *
 * All threads are frozen during setup to prevent interference with syscall
 * injection (CRIU compel-style). The target process is briefly stopped
 * during setup (~10ms for multi-threaded processes).
 * With WP_ASYNC, subsequent write faults are handled inline by the kernel
 * (no userspace handler needed), so the workload runs at near-native speed.
 *
 * Returns 0 on success, -1 on failure (caller should fall back to soft-dirty).
 */
static int setup_userfaultfd_wp_for_process(process_tracker_t *pt, bool uffd_sync_mode)
{
    pid_t pid = pt->pid;
    int ret = -1;

    struct timespec inject_start, inject_end;
    clock_gettime(CLOCK_MONOTONIC, &inject_start);

    /* 1. Seize and interrupt the target process */
    if (ptrace(PTRACE_SEIZE, pid, 0, 0) < 0) {
        fprintf(stderr, "ptrace SEIZE failed (pid=%d): %s\n", pid, strerror(errno));
        return -1;
    }
    if (ptrace(PTRACE_INTERRUPT, pid, 0, 0) < 0) {
        fprintf(stderr, "ptrace INTERRUPT failed: %s\n", strerror(errno));
        ptrace(PTRACE_DETACH, pid, 0, 0);
        return -1;
    }

    int status;
    if (waitpid(pid, &status, 0) < 0) {
        ptrace(PTRACE_DETACH, pid, 0, 0);
        return -1;
    }

    /* 1b. Freeze all sibling threads for safe syscall injection */
    pid_t frozen_tids[MAX_THREADS];
    int frozen_count = 0;
    freeze_other_threads(pid, pid, frozen_tids, &frozen_count);
    if (frozen_count > 0)
        fprintf(stderr, "Froze %d sibling threads (pid=%d)\n", frozen_count, pid);

    /* 2. Save original state */
    struct user_regs_struct saved_regs;
    if (ptrace(PTRACE_GETREGS, pid, 0, &saved_regs) < 0) {
        fprintf(stderr, "GETREGS failed: %s\n", strerror(errno));
        goto detach;
    }

    errno = 0;
    long saved_code = ptrace(PTRACE_PEEKTEXT, pid, saved_regs.rip, 0);
    if (errno) {
        fprintf(stderr, "PEEKTEXT failed: %s\n", strerror(errno));
        goto detach;
    }

    /* 3. Poke 'syscall' instruction (0x0F 0x05) at current RIP */
    long code_with_syscall = (saved_code & ~0xFFFFL) | 0x050FL;
    if (ptrace(PTRACE_POKETEXT, pid, saved_regs.rip, code_with_syscall) < 0) {
        fprintf(stderr, "POKETEXT (syscall) failed: %s\n", strerror(errno));
        goto detach;
    }

    long result;

    /* 4. Inject mmap() to get a scratch page in target's address space */
    if (inject_syscall(pid, saved_regs.rip,
                       __NR_mmap, 0, PAGE_SIZE,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0,
                       &result) < 0 || result < 0) {
        fprintf(stderr, "inject mmap failed: result=%ld\n", result);
        goto restore;
    }
    uint64_t scratch = (uint64_t)result;

    /* 5. Inject userfaultfd() syscall
     *    For async mode: use UFFD_USER_MODE_ONLY to bypass CAP_SYS_PTRACE.
     *    For sync mode: MUST NOT use UFFD_USER_MODE_ONLY because it rejects
     *    kernel-mode WP faults (e.g., on TLS pages accessed during syscalls),
     *    which breaks fault delivery. Instead, temporarily enable sysctl.
     */
    if (!uffd_sync_mode) {
        /* Async mode: try UFFD_USER_MODE_ONLY first */
        if (inject_syscall(pid, saved_regs.rip,
                           __NR_userfaultfd,
                           O_CLOEXEC | O_NONBLOCK | UFFD_USER_MODE_ONLY,
                           0, 0, 0, 0, 0, &result) < 0 || result < 0) {
            /* Fallback without UFFD_USER_MODE_ONLY */
            if (inject_syscall(pid, saved_regs.rip,
                               __NR_userfaultfd,
                               O_CLOEXEC | O_NONBLOCK,
                               0, 0, 0, 0, 0, &result) < 0 || result < 0) {
                fprintf(stderr, "inject userfaultfd failed (pid=%d): result=%ld\n", pid, result);
                fprintf(stderr, "  hint: try 'sysctl -w vm.unprivileged_userfaultfd=1'\n");
                goto cleanup_mmap;
            }
        }
    } else {
        /* Sync mode: enable sysctl, create uffd WITHOUT UFFD_USER_MODE_ONLY */
        FILE *sysctl_f = fopen("/proc/sys/vm/unprivileged_userfaultfd", "r");
        int saved_sysctl = 0;
        if (sysctl_f) { fscanf(sysctl_f, "%d", &saved_sysctl); fclose(sysctl_f); }
        if (!saved_sysctl) {
            sysctl_f = fopen("/proc/sys/vm/unprivileged_userfaultfd", "w");
            if (sysctl_f) { fprintf(sysctl_f, "1"); fclose(sysctl_f); }
        }

        if (inject_syscall(pid, saved_regs.rip,
                           __NR_userfaultfd,
                           O_CLOEXEC | O_NONBLOCK,
                           0, 0, 0, 0, 0, &result) < 0 || result < 0) {
            fprintf(stderr, "inject userfaultfd (sync) failed (pid=%d): result=%ld\n", pid, result);
            goto cleanup_mmap;
        }

        /* Restore sysctl */
        if (!saved_sysctl) {
            sysctl_f = fopen("/proc/sys/vm/unprivileged_userfaultfd", "w");
            if (sysctl_f) { fprintf(sysctl_f, "0"); fclose(sysctl_f); }
        }
    }
    long uffd = result;
    fprintf(stderr, "Injected userfaultfd -> fd=%ld (pid=%d)\n", uffd, pid);

    /* 6. UFFDIO_API: enable WP features */
    {
        struct uffdio_api api = {
            .api = UFFD_API,
            .features = uffd_sync_mode ? 0 : (UFFD_FEATURE_WP_ASYNC | UFFD_FEATURE_WP_UNPOPULATED),
        };
        if (write_to_target(pid, scratch, &api, sizeof(api)) < 0) {
            fprintf(stderr, "write uffdio_api to target failed\n");
            goto cleanup_uffd;
        }
        if (inject_syscall(pid, saved_regs.rip,
                           __NR_ioctl, uffd, UFFDIO_API, scratch,
                           0, 0, 0, &result) < 0 || result < 0) {
            fprintf(stderr, "inject UFFDIO_API failed: result=%ld\n", result);
            goto cleanup_uffd;
        }
        fprintf(stderr, "UFFDIO_API success (%s, pid=%d)\n",
                uffd_sync_mode ? "sync WP mode" : "WP_ASYNC enabled", pid);
    }

    /* 7. Register each writable VMA with UFFDIO_REGISTER_MODE_WP */
    {
        int registered = 0, skipped = 0;
        for (int v = 0; v < pt->vma_count; v++) {
            vma_info_t *vma = &pt->vmas[v];

            /* Only register writable VMAs */
            if (!strchr(vma->perms, 'w')) continue;

            /* Skip shared mappings (must be private 'p') */
            if (!strchr(vma->perms, 'p')) {
                skipped++;
                continue;
            }

            /* Skip vdso/vvar/vsyscall */
            if (vma->type == VMA_VDSO) {
                skipped++;
                continue;
            }

            struct uffdio_register reg = {
                .range = {
                    .start = vma->start,
                    .len = vma->end - vma->start,
                },
                .mode = UFFDIO_REGISTER_MODE_WP,
            };

            if (write_to_target(pid, scratch, &reg, sizeof(reg)) < 0) {
                skipped++;
                continue;
            }

            if (inject_syscall(pid, saved_regs.rip,
                               __NR_ioctl, uffd, UFFDIO_REGISTER, scratch,
                               0, 0, 0, &result) < 0 || result < 0) {
                /* Some VMAs (shared, hugetlb, etc.) may not support WP - skip */
                skipped++;
                continue;
            }
            registered++;

            /* Record registered VMA for re-registration tracking */
            if (pt->registered_vma_count >= pt->registered_vma_capacity) {
                pt->registered_vma_capacity = pt->registered_vma_capacity ? pt->registered_vma_capacity * 2 : 64;
                pt->registered_vma_starts = realloc(pt->registered_vma_starts,
                    pt->registered_vma_capacity * sizeof(uint64_t));
                pt->registered_vma_ends = realloc(pt->registered_vma_ends,
                    pt->registered_vma_capacity * sizeof(uint64_t));
            }
            pt->registered_vma_starts[pt->registered_vma_count] = vma->start;
            pt->registered_vma_ends[pt->registered_vma_count] = vma->end;
            pt->registered_vma_count++;
        }
        fprintf(stderr, "UFFDIO_REGISTER (pid=%d): %d VMAs registered, %d skipped\n",
                pid, registered, skipped);

        if (registered == 0) {
            fprintf(stderr, "No VMAs could be registered for WP (pid=%d)\n", pid);
            goto cleanup_uffd;
        }
    }

    /* Success - don't close uffd (needs to stay open in target for WP to work) */
    pt->target_uffd = uffd;

    /* Copy uffd fd to tracker for VMA re-registration of new VMAs */
    if (pt->tracker_uffd_fd < 0) {
        long pidfd = syscall(__NR_pidfd_open, pid, 0);
        if (pidfd >= 0) {
            pt->tracker_uffd_fd = (int)syscall(__NR_pidfd_getfd, (int)pidfd, (int)uffd, 0);
            close((int)pidfd);
            if (pt->tracker_uffd_fd >= 0) {
                fprintf(stderr, "pidfd_getfd: uffd fd=%ld -> tracker fd=%d (pid=%d)\n",
                        uffd, pt->tracker_uffd_fd, pid);
            }
        }
    }

    ret = 0;
    goto cleanup_mmap;

cleanup_uffd:
    /* Close userfaultfd in target on failure */
    inject_syscall(pid, saved_regs.rip,
                   __NR_close, uffd, 0, 0, 0, 0, 0, &result);

cleanup_mmap:
    /* Free scratch page */
    inject_syscall(pid, saved_regs.rip,
                   __NR_munmap, scratch, PAGE_SIZE, 0, 0, 0, 0, &result);

restore:
    /* Restore original instruction and registers */
    ptrace(PTRACE_POKETEXT, pid, saved_regs.rip, saved_code);
    ptrace(PTRACE_SETREGS, pid, 0, &saved_regs);

detach:
    ptrace(PTRACE_DETACH, pid, 0, 0);
    thaw_other_threads(frozen_tids, frozen_count);

    clock_gettime(CLOCK_MONOTONIC, &inject_end);
    double inject_ms = (inject_end.tv_sec - inject_start.tv_sec) * 1000.0 +
                       (inject_end.tv_nsec - inject_start.tv_nsec) / 1000000.0;
    fprintf(stderr, "ptrace injection (pid=%d) took %.3f ms\n", pid, inject_ms);

    return ret;
}

/**
 * Cleanup userfaultfd write-protection from a process via ptrace injection.
 *
 * Reverse of setup_userfaultfd_wp_for_process(): unregisters all VMAs and
 * closes the uffd fd in the target process. Must be called before CRIU dump
 * to ensure clean state.
 *
 * Safe to call if target process has already exited (returns 0).
 */
static int cleanup_userfaultfd_wp_for_process(process_tracker_t *pt)
{
    pid_t pid = pt->pid;

    if (pt->target_uffd < 0) return 0;

    /* Check if target process still exists */
    if (kill(pid, 0) < 0 && errno == ESRCH) {
        fprintf(stderr, "Target process %d already exited, skipping uffd cleanup\n", pid);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return 0;
    }

    fprintf(stderr, "Cleaning up uffd-wp (fd=%ld) from process %d...\n", pt->target_uffd, pid);

    /* 1. Seize and interrupt the target process */
    if (ptrace(PTRACE_SEIZE, pid, 0, 0) < 0) {
        fprintf(stderr, "cleanup: ptrace SEIZE failed (pid=%d): %s\n", pid, strerror(errno));
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }
    if (ptrace(PTRACE_INTERRUPT, pid, 0, 0) < 0) {
        fprintf(stderr, "cleanup: ptrace INTERRUPT failed: %s\n", strerror(errno));
        ptrace(PTRACE_DETACH, pid, 0, 0);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }

    int status;
    if (waitpid(pid, &status, 0) < 0) {
        ptrace(PTRACE_DETACH, pid, 0, 0);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }

    /* 1b. Freeze all sibling threads for safe syscall injection */
    pid_t frozen_tids[MAX_THREADS];
    int frozen_count = 0;
    freeze_other_threads(pid, pid, frozen_tids, &frozen_count);
    if (frozen_count > 0)
        fprintf(stderr, "cleanup: froze %d sibling threads (pid=%d)\n", frozen_count, pid);

    /* 2. Save original state */
    struct user_regs_struct saved_regs;
    if (ptrace(PTRACE_GETREGS, pid, 0, &saved_regs) < 0) {
        fprintf(stderr, "cleanup: GETREGS failed: %s\n", strerror(errno));
        ptrace(PTRACE_DETACH, pid, 0, 0);
        thaw_other_threads(frozen_tids, frozen_count);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }

    errno = 0;
    long saved_code = ptrace(PTRACE_PEEKTEXT, pid, saved_regs.rip, 0);
    if (errno) {
        ptrace(PTRACE_DETACH, pid, 0, 0);
        thaw_other_threads(frozen_tids, frozen_count);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }

    /* 3. Poke 'syscall' instruction */
    long code_with_syscall = (saved_code & ~0xFFFFL) | 0x050FL;
    if (ptrace(PTRACE_POKETEXT, pid, saved_regs.rip, code_with_syscall) < 0) {
        ptrace(PTRACE_DETACH, pid, 0, 0);
        thaw_other_threads(frozen_tids, frozen_count);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }

    long result;

    /* 4. Inject mmap() for scratch page */
    if (inject_syscall(pid, saved_regs.rip,
                       __NR_mmap, 0, PAGE_SIZE,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0,
                       &result) < 0 || result < 0) {
        goto restore;
    }
    uint64_t scratch = (uint64_t)result;

    /* 5. Re-parse /proc/{pid}/maps and unregister each writable VMA */
    {
        /* Re-read maps since VMAs may have changed since setup */
        parse_maps_for_process(pt);

        int unregistered = 0, skipped = 0;
        for (int v = 0; v < pt->vma_count; v++) {
            vma_info_t *vma = &pt->vmas[v];

            if (!strchr(vma->perms, 'w')) continue;
            if (!strchr(vma->perms, 'p')) { skipped++; continue; }
            if (vma->type == VMA_VDSO) { skipped++; continue; }

            struct uffdio_range range = {
                .start = vma->start,
                .len = vma->end - vma->start,
            };

            if (write_to_target(pid, scratch, &range, sizeof(range)) < 0) {
                skipped++;
                continue;
            }

            if (inject_syscall(pid, saved_regs.rip,
                               __NR_ioctl, pt->target_uffd, UFFDIO_UNREGISTER, scratch,
                               0, 0, 0, &result) < 0 || result < 0) {
                /* VMA may already be unregistered or gone — not fatal */
                skipped++;
                continue;
            }
            unregistered++;
        }
        fprintf(stderr, "UFFDIO_UNREGISTER (pid=%d): %d VMAs unregistered, %d skipped\n",
                pid, unregistered, skipped);
    }

    /* 6. Close userfaultfd in target */
    inject_syscall(pid, saved_regs.rip,
                   __NR_close, pt->target_uffd, 0, 0, 0, 0, 0, &result);
    fprintf(stderr, "Closed uffd fd=%ld in target process %d\n", pt->target_uffd, pid);

    /* 7. Free scratch page */
    inject_syscall(pid, saved_regs.rip,
                   __NR_munmap, scratch, PAGE_SIZE, 0, 0, 0, 0, &result);

restore:
    /* Restore original instruction and registers */
    ptrace(PTRACE_POKETEXT, pid, saved_regs.rip, saved_code);
    ptrace(PTRACE_SETREGS, pid, 0, &saved_regs);
    ptrace(PTRACE_DETACH, pid, 0, 0);
    thaw_other_threads(frozen_tids, frozen_count);

    pt->target_uffd = -1;
    pt->wp_active = false;
    fprintf(stderr, "uffd-wp cleanup complete (pid=%d)\n", pid);
    return 0;
}

/* ===== uffd-sync handler thread ===== */

/**
 * Handler thread for userfaultfd synchronous WP mode.
 *
 * Reads WP fault events from uffd, records the faulting address,
 * and unprotects the page so the target process can resume.
 * This creates the synchronous overhead that OoH measured (~15x).
 */
static void *uffd_sync_handler(void *arg) {
    process_tracker_t *pt = (process_tracker_t *)arg;
    struct uffd_msg msg;
    unsigned long fault_count = 0;
    unsigned long poll_timeout_count = 0;

    while (!pt->stop_sync_handler) {
        /* Pause during collect_sample snapshot + re-protect to prevent race */
        if (pt->pause_sync_handler) {
            usleep(100);
            continue;
        }

        struct pollfd pfd = { .fd = pt->tracker_uffd_fd, .events = POLLIN };
        int ret = poll(&pfd, 1, 100);  /* 100ms timeout for stop check */
        if (ret <= 0) {
            if (ret == 0) poll_timeout_count++;
            continue;
        }

        ssize_t n = read(pt->tracker_uffd_fd, &msg, sizeof(msg));
        if (n != (ssize_t)sizeof(msg)) continue;
        if (msg.event != UFFD_EVENT_PAGEFAULT) continue;
        if (!(msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WP)) continue;

        uint64_t addr = msg.arg.pagefault.address & ~0xFFFUL;
        fault_count++;

        /* Record dirty page */
        pthread_mutex_lock(&pt->sync_dirty.lock);
        if (pt->sync_dirty.count >= pt->sync_dirty.capacity) {
            pt->sync_dirty.capacity = pt->sync_dirty.capacity ? pt->sync_dirty.capacity * 2 : 4096;
            pt->sync_dirty.addrs = realloc(pt->sync_dirty.addrs,
                pt->sync_dirty.capacity * sizeof(uint64_t));
        }
        pt->sync_dirty.addrs[pt->sync_dirty.count++] = addr;
        pthread_mutex_unlock(&pt->sync_dirty.lock);

        /* Unprotect page so target process resumes */
        struct uffdio_writeprotect wp = {
            .range = { .start = addr, .len = PAGE_SIZE },
            .mode = 0,
        };
        ioctl(pt->tracker_uffd_fd, UFFDIO_WRITEPROTECT, &wp);
    }

    fprintf(stderr, "uffd-sync handler: %lu faults handled, %lu poll timeouts\n",
            fault_count, poll_timeout_count);
    return NULL;
}

/**
 * Setup uffd-sync mode for a process after uffd-wp injection.
 *
 * 1. Copy uffd fd from target to tracker via pidfd_getfd
 * 2. Write-protect all writable VMAs
 * 3. Start handler thread
 *
 * Called after setup_userfaultfd_wp_for_process() succeeds and ptrace detaches.
 */
static int setup_uffd_sync_for_process(process_tracker_t *pt) {
    pid_t pid = pt->pid;

    /* 1. Copy uffd fd from target to tracker via pidfd_getfd (skip if already done) */
    if (pt->tracker_uffd_fd < 0) {
        long pidfd = syscall(__NR_pidfd_open, pid, 0);
        if (pidfd < 0) {
            fprintf(stderr, "pidfd_open failed (pid=%d): %s\n", pid, strerror(errno));
            return -1;
        }

        pt->tracker_uffd_fd = (int)syscall(__NR_pidfd_getfd, (int)pidfd, (int)pt->target_uffd, 0);
        close((int)pidfd);

        if (pt->tracker_uffd_fd < 0) {
            fprintf(stderr, "pidfd_getfd failed (pid=%d, target_uffd=%ld): %s\n",
                    pid, pt->target_uffd, strerror(errno));
            return -1;
        }
        fprintf(stderr, "pidfd_getfd: copied uffd fd=%ld -> tracker fd=%d (pid=%d)\n",
                pt->target_uffd, pt->tracker_uffd_fd, pid);
    }

    /* 2. Start handler thread BEFORE WP (handler must be ready for immediate faults) */
    pt->stop_sync_handler = false;
    if (pthread_create(&pt->sync_handler_thread, NULL, uffd_sync_handler, pt) != 0) {
        fprintf(stderr, "Failed to create uffd sync handler thread: %s\n", strerror(errno));
        close(pt->tracker_uffd_fd);
        pt->tracker_uffd_fd = -1;
        return -1;
    }
    pt->sync_handler_running = true;

    /* 3. Write-protect all writable VMAs */
    int wp_count = 0;
    for (int v = 0; v < pt->vma_count; v++) {
        vma_info_t *vma = &pt->vmas[v];
        if (!strchr(vma->perms, 'w')) continue;
        if (!strchr(vma->perms, 'p')) continue;
        if (vma->type == VMA_VDSO) continue;

        struct uffdio_writeprotect wp = {
            .range = { .start = vma->start, .len = vma->end - vma->start },
            .mode = UFFDIO_WRITEPROTECT_MODE_WP,
        };
        if (ioctl(pt->tracker_uffd_fd, UFFDIO_WRITEPROTECT, &wp) == 0) {
            wp_count++;
        }
    }
    fprintf(stderr, "uffd-sync: write-protected %d VMAs (pid=%d)\n", wp_count, pid);

    return 0;
}

/* ===== Child process discovery ===== */

/**
 * Discover descendant processes of a given PID via /proc/{pid}/task/{tid}/children.
 *
 * Recursively finds all children. Results stored in `out_pids` (up to max_pids).
 * Returns number of descendants found.
 */
static int discover_descendants(pid_t root_pid, pid_t *out_pids, int max_pids) {
    int count = 0;

    /* BFS queue */
    pid_t queue[MAX_PROCESSES * 4];
    int queue_head = 0, queue_tail = 0;
    queue[queue_tail++] = root_pid;

    while (queue_head < queue_tail && count < max_pids) {
        pid_t pid = queue[queue_head++];

        /* Read /proc/{pid}/task/{pid}/children */
        char path[128];
        snprintf(path, sizeof(path), "/proc/%d/task/%d/children", pid, pid);

        FILE *f = fopen(path, "r");
        if (!f) continue;

        pid_t child_pid;
        char buf[4096];
        if (fgets(buf, sizeof(buf), f)) {
            char *token = strtok(buf, " \t\n");
            while (token && count < max_pids) {
                child_pid = (pid_t)atoi(token);
                if (child_pid > 0) {
                    out_pids[count++] = child_pid;
                    if (queue_tail < (int)(sizeof(queue) / sizeof(queue[0]))) {
                        queue[queue_tail++] = child_pid;
                    }
                }
                token = strtok(NULL, " \t\n");
            }
        }

        fclose(f);

        /* Also check other threads of this process for children */
        char task_path[64];
        snprintf(task_path, sizeof(task_path), "/proc/%d/task", pid);
        DIR *task_dir = opendir(task_path);
        if (!task_dir) continue;

        struct dirent *entry;
        while ((entry = readdir(task_dir)) != NULL && count < max_pids) {
            pid_t tid = (pid_t)atoi(entry->d_name);
            if (tid <= 0 || tid == pid) continue;  /* Skip . .. and main thread (already done) */

            snprintf(path, sizeof(path), "/proc/%d/task/%d/children", pid, tid);
            f = fopen(path, "r");
            if (!f) continue;

            if (fgets(buf, sizeof(buf), f)) {
                char *token = strtok(buf, " \t\n");
                while (token && count < max_pids) {
                    child_pid = (pid_t)atoi(token);
                    if (child_pid > 0) {
                        /* Check not already in output */
                        bool dup = false;
                        for (int i = 0; i < count; i++) {
                            if (out_pids[i] == child_pid) { dup = true; break; }
                        }
                        if (!dup) {
                            out_pids[count++] = child_pid;
                            if (queue_tail < (int)(sizeof(queue) / sizeof(queue[0]))) {
                                queue[queue_tail++] = child_pid;
                            }
                        }
                    }
                    token = strtok(NULL, " \t\n");
                }
            }
            fclose(f);
        }
        closedir(task_dir);
    }

    return count;
}

/* ===== Unique address tracking ===== */

static void add_unique_addr(tracker_t *t, uint64_t addr) {
    if (t->no_output) return;  /* --no-output: skip unique tracking */

    unsigned int bucket = (unsigned int)(addr >> 12) % UNIQUE_HASH_SIZE;
    unique_node_t *node = t->unique_hash[bucket];
    while (node) {
        if (node->addr == addr) return;  /* already exists */
        node = node->next;
    }
    /* Insert new */
    node = malloc(sizeof(unique_node_t));
    if (!node) return;
    node->addr = addr;
    node->next = t->unique_hash[bucket];
    t->unique_hash[bucket] = node;
    t->unique_count++;
}

/* ===== Dynamic page buffer growth ===== */

static int ensure_page_capacity(dirty_page_t **pages, int *capacity, int required) {
    if (required <= *capacity) return 0;
    int new_cap = *capacity;
    while (new_cap < required) new_cap = new_cap ? new_cap * 2 : 4096;
    dirty_page_t *new_buf = realloc(*pages, new_cap * sizeof(dirty_page_t));
    if (!new_buf) return -1;
    *pages = new_buf;
    *capacity = new_cap;
    return 0;
}

/* ===== VMA dirty summary helpers ===== */

static void append_vma_summary(sample_t *sample, vma_info_t *vma, int dirty_pages) {
    if (sample->vma_summary_count >= sample->vma_summary_capacity) {
        int new_cap = sample->vma_summary_capacity ? sample->vma_summary_capacity * 2 : 128;
        vma_dirty_summary_t *tmp = realloc(sample->vma_summaries,
            new_cap * sizeof(vma_dirty_summary_t));
        if (!tmp) return;  /* OOM: skip this VMA summary */
        sample->vma_summaries = tmp;
        sample->vma_summary_capacity = new_cap;
    }
    vma_dirty_summary_t *vs = &sample->vma_summaries[sample->vma_summary_count++];
    vs->start = vma->start;
    vs->end = vma->end;
    vs->dirty_pages = dirty_pages;
    vs->total_pages = (vma->end - vma->start) / PAGE_SIZE;
    strncpy(vs->perms, vma->perms, sizeof(vs->perms) - 1);
    vs->perms[sizeof(vs->perms) - 1] = '\0';
    vs->vma_type = vma->type;
}

/* ===== Dirty page scanning ===== */

static int read_dirty_pages_pagemap_scan(tracker_t *t, process_tracker_t *pt, sample_t *sample) {
    /*
     * Determine scan mode:
     * - wp_active: Use PM_SCAN_WP_MATCHING for atomic scan+clear (PAGE_IS_WRITTEN)
     * - no_clear && !wp_active: Accumulate soft-dirty bits (no clear)
     * - !no_clear && !wp_active: Use soft-dirty + clear_refs
     *
     * wp_active is set during init if userfaultfd-wp is supported.
     * If WP probe fails, we fall back to soft-dirty permanently.
     */
    bool use_wp = pt->wp_active;
    uint64_t scan_flags = use_wp ? PM_SCAN_WP_MATCHING : 0;

    /* Initial WP setup: inject userfaultfd-wp via ptrace, then WP all pages */
    if (!pt->wp_initialized && !t->no_clear) {
        /*
         * Step 1: Inject userfaultfd-wp registration into target process.
         * This is needed because PM_SCAN_WP_MATCHING requires VM_UFFD_WP
         * on the target's VMAs, which can only be set from within the process.
         */
        if (setup_userfaultfd_wp_for_process(pt, t->uffd_sync) == 0) {
            /* Step 2: Verify WP is now available */
            struct pm_scan_arg check_args = {
                .size = sizeof(check_args),
                .flags = 0,
                .start = 0,
                .end = 0x7fffffffffffULL,
                .vec = (uint64_t)t->regions,
                .vec_len = 1,
                .max_pages = 1,
                .category_mask = PAGE_IS_WPALLOWED,
                .category_anyof_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED,
                .return_mask = PAGE_IS_WPALLOWED,
            };
            long ret = ioctl(pt->pagemap_fd, PAGEMAP_SCAN, &check_args);

            if (ret > 0) {
                fprintf(stderr, "WP mode verified: PAGE_IS_WPALLOWED present (pid=%d)\n", pt->pid);

                /* Step 3: WP all present pages for baseline */
                use_wp = true;
                scan_flags = PM_SCAN_WP_MATCHING;
                for (int v = 0; v < pt->vma_count; v++) {
                    vma_info_t *vma = &pt->vmas[v];
                    if (!strchr(vma->perms, 'w')) continue;

                    struct pm_scan_arg wp_args = {
                        .size = sizeof(wp_args),
                        .flags = PM_SCAN_WP_MATCHING,
                        .start = vma->start,
                        .end = vma->end,
                        .vec = (uint64_t)t->regions,
                        .vec_len = MAX_REGIONS,
                        .max_pages = 0,
                        .category_inverted = PAGE_IS_PFNZERO | PAGE_IS_FILE,
                        .category_mask = PAGE_IS_PFNZERO | PAGE_IS_FILE,
                        .category_anyof_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED,
                        .return_mask = 0,
                    };

                    ret = ioctl(pt->pagemap_fd, PAGEMAP_SCAN, &wp_args);
                    if (ret < 0 && errno == EPERM) {
                        fprintf(stderr, "PM_SCAN_WP_MATCHING failed after setup (pid=%d): %s\n",
                                pt->pid, strerror(errno));
                        use_wp = false;
                        scan_flags = 0;
                        break;
                    }
                }

                if (use_wp) {
                    pt->wp_initialized = true;
                    pt->wp_active = true;
                    fprintf(stderr, "WP mode active (pid=%d): using PM_SCAN_WP_MATCHING\n", pt->pid);
                    /* First interval: baseline (all dirty=0), but record VMA list */
                    for (int v2 = 0; v2 < pt->vma_count; v2++) {
                        vma_info_t *bvma = &pt->vmas[v2];
                        if (!strchr(bvma->perms, 'w')) continue;
                        append_vma_summary(sample, bvma, 0);
                    }
                    return 0;
                }
            } else {
                fprintf(stderr, "WP setup succeeded but WPALLOWED still not set (pid=%d, ret=%ld)\n",
                        pt->pid, ret);
            }
        }

        /*
         * WP setup failed. Do NOT fall back to soft-dirty + clear_refs,
         * because that would interfere with CRIU's soft-dirty tracking.
         */
        fprintf(stderr, "ERROR: uffd-wp setup failed (pid=%d). Cannot track without interfering with soft-dirty.\n",
                pt->pid);
        fprintf(stderr, "Use --no-clear for scan-only mode (no clearing, no WP).\n");
        pt->wp_initialized = true;
        return -1;
    }

    /* Determine dirty flag based on mode */
    uint64_t dirty_flag = use_wp ? PAGE_IS_WRITTEN : PAGE_IS_SOFT_DIRTY;

    /* Scan each writable VMA separately */
    for (int v = 0; v < pt->vma_count; v++) {
        vma_info_t *vma = &pt->vmas[v];

        /* Skip non-writable VMAs */
        if (!strchr(vma->perms, 'w')) continue;

        struct pm_scan_arg args = {
            .size = sizeof(args),
            .flags = scan_flags,
            .start = vma->start,
            .end = vma->end,
            .vec = (uint64_t)t->regions,
            .vec_len = MAX_REGIONS,
            .max_pages = 0,
            .category_inverted = PAGE_IS_PFNZERO | PAGE_IS_FILE,
            .category_mask = PAGE_IS_PFNZERO | PAGE_IS_FILE | (use_wp ? PAGE_IS_WRITTEN : 0),
            .category_anyof_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED,
            .return_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED | dirty_flag,
        };

        long ret = ioctl(pt->pagemap_fd, PAGEMAP_SCAN, &args);
        if (ret < 0) {
            if (errno == EPERM && use_wp) {
                fprintf(stderr, "ERROR: PM_SCAN_WP_MATCHING failed (EPERM, pid=%d).\n", pt->pid);
                return -1;
            }
            /* VMA summary with dirty=0 for failed scans (VMA still exists) */
            append_vma_summary(sample, vma, 0);
            continue;
        }

        /* Count dirty pages for this VMA from regions (independent of page expansion) */
        {
            int vma_dirty_count = 0;
            for (long i = 0; i < ret; i++) {
                if (!use_wp && !(t->regions[i].categories & PAGE_IS_SOFT_DIRTY)) continue;
                vma_dirty_count += (t->regions[i].end - t->regions[i].start) / PAGE_SIZE;
            }
            append_vma_summary(sample, vma, vma_dirty_count);
        }

        /* Process returned regions - in WP mode all returned pages are dirty */
        for (long i = 0; i < ret; i++) {
            /* In soft-dirty mode, filter by soft-dirty flag */
            if (!use_wp && !(t->regions[i].categories & PAGE_IS_SOFT_DIRTY)) continue;

            for (uint64_t addr = t->regions[i].start;
                 addr < t->regions[i].end;
                 addr += PAGE_SIZE) {

                /* Grow buffer if needed */
                if (ensure_page_capacity(&sample->pages, &sample->page_capacity,
                                         sample->page_count + 1) < 0) break;

                dirty_page_t *page = &sample->pages[sample->page_count++];
                page->addr = addr;
                page->vma_type = vma->type;
                strncpy(page->perms, vma->perms, sizeof(page->perms) - 1);
                strncpy(page->pathname, vma->pathname, sizeof(page->pathname) - 1);

                t->vma_type_counts[vma->type]++;
                t->vma_type_sizes[vma->type] += PAGE_SIZE;
                add_unique_addr(t, addr);
            }
        }
    }

    /* Dual-channel: also scan soft-dirty (read-only, never clear) */
    if (t->dual_channel && use_wp) {
        for (int v = 0; v < pt->vma_count; v++) {
            vma_info_t *vma = &pt->vmas[v];
            if (!strchr(vma->perms, 'w')) continue;

            struct pm_scan_arg sd_args = {
                .size = sizeof(sd_args),
                .flags = 0,  /* read-only, no WP, no clear */
                .start = vma->start,
                .end = vma->end,
                .vec = (uint64_t)t->sd_regions,
                .vec_len = MAX_REGIONS,
                .max_pages = 0,
                .category_inverted = PAGE_IS_PFNZERO | PAGE_IS_FILE,
                .category_mask = PAGE_IS_PFNZERO | PAGE_IS_FILE,
                .category_anyof_mask = PAGE_IS_PRESENT | PAGE_IS_SWAPPED,
                .return_mask = PAGE_IS_SOFT_DIRTY,
            };

            long sd_ret = ioctl(pt->pagemap_fd, PAGEMAP_SCAN, &sd_args);
            if (sd_ret < 0) continue;

            for (long i = 0; i < sd_ret; i++) {
                if (!(t->sd_regions[i].categories & PAGE_IS_SOFT_DIRTY)) continue;

                for (uint64_t addr = t->sd_regions[i].start;
                     addr < t->sd_regions[i].end;
                     addr += PAGE_SIZE) {

                    if (ensure_page_capacity(&sample->sd_pages, &sample->sd_page_capacity,
                                             sample->sd_page_count + 1) < 0) break;

                    dirty_page_t *page = &sample->sd_pages[sample->sd_page_count++];
                    page->addr = addr;
                    page->vma_type = vma->type;
                    strncpy(page->perms, vma->perms, sizeof(page->perms) - 1);
                    strncpy(page->pathname, vma->pathname, sizeof(page->pathname) - 1);
                }
            }
        }
    }

    return 0;
}

static int read_dirty_pages_soft_dirty(tracker_t *t, process_tracker_t *pt, sample_t *sample) {
    for (int v = 0; v < pt->vma_count; v++) {
        vma_info_t *vma = &pt->vmas[v];

        /* Skip non-writable VMAs */
        if (!strchr(vma->perms, 'w')) continue;

        uint64_t start_page = vma->start / PAGE_SIZE;
        uint64_t num_pages = (vma->end - vma->start) / PAGE_SIZE;
        off_t offset = start_page * sizeof(uint64_t);

        /* Read pagemap entries */
        size_t buf_size = num_pages * sizeof(uint64_t);
        uint64_t *buf = malloc(buf_size);
        if (!buf) continue;

        ssize_t n = pread(pt->pagemap_fd, buf, buf_size, offset);
        if (n <= 0) {
            free(buf);
            continue;
        }

        size_t entries = n / sizeof(uint64_t);

        /* Count dirty pages for VMA summary */
        int vma_dirty_count = 0;
        for (size_t i = 0; i < entries; i++) {
            if (buf[i] & PM_SOFT_DIRTY) vma_dirty_count++;
        }
        append_vma_summary(sample, vma, vma_dirty_count);

        for (size_t i = 0; i < entries; i++) {
            if (buf[i] & PM_SOFT_DIRTY) {
                if (ensure_page_capacity(&sample->pages, &sample->page_capacity,
                                         sample->page_count + 1) < 0) break;

                dirty_page_t *page = &sample->pages[sample->page_count++];
                page->addr = vma->start + i * PAGE_SIZE;
                page->vma_type = vma->type;
                strncpy(page->perms, vma->perms, sizeof(page->perms) - 1);
                strncpy(page->pathname, vma->pathname, sizeof(page->pathname) - 1);

                t->vma_type_counts[vma->type]++;
                t->vma_type_sizes[vma->type] += PAGE_SIZE;
                add_unique_addr(t, page->addr);
            }
        }

        free(buf);
    }

    return 0;
}

/* ===== Helper: check if PID is in exclude list ===== */

static bool is_pid_excluded(tracker_t *t, pid_t pid) {
    for (int i = 0; i < t->exclude_pid_count; i++) {
        if (t->exclude_pids[i] == pid) return true;
    }
    return false;
}

static bool is_pid_known(tracker_t *t, pid_t pid) {
    for (int i = 0; i < t->known_pid_count; i++) {
        if (t->known_pids[i] == pid) return true;
    }
    return false;
}

/* ===== Sample collection ===== */

static int collect_sample(tracker_t *t) {

    /* 1. Discover new child processes (if enabled) */
    if (t->track_children) {
        pid_t descendants[MAX_PROCESSES * 2];
        int desc_count = discover_descendants(t->root_pid, descendants, MAX_PROCESSES * 2);

        for (int i = 0; i < desc_count; i++) {
            pid_t dpid = descendants[i];

            if (is_pid_excluded(t, dpid)) continue;
            if (is_pid_known(t, dpid)) continue;
            if (t->process_count >= MAX_PROCESSES) break;

            /* New child process found */
            process_tracker_t *pt = process_tracker_init(dpid);
            if (!pt) {
                fprintf(stderr, "Failed to init tracker for child pid=%d, skipping\n", dpid);
                /* Still add to known_pids to avoid retrying */
                if (t->known_pid_count < (int)(sizeof(t->known_pids) / sizeof(t->known_pids[0]))) {
                    t->known_pids[t->known_pid_count++] = dpid;
                }
                continue;
            }

            fprintf(stderr, "Discovered child process: pid=%d (parent tree of %d)\n",
                    dpid, t->root_pid);

            t->processes[t->process_count++] = pt;
            if (t->known_pid_count < (int)(sizeof(t->known_pids) / sizeof(t->known_pids[0]))) {
                t->known_pids[t->known_pid_count++] = dpid;
            }
        }
    }

    /* 2. Remove dead processes */
    for (int i = t->process_count - 1; i >= 0; i--) {
        process_tracker_t *pt = t->processes[i];
        if (!pt) continue;

        if (kill(pt->pid, 0) < 0 && errno == ESRCH) {
            fprintf(stderr, "Process %d exited, removing from tracking\n", pt->pid);
            /* Detach ptrace so parent can reap zombie */
            ptrace(PTRACE_DETACH, pt->pid, 0, 0);
            process_tracker_cleanup(pt);
            /* Shift array */
            for (int j = i; j < t->process_count - 1; j++) {
                t->processes[j] = t->processes[j + 1];
            }
            t->processes[t->process_count - 1] = NULL;
            t->process_count--;
        }
    }

    if (t->process_count == 0) {
        fprintf(stderr, "All tracked processes have exited\n");
        return -1;
    }

    /* 3. Allocate sample */
    sample_t *sample = &t->current_sample;
    memset(sample, 0, sizeof(*sample));
    sample->timestamp_ms = get_elapsed_ms(&t->start_time);

    /* Allocate pages buffer (shared across all processes for this sample) */
    sample->page_capacity = 4096;  /* start small, grows dynamically */
    sample->pages = malloc(sample->page_capacity * sizeof(dirty_page_t));
    if (!sample->pages) return -1;
    sample->page_count = 0;

    if (t->dual_channel) {
        sample->sd_page_capacity = 4096;
        sample->sd_pages = malloc(sample->sd_page_capacity * sizeof(dirty_page_t));
        if (!sample->sd_pages) {
            free(sample->pages);
            sample->pages = NULL;
            return -1;
        }
        sample->sd_page_count = 0;
    }

    /* Allocate pids_tracked */
    sample->pids_tracked = malloc(t->process_count * sizeof(pid_t));
    sample->pids_tracked_count = 0;

    /* 4. Collect dirty pages from each process */
    for (int i = 0; i < t->process_count; i++) {
        process_tracker_t *pt = t->processes[i];
        if (!pt) continue;

        /* Parse maps for this process */
        if (parse_maps_for_process(pt) < 0) continue;

        /* Re-register new VMAs that appeared since initial setup */
        if (pt->tracker_uffd_fd >= 0 && pt->wp_active && !t->sd_only) {
            for (int v = 0; v < pt->vma_count; v++) {
                vma_info_t *vma = &pt->vmas[v];

                /* Same filter as setup_userfaultfd_wp_for_process */
                if (!strchr(vma->perms, 'w')) continue;
                if (!strchr(vma->perms, 'p')) continue;
                if (vma->type == VMA_VDSO) continue;

                /* Check if already registered */
                bool already = false;
                for (int r = 0; r < pt->registered_vma_count; r++) {
                    if (pt->registered_vma_starts[r] == vma->start &&
                        pt->registered_vma_ends[r] == vma->end) {
                        already = true;
                        break;
                    }
                }
                if (already) continue;

                /* New VMA — register via tracker's uffd fd */
                struct uffdio_register reg = {
                    .range = { .start = vma->start, .len = vma->end - vma->start },
                    .mode = UFFDIO_REGISTER_MODE_WP,
                };
                if (ioctl(pt->tracker_uffd_fd, UFFDIO_REGISTER, &reg) == 0) {
                    if (pt->registered_vma_count >= pt->registered_vma_capacity) {
                        pt->registered_vma_capacity = pt->registered_vma_capacity ? pt->registered_vma_capacity * 2 : 64;
                        pt->registered_vma_starts = realloc(pt->registered_vma_starts,
                            pt->registered_vma_capacity * sizeof(uint64_t));
                        pt->registered_vma_ends = realloc(pt->registered_vma_ends,
                            pt->registered_vma_capacity * sizeof(uint64_t));
                    }
                    pt->registered_vma_starts[pt->registered_vma_count] = vma->start;
                    pt->registered_vma_ends[pt->registered_vma_count] = vma->end;
                    pt->registered_vma_count++;
                    if (t->verbose)
                        fprintf(stderr, "Re-registered new VMA: %s [%lx-%lx] (pid=%d)\n",
                                vma->pathname, (unsigned long)vma->start, (unsigned long)vma->end, pt->pid);
                }
                /* ioctl failure silently ignored (shared VMAs, special mappings) */
            }
        }

        /* Record this PID */
        sample->pids_tracked[sample->pids_tracked_count++] = pt->pid;

        /* Collect memory usage (nearly zero overhead) */
        sample->rss_bytes += read_rss_bytes(pt->pid);
        sample->writable_vma_bytes += calc_writable_vma_bytes(pt);

        /* Read dirty pages */
        int ret = 0;

        if (t->sd_only) {
            /* sd-only mode: soft-dirty read + clear (no uffd) */
            ret = read_dirty_pages_soft_dirty(t, pt, sample);
        } else if (t->uffd_sync && pt->tracker_uffd_fd >= 0) {
            /* uffd-sync mode: pause handler → snapshot → re-protect → resume */

            /* 1. Pause handler to prevent unprotect during re-protect */
            pt->pause_sync_handler = true;
            usleep(1000);  /* 1ms: let in-flight ioctl complete */

            /* 2. Snapshot dirty set + per-VMA dirty counts */
            int vma_dirty_counts[MAX_VMAS];
            memset(vma_dirty_counts, 0, pt->vma_count * sizeof(int));

            pthread_mutex_lock(&pt->sync_dirty.lock);
            for (size_t di = 0; di < pt->sync_dirty.count; di++) {
                if (ensure_page_capacity(&sample->pages, &sample->page_capacity,
                                         sample->page_count + 1) < 0) break;
                uint64_t addr = pt->sync_dirty.addrs[di];

                /* Find matching VMA for classification */
                vma_type_t vtype = VMA_UNKNOWN;
                const char *vperms = "----";
                const char *vpathname = "";
                int vma_idx = -1;
                for (int v = 0; v < pt->vma_count; v++) {
                    if (addr >= pt->vmas[v].start && addr < pt->vmas[v].end) {
                        vtype = pt->vmas[v].type;
                        vperms = pt->vmas[v].perms;
                        vpathname = pt->vmas[v].pathname;
                        vma_idx = v;
                        break;
                    }
                }
                if (vma_idx >= 0) vma_dirty_counts[vma_idx]++;

                dirty_page_t *page = &sample->pages[sample->page_count++];
                page->addr = addr;
                page->vma_type = vtype;
                strncpy(page->perms, vperms, sizeof(page->perms) - 1);
                strncpy(page->pathname, vpathname, sizeof(page->pathname) - 1);

                t->vma_type_counts[vtype]++;
                t->vma_type_sizes[vtype] += PAGE_SIZE;
                add_unique_addr(t, addr);
            }
            pt->sync_dirty.count = 0;
            pthread_mutex_unlock(&pt->sync_dirty.lock);

            /* Append VMA summaries for all writable VMAs (dirty=0 included) */
            for (int v = 0; v < pt->vma_count; v++) {
                if (!strchr(pt->vmas[v].perms, 'w')) continue;
                append_vma_summary(sample, &pt->vmas[v], vma_dirty_counts[v]);
            }

            /* 3. Re-protect all writable VMAs (handler paused, no race) */
            for (int v = 0; v < pt->vma_count; v++) {
                if (!strchr(pt->vmas[v].perms, 'w')) continue;
                if (!strchr(pt->vmas[v].perms, 'p')) continue;
                if (pt->vmas[v].type == VMA_VDSO) continue;
                struct uffdio_writeprotect wp = {
                    .range = { .start = pt->vmas[v].start,
                               .len = pt->vmas[v].end - pt->vmas[v].start },
                    .mode = UFFDIO_WRITEPROTECT_MODE_WP,
                };
                ioctl(pt->tracker_uffd_fd, UFFDIO_WRITEPROTECT, &wp);
            }

            /* 4. Resume handler */
            pt->pause_sync_handler = false;
        } else if (t->uffd_sync && !pt->wp_initialized) {
            /* uffd-sync mode: first call — setup uffd-wp + sync handler */
            if (setup_userfaultfd_wp_for_process(pt, true) == 0) {
                pt->wp_initialized = true;
                pt->wp_active = true;
                /* Setup sync handler (pidfd_getfd + WP + thread) */
                if (setup_uffd_sync_for_process(pt) < 0) {
                    fprintf(stderr, "ERROR: uffd-sync setup failed for pid=%d\n", pt->pid);
                    ret = -1;
                }
                /* First interval: empty (baseline just established) */
            } else {
                fprintf(stderr, "ERROR: uffd-wp injection failed for sync mode (pid=%d)\n", pt->pid);
                pt->wp_initialized = true;
                ret = -1;
            }
        } else if (pt->use_pagemap_scan) {
            ret = read_dirty_pages_pagemap_scan(t, pt, sample);
        } else {
            ret = read_dirty_pages_soft_dirty(t, pt, sample);
        }

        if (ret < 0) {
            /* Non-fatal for individual process — continue with others */
            fprintf(stderr, "Warning: dirty page scan failed for pid=%d\n", pt->pid);
        }
    }

    t->total_dirty_pages += sample->page_count;
    t->sample_count++;

    /*
     * WP channel: PM_SCAN_WP_MATCHING handles clearing atomically.
     * SD channel (dual-channel --sd-clear): clear soft-dirty after scan.
     * sd-only: clear soft-dirty after each read.
     * Default: never clear soft-dirty (independent from CRIU).
     */
    if (t->sd_only) {
        for (int i = 0; i < t->process_count; i++) {
            if (t->processes[i]) {
                clear_soft_dirty_for_process(t->processes[i]);
            }
        }
    }
    if (t->dual_channel && t->sd_clear) {
        for (int i = 0; i < t->process_count; i++) {
            if (t->processes[i]) {
                clear_soft_dirty_for_process(t->processes[i]);
            }
        }
    }

    return 0;
}

/* ===== JSON streaming output ===== */

static void write_json_header(tracker_t *t, const char *workload) {
    FILE *f = t->output_fp;
    if (!f) return;

    fprintf(f, "{\n");
    fprintf(f, "  \"workload\": \"%s\",\n", workload);
    fprintf(f, "  \"root_pid\": %d,\n", t->root_pid);
    fprintf(f, "  \"track_children\": %s,\n", t->track_children ? "true" : "false");
    fprintf(f, "  \"page_size\": %lu,\n", (unsigned long)PAGE_SIZE);

    /* pagemap_scan_used: true if root process uses it */
    bool root_uses_pagemap = false;
    if (t->process_count > 0 && t->processes[0]) {
        root_uses_pagemap = t->processes[0]->use_pagemap_scan;
    }
    fprintf(f, "  \"pagemap_scan_used\": %s,\n", root_uses_pagemap ? "true" : "false");
    fprintf(f, "  \"clear_on_scan\": %s,\n", t->no_clear ? "false" : "true");
    fprintf(f, "  \"dual_channel\": %s,\n", t->dual_channel ? "true" : "false");

    /* Tracking mode for OoH comparison */
    const char *tracking_mode = "uffd-wp-async";
    if (t->sd_only) tracking_mode = "sd-only";
    else if (t->uffd_sync) tracking_mode = "uffd-sync";
    else if (t->no_clear) tracking_mode = "soft-dirty-readonly";
    fprintf(f, "  \"tracking_mode\": \"%s\",\n", tracking_mode);

    fprintf(f, "  \"samples\": [\n");
    fflush(f);
}

static void write_sample_json(FILE *f, tracker_t *t, sample_t *sample) {
    fprintf(f, "    {\n");
    fprintf(f, "      \"timestamp_ms\": %.3f,\n", sample->timestamp_ms);

    if (t->dual_channel) {
        /* Dual-channel: wp_channel + sd_channel */
        fprintf(f, "      \"wp_channel\": {\n");
        fprintf(f, "        \"dirty_pages\": [\n");
        for (int p = 0; p < sample->page_count; p++) {
            dirty_page_t *page = &sample->pages[p];
            fprintf(f, "          {\"addr\": \"0x%lx\", \"vma_type\": \"%s\", \"vma_perms\": \"%s\", \"pathname\": \"%s\", \"size\": %lu}%s\n",
                    page->addr, vma_type_str(page->vma_type), page->perms, page->pathname, (unsigned long)PAGE_SIZE,
                    p < sample->page_count - 1 ? "," : "");
        }
        fprintf(f, "        ],\n");
        fprintf(f, "        \"dirty_count\": %d,\n", sample->page_count);
        fprintf(f, "        \"dirty_size_bytes\": %lu\n", (unsigned long)sample->page_count * PAGE_SIZE);
        fprintf(f, "      },\n");

        fprintf(f, "      \"sd_channel\": {\n");
        fprintf(f, "        \"dirty_pages\": [\n");
        for (int p = 0; p < sample->sd_page_count; p++) {
            dirty_page_t *page = &sample->sd_pages[p];
            fprintf(f, "          {\"addr\": \"0x%lx\", \"vma_type\": \"%s\", \"vma_perms\": \"%s\", \"pathname\": \"%s\", \"size\": %lu}%s\n",
                    page->addr, vma_type_str(page->vma_type), page->perms, page->pathname, (unsigned long)PAGE_SIZE,
                    p < sample->sd_page_count - 1 ? "," : "");
        }
        fprintf(f, "        ],\n");
        fprintf(f, "        \"dirty_count\": %d,\n", sample->sd_page_count);
        fprintf(f, "        \"dirty_size_bytes\": %lu\n", (unsigned long)sample->sd_page_count * PAGE_SIZE);
        fprintf(f, "      },\n");
    } else {
        /* Single-channel: original format */
        fprintf(f, "      \"dirty_pages\": [\n");
        for (int p = 0; p < sample->page_count; p++) {
            dirty_page_t *page = &sample->pages[p];
            fprintf(f, "        {\"addr\": \"0x%lx\", \"vma_type\": \"%s\", \"vma_perms\": \"%s\", \"pathname\": \"%s\", \"size\": %lu}%s\n",
                    page->addr, vma_type_str(page->vma_type), page->perms, page->pathname, (unsigned long)PAGE_SIZE,
                    p < sample->page_count - 1 ? "," : "");
        }
        fprintf(f, "      ],\n");
        fprintf(f, "      \"delta_dirty_count\": %d,\n", sample->page_count);
    }

    /* pids_tracked array */
    fprintf(f, "      \"pids_tracked\": [");
    for (int p = 0; p < sample->pids_tracked_count; p++) {
        fprintf(f, "%d%s", sample->pids_tracked[p],
                p < sample->pids_tracked_count - 1 ? ", " : "");
    }
    fprintf(f, "],\n");
    fprintf(f, "      \"rss_bytes\": %ld,\n", sample->rss_bytes);
    fprintf(f, "      \"writable_vma_bytes\": %ld\n", sample->writable_vma_bytes);
    fprintf(f, "    }");
}

static void flush_and_free_sample(tracker_t *t, sample_t *sample) {
    /* 1. Rate calculation */
    double rate = 0;
    if (t->sample_count > 1 && t->prev_timestamp_ms > 0) {
        double dt = (sample->timestamp_ms - t->prev_timestamp_ms) / 1000.0;
        if (dt > 0) rate = (double)sample->page_count / dt;
    }

    /* 2. Update incremental summary */
    t->cumulative_dirty += sample->page_count;
    if (rate > 0) {
        t->sum_rate += rate;
        t->rate_count++;
        if (rate > t->peak_rate) t->peak_rate = rate;
    }

    /* 3. Accumulate timeline entry (with VMA summary pointer transfer) */
    if (t->timeline_count >= t->timeline_capacity) {
        t->timeline_capacity *= 2;
        t->timeline = realloc(t->timeline, t->timeline_capacity * sizeof(timeline_entry_t));
    }
    if (t->timeline) {
        timeline_entry_t *entry = &t->timeline[t->timeline_count++];
        entry->timestamp_ms = sample->timestamp_ms;
        entry->rate_pages_per_sec = rate;
        entry->cumulative_pages = t->cumulative_dirty;
        entry->processes_tracked = sample->pids_tracked_count;
        entry->vma_summaries = sample->vma_summaries;       /* transfer ownership */
        entry->vma_summary_count = sample->vma_summary_count;
        sample->vma_summaries = NULL;  /* prevent double-free */
    }

    /* 4. Write to output (unless --no-output) */
    if (!t->no_output && t->output_fp) {
        if (t->samples_written > 0) fprintf(t->output_fp, ",\n");
        write_sample_json(t->output_fp, t, sample);
        t->samples_written++;
    }

    /* 5. Free sample memory */
    free(sample->pages);        sample->pages = NULL;
    free(sample->sd_pages);     sample->sd_pages = NULL;
    free(sample->pids_tracked); sample->pids_tracked = NULL;
    free(sample->vma_summaries); sample->vma_summaries = NULL;

    t->prev_timestamp_ms = sample->timestamp_ms;
}

static void write_json_footer(tracker_t *t) {
    FILE *f = t->output_fp;
    if (!f) return;

    /* Close samples array */
    fprintf(f, "\n  ],\n");

    /* tracking_duration_ms from last timeline entry */
    double tracking_duration = 0;
    if (t->timeline_count > 0) {
        tracking_duration = t->timeline[t->timeline_count - 1].timestamp_ms;
    }
    fprintf(f, "  \"tracking_duration_ms\": %.3f,\n", tracking_duration);

    /* Summary (using incremental stats) */
    double avg_rate = t->rate_count > 0 ? t->sum_rate / t->rate_count : 0;

    /* Calculate VMA distribution */
    int total_vma_events = 0;
    for (int i = 0; i < 7; i++) total_vma_events += t->vma_type_counts[i];

    /* max_processes_tracked from timeline */
    int max_processes_tracked = 0;
    for (int i = 0; i < t->timeline_count; i++) {
        if (t->timeline[i].processes_tracked > max_processes_tracked) {
            max_processes_tracked = t->timeline[i].processes_tracked;
        }
    }

    fprintf(f, "  \"summary\": {\n");
    fprintf(f, "    \"total_unique_pages\": %d,\n", t->unique_count);
    fprintf(f, "    \"total_dirty_events\": %d,\n", t->total_dirty_pages);
    fprintf(f, "    \"total_dirty_size_bytes\": %lu,\n", (unsigned long)t->total_dirty_pages * PAGE_SIZE);
    fprintf(f, "    \"avg_dirty_rate_per_sec\": %.2f,\n", avg_rate);
    fprintf(f, "    \"peak_dirty_rate\": %.2f,\n", t->peak_rate);

    /* VMA distribution (ratios) */
    fprintf(f, "    \"vma_distribution\": {");
    {
        int first = 1;
        const char *type_names[] = {"heap", "stack", "anonymous", "code", "data", "vdso", "unknown"};
        for (int i = 0; i < 7; i++) {
            if (t->vma_type_counts[i] > 0) {
                double ratio = total_vma_events > 0 ? (double)t->vma_type_counts[i] / total_vma_events : 0;
                fprintf(f, "%s\"%s\": %.6f", first ? "" : ", ", type_names[i], ratio);
                first = 0;
            }
        }
    }
    fprintf(f, "},\n");

    /* VMA size distribution (bytes) */
    fprintf(f, "    \"vma_size_distribution\": {");
    {
        int first = 1;
        const char *type_names[] = {"heap", "stack", "anonymous", "code", "data", "vdso", "unknown"};
        for (int i = 0; i < 7; i++) {
            if (t->vma_type_sizes[i] > 0) {
                fprintf(f, "%s\"%s\": %d", first ? "" : ", ", type_names[i], t->vma_type_sizes[i]);
                first = 0;
            }
        }
    }
    fprintf(f, "},\n");

    fprintf(f, "    \"sample_count\": %d,\n", t->sample_count);
    fprintf(f, "    \"interval_ms\": %d,\n", t->interval_ms);
    fprintf(f, "    \"max_processes_tracked\": %d,\n", max_processes_tracked);

    /* total_pids_seen: all known PIDs */
    fprintf(f, "    \"total_pids_seen\": [");
    for (int i = 0; i < t->known_pid_count; i++) {
        fprintf(f, "%d%s", t->known_pids[i], i < t->known_pid_count - 1 ? ", " : "");
    }
    fprintf(f, "]\n");
    fprintf(f, "  },\n");

    /* Dirty rate timeline (from accumulated lightweight entries) */
    fprintf(f, "  \"dirty_rate_timeline\": [\n");
    for (int i = 0; i < t->timeline_count; i++) {
        timeline_entry_t *e = &t->timeline[i];
        fprintf(f, "    {\"timestamp_ms\": %.3f, \"rate_pages_per_sec\": %.2f, "
                    "\"cumulative_pages\": %d, \"processes_tracked\": %d, "
                    "\"vma_dirty\": [",
                e->timestamp_ms, e->rate_pages_per_sec,
                e->cumulative_pages, e->processes_tracked);
        for (int v = 0; v < e->vma_summary_count; v++) {
            vma_dirty_summary_t *vs = &e->vma_summaries[v];
            fprintf(f, "{\"start\": \"0x%lx\", \"end\": \"0x%lx\", "
                       "\"dirty\": %d, \"total\": %d, "
                       "\"perms\": \"%s\", \"type\": \"%s\"}",
                    (unsigned long)vs->start, (unsigned long)vs->end,
                    vs->dirty_pages, vs->total_pages,
                    vs->perms, vma_type_str(vs->vma_type));
            if (v < e->vma_summary_count - 1) fprintf(f, ", ");
        }
        fprintf(f, "]}%s\n", i < t->timeline_count - 1 ? "," : "");
    }
    fprintf(f, "  ]\n");

    fprintf(f, "}\n");
    fflush(f);
}

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s -p PID [options]\n", prog);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  -p, --pid PID            Process ID to track (required)\n");
    fprintf(stderr, "  -i, --interval MS        Sampling interval in milliseconds (default: 100)\n");
    fprintf(stderr, "  -d, --duration SEC       Tracking duration in seconds (default: 10)\n");
    fprintf(stderr, "  -o, --output FILE        Output JSON file (default: stdout)\n");
    fprintf(stderr, "  -w, --workload NAME      Workload name (default: unknown)\n");
    fprintf(stderr, "  -n, --no-clear           Don't clear dirty bits after scan (accumulate mode)\n");
    fprintf(stderr, "  -D, --dual-channel       Collect both WP and soft-dirty channels simultaneously\n");
    fprintf(stderr, "  -S, --sd-clear           Clear soft-dirty after each dual-channel scan (delta mode)\n");
    fprintf(stderr, "  -C, --no-track-children  Don't track child/descendant processes\n");
    fprintf(stderr, "  -E, --exclude-pid PID    Exclude PID from tracking (can be repeated)\n");
    fprintf(stderr, "  -O, --sd-only            Soft-dirty clear+read only, no uffd (OoH /proc comparison)\n");
    fprintf(stderr, "  -Y, --uffd-sync          Userfaultfd synchronous WP mode (OoH ufd comparison)\n");
    fprintf(stderr, "  -Q, --no-output          Scan+track but don't store/write dirty page data\n");
    fprintf(stderr, "  -v, --verbose            Print per-sample progress and VMA re-register messages\n");
    fprintf(stderr, "  -h, --help               Show this help\n");
    fprintf(stderr, "\nModes:\n");
    fprintf(stderr, "  Default: Uses uffd-wp + PM_SCAN_WP_MATCHING for atomic scan+clear.\n");
    fprintf(stderr, "           Independent from soft-dirty (does not touch clear_refs).\n");
    fprintf(stderr, "           Exits with error if uffd-wp setup fails (no soft-dirty fallback).\n");
    fprintf(stderr, "  --no-clear: Scan-only mode using soft-dirty bits (read-only, no clearing).\n");
    fprintf(stderr, "              Safe to use alongside CRIU since it never writes clear_refs.\n");
    fprintf(stderr, "  --dual-channel: Collects both WP (delta) and soft-dirty (cumulative) per sample.\n");
    fprintf(stderr, "  --dual-channel --sd-clear: Both channels in delta mode (clears soft-dirty too).\n");
    fprintf(stderr, "                             WARNING: --sd-clear interferes with CRIU tracking.\n");
    fprintf(stderr, "  --sd-only: Soft-dirty clear_refs + pagemap read (no uffd-wp). OoH /proc method.\n");
    fprintf(stderr, "             Each interval: read soft-dirty -> clear soft-dirty -> next interval.\n");
    fprintf(stderr, "  --uffd-sync: Userfaultfd synchronous WP (no WP_ASYNC). OoH ufd method.\n");
    fprintf(stderr, "               Each write faults into handler thread (~15x overhead).\n");
    fprintf(stderr, "\nChild process tracking:\n");
    fprintf(stderr, "  By default, descendant processes are automatically discovered and tracked.\n");
    fprintf(stderr, "  Use --no-track-children to only track the root PID.\n");
    fprintf(stderr, "  Use --exclude-pid to skip specific PIDs from tracking.\n");
}

int main(int argc, char *argv[]) {
    int pid = 0;
    int interval_ms = 100;
    int duration_sec = 10;
    const char *output_file = NULL;
    const char *workload = "unknown";
    bool no_clear = false;
    bool dual_channel = false;
    bool sd_clear = false;
    bool track_children = true;
    bool sd_only = false;
    bool uffd_sync = false;
    bool no_output = false;
    bool verbose = false;
    pid_t exclude_pids[64];
    int exclude_pid_count = 0;

    static struct option long_options[] = {
        {"pid", required_argument, 0, 'p'},
        {"interval", required_argument, 0, 'i'},
        {"duration", required_argument, 0, 'd'},
        {"output", required_argument, 0, 'o'},
        {"workload", required_argument, 0, 'w'},
        {"no-clear", no_argument, 0, 'n'},
        {"dual-channel", no_argument, 0, 'D'},
        {"sd-clear", no_argument, 0, 'S'},
        {"no-track-children", no_argument, 0, 'C'},
        {"exclude-pid", required_argument, 0, 'E'},
        {"sd-only", no_argument, 0, 'O'},
        {"uffd-sync", no_argument, 0, 'Y'},
        {"no-output", no_argument, 0, 'Q'},
        {"verbose", no_argument, 0, 'v'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "p:i:d:o:w:nDSCE:OYQvh", long_options, NULL)) != -1) {
        switch (opt) {
            case 'p': pid = atoi(optarg); break;
            case 'i': interval_ms = atoi(optarg); break;
            case 'd': duration_sec = atoi(optarg); break;
            case 'o': output_file = optarg; break;
            case 'w': workload = optarg; break;
            case 'n': no_clear = true; break;
            case 'D': dual_channel = true; break;
            case 'S': sd_clear = true; break;
            case 'C': track_children = false; break;
            case 'O': sd_only = true; break;
            case 'Y': uffd_sync = true; break;
            case 'Q': no_output = true; break;
            case 'v': verbose = true; break;
            case 'E':
                if (exclude_pid_count < 64) {
                    exclude_pids[exclude_pid_count++] = (pid_t)atoi(optarg);
                }
                break;
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
    if (dual_channel && no_clear) {
        fprintf(stderr, "Error: --dual-channel requires WP mode (incompatible with --no-clear)\n");
        return 1;
    }
    if (sd_clear && !dual_channel) {
        fprintf(stderr, "Error: --sd-clear requires --dual-channel\n");
        return 1;
    }
    if (sd_only && (dual_channel || no_clear || uffd_sync)) {
        fprintf(stderr, "Error: --sd-only is incompatible with --dual-channel, --no-clear, --uffd-sync\n");
        return 1;
    }
    if (uffd_sync && (dual_channel || no_clear)) {
        fprintf(stderr, "Error: --uffd-sync is incompatible with --dual-channel, --no-clear\n");
        return 1;
    }

    if (tracker_init(&tracker, pid, interval_ms, no_clear, dual_channel, sd_clear,
                     track_children, sd_only, uffd_sync, no_output, verbose) < 0) {
        return 1;
    }

    /* Copy exclude PIDs */
    for (int i = 0; i < exclude_pid_count; i++) {
        tracker.exclude_pids[i] = exclude_pids[i];
    }
    tracker.exclude_pid_count = exclude_pid_count;

    const char *mode_str = "uffd-wp-async";
    if (sd_only) mode_str = "sd-only";
    else if (uffd_sync) mode_str = "uffd-sync";
    else if (no_clear) mode_str = "soft-dirty-readonly";

    fprintf(stderr, "Tracking PID %d for %d seconds (interval=%dms, mode=%s%s%s)\n",
            pid, duration_sec, interval_ms, mode_str,
            dual_channel ? ", dual-channel" : "",
            track_children ? ", track-children" : "");

    if (exclude_pid_count > 0) {
        fprintf(stderr, "Excluding PIDs:");
        for (int i = 0; i < exclude_pid_count; i++) {
            fprintf(stderr, " %d", exclude_pids[i]);
        }
        fprintf(stderr, "\n");
    }

    /*
     * Do NOT clear soft-dirty by default. uffd-wp uses PAGE_IS_WRITTEN
     * which is independent from soft-dirty. Exception: --sd-clear in
     * dual-channel mode explicitly opts into soft-dirty clearing for
     * delta comparison between channels.
     */
    if (sd_only) {
        for (int i = 0; i < tracker.process_count; i++) {
            if (tracker.processes[i]) {
                clear_soft_dirty_for_process(tracker.processes[i]);
            }
        }
        fprintf(stderr, "Cleared soft-dirty for baseline (--sd-only)\n");
    }
    if (dual_channel && sd_clear) {
        for (int i = 0; i < tracker.process_count; i++) {
            if (tracker.processes[i]) {
                clear_soft_dirty_for_process(tracker.processes[i]);
            }
        }
        fprintf(stderr, "Cleared soft-dirty for baseline (--sd-clear)\n");
    }

    /* Open output file for streaming.
     * With -Q (no_output): still open if -o specified (footer-only: empty samples + timeline).
     * Without -Q: open -o file or stdout. */
    if (output_file || !no_output) {
        tracker.output_fp = output_file ? fopen(output_file, "w") : stdout;
        if (!tracker.output_fp) {
            fprintf(stderr, "Failed to open output file: %s\n", strerror(errno));
            tracker_cleanup(&tracker);
            return 1;
        }
        write_json_header(&tracker, workload);
    }

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

        /* Progress report (before flush frees the data) */
        if (tracker.verbose && sample_count % 10 == 0) {
            sample_t *cur = &tracker.current_sample;
            if (tracker.dual_channel) {
                fprintf(stderr, "Sample %d: wp=%d sd=%d dirty pages, %d processes\n",
                        sample_count, cur->page_count, cur->sd_page_count,
                        cur->pids_tracked_count);
            } else {
                fprintf(stderr, "Sample %d: %d dirty pages, %d processes\n",
                        sample_count, cur->page_count, cur->pids_tracked_count);
            }
        }

        /* Flush sample to output + free memory */
        flush_and_free_sample(&tracker, &tracker.current_sample);

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

    fprintf(stderr, "Stopped tracking (total %d samples, %d processes seen)\n",
            tracker.sample_count, tracker.known_pid_count);

    /* Write footer + close output */
    if (tracker.output_fp) {
        write_json_footer(&tracker);
        if (output_file) {
            fclose(tracker.output_fp);
            tracker.output_fp = NULL;
            fprintf(stderr, "Output written to %s\n", output_file);
        }
    }

    tracker_cleanup(&tracker);
    return 0;
}
