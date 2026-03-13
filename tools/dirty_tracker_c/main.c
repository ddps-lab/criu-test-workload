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
#define MAX_SAMPLES 10000
#define MAX_PROCESSES 64

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

    /* Per-process VMAs */
    vma_info_t *vmas;
    int vma_count;
    int vma_capacity;

    /* Whether this process supports PAGEMAP_SCAN */
    bool use_pagemap_scan;
} process_tracker_t;

/* Sample */
typedef struct {
    double timestamp_ms;
    dirty_page_t *pages;    /* Primary channel (WP or soft-dirty depending on mode) */
    int page_count;
    dirty_page_t *sd_pages; /* Soft-dirty channel (dual-channel mode only) */
    int sd_page_count;
    pid_t *pids_tracked;    /* Array of PIDs tracked in this sample */
    int pids_tracked_count;
    /* Memory usage (aggregate across all tracked processes) */
    long rss_bytes;             /* Resident Set Size from /proc/pid/statm */
    long writable_vma_bytes;    /* Sum of writable VMA sizes from /proc/pid/maps */
} sample_t;

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

    /* Samples (aggregate across all processes) */
    sample_t samples[MAX_SAMPLES];
    int sample_count;
    struct timespec start_time;

    /* Aggregate statistics */
    int total_dirty_pages;
    uint64_t *unique_addrs;
    int unique_count;
    int unique_capacity;

    /* VMA type counters (indexed by vma_type_t) */
    int vma_type_counts[7];
    int vma_type_sizes[7];

    /* Dual-channel mode */
    bool dual_channel;     /* collect both WP and soft-dirty simultaneously */
    bool sd_clear;         /* clear soft-dirty after each dual-channel scan */
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
    pt->is_alive = true;

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

    /* Clean up uffd-wp in target process */
    if (pt->wp_active && pt->target_uffd >= 0) {
        cleanup_userfaultfd_wp_for_process(pt);
    }

    if (pt->pagemap_fd >= 0) close(pt->pagemap_fd);
    if (pt->clear_refs_fd >= 0) close(pt->clear_refs_fd);
    free(pt->vmas);
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
                        bool track_children) {
    memset(t, 0, sizeof(*t));

    t->root_pid = root_pid;
    t->interval_ms = interval_ms;
    t->no_clear = no_clear;
    t->dual_channel = dual_channel;
    t->sd_clear = sd_clear;
    t->track_children = track_children;

    /* Initialize unique address tracking */
    t->unique_capacity = 65536;
    t->unique_addrs = malloc(t->unique_capacity * sizeof(uint64_t));
    if (!t->unique_addrs) return -1;

    /* Initialize root process tracker */
    process_tracker_t *root_pt = process_tracker_init(root_pid);
    if (!root_pt) {
        free(t->unique_addrs);
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

    /* Free samples */
    for (int i = 0; i < t->sample_count; i++) {
        free(t->samples[i].pages);
        free(t->samples[i].sd_pages);
        free(t->samples[i].pids_tracked);
    }

    free(t->unique_addrs);
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

/**
 * Setup userfaultfd write-protection on a process via ptrace injection.
 *
 * This enables PM_SCAN_WP_MATCHING to work by registering each writable VMA
 * with userfaultfd in UFFDIO_REGISTER_MODE_WP + UFFD_FEATURE_WP_ASYNC.
 *
 * The target process is briefly stopped during setup (~1ms for typical VMAs).
 * With WP_ASYNC, subsequent write faults are handled inline by the kernel
 * (no userspace handler needed), so the workload runs at near-native speed.
 *
 * Returns 0 on success, -1 on failure (caller should fall back to soft-dirty).
 */
static int setup_userfaultfd_wp_for_process(process_tracker_t *pt)
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
     *    Use UFFD_USER_MODE_ONLY to bypass CAP_SYS_PTRACE requirement
     *    (available since Linux 6.1). Fallback to privileged mode if needed.
     */
    if (inject_syscall(pid, saved_regs.rip,
                       __NR_userfaultfd,
                       O_CLOEXEC | O_NONBLOCK | UFFD_USER_MODE_ONLY,
                       0, 0, 0, 0, 0, &result) < 0 || result < 0) {
        /* Try without UFFD_USER_MODE_ONLY (requires CAP_SYS_PTRACE or sysctl) */
        if (inject_syscall(pid, saved_regs.rip,
                           __NR_userfaultfd,
                           O_CLOEXEC | O_NONBLOCK,
                           0, 0, 0, 0, 0, &result) < 0 || result < 0) {
            fprintf(stderr, "inject userfaultfd failed (pid=%d): result=%ld\n", pid, result);
            fprintf(stderr, "  hint: try 'sysctl -w vm.unprivileged_userfaultfd=1'\n");
            goto cleanup_mmap;
        }
    }
    long uffd = result;
    fprintf(stderr, "Injected userfaultfd -> fd=%ld (pid=%d)\n", uffd, pid);

    /* 6. UFFDIO_API: enable WP_ASYNC feature */
    {
        struct uffdio_api api = {
            .api = UFFD_API,
            .features = UFFD_FEATURE_WP_ASYNC,
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
        fprintf(stderr, "UFFDIO_API success (WP_ASYNC enabled, pid=%d)\n", pid);
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

    /* 2. Save original state */
    struct user_regs_struct saved_regs;
    if (ptrace(PTRACE_GETREGS, pid, 0, &saved_regs) < 0) {
        fprintf(stderr, "cleanup: GETREGS failed: %s\n", strerror(errno));
        ptrace(PTRACE_DETACH, pid, 0, 0);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }

    errno = 0;
    long saved_code = ptrace(PTRACE_PEEKTEXT, pid, saved_regs.rip, 0);
    if (errno) {
        ptrace(PTRACE_DETACH, pid, 0, 0);
        pt->target_uffd = -1;
        pt->wp_active = false;
        return -1;
    }

    /* 3. Poke 'syscall' instruction */
    long code_with_syscall = (saved_code & ~0xFFFFL) | 0x050FL;
    if (ptrace(PTRACE_POKETEXT, pid, saved_regs.rip, code_with_syscall) < 0) {
        ptrace(PTRACE_DETACH, pid, 0, 0);
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

    pt->target_uffd = -1;
    pt->wp_active = false;
    fprintf(stderr, "uffd-wp cleanup complete (pid=%d)\n", pid);
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
        if (setup_userfaultfd_wp_for_process(pt) == 0) {
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
                    /* First interval: empty (baseline WP just established) */
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
            continue;
        }

        /* Process returned regions - in WP mode all returned pages are dirty */
        for (long i = 0; i < ret; i++) {
            /* In soft-dirty mode, filter by soft-dirty flag */
            if (!use_wp && !(t->regions[i].categories & PAGE_IS_SOFT_DIRTY)) continue;

            for (uint64_t addr = t->regions[i].start;
                 addr < t->regions[i].end;
                 addr += PAGE_SIZE) {

                /* Grow buffer if needed */
                if (sample->page_count >= 65536) break;  /* Safety limit per sample */

                int capacity = sample->page_count + 1;
                if (capacity > 4096 && (capacity & (capacity - 1)) == 0) {
                    /* Power of two — realloc */
                }
                /* Pages buffer is pre-allocated or grown in collect_sample */

                dirty_page_t *page = &sample->pages[sample->page_count];
                if (sample->page_count >= 65536) break;
                sample->page_count++;
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

                    if (sample->sd_page_count >= 65536) break;

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

        for (size_t i = 0; i < entries; i++) {
            if (buf[i] & PM_SOFT_DIRTY) {
                if (sample->page_count >= 65536) break;

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
    if (t->sample_count >= MAX_SAMPLES) return -1;

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
    sample_t *sample = &t->samples[t->sample_count];
    memset(sample, 0, sizeof(*sample));
    sample->timestamp_ms = get_elapsed_ms(&t->start_time);

    /* Allocate pages buffer (shared across all processes for this sample) */
    sample->pages = malloc(65536 * sizeof(dirty_page_t));
    if (!sample->pages) return -1;
    sample->page_count = 0;

    if (t->dual_channel) {
        sample->sd_pages = malloc(65536 * sizeof(dirty_page_t));
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

        /* Record this PID */
        sample->pids_tracked[sample->pids_tracked_count++] = pt->pid;

        /* Collect memory usage (nearly zero overhead) */
        sample->rss_bytes += read_rss_bytes(pt->pid);
        sample->writable_vma_bytes += calc_writable_vma_bytes(pt);

        /* Read dirty pages */
        int ret;
        if (pt->use_pagemap_scan) {
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
     * Default: never clear soft-dirty (independent from CRIU).
     */
    if (t->dual_channel && t->sd_clear) {
        for (int i = 0; i < t->process_count; i++) {
            if (t->processes[i]) {
                clear_soft_dirty_for_process(t->processes[i]);
            }
        }
    }

    return 0;
}

/* ===== JSON output ===== */

static void write_json_output(tracker_t *t, const char *workload, const char *output_file) {
    FILE *f = output_file ? fopen(output_file, "w") : stdout;
    if (!f) {
        fprintf(stderr, "Failed to open output file: %s\n", strerror(errno));
        return;
    }

    /* Calculate rates for timeline */
    double *rates = NULL;
    int *cumulative = NULL;
    double avg_rate = 0, peak_rate = 0;

    if (t->sample_count > 0) {
        rates = calloc(t->sample_count, sizeof(double));
        cumulative = calloc(t->sample_count, sizeof(int));

        cumulative[0] = t->samples[0].page_count;
        rates[0] = 0;

        for (int i = 1; i < t->sample_count; i++) {
            cumulative[i] = cumulative[i-1] + t->samples[i].page_count;
            double delta_time = (t->samples[i].timestamp_ms - t->samples[i-1].timestamp_ms) / 1000.0;
            if (delta_time > 0) {
                rates[i] = t->samples[i].page_count / delta_time;
            }
        }

        /* Calculate avg and peak rates */
        double rate_sum = 0;
        int positive_count = 0;
        for (int i = 0; i < t->sample_count; i++) {
            if (rates[i] > 0) {
                rate_sum += rates[i];
                positive_count++;
                if (rates[i] > peak_rate) peak_rate = rates[i];
            }
        }
        if (positive_count > 0) avg_rate = rate_sum / positive_count;
    }

    /* Calculate VMA distribution */
    int total_vma_events = 0;
    for (int i = 0; i < 7; i++) total_vma_events += t->vma_type_counts[i];

    /* Determine max processes tracked across all samples */
    int max_processes_tracked = 0;
    for (int s = 0; s < t->sample_count; s++) {
        if (t->samples[s].pids_tracked_count > max_processes_tracked) {
            max_processes_tracked = t->samples[s].pids_tracked_count;
        }
    }

    fprintf(f, "{\n");
    fprintf(f, "  \"workload\": \"%s\",\n", workload);
    fprintf(f, "  \"root_pid\": %d,\n", t->root_pid);
    fprintf(f, "  \"track_children\": %s,\n", t->track_children ? "true" : "false");
    fprintf(f, "  \"tracking_duration_ms\": %.3f,\n",
            t->sample_count > 0 ? t->samples[t->sample_count - 1].timestamp_ms : 0.0);
    fprintf(f, "  \"page_size\": %lu,\n", (unsigned long)PAGE_SIZE);

    /* pagemap_scan_used: true if root process uses it */
    bool root_uses_pagemap = false;
    if (t->process_count > 0 && t->processes[0]) {
        root_uses_pagemap = t->processes[0]->use_pagemap_scan;
    }
    fprintf(f, "  \"pagemap_scan_used\": %s,\n", root_uses_pagemap ? "true" : "false");
    fprintf(f, "  \"clear_on_scan\": %s,\n", t->no_clear ? "false" : "true");

    fprintf(f, "  \"dual_channel\": %s,\n", t->dual_channel ? "true" : "false");

    /* Samples */
    fprintf(f, "  \"samples\": [\n");
    for (int s = 0; s < t->sample_count; s++) {
        sample_t *sample = &t->samples[s];
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
        fprintf(f, "    }%s\n", s < t->sample_count - 1 ? "," : "");
    }
    fprintf(f, "  ],\n");

    /* Summary */
    fprintf(f, "  \"summary\": {\n");
    fprintf(f, "    \"total_unique_pages\": %d,\n", t->unique_count);
    fprintf(f, "    \"total_dirty_events\": %d,\n", t->total_dirty_pages);
    fprintf(f, "    \"total_dirty_size_bytes\": %lu,\n", (unsigned long)t->total_dirty_pages * PAGE_SIZE);
    fprintf(f, "    \"avg_dirty_rate_per_sec\": %.2f,\n", avg_rate);
    fprintf(f, "    \"peak_dirty_rate\": %.2f,\n", peak_rate);

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

    /* Dirty rate timeline */
    fprintf(f, "  \"dirty_rate_timeline\": [\n");
    for (int i = 0; i < t->sample_count; i++) {
        fprintf(f, "    {\"timestamp_ms\": %.3f, \"rate_pages_per_sec\": %.2f, \"cumulative_pages\": %d, \"processes_tracked\": %d}%s\n",
                t->samples[i].timestamp_ms,
                rates ? rates[i] : 0.0,
                cumulative ? cumulative[i] : 0,
                t->samples[i].pids_tracked_count,
                i < t->sample_count - 1 ? "," : "");
    }
    fprintf(f, "  ]\n");

    fprintf(f, "}\n");

    free(rates);
    free(cumulative);

    if (output_file) fclose(f);
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
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "p:i:d:o:w:nDSCE:h", long_options, NULL)) != -1) {
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

    if (tracker_init(&tracker, pid, interval_ms, no_clear, dual_channel, sd_clear, track_children) < 0) {
        return 1;
    }

    /* Copy exclude PIDs */
    for (int i = 0; i < exclude_pid_count; i++) {
        tracker.exclude_pids[i] = exclude_pids[i];
    }
    tracker.exclude_pid_count = exclude_pid_count;

    fprintf(stderr, "Tracking PID %d for %d seconds (interval=%dms, clear=%s%s%s)\n",
            pid, duration_sec, interval_ms, no_clear ? "off" : "on",
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
    if (dual_channel && sd_clear) {
        for (int i = 0; i < tracker.process_count; i++) {
            if (tracker.processes[i]) {
                clear_soft_dirty_for_process(tracker.processes[i]);
            }
        }
        fprintf(stderr, "Cleared soft-dirty for baseline (--sd-clear)\n");
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
        if (sample_count % 10 == 0) {
            sample_t *last = &tracker.samples[tracker.sample_count - 1];
            if (tracker.dual_channel) {
                fprintf(stderr, "Sample %d: wp=%d sd=%d dirty pages, %d processes\n",
                        sample_count, last->page_count, last->sd_page_count,
                        last->pids_tracked_count);
            } else {
                fprintf(stderr, "Sample %d: %d dirty pages, %d processes\n",
                        sample_count, last->page_count, last->pids_tracked_count);
            }
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

    fprintf(stderr, "Stopped tracking (total %d samples, %d processes seen)\n",
            tracker.sample_count, tracker.known_pid_count);

    /* Write output */
    write_json_output(&tracker, workload, output_file);

    if (output_file) {
        fprintf(stderr, "Output written to %s\n", output_file);
    }

    tracker_cleanup(&tracker);
    return 0;
}
