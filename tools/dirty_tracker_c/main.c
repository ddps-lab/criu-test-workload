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
#include <sys/ptrace.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <sys/mman.h>
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
    dirty_page_t *pages;    /* Primary channel (WP or soft-dirty depending on mode) */
    int page_count;
    dirty_page_t *sd_pages; /* Soft-dirty channel (dual-channel mode only) */
    int sd_page_count;
    int pid;
} sample_t;

/* Tracker state */
typedef struct {
    int pid;
    int interval_ms;
    int pagemap_fd;
    int clear_refs_fd;
    bool use_pagemap_scan;
    bool no_clear;  /* If true, don't clear dirty bits after scan */

    vma_info_t vmas[MAX_VMAS];
    int vma_count;

    struct page_region regions[MAX_REGIONS];
    struct page_region sd_regions[MAX_REGIONS]; /* For dual-channel soft-dirty scan */

    sample_t samples[MAX_SAMPLES];
    int sample_count;

    struct timespec start_time;

    /* Statistics */
    int total_dirty_pages;
    uint64_t *unique_addrs;
    int unique_count;
    int unique_capacity;

    /* VMA type counters (indexed by vma_type_t) */
    int vma_type_counts[7];
    int vma_type_sizes[7];

    /* WP mode state */
    bool wp_initialized;   /* set after initial WP probe/setup */
    bool wp_active;        /* true if WP mode is actually working */

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

static int tracker_init(tracker_t *t, int pid, int interval_ms, bool no_clear, bool dual_channel, bool sd_clear) {
    memset(t, 0, sizeof(*t));

    t->pid = pid;
    t->interval_ms = interval_ms;
    t->no_clear = no_clear;
    t->dual_channel = dual_channel;
    t->sd_clear = sd_clear;
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

    /* Open clear_refs (optional — not used in WP mode or no-clear mode) */
    snprintf(path, sizeof(path), "/proc/%d/clear_refs", pid);
    t->clear_refs_fd = open(path, O_WRONLY);
    /* Not fatal if this fails — WP mode doesn't need it */

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

static void clear_soft_dirty(tracker_t *t) {
    if (t->clear_refs_fd >= 0) {
        lseek(t->clear_refs_fd, 0, SEEK_SET);
        if (write(t->clear_refs_fd, "4", 1) < 0) {
            fprintf(stderr, "Warning: clear_refs write failed: %s\n", strerror(errno));
        }
    }
}

static void tracker_cleanup(tracker_t *t) {
    if (t->pagemap_fd >= 0) close(t->pagemap_fd);
    if (t->clear_refs_fd >= 0) close(t->clear_refs_fd);

    for (int i = 0; i < t->sample_count; i++) {
        free(t->samples[i].pages);
        free(t->samples[i].sd_pages);
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
 * Setup userfaultfd write-protection on the target process via ptrace injection.
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
static int setup_userfaultfd_wp(tracker_t *t)
{
    pid_t pid = t->pid;
    int ret = -1;

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
            fprintf(stderr, "inject userfaultfd failed: result=%ld\n", result);
            fprintf(stderr, "  hint: try 'sysctl -w vm.unprivileged_userfaultfd=1'\n");
            goto cleanup_mmap;
        }
    }
    long uffd = result;
    fprintf(stderr, "Injected userfaultfd -> fd=%ld\n", uffd);

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
        fprintf(stderr, "UFFDIO_API success (WP_ASYNC enabled)\n");
    }

    /* 7. Register each writable VMA with UFFDIO_REGISTER_MODE_WP */
    {
        int registered = 0, skipped = 0;
        for (int v = 0; v < t->vma_count; v++) {
            vma_info_t *vma = &t->vmas[v];

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
        fprintf(stderr, "UFFDIO_REGISTER: %d VMAs registered, %d skipped\n",
                registered, skipped);

        if (registered == 0) {
            fprintf(stderr, "No VMAs could be registered for WP\n");
            goto cleanup_uffd;
        }
    }

    /* Success - don't close uffd (needs to stay open in target for WP to work) */
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
    return ret;
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

    /*
     * Determine scan mode:
     * - wp_active: Use PM_SCAN_WP_MATCHING for atomic scan+clear (PAGE_IS_WRITTEN)
     * - no_clear && !wp_active: Accumulate soft-dirty bits (no clear)
     * - !no_clear && !wp_active: Use soft-dirty + clear_refs
     *
     * wp_active is set during init if userfaultfd-wp is supported.
     * If WP probe fails, we fall back to soft-dirty permanently.
     */
    bool use_wp = t->wp_active;
    uint64_t scan_flags = use_wp ? PM_SCAN_WP_MATCHING : 0;

    /* Initial WP setup: inject userfaultfd-wp via ptrace, then WP all pages */
    if (!t->wp_initialized && !t->no_clear) {
        /*
         * Step 1: Inject userfaultfd-wp registration into target process.
         * This is needed because PM_SCAN_WP_MATCHING requires VM_UFFD_WP
         * on the target's VMAs, which can only be set from within the process.
         */
        if (setup_userfaultfd_wp(t) == 0) {
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
            long ret = ioctl(t->pagemap_fd, PAGEMAP_SCAN, &check_args);

            if (ret > 0) {
                fprintf(stderr, "WP mode verified: PAGE_IS_WPALLOWED present\n");

                /* Step 3: WP all present pages for baseline */
                use_wp = true;
                scan_flags = PM_SCAN_WP_MATCHING;
                for (int v = 0; v < t->vma_count; v++) {
                    vma_info_t *vma = &t->vmas[v];
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

                    ret = ioctl(t->pagemap_fd, PAGEMAP_SCAN, &wp_args);
                    if (ret < 0 && errno == EPERM) {
                        fprintf(stderr, "PM_SCAN_WP_MATCHING failed after setup: %s\n", strerror(errno));
                        use_wp = false;
                        scan_flags = 0;
                        break;
                    }
                }

                if (use_wp) {
                    t->wp_initialized = true;
                    t->wp_active = true;
                    fprintf(stderr, "WP mode active: using PM_SCAN_WP_MATCHING for atomic dirty tracking\n");
                    /* First interval: empty sample (baseline WP just established) */
                    return 0;
                }
            } else {
                fprintf(stderr, "WP setup succeeded but WPALLOWED still not set (ret=%ld)\n", ret);
            }
        }

        /*
         * WP setup failed. Do NOT fall back to soft-dirty + clear_refs,
         * because that would interfere with CRIU's soft-dirty tracking.
         * The whole point of uffd-wp is to be an independent channel.
         */
        fprintf(stderr, "ERROR: uffd-wp setup failed. Cannot track without interfering with soft-dirty.\n");
        fprintf(stderr, "Use --no-clear for scan-only mode (no clearing, no WP).\n");
        t->wp_initialized = true;
        return -1;
    }

    /* Determine dirty flag based on mode */
    uint64_t dirty_flag = use_wp ? PAGE_IS_WRITTEN : PAGE_IS_SOFT_DIRTY;

    /* Scan each writable VMA separately */
    for (int v = 0; v < t->vma_count; v++) {
        vma_info_t *vma = &t->vmas[v];

        /* Skip non-writable VMAs */
        if (!strchr(vma->perms, 'w')) continue;

        /*
         * For WP mode: require PAGE_IS_WRITTEN (pages written since last WP).
         * For soft-dirty mode: match any present/swapped page, filter by soft-dirty after.
         */
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

        long ret = ioctl(t->pagemap_fd, PAGEMAP_SCAN, &args);
        if (ret < 0) {
            if (errno == EPERM && use_wp) {
                /* WP failed at runtime — do not fall back to soft-dirty */
                fprintf(stderr, "ERROR: PM_SCAN_WP_MATCHING failed (EPERM). "
                        "uffd-wp lost, cannot continue without soft-dirty interference.\n");
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

                t->vma_type_counts[vma->type]++;
                t->vma_type_sizes[vma->type] += PAGE_SIZE;
                add_unique_addr(t, addr);
            }
        }
    }

    /* Dual-channel: also scan soft-dirty (read-only, never clear) */
    if (t->dual_channel && use_wp) {
        int sd_capacity = 4096;
        sample->sd_pages = malloc(sd_capacity * sizeof(dirty_page_t));
        if (!sample->sd_pages) return -1;
        sample->sd_page_count = 0;

        for (int v = 0; v < t->vma_count; v++) {
            vma_info_t *vma = &t->vmas[v];
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

            long sd_ret = ioctl(t->pagemap_fd, PAGEMAP_SCAN, &sd_args);
            if (sd_ret < 0) continue;

            for (long i = 0; i < sd_ret; i++) {
                if (!(t->sd_regions[i].categories & PAGE_IS_SOFT_DIRTY)) continue;

                for (uint64_t addr = t->sd_regions[i].start;
                     addr < t->sd_regions[i].end;
                     addr += PAGE_SIZE) {

                    if (sample->sd_page_count >= sd_capacity) {
                        sd_capacity *= 2;
                        sample->sd_pages = realloc(sample->sd_pages, sd_capacity * sizeof(dirty_page_t));
                        if (!sample->sd_pages) return -1;
                    }

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

                t->vma_type_counts[vma->type]++;
                t->vma_type_sizes[vma->type] += PAGE_SIZE;
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
        /* PAGEMAP_SCAN with PM_SCAN_WP_MATCHING does atomic scan+clear */
        ret = read_dirty_pages_pagemap_scan(t, sample);
    } else {
        ret = read_dirty_pages_soft_dirty(t, sample);
    }

    if (ret < 0) return ret;

    t->total_dirty_pages += sample->page_count;
    t->sample_count++;

    /*
     * WP channel: PM_SCAN_WP_MATCHING handles clearing atomically.
     * SD channel (dual-channel --sd-clear): clear soft-dirty after scan.
     * Default: never clear soft-dirty (independent from CRIU).
     */
    if (t->dual_channel && t->sd_clear) {
        clear_soft_dirty(t);
    }

    return 0;
}

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

    fprintf(f, "{\n");
    fprintf(f, "  \"workload\": \"%s\",\n", workload);
    fprintf(f, "  \"root_pid\": %d,\n", t->pid);
    fprintf(f, "  \"track_children\": false,\n");
    fprintf(f, "  \"tracking_duration_ms\": %.3f,\n",
            t->sample_count > 0 ? t->samples[t->sample_count - 1].timestamp_ms : 0.0);
    fprintf(f, "  \"page_size\": %lu,\n", PAGE_SIZE);
    fprintf(f, "  \"pagemap_scan_used\": %s,\n", t->use_pagemap_scan ? "true" : "false");
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
                        page->addr, vma_type_str(page->vma_type), page->perms, page->pathname, PAGE_SIZE,
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
                        page->addr, vma_type_str(page->vma_type), page->perms, page->pathname, PAGE_SIZE,
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
                        page->addr, vma_type_str(page->vma_type), page->perms, page->pathname, PAGE_SIZE,
                        p < sample->page_count - 1 ? "," : "");
            }
            fprintf(f, "      ],\n");
            fprintf(f, "      \"delta_dirty_count\": %d,\n", sample->page_count);
        }

        fprintf(f, "      \"pids_tracked\": [%d]\n", sample->pid);
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
    fprintf(f, "    \"max_processes_tracked\": 1,\n");
    fprintf(f, "    \"total_pids_seen\": [%d]\n", t->pid);
    fprintf(f, "  },\n");

    /* Dirty rate timeline */
    fprintf(f, "  \"dirty_rate_timeline\": [\n");
    for (int i = 0; i < t->sample_count; i++) {
        fprintf(f, "    {\"timestamp_ms\": %.3f, \"rate_pages_per_sec\": %.2f, \"cumulative_pages\": %d, \"processes_tracked\": 1}%s\n",
                t->samples[i].timestamp_ms,
                rates ? rates[i] : 0.0,
                cumulative ? cumulative[i] : 0,
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
    fprintf(stderr, "  -p, --pid PID        Process ID to track (required)\n");
    fprintf(stderr, "  -i, --interval MS    Sampling interval in milliseconds (default: 100)\n");
    fprintf(stderr, "  -d, --duration SEC   Tracking duration in seconds (default: 10)\n");
    fprintf(stderr, "  -o, --output FILE    Output JSON file (default: stdout)\n");
    fprintf(stderr, "  -w, --workload NAME  Workload name (default: unknown)\n");
    fprintf(stderr, "  -n, --no-clear       Don't clear dirty bits after scan (accumulate mode)\n");
    fprintf(stderr, "  -D, --dual-channel   Collect both WP and soft-dirty channels simultaneously\n");
    fprintf(stderr, "  -S, --sd-clear       Clear soft-dirty after each dual-channel scan (delta mode)\n");
    fprintf(stderr, "  -h, --help           Show this help\n");
    fprintf(stderr, "\nModes:\n");
    fprintf(stderr, "  Default: Uses uffd-wp + PM_SCAN_WP_MATCHING for atomic scan+clear.\n");
    fprintf(stderr, "           Independent from soft-dirty (does not touch clear_refs).\n");
    fprintf(stderr, "           Exits with error if uffd-wp setup fails (no soft-dirty fallback).\n");
    fprintf(stderr, "  --no-clear: Scan-only mode using soft-dirty bits (read-only, no clearing).\n");
    fprintf(stderr, "              Safe to use alongside CRIU since it never writes clear_refs.\n");
    fprintf(stderr, "  --dual-channel: Collects both WP (delta) and soft-dirty (cumulative) per sample.\n");
    fprintf(stderr, "  --dual-channel --sd-clear: Both channels in delta mode (clears soft-dirty too).\n");
    fprintf(stderr, "                             WARNING: --sd-clear interferes with CRIU tracking.\n");
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

    static struct option long_options[] = {
        {"pid", required_argument, 0, 'p'},
        {"interval", required_argument, 0, 'i'},
        {"duration", required_argument, 0, 'd'},
        {"output", required_argument, 0, 'o'},
        {"workload", required_argument, 0, 'w'},
        {"no-clear", no_argument, 0, 'n'},
        {"dual-channel", no_argument, 0, 'D'},
        {"sd-clear", no_argument, 0, 'S'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "p:i:d:o:w:nDSh", long_options, NULL)) != -1) {
        switch (opt) {
            case 'p': pid = atoi(optarg); break;
            case 'i': interval_ms = atoi(optarg); break;
            case 'd': duration_sec = atoi(optarg); break;
            case 'o': output_file = optarg; break;
            case 'w': workload = optarg; break;
            case 'n': no_clear = true; break;
            case 'D': dual_channel = true; break;
            case 'S': sd_clear = true; break;
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

    if (tracker_init(&tracker, pid, interval_ms, no_clear, dual_channel, sd_clear) < 0) {
        return 1;
    }

    fprintf(stderr, "Tracking PID %d for %d seconds (interval=%dms, clear=%s%s)\n",
            pid, duration_sec, interval_ms, no_clear ? "off" : "on",
            dual_channel ? ", dual-channel" : "");

    /*
     * Do NOT clear soft-dirty by default. uffd-wp uses PAGE_IS_WRITTEN
     * which is independent from soft-dirty. Exception: --sd-clear in
     * dual-channel mode explicitly opts into soft-dirty clearing for
     * delta comparison between channels.
     */
    if (dual_channel && sd_clear) {
        clear_soft_dirty(&tracker);
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
                fprintf(stderr, "Sample %d: wp=%d sd=%d dirty pages\n",
                        sample_count, last->page_count, last->sd_page_count);
            } else {
                fprintf(stderr, "Sample %d: %d dirty pages\n",
                        sample_count, last->page_count);
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

    fprintf(stderr, "Stopped tracking (total %d samples)\n", tracker.sample_count);

    /* Write output */
    write_json_output(&tracker, workload, output_file);

    if (output_file) {
        fprintf(stderr, "Output written to %s\n", output_file);
    }

    tracker_cleanup(&tracker);
    return 0;
}
