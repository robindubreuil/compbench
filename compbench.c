/*
 * compbench - Btrfs transparent compression benchmark
 *
 * Measures read/write throughput, compression ratio, CPU usage, and disk I/O
 * for Btrfs compression algorithms using direct kernel interfaces.
 * Zero text-parsing of external tool output for measurements.
 *
 * Copyright (C) 2024 Robin Dubreuil
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <limits.h>
#include <ctype.h>

#include <linux/fs.h>

#define VERSION "2.3"
#define MAX_RESULTS 4096
#define BUF_SIZE (1 << 20)

enum force_mode { FORCE_NONE, FORCE_ZSTD, FORCE_LZO, FORCE_ALL, FORCE_BOTH };

struct config {
	const char *device;
	const char *archive;
	const char *output_csv;
	int zstd_min;
	int zstd_max;
	int repeat;
	int hdd_percent;
	enum force_mode force;
	int verbose;
	int integrity_check;
};

struct result {
	char location[16];
	char target[32];
	char force_label[16];
	double elapsed;
	double elapsed_sync;
	double rate_sync_mib;
	unsigned long long compressed_bytes;
	double ratio;
	double cpu_pct;
	double sec_per_reduction;
	unsigned long long disk_bytes;
	double disk_rate_sync_mib;
	double read_rate_mib;
	double read_cpu_pct;
	unsigned long long read_disk_bytes;
	double read_disk_rate_mib;
};

struct state {
	char tmpdir[PATH_MAX];
	char mnt[PATH_MAX];
	char tmpfs[PATH_MAX];
	char archive_path[PATH_MAX];
	char part1[PATH_MAX];
	char part2[PATH_MAX];
	unsigned long long data_size;
	unsigned long long archive_size;
	int tmpfs_mounted;
	int dev_mounted;
	int partitions_created;
};

static struct config g_cfg = {
	.zstd_min = 1,
	.zstd_max = 15,
	.repeat = 1,
	.force = FORCE_NONE,
	.integrity_check = 1,
};

static struct state g_st;
static struct result g_results[MAX_RESULTS];
static int g_nresults;
static volatile sig_atomic_t g_signaled;

static void		die(const char *, ...) __attribute__((noreturn, format(printf, 1, 2)));
static void		cleanup(void);

/* ------------------------------------------------------------------ */
/* Helpers                                                            */
/* ------------------------------------------------------------------ */

static void
die(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "error: ");
	vfprintf(stderr, fmt, ap);
	fprintf(stderr, "\n");
	va_end(ap);
	cleanup();
	_exit(1);
}

static void
msg(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vprintf(fmt, ap);
	va_end(ap);
	fflush(stdout);
}

static void
verbose(const char *fmt, ...)
{
	if (!g_cfg.verbose)
		return;
	va_list ap;
	va_start(ap, fmt);
	vprintf(fmt, ap);
	va_end(ap);
	fflush(stdout);
}

static void
status(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	if (!g_cfg.verbose && isatty(STDOUT_FILENO))
		printf("\033[K\r");
	vprintf(fmt, ap);
	va_end(ap);
	fflush(stdout);
}

/* Returns pointer to static buffer; safe for single-threaded use only. */
static const char *
bytes_human(unsigned long long bytes)
{
	static char buf[32];
	if (bytes >= (1ULL << 30))
		snprintf(buf, sizeof(buf), "%lluG", bytes >> 30);
	else if (bytes >= (1ULL << 20))
		snprintf(buf, sizeof(buf), "%lluM", bytes >> 20);
	else
		snprintf(buf, sizeof(buf), "%lluB", bytes);
	return buf;
}

static void
format_time(double seconds, char *buf, size_t sz)
{
	if (seconds < 0)
		seconds = 0;
	int h = (int)seconds / 3600;
	int m = ((int)seconds % 3600) / 60;
	int s = (int)seconds % 60;
	if (h > 0)
		snprintf(buf, sz, "%dh%02dm", h, m);
	else if (m > 0)
		snprintf(buf, sz, "%dm%02ds", m, s);
	else
		snprintf(buf, sz, "%ds", s);
}

/* ------------------------------------------------------------------ */
/* Timing                                                             */
/* ------------------------------------------------------------------ */

static double
ts_diff_sec(const struct timespec *start, const struct timespec *end)
{
	return (double)(end->tv_sec - start->tv_sec)
	     + (double)(end->tv_nsec - start->tv_nsec) / 1e9;
}

static void
format_eta(const struct timespec *start, int done, int total,
	   char *buf, size_t sz)
{
	if (done <= 0) {
		buf[0] = '\0';
		return;
	}
	struct timespec now;
	clock_gettime(CLOCK_MONOTONIC_RAW, &now);
	double elapsed = ts_diff_sec(start, &now);
	double remaining = (elapsed / done) * (total - done);
	char e[32], r[32];
	format_time(elapsed, e, sizeof(e));
	format_time(remaining, r, sizeof(r));
	snprintf(buf, sz, " [%s elapsed, ~%s remaining]", e, r);
}

/* ------------------------------------------------------------------ */
/* CPU measurement via /proc/stat                                     */
/* ------------------------------------------------------------------ */

struct cpu_sample {
	unsigned long long total;
	unsigned long long idle;
};

static struct cpu_sample
read_cpu(void)
{
	FILE *f = fopen("/proc/stat", "r");
	if (!f)
		return (struct cpu_sample){0, 0};
	unsigned long long user, nice, sys, idle, iowait, irq, softirq, steal;
	if (fscanf(f, "cpu %llu %llu %llu %llu %llu %llu %llu %llu",
		   &user, &nice, &sys, &idle, &iowait, &irq, &softirq, &steal) != 8) {
		fclose(f);
		return (struct cpu_sample){0, 0};
	}
	fclose(f);
	return (struct cpu_sample){
		.total = user + nice + sys + idle + iowait + irq + softirq + steal,
		.idle  = idle + iowait,
	};
}

static double
cpu_pct(struct cpu_sample before, struct cpu_sample after)
{
	unsigned long long dt = after.total - before.total;
	if (dt == 0)
		return 0.0;
	return 100.0 * (1.0 - (double)(after.idle - before.idle) / (double)dt);
}

/* ------------------------------------------------------------------ */
/* Disk I/O via /sys/class/block/<dev>/stat                           */
/* ------------------------------------------------------------------ */

static const char *
dev_basename(const char *dev)
{
	const char *p = strrchr(dev, '/');
	return p ? p + 1 : dev;
}

/* diskstats field indices (1-based), see Documentation/admin-stat/iostats.rst */
enum {
	STAT_SECTORS_READ    = 3,
	STAT_SECTORS_WRITTEN = 7,
};

static unsigned long long
read_disk_field(const char *dev, int field)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "/sys/class/block/%s/stat",
		 dev_basename(dev));
	FILE *f = fopen(path, "r");
	if (!f)
		return 0;
	char line[512];
	if (!fgets(line, sizeof(line), f)) {
		fclose(f);
		return 0;
	}
	fclose(f);
	char *p = line;
	while (*p && isspace((unsigned char)*p))
		p++;
	for (int i = 1; i < field; i++) {
		while (*p && !isspace((unsigned char)*p))
			p++;
		while (*p && isspace((unsigned char)*p))
			p++;
	}
	return strtoull(p, NULL, 10);
}

/* ------------------------------------------------------------------ */
/* System info                                                        */
/* ------------------------------------------------------------------ */

static unsigned long long
get_available_ram(void)
{
	FILE *f = fopen("/proc/meminfo", "r");
	if (!f)
		return 0;
	unsigned long long kb = 0;
	char line[256];
	while (fgets(line, sizeof(line), f)) {
		if (sscanf(line, "MemAvailable: %llu kB", &kb) == 1)
			break;
	}
	fclose(f);
	return kb * 1024;
}

static void
drop_caches(void)
{
	static int warned;
	int err = 0;
	FILE *f = fopen("/proc/sys/vm/drop_caches", "w");
	if (!f) {
		err = errno;
	} else {
		if (fprintf(f, "3\n") < 0)
			err = errno;
		if (fclose(f) != 0 && err == 0)
			err = errno;
	}
	if (err && !warned) {
		warned = 1;
		msg("warning: cannot drop page cache: %s; "
		    "read results may be inflated\n", strerror(err));
	}
}

/* ------------------------------------------------------------------ */
/* External command execution (fork/exec, no shell)                   */
/* ------------------------------------------------------------------ */

static int
run_cmd(const char *prog, ...)
{
	const char *argv[64];
	argv[0] = prog;
	va_list ap;
	va_start(ap, prog);
	int i = 1;
	const char *arg;
	while ((arg = va_arg(ap, const char *)) != NULL && i < 63)
		argv[i++] = arg;
	argv[i] = NULL;
	va_end(ap);

	pid_t pid = fork();
	if (pid < 0)
		return -1;
	if (pid == 0) {
		if (!g_cfg.verbose) {
			int dn = open("/dev/null", O_WRONLY);
			if (dn >= 0) {
				dup2(dn, STDOUT_FILENO);
				dup2(dn, STDERR_FILENO);
				close(dn);
			}
		}
		execvp(argv[0], (char *const *)argv);
		_exit(127);
	}
	int status;
	while (waitpid(pid, &status, 0) < 0) {
		if (errno != EINTR)
			return -1;
	}
	return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/* ------------------------------------------------------------------ */
/* File copy: copy_file_range with read/write fallback                */
/* ------------------------------------------------------------------ */

static int
copy_single_file(const char *src, const char *dst, mode_t mode)
{
	int sfd = open(src, O_RDONLY);
	if (sfd < 0)
		return -1;
	int dfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, mode);
	if (dfd < 0) {
		close(sfd);
		return -1;
	}

	struct stat st;
	if (fstat(sfd, &st) < 0) {
		close(sfd);
		close(dfd);
		return -1;
	}
	off_t remaining = st.st_size;

	if (remaining == 0)
		goto done;

	off_t off_in = 0, off_out = 0;
	int use_cfr = 1;

	while (remaining > 0) {
		ssize_t n;
		if (use_cfr) {
			size_t chunk = remaining > (off_t)0x7ffff000
			       ? 0x7ffff000
			       : (size_t)remaining;
			n = copy_file_range(sfd, &off_in, dfd, &off_out, chunk, 0);
			if (n < 0 && (errno == EOPNOTSUPP || errno == EXDEV
				   || errno == EINVAL || errno == ENOSYS)) {
				use_cfr = 0;
				if (off_in > 0) {
					lseek(sfd, off_in, SEEK_SET);
					lseek(dfd, off_out, SEEK_SET);
				}
				continue;
			}
			if (n <= 0)
				break;
		} else {
			static char buf[BUF_SIZE];
			size_t toread = remaining > (off_t)sizeof(buf)
					? sizeof(buf)
					: (size_t)remaining;
			n = read(sfd, buf, toread);
			if (n <= 0)
				break;
			char *p = buf;
			ssize_t left = n;
			while (left > 0) {
				ssize_t w = write(dfd, p, (size_t)left);
				if (w <= 0) {
					close(sfd);
					close(dfd);
					return -1;
				}
				p += w;
				left -= w;
			}
		}
		remaining -= n;
	}

done:
	close(sfd);
	close(dfd);
	return remaining == 0 ? 0 : -1;
}

static int
copy_tree(const char *src, const char *dst)
{
	DIR *d = opendir(src);
	if (!d)
		return -1;
	if (mkdir(dst, 0755) < 0 && errno != EEXIST) {
		closedir(d);
		return -1;
	}

	struct dirent *ent;
	while ((ent = readdir(d)) != NULL) {
		if (ent->d_name[0] == '.' &&
		    (ent->d_name[1] == '\0' ||
		     (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
			continue;

		char sp[PATH_MAX], dp[PATH_MAX];
		snprintf(sp, sizeof(sp), "%s/%s", src, ent->d_name);
		snprintf(dp, sizeof(dp), "%s/%s", dst, ent->d_name);

		struct stat st;
		if (lstat(sp, &st) < 0)
			continue;

		int rc = 0;
		if (S_ISDIR(st.st_mode)) {
			rc = copy_tree(sp, dp);
			if (rc == 0)
				chmod(dp, st.st_mode);
		} else if (S_ISREG(st.st_mode)) {
			rc = copy_single_file(sp, dp, st.st_mode);
		} else if (S_ISLNK(st.st_mode)) {
			char target[PATH_MAX];
			ssize_t len = readlink(sp, target, sizeof(target) - 1);
			if (len < 0)
				continue;
			target[len] = '\0';
			unlink(dp);
			if (symlink(target, dp) < 0)
				continue;
		} else {
			continue;
		}

		if (rc < 0) {
			closedir(d);
			return -1;
		}

		struct timespec ts[2] = { st.st_atim, st.st_mtim };
		utimensat(AT_FDCWD, dp, ts, AT_SYMLINK_NOFOLLOW);
	}

	closedir(d);
	return 0;
}

static int
read_tree(const char *path)
{
	static char buf[BUF_SIZE];
	DIR *d = opendir(path);
	if (!d)
		return -1;
	struct dirent *ent;
	while ((ent = readdir(d)) != NULL) {
		if (ent->d_name[0] == '.' &&
		    (ent->d_name[1] == '\0' ||
		     (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
			continue;
		char fp[PATH_MAX];
		snprintf(fp, sizeof(fp), "%s/%s", path, ent->d_name);
		struct stat st;
		if (lstat(fp, &st) < 0)
			continue;
		int rc = 0;
		if (S_ISDIR(st.st_mode))
			rc = read_tree(fp);
		else if (S_ISREG(st.st_mode)) {
			int fd = open(fp, O_RDONLY);
			if (fd < 0)
				continue;
			ssize_t n;
			while ((n = read(fd, buf, sizeof(buf))) != 0) {
				if (n < 0) {
					if (errno == EINTR)
						continue;
					break;
				}
			}
			close(fd);
		}
		if (rc < 0) {
			closedir(d);
			return -1;
		}
	}
	closedir(d);
	return 0;
}

/* ------------------------------------------------------------------ */
/* Directory size (recursive stat, no external tools)                 */
/* ------------------------------------------------------------------ */

static unsigned long long
tree_size(const char *path)
{
	DIR *d = opendir(path);
	if (!d)
		return 0;
	unsigned long long total = 0;
	struct dirent *ent;
	while ((ent = readdir(d)) != NULL) {
		if (ent->d_name[0] == '.' &&
		    (ent->d_name[1] == '\0' ||
		     (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
			continue;
		char fp[PATH_MAX];
		snprintf(fp, sizeof(fp), "%s/%s", path, ent->d_name);
		struct stat st;
		if (lstat(fp, &st) < 0)
			continue;
		if (S_ISDIR(st.st_mode))
			total += tree_size(fp);
		else if (S_ISREG(st.st_mode))
			total += st.st_size;
		else if (S_ISLNK(st.st_mode))
			total += st.st_size;
	}
	closedir(d);
	return total;
}

/* ------------------------------------------------------------------ */
/* Cleanup                                                            */
/* ------------------------------------------------------------------ */

static void
cleanup(void)
{
	if (g_st.dev_mounted) {
		umount(g_st.mnt);
		g_st.dev_mounted = 0;
	}
	if (g_st.tmpfs_mounted) {
		umount(g_st.tmpfs);
		g_st.tmpfs_mounted = 0;
	}
	if (g_st.tmpdir[0]) {
		run_cmd("/bin/rm", "-rf", g_st.tmpdir, NULL);
		g_st.tmpdir[0] = '\0';
	}
}

static void
signal_handler(int sig)
{
	(void)sig;
	g_signaled = 1;
}

/* ------------------------------------------------------------------ */
/* Disk operations                                                    */
/* ------------------------------------------------------------------ */

static unsigned long long
get_disk_size(const char *disk)
{
	int fd = open(disk, O_RDONLY);
	if (fd < 0)
		die("cannot open %s: %s", disk, strerror(errno));
	unsigned long long size;
	if (ioctl(fd, BLKGETSIZE64, &size) < 0) {
		close(fd);
		die("cannot get size of %s: %s", disk, strerror(errno));
	}
	close(fd);
	return size;
}

static void
get_partition_names(const char *disk, char *p1, char *p2, size_t sz)
{
	const char *base = dev_basename(disk);
	int need_p = isdigit((unsigned char)base[strlen(base) - 1]);
	if (need_p) {
		snprintf(p1, sz, "%sp1", disk);
		snprintf(p2, sz, "%sp2", disk);
	} else {
		snprintf(p1, sz, "%s1", disk);
		snprintf(p2, sz, "%s2", disk);
	}
}

static int
wait_for_block_dev(const char *path, int timeout_ms)
{
	for (int ms = 0; ms <= timeout_ms; ms += 100) {
		struct stat st;
		if (stat(path, &st) == 0 && S_ISBLK(st.st_mode))
			return 0;
		usleep(100000);
	}
	return -1;
}

/* Ask the kernel to re-read the partition table after sgdisk rewrote
 * it (BLKRRPART), so the freshly created partitions become usable. */
static int
reread_partition_table(const char *disk)
{
	int fd = open(disk, O_RDONLY);
	if (fd < 0)
		return -1;
	int rc = ioctl(fd, BLKRRPART, 0);
	int saved = errno;
	close(fd);
	errno = saved;
	return rc;
}

static const char *
read_disk_model(const char *device)
{
	static char model[256];
	const char *base = dev_basename(device);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "/sys/block/%s/device/model", base);
	FILE *f = fopen(path, "r");
	if (!f) {
		snprintf(path, sizeof(path),
			 "/sys/block/%s/device/device/model", base);
		f = fopen(path, "r");
	}
	if (!f)
		return NULL;
	if (!fgets(model, sizeof(model), f)) {
		fclose(f);
		return NULL;
	}
	fclose(f);
	size_t len = strlen(model);
	while (len > 0 && isspace((unsigned char)model[len - 1]))
		model[--len] = '\0';
	return len > 0 ? model : NULL;
}

static void
print_disk_info(const char *device)
{
	unsigned long long size = get_disk_size(device);
	const char *model = read_disk_model(device);
	if (model)
		msg("Disk: %s (%s, %s)\n", device, model, bytes_human(size));
	else
		msg("Disk: %s (%s)\n", device, bytes_human(size));
}

/* ------------------------------------------------------------------ */
/* Archive operations                                                 */
/* ------------------------------------------------------------------ */

static int
is_url(const char *s)
{
	return strncmp(s, "http://", 7) == 0
	    || strncmp(s, "https://", 8) == 0;
}

static void
download_archive(const char *url, const char *dest)
{
	status("Downloading archive...\n");
	verbose("Downloading %s -> %s\n", url, dest);
	if (run_cmd("curl", "-L", "-sS", "-o", dest, url, NULL) != 0)
		die("download failed");
	verbose("Download complete.\n");
}

static void
check_integrity(const char *path)
{
	status("Checking archive integrity...\n");
	if (run_cmd("xz", "-t", path, NULL) != 0)
		die("archive integrity check failed");
	verbose("Archive integrity OK.\n");
}

static void
extract_archive(const char *archive, const char *dest)
{
	status("Extracting archive...\n");
	verbose("Extracting %s -> %s\n", archive, dest);
	if (run_cmd("tar", "-xJf", archive, "-C", dest, NULL) != 0)
		die("extraction failed");
	verbose("Extraction complete.\n");
}

static unsigned long long
get_archive_size(const char *path)
{
	struct stat st;
	if (stat(path, &st) < 0)
		return 0;
	return (unsigned long long)st.st_size;
}

/* ------------------------------------------------------------------ */
/* Force variant logic                                                */
/* ------------------------------------------------------------------ */

static const char *
force_mode_name(enum force_mode f)
{
	switch (f) {
	case FORCE_NONE: return "none";
	case FORCE_ZSTD: return "zstd";
	case FORCE_LZO:  return "lzo";
	case FORCE_ALL:  return "all";
	case FORCE_BOTH: return "both";
	}
	return "unknown";
}

static void
get_force_variants(const char *algo, enum force_mode force,
		   int *do_standard, int *do_force)
{
	*do_standard = 0;
	*do_force = 0;

	if (strcmp(algo, "none") == 0) {
		*do_standard = 1;
		return;
	}

	switch (force) {
	case FORCE_NONE:
		*do_standard = 1;
		break;
	case FORCE_ZSTD:
		if (strcmp(algo, "zstd") == 0)
			*do_force = 1;
		else
			*do_standard = 1;
		break;
	case FORCE_LZO:
		if (strcmp(algo, "lzo") == 0)
			*do_force = 1;
		else
			*do_standard = 1;
		break;
	case FORCE_ALL:
		*do_force = 1;
		break;
	case FORCE_BOTH:
		*do_standard = 1;
		*do_force = 1;
		break;
	}
}

/* ------------------------------------------------------------------ */
/* Single test runner                                                 */
/* ------------------------------------------------------------------ */

static int
run_single_test(const char *device, const char *location,
		const char *target, int force_flag,
		unsigned long long data_size,
		struct result *out)
{
	drop_caches();
	if (run_cmd("mkfs.btrfs", "-f", device, NULL) != 0) {
		msg("warning: mkfs.btrfs failed for %s on %s\n", target, device);
		return -1;
	}

	char mntopts[128];
	if (strcmp(target, "none") == 0) {
		mntopts[0] = '\0';
	} else if (force_flag) {
		snprintf(mntopts, sizeof(mntopts), "compress-force=%s", target);
	} else {
		snprintf(mntopts, sizeof(mntopts), "compress=%s", target);
	}

	mkdir(g_st.mnt, 0755);
	unsigned long mflags = 0;
	int mrc;
	if (mntopts[0])
		mrc = mount(device, g_st.mnt, "btrfs", mflags, mntopts);
	else
		mrc = mount(device, g_st.mnt, "btrfs", mflags, "");
	if (mrc < 0) {
		msg("warning: mount failed: %s\n", strerror(errno));
		return -1;
	}
	g_st.dev_mounted = 1;

	verbose("Mounted %s on %s with options '%s'\n", device, g_st.mnt,
		mntopts[0] ? mntopts : "defaults");

	struct statvfs sv_before;
	statvfs(g_st.mnt, &sv_before);
	struct cpu_sample cpu_before = read_cpu();
	unsigned long long disk_sectors_before = read_disk_field(device,
								STAT_SECTORS_WRITTEN);

	struct timespec ts_start, ts_copy, ts_sync;
	clock_gettime(CLOCK_MONOTONIC_RAW, &ts_start);

	if (copy_tree(g_st.tmpfs, g_st.mnt) < 0) {
		msg("warning: copy failed for %s\n", target);
		umount(g_st.mnt);
		g_st.dev_mounted = 0;
		return -1;
	}

	clock_gettime(CLOCK_MONOTONIC_RAW, &ts_copy);

	sync();

	clock_gettime(CLOCK_MONOTONIC_RAW, &ts_sync);

	struct statvfs sv_after;
	statvfs(g_st.mnt, &sv_after);
	struct cpu_sample cpu_after = read_cpu();
	unsigned long long disk_sectors_after = read_disk_field(device,
								STAT_SECTORS_WRITTEN);

	unsigned long long free_before = sv_before.f_bfree * (unsigned long long)sv_before.f_frsize;
	unsigned long long free_after  = sv_after.f_bfree  * (unsigned long long)sv_after.f_frsize;
	unsigned long long compressed_bytes = 0;
	if (free_before > free_after)
		compressed_bytes = free_before - free_after;

	double elapsed      = ts_diff_sec(&ts_start, &ts_copy);
	double elapsed_sync = ts_diff_sec(&ts_start, &ts_sync);
	double ratio = data_size > 0 ? (double)compressed_bytes / (double)data_size : 1.0;
	double sec_red = (ratio >= 1.0) ? INFINITY : elapsed / (1.0 - ratio);

	unsigned long long disk_sectors_delta = 0;
	if (disk_sectors_after > disk_sectors_before)
		disk_sectors_delta = disk_sectors_after - disk_sectors_before;
	unsigned long long disk_bytes = disk_sectors_delta * 512;

	memset(out, 0, sizeof(*out));
	strncpy(out->location, location, sizeof(out->location) - 1);
	strncpy(out->target, target, sizeof(out->target) - 1);
	strncpy(out->force_label, force_flag ? "force" : "standard",
		sizeof(out->force_label) - 1);
	out->elapsed         = elapsed;
	out->elapsed_sync    = elapsed_sync;
	out->rate_sync_mib   = elapsed_sync > 0 ? (double)data_size / elapsed_sync / (1024.0 * 1024.0) : 0;
	out->compressed_bytes = compressed_bytes;
	out->ratio           = ratio;
	out->cpu_pct         = cpu_pct(cpu_before, cpu_after);
	out->sec_per_reduction = sec_red;
	out->disk_bytes      = disk_bytes;
	out->disk_rate_sync_mib = elapsed_sync > 0 ? (double)disk_bytes / elapsed_sync / (1024.0 * 1024.0) : 0;

	umount(g_st.mnt);
	g_st.dev_mounted = 0;

	verbose("Unmounted, remounting for read test...\n");

	/* Drop page cache so the read test measures real disk reads,
	 * not data still cached from the write phase. tmpfs source data
	 * (shmem) is not evicted by this, so subsequent copies stay fast. */
	drop_caches();

	mrc = mntopts[0]
	    ? mount(device, g_st.mnt, "btrfs", mflags, mntopts)
	    : mount(device, g_st.mnt, "btrfs", mflags, "");
	if (mrc < 0) {
		msg("warning: remount failed for read test: %s\n",
		    strerror(errno));
		return -1;
	}
	g_st.dev_mounted = 1;

	{
		struct cpu_sample rd_cpu_before = read_cpu();
		unsigned long long rd_sec_before = read_disk_field(device,
								   STAT_SECTORS_READ);
		struct timespec ts_rd_start, ts_rd_end;
		clock_gettime(CLOCK_MONOTONIC_RAW, &ts_rd_start);
		int rdrc = read_tree(g_st.mnt);
		clock_gettime(CLOCK_MONOTONIC_RAW, &ts_rd_end);
		if (rdrc == 0) {
			struct cpu_sample rd_cpu_after = read_cpu();
			unsigned long long rd_sec_after = read_disk_field(device,
									  STAT_SECTORS_READ);
			double rd_elapsed = ts_diff_sec(&ts_rd_start, &ts_rd_end);
			out->read_rate_mib = rd_elapsed > 0
				? (double)data_size / rd_elapsed
				  / (1024.0 * 1024.0)
				: 0;
			out->read_cpu_pct = cpu_pct(rd_cpu_before, rd_cpu_after);
			unsigned long long rd_sec_delta = 0;
			if (rd_sec_after > rd_sec_before)
				rd_sec_delta = rd_sec_after - rd_sec_before;
			out->read_disk_bytes = rd_sec_delta * 512;
			out->read_disk_rate_mib = rd_elapsed > 0
				? (double)out->read_disk_bytes / rd_elapsed
				  / (1024.0 * 1024.0)
				: 0;
		}
	}

	umount(g_st.mnt);
	g_st.dev_mounted = 0;

	return 0;
}

/* ------------------------------------------------------------------ */
/* Test count                                                         */
/* ------------------------------------------------------------------ */

/* Simple algorithms tested once each (no level parameter). */
static const char *const g_simple_algos[] = { "lzo", "zlib" };
#define N_SIMPLE_ALGOS ((int)(sizeof(g_simple_algos) / sizeof(g_simple_algos[0])))

static int
total_test_count(const struct config *cfg)
{
	int zstd_n = cfg->zstd_max - cfg->zstd_min + 1;
	int simple = N_SIMPLE_ALGOS;
	int per_loc;
	if (cfg->force == FORCE_BOTH)
		per_loc = 2 * zstd_n + 2 * simple + 1;
	else
		per_loc = zstd_n + simple + 1;
	int locations = cfg->hdd_percent > 0 ? 2 : 1;
	return per_loc * locations * cfg->repeat;
}

/* ------------------------------------------------------------------ */
/* Main test loop                                                     */
/* ------------------------------------------------------------------ */

/* Run all force-variants and repeats for one target. Every attempted
 * test advances progress (for ETA accuracy); only successes are stored. */
static void
run_target(const struct config *cfg, const char *device, const char *location,
	   const char *algo, const char *target,
	   int *test_num, int *tests_done, int total,
	   const struct timespec *overall_start)
{
	int do_std, do_frc;
	get_force_variants(algo, cfg->force, &do_std, &do_frc);

	for (int v = 0; v < 2; v++) {
		if (v == 0 && !do_std)
			continue;
		if (v == 1 && !do_frc)
			continue;
		for (int r = 0; r < cfg->repeat; r++) {
			if (g_signaled)
				return;
			(*test_num)++;
			char eta_buf[128];
			format_eta(overall_start, *tests_done, total,
				   eta_buf, sizeof(eta_buf));
			status("Test %d/%d: %s %s at %s (repeat %d/%d)%s\n",
			       *test_num, total, target, v ? "force" : "standard",
			       location, r + 1, cfg->repeat, eta_buf);
			struct result *res = &g_results[g_nresults];
			if (run_single_test(device, location, target, v,
					    g_st.data_size, res) == 0)
				g_nresults++;
			(*tests_done)++;
			if (g_nresults >= MAX_RESULTS)
				return;
		}
	}
}

static void
run_all_tests(const struct config *cfg)
{
	const char *locations[2] = {"begin", "end"};
	const char *devices[2];
	int nlocations;

	devices[0] = g_st.part1;
	devices[1] = g_st.part2;
	nlocations = cfg->hdd_percent > 0 ? 2 : 1;

	int total = total_test_count(cfg);
	int test_num = 0;
	int tests_done = 0;
	struct timespec overall_start;
	clock_gettime(CLOCK_MONOTONIC_RAW, &overall_start);
	g_nresults = 0;

	for (int loc = 0; loc < nlocations; loc++) {
		for (int lvl = cfg->zstd_max; lvl >= cfg->zstd_min; lvl--) {
			char target[32];
			snprintf(target, sizeof(target), "zstd:%d", lvl);
			run_target(cfg, devices[loc], locations[loc],
				   "zstd", target, &test_num, &tests_done,
				   total, &overall_start);
			if (g_signaled || g_nresults >= MAX_RESULTS) {
				status("\n");
				return;
			}
		}

		for (int a = 0; a < N_SIMPLE_ALGOS; a++) {
			run_target(cfg, devices[loc], locations[loc],
				   g_simple_algos[a], g_simple_algos[a],
				   &test_num, &tests_done, total, &overall_start);
			if (g_signaled || g_nresults >= MAX_RESULTS) {
				status("\n");
				return;
			}
		}

		run_target(cfg, devices[loc], locations[loc],
			   "none", "none", &test_num, &tests_done,
			   total, &overall_start);
		if (g_signaled || g_nresults >= MAX_RESULTS) {
			status("\n");
			return;
		}
	}

	status("\n");
}

/* ------------------------------------------------------------------ */
/* Results: grouping helpers                                          */
/* ------------------------------------------------------------------ */

static int
same_key(const struct result *a, const struct result *b)
{
	return strcmp(a->location, b->location) == 0
	    && strcmp(a->target, b->target) == 0
	    && strcmp(a->force_label, b->force_label) == 0;
}

struct avg {
	char location[16];
	char target[32];
	char force_label[16];
	double rate_sync_mib;
	double disk_rate_sync_mib;
	double ratio;
	double cpu_pct;
	double sec_per_reduction;
	double elapsed_sync;
	unsigned long long compressed_bytes;
	unsigned long long disk_bytes;
	int count;
	double read_rate_mib;
	double read_cpu_pct;
	unsigned long long read_disk_bytes;
	double read_disk_rate_mib;
	double rate_sync_stdev;
	double read_rate_stdev;
};

static int
compute_averages(struct avg *avgs, int max_avgs)
{
	int navg = 0;
	int processed[MAX_RESULTS] = {0};

	for (int i = 0; i < g_nresults && navg < max_avgs; i++) {
		if (processed[i])
			continue;

		struct avg *a = &avgs[navg];
		memset(a, 0, sizeof(*a));
		memcpy(a->location, g_results[i].location, sizeof(a->location));
		memcpy(a->target, g_results[i].target, sizeof(a->target));
		memcpy(a->force_label, g_results[i].force_label, sizeof(a->force_label));
		a->location[sizeof(a->location) - 1] = '\0';
		a->target[sizeof(a->target) - 1] = '\0';
		a->force_label[sizeof(a->force_label) - 1] = '\0';

		int count = 0;
		double sum_rate_sync = 0;
		double sum_disk_rate_sync = 0;
		double sum_ratio = 0, sum_cpu = 0, sum_sec_red = 0;
		double sum_read_rate = 0, sum_read_cpu = 0, sum_read_disk_rate = 0;
		unsigned long long sum_read_disk = 0;
		double sum_elapsed_sync = 0;
		unsigned long long sum_compressed = 0, sum_disk = 0;
		double sq_rate_sync = 0, sq_read_rate = 0;
		int sec_red_count = 0;

		for (int j = i; j < g_nresults; j++) {
			if (processed[j])
				continue;
			if (!same_key(&g_results[i], &g_results[j]))
				continue;
			processed[j] = 1;
			count++;
			sum_rate_sync	 += g_results[j].rate_sync_mib;
			sum_disk_rate_sync += g_results[j].disk_rate_sync_mib;
			sum_ratio	 += g_results[j].ratio;
			sum_cpu		 += g_results[j].cpu_pct;
			sum_compressed	 += g_results[j].compressed_bytes;
			sum_disk	 += g_results[j].disk_bytes;
			sum_elapsed_sync += g_results[j].elapsed_sync;
			sum_read_rate     += g_results[j].read_rate_mib;
			sum_read_cpu      += g_results[j].read_cpu_pct;
			sum_read_disk     += g_results[j].read_disk_bytes;
			sum_read_disk_rate += g_results[j].read_disk_rate_mib;
			sq_rate_sync      += g_results[j].rate_sync_mib
					   * g_results[j].rate_sync_mib;
			sq_read_rate      += g_results[j].read_rate_mib
					   * g_results[j].read_rate_mib;
			if (isfinite(g_results[j].sec_per_reduction)) {
				sum_sec_red += g_results[j].sec_per_reduction;
				sec_red_count++;
			}
		}

		a->rate_sync_mib	= sum_rate_sync / count;
		a->disk_rate_sync_mib	= sum_disk_rate_sync / count;
		a->ratio		= sum_ratio / count;
		a->cpu_pct		= sum_cpu / count;
		a->compressed_bytes	= sum_compressed / count;
		a->disk_bytes		= sum_disk / count;
		a->elapsed_sync		= sum_elapsed_sync / count;
		a->sec_per_reduction	= sec_red_count > 0
					  ? sum_sec_red / sec_red_count
					  : INFINITY;
		a->read_rate_mib      = sum_read_rate / count;
		a->read_cpu_pct       = sum_read_cpu / count;
		a->read_disk_bytes    = sum_read_disk / count;
		a->read_disk_rate_mib = sum_read_disk_rate / count;
		/* population standard deviation; 0 when count == 1 */
		double var_wr = sq_rate_sync / count - a->rate_sync_mib * a->rate_sync_mib;
		double var_rd = sq_read_rate / count - a->read_rate_mib * a->read_rate_mib;
		a->rate_sync_stdev = var_wr > 0 ? sqrt(var_wr) : 0;
		a->read_rate_stdev = var_rd > 0 ? sqrt(var_rd) : 0;
		a->count = count;
		navg++;
	}
	return navg;
}

/* ------------------------------------------------------------------ */
/* Results: printing                                                  */
/* ------------------------------------------------------------------ */

static int
col_width(const char *header, const char *val)
{
	int hw = (int)strlen(header);
	int vw = (int)strlen(val);
	return hw > vw ? hw : vw;
}

static void
print_results_table(struct avg *avgs, int navg)
{
	if (navg == 0) {
		msg("No results to display.\n");
		return;
	}

	int wl = 0, wt = 0, wws = 0, wrr = 0, wwd = 0, wrd = 0, wra = 0, wwc = 0, wrc = 0, wsr = 0, wf = 0, wn = 0;

	for (int i = 0; i < navg; i++) {
		char tmp[64];
		int v;
		v = col_width("Location", avgs[i].location);	if (v > wl)  wl  = v;
		v = col_width("Target", avgs[i].target);	if (v > wt)  wt  = v;
		snprintf(tmp, sizeof(tmp), "%.1f", avgs[i].rate_sync_mib);
		v = col_width("WrMiB/s", tmp);			if (v > wws) wws = v;
		snprintf(tmp, sizeof(tmp), "%.1f", avgs[i].read_rate_mib);
		v = col_width("RdMiB/s", tmp);			if (v > wrr) wrr = v;
		snprintf(tmp, sizeof(tmp), "%.1f", avgs[i].disk_rate_sync_mib);
		v = col_width("WrDisk*", tmp);			if (v > wwd) wwd = v;
		snprintf(tmp, sizeof(tmp), "%.1f", avgs[i].read_disk_rate_mib);
		v = col_width("RdDisk*", tmp);			if (v > wrd) wrd = v;
		snprintf(tmp, sizeof(tmp), "%.4f", avgs[i].ratio);
		v = col_width("Ratio", tmp);			if (v > wra) wra = v;
		snprintf(tmp, sizeof(tmp), "%.1f", avgs[i].cpu_pct);
		v = col_width("WrCPU%", tmp);			if (v > wwc) wwc = v;
		snprintf(tmp, sizeof(tmp), "%.1f", avgs[i].read_cpu_pct);
		v = col_width("RdCPU%", tmp);			if (v > wrc) wrc = v;
		if (isfinite(avgs[i].sec_per_reduction))
			snprintf(tmp, sizeof(tmp), "%.3f", avgs[i].sec_per_reduction);
		else
			snprintf(tmp, sizeof(tmp), "inf");
		v = col_width("Sec/Red", tmp);			if (v > wsr) wsr = v;
		v = col_width("Force", avgs[i].force_label);	if (v > wf)  wf  = v;
		snprintf(tmp, sizeof(tmp), "%d", avgs[i].count);
		v = col_width("n", tmp);			if (v > wn)  wn  = v;
	}

	if (g_cfg.repeat > 1)
		msg("All results averaged over %d runs.\n\n", g_cfg.repeat);

	msg("Benchmark Results:\n");
	msg("%-*s  %-*s  %*s  %*s  %*s  %*s  %*s  %*s  %*s  %*s  %-*s  %*s\n",
	    wl, "Location", wt, "Target",
	    wws, "WrMiB/s", wrr, "RdMiB/s",
	    wwd, "WrDisk*", wrd, "RdDisk*", wra, "Ratio",
	    wwc, "WrCPU%", wrc, "RdCPU%", wsr, "Sec/Red", wf, "Force", wn, "n");

	/* 12 columns joined by two spaces */
	int sep = wl + wt + wws + wrr + wwd + wrd + wra + wwc + wrc + wsr + wf + wn
		+ 2 * 11;
	for (int i = 0; i < sep; i++)
		msg("-");
	msg("\n");

	for (int i = 0; i < navg; i++) {
		char sr[16];
		if (isfinite(avgs[i].sec_per_reduction))
			snprintf(sr, sizeof(sr), "%.3f", avgs[i].sec_per_reduction);
		else
			snprintf(sr, sizeof(sr), "inf");

		msg("%-*s  %-*s  %*.1f  %*.1f  %*.1f  %*.1f  %*.*f  %*.1f  %*.1f  %*s  %-*s  %*d\n",
		    wl, avgs[i].location, wt, avgs[i].target,
		    wws, avgs[i].rate_sync_mib,
		    wrr, avgs[i].read_rate_mib,
		    wwd, avgs[i].disk_rate_sync_mib,
		    wrd, avgs[i].read_disk_rate_mib,
		    wra, 4, avgs[i].ratio, wwc, avgs[i].cpu_pct,
		    wrc, avgs[i].read_cpu_pct,
		    wsr, sr, wf, avgs[i].force_label, wn, avgs[i].count);
	}
	msg("\n");
}

static int
append_interpolated(struct avg *avgs, int navg, int max_avgs)
{
	if (g_cfg.hdd_percent <= 0)
		return navg;

	int added = 0;

	for (int i = 0; i < navg && (navg + added) < max_avgs; i++) {
		if (strcmp(avgs[i].location, "begin") != 0)
			continue;

		int found = -1;
		for (int j = 0; j < navg; j++) {
			if (strcmp(avgs[j].location, "end") != 0)
				continue;
			if (strcmp(avgs[j].target, avgs[i].target) != 0)
				continue;
			if (strcmp(avgs[j].force_label, avgs[i].force_label) != 0)
				continue;
			found = j;
			break;
		}
		if (found < 0)
			continue;

		struct avg *a = &avgs[navg + added];
		memset(a, 0, sizeof(*a));
		strncpy(a->location, "interpolated", sizeof(a->location) - 1);
		strncpy(a->target, avgs[i].target, sizeof(a->target) - 1);
		strncpy(a->force_label, avgs[i].force_label, sizeof(a->force_label) - 1);
		a->rate_sync_mib      = (avgs[i].rate_sync_mib      + avgs[found].rate_sync_mib)      / 2;
		a->disk_rate_sync_mib = (avgs[i].disk_rate_sync_mib  + avgs[found].disk_rate_sync_mib) / 2;
		a->ratio              = (avgs[i].ratio               + avgs[found].ratio)               / 2;
		a->cpu_pct            = (avgs[i].cpu_pct             + avgs[found].cpu_pct)             / 2;
		a->compressed_bytes   = (avgs[i].compressed_bytes    + avgs[found].compressed_bytes)    / 2;
		a->disk_bytes         = (avgs[i].disk_bytes          + avgs[found].disk_bytes)          / 2;
		a->elapsed_sync       = (avgs[i].elapsed_sync        + avgs[found].elapsed_sync)        / 2;
		a->read_rate_mib      = (avgs[i].read_rate_mib       + avgs[found].read_rate_mib)       / 2;
		a->read_cpu_pct       = (avgs[i].read_cpu_pct        + avgs[found].read_cpu_pct)        / 2;
		a->read_disk_bytes    = (avgs[i].read_disk_bytes     + avgs[found].read_disk_bytes)     / 2;
		a->read_disk_rate_mib = (avgs[i].read_disk_rate_mib  + avgs[found].read_disk_rate_mib)  / 2;
		double sr1 = avgs[i].sec_per_reduction;
		double sr2 = avgs[found].sec_per_reduction;
		if (isfinite(sr1) && isfinite(sr2))
			a->sec_per_reduction = (sr1 + sr2) / 2;
		else
			a->sec_per_reduction = INFINITY;
		a->count = avgs[i].count + avgs[found].count;
		added++;
	}

	return navg + added;
}

static void
print_interpolated(struct avg *avgs, int navg)
{
	if (g_cfg.hdd_percent <= 0)
		return;

	int ninterp = 0;
	for (int i = 0; i < navg; i++) {
		if (strcmp(avgs[i].location, "interpolated") == 0)
			ninterp++;
	}
	if (ninterp > 0) {
		msg("\nInterpolated Results (average across disk):\n");
		print_results_table(avgs + (navg - ninterp), ninterp);
	}
}

static void
print_best(struct avg *avgs, int navg)
{
	if (navg == 0)
		return;

	msg("\nBest Results:\n");

	const char *locs[3] = {"begin", "end", "interpolated"};
	int nlocs = g_cfg.hdd_percent > 0 ? 3 : 1;

	for (int li = 0; li < nlocs; li++) {
		static struct avg subset[MAX_RESULTS];
		int nsub = 0;
		for (int i = 0; i < navg; i++) {
			if (strcmp(avgs[i].location, locs[li]) == 0)
				subset[nsub++] = avgs[i];
		}
		if (nsub == 0)
			continue;

		int fi = 0, ci = 0, bi = 0, ri = 0;
		for (int i = 1; i < nsub; i++) {
			if (subset[i].rate_sync_mib > subset[fi].rate_sync_mib)
				fi = i;
			if (subset[i].read_rate_mib > subset[ri].read_rate_mib)
				ri = i;
			if (subset[i].ratio < subset[ci].ratio)
				ci = i;
			double sr_i = isfinite(subset[i].sec_per_reduction)
				      ? subset[i].sec_per_reduction : 1e18;
			double sr_b = isfinite(subset[bi].sec_per_reduction)
				      ? subset[bi].sec_per_reduction : 1e18;
			if (sr_i < sr_b)
				bi = i;
		}

		const char *loc_label = locs[li];
		msg("  %s:\n", loc_label);
		msg("    Fastest write:   %s (%.1f MiB/s, %.1f%% CPU, %s)\n",
		    subset[fi].target, subset[fi].rate_sync_mib,
		    subset[fi].cpu_pct, subset[fi].force_label);
		msg("    Fastest read:    %s (%.1f MiB/s, %.1f%% CPU, %s)\n",
		    subset[ri].target, subset[ri].read_rate_mib,
		    subset[ri].read_cpu_pct, subset[ri].force_label);
		msg("    Best compressed: %s (ratio %.4f, %.1f%% CPU, %s)\n",
		    subset[ci].target, subset[ci].ratio,
		    subset[ci].cpu_pct, subset[ci].force_label);
		char sr_buf[32];
		if (isfinite(subset[bi].sec_per_reduction))
			snprintf(sr_buf, sizeof(sr_buf), "%.3f",
				 subset[bi].sec_per_reduction);
		else
			snprintf(sr_buf, sizeof(sr_buf), "inf");
		msg("    Best balanced:   %s (sec/red %s, %.1f%% CPU, %s)\n",
		    subset[bi].target, sr_buf,
		    subset[bi].cpu_pct, subset[bi].force_label);
	}
}

static void
print_comparison(struct avg *avgs, int navg)
{
	if (g_cfg.force != FORCE_BOTH)
		return;

	msg("\nForce vs Standard Comparison (percentage differences):\n");
	msg("%-10s %-10s %10s %10s %10s %10s %10s %10s %10s\n",
	    "Location", "Target", "Time%", "WrRate%", "RdRate%",
	    "Compr%", "Ratio%", "WrCPU%", "RdCPU%");
	msg("--------------------------------------------------------------------------------------------------\n");

	for (int i = 0; i < navg; i++) {
		if (strcmp(avgs[i].force_label, "standard") != 0)
			continue;
		if (strcmp(avgs[i].target, "none") == 0)
			continue;

		for (int j = 0; j < navg; j++) {
			if (strcmp(avgs[j].force_label, "force") != 0)
				continue;
			if (strcmp(avgs[j].location, avgs[i].location) != 0)
				continue;
			if (strcmp(avgs[j].target, avgs[i].target) != 0)
				continue;

			double std_rate = avgs[i].rate_sync_mib;
			double frc_rate = avgs[j].rate_sync_mib;
			double rate_diff = std_rate != 0 ? (frc_rate - std_rate) / std_rate * 100.0 : 0;

			double std_rd_rate = avgs[i].read_rate_mib;
			double frc_rd_rate = avgs[j].read_rate_mib;
			double rd_rate_diff = std_rd_rate != 0
				? (frc_rd_rate - std_rd_rate) / std_rd_rate * 100.0
				: 0;

			double std_time = avgs[i].elapsed_sync;
			double frc_time = avgs[j].elapsed_sync;
			double time_diff = std_time != 0 ? (frc_time - std_time) / std_time * 100.0 : 0;

			double std_comp = (double)avgs[i].compressed_bytes;
			double frc_comp = (double)avgs[j].compressed_bytes;
			double comp_diff = std_comp != 0 ? (frc_comp - std_comp) / std_comp * 100.0 : 0;

			double std_ratio = avgs[i].ratio;
			double frc_ratio = avgs[j].ratio;
			double ratio_diff = std_ratio != 0 ? (frc_ratio - std_ratio) / std_ratio * 100.0 : 0;

			double std_cpu = avgs[i].cpu_pct;
			double frc_cpu = avgs[j].cpu_pct;
			double cpu_diff = std_cpu != 0 ? (frc_cpu - std_cpu) / std_cpu * 100.0 : 0;

			double std_rd_cpu = avgs[i].read_cpu_pct;
			double frc_rd_cpu = avgs[j].read_cpu_pct;
			double rd_cpu_diff = std_rd_cpu != 0
				? (frc_rd_cpu - std_rd_cpu) / std_rd_cpu * 100.0
				: 0;

			msg("%-10s %-10s %+9.1f%% %+9.1f%% %+9.1f%% %+9.1f%% %+9.1f%% %+9.1f%% %+9.1f%%\n",
			    avgs[i].location, avgs[i].target,
			    time_diff, rate_diff, rd_rate_diff,
			    comp_diff, ratio_diff, cpu_diff, rd_cpu_diff);
			break;
		}
	}
	msg("\n");
}

/* ------------------------------------------------------------------ */
/* CSV export                                                         */
/* ------------------------------------------------------------------ */

static void
export_csv(struct avg *avgs, int navg)
{
	if (!g_cfg.output_csv)
		return;

	FILE *f = fopen(g_cfg.output_csv, "w");
	if (!f) {
		msg("warning: cannot create %s: %s\n", g_cfg.output_csv,
		    strerror(errno));
		return;
	}

	fprintf(f, "Location,Target,WrMiB/s,RdMiB/s,WrDisk_MiB/s,"
		   "RdDisk_MiB/s,Compressed,Ratio,WrCPU%%,RdCPU%%,Sec/Red,"
		   "Force,n,WrStddev,RdStddev\n");

	for (int i = 0; i < navg; i++) {
		char sr[32];
		if (isfinite(avgs[i].sec_per_reduction))
			snprintf(sr, sizeof(sr), "%.4f", avgs[i].sec_per_reduction);
		else
			snprintf(sr, sizeof(sr), "inf");
		fprintf(f, "%s,%s,%.1f,%.1f,%.1f,%.1f,%llu,%.4f,%.1f,%.1f,%s,%s,%d,%.2f,%.2f\n",
			avgs[i].location, avgs[i].target,
			avgs[i].rate_sync_mib,
			avgs[i].read_rate_mib,
			avgs[i].disk_rate_sync_mib,
			avgs[i].read_disk_rate_mib,
			avgs[i].compressed_bytes, avgs[i].ratio,
			avgs[i].cpu_pct, avgs[i].read_cpu_pct,
			sr,
			avgs[i].force_label, avgs[i].count,
			avgs[i].rate_sync_stdev, avgs[i].read_rate_stdev);
	}

	fprintf(f, "\n# compbench " VERSION "\n");
	fprintf(f, "# Archive: %s\n", g_cfg.archive);
	fprintf(f, "# Dataset: %llu bytes\n", g_st.data_size);
	fprintf(f, "# Archive size: %llu bytes\n", g_st.archive_size);
	fprintf(f, "# Repeats: %d\n", g_cfg.repeat);
	fprintf(f, "# zstd: %d-%d\n", g_cfg.zstd_min, g_cfg.zstd_max);
	fprintf(f, "# Force: %s\n", force_mode_name(g_cfg.force));
	{
		struct utsname uts;
		if (uname(&uts) == 0) {
			fprintf(f, "# Host: %s\n", uts.nodename);
			fprintf(f, "# Kernel: %s\n", uts.release);
		}
		time_t now = time(NULL);
		fprintf(f, "# Date: %s", ctime(&now));
	}

	fclose(f);
	msg("Results saved to %s\n", g_cfg.output_csv);
}

/* ------------------------------------------------------------------ */
/* Argument parsing                                                   */
/* ------------------------------------------------------------------ */

static enum force_mode
parse_force(const char *s)
{
	if (strcmp(s, "none") == 0) return FORCE_NONE;
	if (strcmp(s, "zstd") == 0) return FORCE_ZSTD;
	if (strcmp(s, "lzo") == 0)  return FORCE_LZO;
	if (strcmp(s, "all") == 0)  return FORCE_ALL;
	if (strcmp(s, "both") == 0) return FORCE_BOTH;
	die("invalid force mode: %s (none, zstd, lzo, all, both)", s);
	return FORCE_NONE;
}

static int
parse_int_opt(const char *s, const char *opt)
{
	char *end;
	errno = 0;
	long v = strtol(s, &end, 10);
	if (errno != 0 || end == s || *end != '\0'
	    || v < INT_MIN || v > INT_MAX)
		die("invalid integer for --%s: '%s'", opt, s);
	return (int)v;
}

static void
parse_zstd(const char *s, int *min, int *max)
{
	char *end;
	errno = 0;
	long lo = strtol(s, &end, 10);
	if (errno != 0 || end == s)
		die("invalid zstd range: %s (-15..-1 or 1..15)", s);

	long hi;
	if (*end == '\0') {
		hi = lo;
	} else if (*end == '-') {
		char *end2;
		errno = 0;
		hi = strtol(end + 1, &end2, 10);
		if (errno != 0 || end2 == end + 1 || *end2 != '\0')
			die("invalid zstd range: %s (-15..-1 or 1..15)", s);
	} else {
		die("invalid zstd range: %s (-15..-1 or 1..15)", s);
	}
	if (lo < -15 || hi > 15 || lo > hi || (lo <= 0 && hi >= 0))
		die("invalid zstd range: %s (-15..-1 or 1..15)", s);
	*min = (int)lo;
	*max = (int)hi;
}

static void
usage(FILE *out)
{
	fprintf(out,
"Usage: compbench [OPTIONS] DEVICE ARCHIVE\n"
"\n"
"Btrfs transparent compression benchmark.\n"
"Tests zstd (configurable levels), lzo, zlib, and no compression.\n"
"DEVICE is the block device to test (e.g. /dev/sdX).\n"
"ARCHIVE is a local path or URL to a .tar.xz archive.\n"
"\n"
"WARNING: All data on DEVICE will be erased.\n"
"\n"
"Options:\n"
"  -n, --repeat N          Repeat each test N times (default: 1)\n"
"      --zstd MIN-MAX      zstd level range (-15..-1 or 1..15, default: 1-15)\n"
"  -f, --force MODE        Force mode: none|zstd|lzo|all|both (default: none)\n"
"      --hdd PERCENT       Test on first/last N%% of disk (1-50)\n"
"  -o, --output FILE       Export results to CSV\n"
"      --no-integrity-check\n"
"                         Skip archive integrity verification\n"
"  -v, --verbose           Verbose output\n"
"  -h, --help              Show this help\n"
"  -V, --version           Show version\n"
"\n"
"Measured metrics (all via kernel interfaces, zero text parsing):\n"
"  WrMiB/s  Synced write throughput (copy_file_range + sync)\n"
"  RdMiB/s  Read throughput after cache clear (unmount/remount + read)\n"
"  WrDisk*  Actual bytes written to physical media (/sys/block)\n"
"  RdDisk*  Actual bytes read from physical media (/sys/block)\n"
"  Ratio    Space consumed vs uncompressed data (statvfs)\n"
"  WrCPU%%   System CPU usage during write (/proc/stat)\n"
"  RdCPU%%   System CPU usage during read (/proc/stat)\n"
"  Sec/Red  Seconds per 1%% space reduction (speed/compression balance)\n");
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

int
main(int argc, char **argv)
{
	memset(&g_st, 0, sizeof(g_st));

	static struct option long_opts[] = {
		{"repeat",              required_argument, NULL, 'n'},
		{"zstd",                required_argument, NULL, 'z'},
		{"force",               required_argument, NULL, 'f'},
		{"hdd",                 required_argument, NULL, 'H'},
		{"output",              required_argument, NULL, 'o'},
		{"no-integrity-check",  no_argument,       NULL, 'I'},
		{"verbose",             no_argument,       NULL, 'v'},
		{"help",                no_argument,       NULL, 'h'},
		{"version",             no_argument,       NULL, 'V'},
		{NULL, 0, NULL, 0}
	};

	int c;
	while ((c = getopt_long(argc, argv, "n:f:o:vVh", long_opts, NULL)) != -1) {
		switch (c) {
		case 'n':
			g_cfg.repeat = parse_int_opt(optarg, "repeat");
			if (g_cfg.repeat < 1)
				die("repeat must be >= 1");
			break;
		case 'z':
			parse_zstd(optarg, &g_cfg.zstd_min, &g_cfg.zstd_max);
			break;
		case 'f':
			g_cfg.force = parse_force(optarg);
			break;
		case 'H':
			g_cfg.hdd_percent = parse_int_opt(optarg, "hdd");
			if (g_cfg.hdd_percent < 1 || g_cfg.hdd_percent > 50)
				die("hdd percentage must be 1-50");
			break;
		case 'o':
			g_cfg.output_csv = optarg;
			break;
		case 'I':
			g_cfg.integrity_check = 0;
			break;
		case 'v':
			g_cfg.verbose = 1;
			break;
		case 'V':
			msg("compbench " VERSION "\n");
			return 0;
		case 'h':
			usage(stdout);
			return 0;
		default:
			usage(stderr);
			return 1;
		}
	}

	if (argc - optind != 2) {
		fprintf(stderr, "error: expected DEVICE and ARCHIVE arguments\n\n");
		usage(stderr);
		return 1;
	}

	g_cfg.device  = argv[optind];
	g_cfg.archive = argv[optind + 1];

	if (geteuid() != 0)
		die("this tool must be run as root");

	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = signal_handler;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

	msg("WARNING: All data on %s will be erased!\n", g_cfg.device);
	msg("Do you confirm? (yes/no): ");
	fflush(stdout);
	char confirm[16];
	if (!fgets(confirm, sizeof(confirm), stdin)
	    || strcmp(confirm, "yes\n") != 0) {
		msg("Aborted.\n");
		return 0;
	}

	print_disk_info(g_cfg.device);

	snprintf(g_st.tmpdir, sizeof(g_st.tmpdir),
		 "/tmp/compbench.%d", getpid());
	if (mkdir(g_st.tmpdir, 0700) < 0)
		die("cannot create temp dir: %s", strerror(errno));

	snprintf(g_st.mnt, sizeof(g_st.mnt), "%s/mnt", g_st.tmpdir);
	mkdir(g_st.mnt, 0755);

	snprintf(g_st.archive_path, sizeof(g_st.archive_path),
		 "%s/archive.tar.xz", g_st.tmpdir);

	if (is_url(g_cfg.archive))
		download_archive(g_cfg.archive, g_st.archive_path);
	else {
		char resolved[PATH_MAX];
		if (!realpath(g_cfg.archive, resolved))
			die("cannot access archive: %s", strerror(errno));
		snprintf(g_st.archive_path, sizeof(g_st.archive_path), "%s", resolved);
	}

	g_st.archive_size = get_archive_size(g_st.archive_path);
	if (g_st.archive_size == 0)
		die("archive is empty or missing");

	if (g_cfg.integrity_check)
		check_integrity(g_st.archive_path);

	unsigned long long available = get_available_ram();
	unsigned long long estimated = g_st.archive_size * 5;
	verbose("Archive size: %s (%llu bytes)\n",
		bytes_human(g_st.archive_size), g_st.archive_size);
	verbose("Available RAM: %s (%llu bytes)\n",
		bytes_human(available), available);
	if (available < estimated) {
		msg("Warning: available RAM (%s) may be insufficient for this benchmark.\n",
		    bytes_human(available));
		msg("Continue anyway? (y/n): ");
		fflush(stdout);
		char answer[8];
		if (!fgets(answer, sizeof(answer), stdin)
		    || (answer[0] != 'y' && answer[0] != 'Y'))
			die("aborted");
	}

	int total = total_test_count(&g_cfg);
	msg("\nThis benchmark will run %d tests on %s.\n",
	    total, g_cfg.device);
	msg("Continue? (y/n): ");
	fflush(stdout);
	char answer[8];
	if (!fgets(answer, sizeof(answer), stdin)
	    || (answer[0] != 'y' && answer[0] != 'Y'))
		die("aborted");

	if (g_cfg.hdd_percent > 0) {
		unsigned long long disk_size = get_disk_size(g_cfg.device);
		unsigned long long part_bytes = disk_size * (unsigned long long)g_cfg.hdd_percent / 100;
		char part_str[32];
		snprintf(part_str, sizeof(part_str), "%lluM", part_bytes >> 20);
		verbose("Disk size: %s, partition size: %s each\n",
			bytes_human(disk_size), part_str);
		status("Creating partitions...\n");

		get_partition_names(g_cfg.device, g_st.part1, g_st.part2,
				    sizeof(g_st.part1));

		if (run_cmd("sgdisk", "--zap-all", g_cfg.device, NULL) != 0) {
			msg("warning: sgdisk --zap-all failed\n");
		}
		char start_arg[64], end_arg[64];
		snprintf(start_arg, sizeof(start_arg), "-n1:2048:+%s", part_str);
		snprintf(end_arg, sizeof(end_arg), "-n2:-%s:0", part_str);
		if (run_cmd("sgdisk", start_arg, g_cfg.device, NULL) != 0)
			die("failed to create first partition");
		if (run_cmd("sgdisk", end_arg, g_cfg.device, NULL) != 0)
			die("failed to create second partition");
		if (reread_partition_table(g_cfg.device) < 0)
			msg("warning: cannot reread partition table: %s\n",
			    strerror(errno));
		if (wait_for_block_dev(g_st.part1, 5000) < 0 ||
		    wait_for_block_dev(g_st.part2, 5000) < 0)
			die("partitions did not appear: %s, %s",
			    g_st.part1, g_st.part2);
		g_st.partitions_created = 1;
		verbose("Partitions: %s (begin) and %s (end)\n",
			g_st.part1, g_st.part2);
	} else {
		snprintf(g_st.part1, sizeof(g_st.part1), "%s", g_cfg.device);
	}

	snprintf(g_st.tmpfs, sizeof(g_st.tmpfs), "%s/tmpfs", g_st.tmpdir);
	mkdir(g_st.tmpfs, 0755);

	unsigned long long tmpfs_size = available * 80 / 100;
	char tmpfs_opts[64];
	snprintf(tmpfs_opts, sizeof(tmpfs_opts),
		 "size=%lluk", tmpfs_size / 1024);
	if (mount("tmpfs", g_st.tmpfs, "tmpfs", 0, tmpfs_opts) < 0)
		die("mount tmpfs: %s", strerror(errno));
	g_st.tmpfs_mounted = 1;
	verbose("Mounted tmpfs (%s) on %s\n", bytes_human(tmpfs_size), g_st.tmpfs);

	extract_archive(g_st.archive_path, g_st.tmpfs);

	g_st.data_size = tree_size(g_st.tmpfs);
	if (g_st.data_size == 0)
		die("extracted data is empty");
	msg("Dataset size: %s (%llu bytes)\n",
	    bytes_human(g_st.data_size), g_st.data_size);

	double compression_factor = g_st.archive_size > 0
		? (double)g_st.data_size / (double)g_st.archive_size
		: 0;
	msg("Archive compression factor: %.2f\n", compression_factor);

	{
		unsigned long long total_written = g_st.data_size
			* (unsigned long long)total_test_count(&g_cfg);
		msg("Estimated total data written: %s across %d tests.\n",
		    bytes_human(total_written), total_test_count(&g_cfg));
	}

	msg("\n");
	run_all_tests(&g_cfg);

	if (g_signaled) {
		msg("\nInterrupted. Cleaning up...\n");
		cleanup();
		return 1;
	}

	static struct avg avgs[MAX_RESULTS];
	int navg = compute_averages(avgs, MAX_RESULTS);

	print_results_table(avgs, navg);
	navg = append_interpolated(avgs, navg, MAX_RESULTS);
	print_interpolated(avgs, navg);
	print_best(avgs, navg);
	print_comparison(avgs, navg);

	{
		int zstd_count = 0;
		double first_ratio = -1;
		int all_same = 1;
		for (int i = 0; i < navg; i++) {
			if (strncmp(avgs[i].target, "zstd:", 5) == 0) {
				zstd_count++;
				if (first_ratio < 0)
					first_ratio = avgs[i].ratio;
				else if (fabs(avgs[i].ratio - first_ratio) > 0.0001)
					all_same = 0;
			}
		}
		if (zstd_count > 1 && all_same)
			msg("\nNote: All zstd levels produced identical "
			    "compression ratios (%.4f).\n"
			    "Consider using a more diverse dataset to "
			    "differentiate levels.\n", first_ratio);
	}

	export_csv(avgs, navg);

	cleanup();
	return 0;
}
