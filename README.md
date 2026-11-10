<p align="center">
  <img src="https://raw.githubusercontent.com/robindubreuil/compbench/main/logo.svg" alt="Compbench Logo" width="300">
</p>

# An exhaustive benchmark for Btrfs transparent compression

Compbench is a C tool that benchmarks Btrfs compression algorithms (zstd, lzo, zlib) by measuring read and write throughput, compression ratio, CPU usage, and disk I/O — all through direct kernel interfaces with zero text parsing of external tool output.

## Features

- **Direct kernel interfaces** — compression ratio via `statvfs()`, CPU via `/proc/stat`, disk I/O via `/sys/block/*/stat`, timing via `clock_gettime(CLOCK_MONOTONIC_RAW)`, data copy via `copy_file_range()`
- **Exhaustive testing** — configurable zstd level range (-15..-1 or 1..15), lzo, zlib, and no compression
- **Cold-cache reads** — the page cache is dropped before the read test, so `RdMiB/s` reflects real disk throughput, not memory
- **Force mode comparison** — run standard `compress` and/or `compress-force` mount options, with a side-by-side percentage comparison table
- **HDD zone testing** — benchmark first/last N% of disk to evaluate spatial performance variations, with interpolated results
- **Repeatable** — average results over multiple runs, with throughput standard deviation reported in the CSV
- **Disk-level I/O stats** — actual bytes written to physical media, separate from apparent throughput
- **CSV export** — machine-readable output with full metadata (archive, dataset, host, kernel, date)

## Requirements

### System Packages (Debian/Ubuntu)

```bash
sudo apt install gcc make btrfs-progs xz-utils gdisk curl
```

`tar` and `util-linux` are typically installed by default.

## Building

```bash
make
sudo make install
```

This installs the `compbench` binary and its man page (`compbench(1)`).
The binary is dynamically linked against `libm` and `libc` only.

## Usage

> **Warning:** This tool formats the target device, erasing all existing data.
> The device is not restored after a run: it is left with the Btrfs filesystem
> of the last test (`--hdd` additionally leaves a GPT table with two partitions).
> Before writing anything, compbench prints the disk model and size and asks
> for confirmation.

### Basic Command

```bash
sudo compbench /dev/sdX https://example.com/archive.tar.xz
```

### Command-Line Options

```
Usage: compbench [OPTIONS] DEVICE ARCHIVE

DEVICE is the block device to test (e.g. /dev/sdX).
ARCHIVE is a local path or URL to a .tar.xz archive.

Options:
  -n, --repeat N          Repeat each test N times (default: 1)
      --zstd LEVEL|RANGE  zstd level or MIN-MAX range
                         (-15..-1 or 1..15, default: 1-15)
  -f, --force MODE        Force mode: none|zstd|lzo|all|both (default: none)
      --hdd PERCENT       Test on first/last N% of disk (1-50)
  -o, --output FILE       Export results to CSV
      --no-integrity-check
                         Skip archive integrity verification
  -v, --verbose           Verbose output
  -h, --help              Show help
  -V, --version           Show version
```

### Force Modes

| Mode | Effect |
|------|--------|
| `none` | Standard `compress=` for all tests |
| `zstd` | `compress-force=` for zstd, standard for lzo |
| `lzo` | `compress-force=` for lzo, standard for zstd |
| `all` | `compress-force=` for all algorithms |
| `both` | Run each test twice (standard + force) and show comparison |

### Example

```bash
sudo compbench /dev/sdX https://example.com/archive.tar.xz -n 3 --zstd 1-10 -f both --hdd 10 -v
```

This will:
- Test zstd levels 10 down to 1, plus lzo, zlib and no compression
- Repeat the sequence 3 times with averaged results
- Run both standard and force modes with a comparison table
- Test on the first and last 10% of the disk
- Run in verbose mode

### Measured Metrics

All metrics use kernel interfaces directly:

| Column | Description | Source |
|--------|-------------|--------|
| WrMiB/s | Synced write throughput (copy + sync) | `clock_gettime` |
| RdMiB/s | Read throughput after cache clear (page cache dropped + remount + full tree read) | `clock_gettime` |
| WrDisk* | Actual bytes written to physical media | `/sys/block/*/stat` |
| RdDisk* | Actual bytes read from physical media | `/sys/block/*/stat` |
| Ratio | Space consumed vs uncompressed | `statvfs` |
| WrCPU% | System CPU usage during write | `/proc/stat` |
| RdCPU% | System CPU usage during read | `/proc/stat` |
| Sec/Red | Seconds per 1% space reduction | Derived |

### Measurement Caveats

- **CPU usage is system-wide.** `WrCPU%`/`RdCPU%` come from `/proc/stat` across all cores and all processes — run compbench on an otherwise idle system for meaningful numbers.
- **Ratio granularity.** The compression ratio is measured with `statvfs()`. Btrfs allocates space in chunks (and metadata globally), so small datasets can show quantized ratios — use a dataset that is large relative to the test filesystem.
- **`sync()` is system-wide.** Synced write timings (`WrMiB/s`) include flushing any unrelated dirty data on the machine.
- **Cold reads need a writable `/proc/sys/vm/drop_caches`.** When the page cache cannot be dropped (e.g. inside some containers), `RdMiB/s` may be inflated; compbench prints a warning.
- **`Sec/Red` vs `WrMiB/s` time basis.** `Sec/Red` is derived from the copy-only write time, while `WrMiB/s` includes the `sync()` wait — the two columns are not on the same time basis.
- **`WrCPU%` includes the sync wait.** The CPU sample spans the whole write phase (copy + `sync()`); iowait counts as idle, so on slow devices the compression CPU cost is understated.
- **Fixed test order.** Targets always run highest zstd level first. On long runs, thermal throttling or SLC-cache exhaustion can penalize later targets; use `--repeat` and compare averaged results.
- **Dataset shape.** Hard links in the archive are written as independent files (shifting `Ratio` for hard-link-heavy datasets); special files are skipped (noted in verbose mode).

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All requested tests completed and succeeded |
| `1` | Error (bad arguments, missing privileges, user abort, interrupt), or at least one test failed |

Failed tests are excluded from the results and reported at the end; if every test fails, no results are produced.

## Installation (Debian Package)

Download the `.deb` from [GitHub Releases](https://github.com/robindubreuil/compbench/releases) and install:

```bash
sudo dpkg -i compbench_*.deb
sudo apt-get install -f
```

## Building a Debian Package

```bash
dpkg-buildpackage -us -uc
```

## Running Tests

The test suite uses loop devices (no real disks needed):

```bash
sudo bash test.sh
```

## Contributing

Contributions, bug reports, and suggestions are welcome! Please open an issue or submit a pull request on GitHub.

## License

GNU General Public License v3.0
