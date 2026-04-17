<p align="center">
  <img src="https://raw.githubusercontent.com/robindubreuil/compbench/main/logo.svg" alt="Compbench Logo" width="300">
</p>

# An exhaustive benchmark for Btrfs transparent compression

Compbench is a C tool that benchmarks Btrfs compression algorithms (zstd, lzo) by measuring write throughput, compression ratio, CPU usage, and disk I/O — all through direct kernel interfaces with zero text parsing of external tool output.

## Features

- **Direct kernel interfaces** — compression ratio via `statvfs()`, CPU via `/proc/stat`, disk I/O via `/sys/block/*/stat`, timing via `clock_gettime(CLOCK_MONOTONIC_RAW)`, data copy via `copy_file_range()`
- **Exhaustive testing** — configurable zstd level range (-15..15), lzo, and no compression
- **Force mode comparison** — run standard `compress` and/or `compress-force` mount options, with a side-by-side percentage comparison table
- **HDD zone testing** — benchmark first/last N% of disk to evaluate spatial performance variations, with interpolated results
- **Repeatable** — average results over multiple runs for reliability
- **Disk-level I/O stats** — actual bytes written to physical media, separate from apparent throughput
- **CSV export** — machine-readable output with full metadata

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

The binary is dynamically linked against `libm` and `libc` only.

## Usage

> **Warning:** This tool formats the target device, erasing all existing data.

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
      --zstd MIN-MAX      zstd level range (default: 1-15)
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
- Test zstd levels 10 down to 1, plus lzo and no compression
- Repeat the sequence 3 times with averaged results
- Run both standard and force modes with a comparison table
- Test on the first and last 10% of the disk
- Run in verbose mode

### Measured Metrics

All metrics use kernel interfaces directly:

| Column | Description | Source |
|--------|-------------|--------|
| WrMiB/s | Synced write throughput (copy + sync) | `clock_gettime` |
| RdMiB/s | Read throughput after cache clear (unmount/remount + full tree read) | `clock_gettime` |
| WrDisk* | Actual bytes written to physical media | `/sys/block/*/stat` |
| RdDisk* | Actual bytes read from physical media | `/sys/block/*/stat` |
| Ratio | Space consumed vs uncompressed | `statvfs` |
| WrCPU% | System CPU usage during write | `/proc/stat` |
| RdCPU% | System CPU usage during read | `/proc/stat` |
| Sec/Red | Seconds per 1% space reduction | Derived |

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
