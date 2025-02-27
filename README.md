<p align="center">
  <img src="https://raw.githubusercontent.com/Robin-DUBREUIL/compbench/main/logo.svg" alt="Compbench Logo" width="300">
</p>

# An exhaustive benchmark for Btrfs transparent compression

Compbench is a comprehensive Python tool designed to evaluate and compare the performance of Btrfs transparent compression algorithms. It allows you to benchmark different compression methods (e.g., zstd, lzo, none) under various conditions, providing detailed metrics and comparative results—all while offering a user-friendly interface with a minimal output mode for beginners.

## Features

- **Exhaustive Testing:**  
  Test a full range of zstd levels (configurable with `--max-zstd`), as well as lzo and no compression.

- **Repeatable Benchmarks:**  
  Repeat the entire test sequence multiple times (`-n`/`--repeat`) to obtain averaged, reliable results.

- **Force Mode Options:**  
  Use standard `compress` or force `compress-force` modes (via `-f`/`--force` with options `none`, `zstd`, `lzo`, `all`, or `both`).  
  When `--force both` is selected, the tool runs each test twice (standard and force) and compares the results.

- **HDD Testing:**  
  Optionally test on specific zones of a disk using the `--hdd` parameter (e.g., first and last *n*% of the disk) to evaluate spatial performance variations.

- **RAM Estimation & Warning:**  
  Before starting, the script estimates the required RAM based on the decompressed size of the archive and warns the user if available memory might be insufficient. The script also calculates the total volume of data that will be written to the target disk (based on the decompressed archive size and the number of tests) and warns the user accordingly.

- **Synchronized Writes Option:**  
  An optional sync mode can be enabled (using an extra argument, e.g., `-s`) to wait until the disk is fully synchronized before measuring performance. This reflects more realistic write performance.

- **Minimal & Verbose Modes:**  
  In non-verbose mode, the script updates the status on a single line to provide a clean, minimal interface. Verbose mode (`-v`/`--verbose`) displays detailed logs.

- **Detailed Results & CSV Export:**  
  Final results include throughput, compression ratios, CPU load, and best-case summaries. In `--force both` mode, a comparison table shows percentage differences between standard and force variants. All results, along with archive metadata, can be exported to CSV.

## Requirements

### System Packages (Debian 12)

```bash
sudo apt update && sudo apt install python3 python3-psutil xz-utils pv btrfs-progs btrfs-compsize gdisk
```

> **Note:**  
> The `tar` and `blockdev` commands (provided by `util-linux`) are usually installed by default.

### Python Modules

- [psutil](https://pypi.org/project/psutil/)  
  Install via `pip install psutil` if not available.

## Usage

> **Warning:**  
> **This script formats the specified disk, erasing all existing data.**  
> Use it on a test disk or in a controlled environment.

### Basic Command

```bash
sudo compbench /dev/sdX https://example.com/archive.tar.xz
```

### Command-Line Options

- `-v, --verbose`  
  Enable detailed logging output.

- `-n, --repeat <number>`  
  Number of times to repeat the test sequence (default: 1).

- `--max-zstd <level>`  
  Maximum zstd level to test (default: 15).

- `-f, --force <option>`  
  Force option for compression mode. Options:  
  - `none` (default): Use standard `compress` for all tests.  
  - `zstd`: Use `compress-force` for zstd tests only.  
  - `lzo`: Use `compress-force` for lzo tests only.  
  - `all`: Force `compress-force` for all tests.  
  - `both`: Run tests twice (standard and force mode) for comparison.

- `--hdd <percentage>`  
  Specify the percentage of the disk to use for each partition (between 1 and 50).  
  For example, `--hdd 10` tests on the first 10% and last 10% of the disk.

- `--nointegritycheck`  
  Skip the integrity check of the xz archive.

- `-s, --sync`  
  Measure performance after forcing disk synchronization (using `sync`), providing lower throughput numbers that better reflect real-world write performance.

### Example

```bash
sudo compbench /dev/sdX https://example.com/archive.tar.xz -n 3 --max-zstd 10 -f both --hdd 10 -v
```

This command will:
- Test on `/dev/sdX` using the archive from the given URL.
- Repeat the entire sequence 3 times.
- Test zstd compression levels from 10 down to 1, plus lzo and no compression.
- Run each test twice (standard and force mode), and compare the two.
- Partition the disk to test on the first and last 10%.
- Run in verbose mode.

## How It Works

1. **Preparation:**  
   The script downloads the archive and, if possible, checks its size via an HTTP HEAD request (or using `file://` for local files). It estimates the required RAM based on the decompressed size and warns the user if available memory may be insufficient.

2. **Extraction:**  
   The archive is extracted into a tmpfs (RAM disk) sized according to the decompressed data, ensuring a consistent, isolated dataset for testing.

3. **Testing:**  
   The script formats the target disk (or partitions in HDD mode), mounts it with Btrfs using the chosen compression options, and copies data from the tmpfs while measuring throughput, CPU load, and disk usage (via `compsize`).

4. **Results & Comparison:**  
   Results are aggregated and averaged (if tests are repeated) and displayed in detailed tables. In `--force both` mode, additional tables compare standard vs. force modes, highlighting percentage differences.

5. **CSV Export:**  
   All results and metadata (archive URL, decompressed size, archive size, overall compression factor, and test parameters) can be exported to a CSV file.

6. **Minimal Interface:**  
   In non-verbose mode, progress is shown as a single, updating line (e.g., "Downloading archive...") so that the output remains clean. Detailed results are displayed at the end.

## Contributing

Contributions, bug reports, and suggestions are welcome! Please open an issue or submit a pull request on GitHub.

## License

This project is licensed under the GNU GPL v3 License.
