#!/usr/bin/env python3
import os, sys, argparse, subprocess, tempfile, shutil, time, math, urllib.request, csv, tarfile, signal, threading
from collections import defaultdict
import psutil

# Global variables for cleanup and verbosity
TEMP_DIR = None
MOUNT_POINT = None
TMPFS_EXTRACT = None
VERBOSE = False

def status_update(msg, end='\r'):
    """In non-verbose mode, update the same line; in verbose, print normally."""
    if not VERBOSE:
        sys.stdout.write("\033[K\r" + msg + end)
        sys.stdout.flush()
    else:
        print(msg)

def vprint(*args, **kwargs):
    """Print only if VERBOSE is True."""
    if VERBOSE:
        print(*args, **kwargs)

# -----------------------
# Helper Functions
# -----------------------

def download_archive(url, dest_path):
    status_update("Downloading archive...")
    vprint(f"Downloading archive from {url} …")
    urllib.request.urlretrieve(url, dest_path)
    vprint("Download complete.")

def test_archive_integrity(archive_path):
    status_update("Testing archive integrity...")
    result = subprocess.run(["xz", "-t", archive_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode != 0:
        raise Exception("Archive appears to be corrupt.")
    vprint("Archive integrity OK.")

def extract_archive(archive_path, extract_path):
    status_update("Extracting archive...")
    vprint(f"Extracting archive to {extract_path} …")
    os.makedirs(extract_path, exist_ok=True)
    cmd = f"tar --use-compress-program='xz -T0 -d' -xf {archive_path} -C {extract_path}"
    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL if not VERBOSE else None)
    vprint("Extraction complete.")

def format_disk(disk):
    vprint(f"Formatting {disk}...")
    subprocess.run(["mkfs.btrfs", "-f", disk], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL if not VERBOSE else None)
    vprint("Formatting complete.")

def mount_btrfs(disk, mount_point, compress_option, force_flag=False):
    """
    Mounts the disk with the specified compression option.
    Uses 'compress-force' if force_flag is True.
    In non-verbose mode, only a minimal status message is updated on the same line.
    After mounting, the mount info line (from 'mount') is printed.
    """
    vprint(f"Mounting {disk} on {mount_point}...")
    os.makedirs(mount_point, exist_ok=True)
    run_kwargs = {} if VERBOSE else {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    if compress_option:
        if force_flag:
            options = f"compress-force={compress_option}"
        else:
            options = f"compress={compress_option}"
        subprocess.run(["mount", "-o", options, disk, mount_point], check=True, **run_kwargs)
    else:
        subprocess.run(["mount", disk, mount_point], check=True, **run_kwargs)
    try:
        mount_output = subprocess.check_output(["mount"], text=True)
        for line in mount_output.splitlines():
            if mount_point in line:
                vprint(f"[MOUNT INFO] {line}")
                break
    except Exception as e:
        print(f"Error retrieving mount information: {e}")

def unmount(mount_point):
    subprocess.run(["umount", mount_point], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def get_total_size(path):
    """Uses 'du -sb' to get the real size (in bytes) of the directory."""
    try:
        output = subprocess.check_output(["du", "-sb", path], text=True)
        size = int(output.split()[0])
        return size
    except Exception as e:
        print(f"Error running 'du': {e}")
        print(f"Falling back os.walk, directory size may be inaccurate for {path}")
        total = 0
        for root, dirs, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    total += os.path.getsize(fp)
                except FileNotFoundError:
                    pass
        return total

def copy_data(src, dest):
    """
    Copies data using a tar | pv | tar pipeline.
    Concurrently samples CPU usage via psutil.
    Returns: (elapsed seconds, rate in MiB/s, total bytes, average CPU %)
    """
    vprint("Copying data...")
    cmd = f"tar -cf - -C {src} . | pv -cN data | tar -xf - -C {dest}"
    cpu_samples = []
    stop_event = threading.Event()
    def sample_cpu():
        while not stop_event.is_set():
            cpu = psutil.cpu_percent(interval=0.1)
            cpu_samples.append(cpu)
    sampler = threading.Thread(target=sample_cpu)
    sampler.start()
    start = time.time()
    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elapsed = time.time() - start
    stop_event.set()
    sampler.join()
    subprocess.run(["sync"])
    elapsed_sync = time.time() - start
    avg_cpu = sum(cpu_samples)/len(cpu_samples) if cpu_samples else 0
    return elapsed, elapsed_sync, avg_cpu

def run_compsize(mount_point):
    """
    Runs 'compsize -b' and returns (disk_usage, ratio) based on the "Perc" column
    from the line whose first column matches the algorithm.
    """
    vprint("Running compsize...")
    result = subprocess.run(["compsize", "-b", mount_point], stdout=subprocess.PIPE, text=True, check=True)
    output = result.stdout
    vprint(output)
    data_line = None
    for line in output.splitlines():
        if line.startswith("TOTAL"):
            parts = line.split()
            if len(parts) < 3: continue
            try:
                perc = parts[1]
                if perc.endswith('%'):
                    ratio = float(perc.rstrip('%'))/100.0
            except ValueError:
                continue
            try:
                disk_usage = int(parts[2])
            except (IndexError, ValueError):
                continue
            data_line = (disk_usage, ratio)
            break
    if data_line is None:
        raise Exception(f"Unable to parse compsize output.")
    return data_line

def get_decompressed_size_from_archive(archive_path):
    status_update(f"Calculating archive decompressed size...")
    vprint(f"Calculating decompressed size from archive {archive_path} …")
    total = 0
    with tarfile.open(archive_path, 'r:xz') as tar:
        for member in tar.getmembers():
            if member.isfile():
                total += member.size
    return total

def bytes_to_human(size_bytes):
    if size_bytes >= 1 << 30:
        size = size_bytes/(1<<30)
        return f"{size:.0f}G"
    elif size_bytes >= 1 << 20:
        size = size_bytes/(1<<20)
        return f"{size:.0f}M"
    else:
        return f"{size_bytes}B"

def get_partition_names(disk):
    if disk[-1].isdigit():
        return disk + "p1", disk + "p2"
    else:
        return disk + "1", disk + "2"

def get_disk_size_bytes(disk):
    return int(subprocess.check_output(["blockdev", "--getsize64", disk]).strip())

def get_archive_size(url):
    if url.startswith("file://"):
        local_path = url[7:]
        try:
            return os.path.getsize(local_path)
        except Exception as e:
            vprint(f"Error accessing local file {local_path}: {e}")
            return None
    else:
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req) as response:
                cl = response.getheader("Content-Length")
                if cl is not None:
                    return int(cl)
        except Exception as e:
            vprint(f"Warning: Unable to determine archive size: {e}")
    return None

def calculate_total_tests(repeat, max_zstd, force_option, hdd):
    """
    Calcule le nombre total de tests.
    
    - Pour les tests "zstd" : max_zstd niveaux sont testés.
    - Pour "lzo" : 1 test.
    - Pour "none" : 1 test (toujours standard).
    
    Si force_option == "both", les tests pour "zstd" et "lzo" sont effectués en 2 variantes (standard et force).
    
    Le nombre total est multiplié par le nombre de zones (2 si hdd est activé, sinon 1)
    et par le nombre de répétitions.
    """
    tests_with_target = max_zstd + 1  # zstd (max_zstd) + lzo (1)
    factor = 2 if force_option == "both" else 1
    tests_for_target = tests_with_target * factor
    tests_for_none = 1  # pour "none", toujours une seule variante
    tests_per_location = tests_for_target + tests_for_none
    locations = 2 if hdd is not None else 1
    total_tests = tests_per_location * locations * repeat
    return total_tests

# -----------------------
# Cleanup and Signal Handler
# -----------------------

def cleanup_resources():
    global TEMP_DIR, MOUNT_POINT, TMPFS_EXTRACT
    print("\nCleaning up temporary files …")
    if MOUNT_POINT:
        subprocess.run(["umount", MOUNT_POINT], check=False)
    if TMPFS_EXTRACT:
        subprocess.run(["umount", TMPFS_EXTRACT], check=False)
    if TEMP_DIR:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print("Cleanup complete.")

def signal_handler(sig, frame):
    print("\nInterrupt received, performing cleanup …")
    cleanup_resources()
    sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)

# -----------------------
# Main Program
# -----------------------

def main():
    global VERSION, TEMP_DIR, MOUNT_POINT, TMPFS_EXTRACT, VERBOSE

    VERSION = "1.0rc1"
    
    parser = argparse.ArgumentParser(description="BTRFS Compression Benchmark (compbench) {VERSION}")
    parser.add_argument("disk", help="Disk to format and test (e.g. /dev/sdX)")
    parser.add_argument("archive_url", help="URL of the .tar.xz archive containing test data")
    parser.add_argument("-o", "--output", help="Output CSV file path", default=None)
    parser.add_argument("--hdd", type=int, help="Percentage of disk used by each partition (1-50); e.g., '--hdd 10' tests on first 10% and last 10% of disk", default=None)
    parser.add_argument("-n", "--repeat", type=int, default=1, help="Number of times to repeat the test sequence (default 1)")
    parser.add_argument("--max-zstd", type=int, default=15, help="Maximum zstd level to test (1-15; default 15)")
    parser.add_argument("-s", "--sync", action="store_true", help="Wait for disks to synchronize to determine elapsed times and transfer rates.")
    parser.add_argument("-f", "--force", type=str, default="none", choices=["none", "zstd", "lzo", "all", "both"],
                        help="Force option: 'none' (default), 'zstd', 'lzo', 'all', or 'both'")
    parser.add_argument("--nointegritycheck", action="store_true", help="Skip integrity check of the xz archive")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()
    VERBOSE = args.verbose

    if os.geteuid() != 0:
        print("This script must be run as root.")
        sys.exit(1)
    print(f"WARNING: All data on {args.disk} will be erased!")
    confirm = input("Do you confirm (yes/no)? : ")
    if confirm.lower() != "yes":
        print("Operation cancelled.")
        sys.exit(0)

    # Create global temporary directory
    TEMP_DIR = tempfile.mkdtemp(prefix="compbench_")
    archive_path = os.path.join(TEMP_DIR, "data.tar.xz")
    MOUNT_POINT = os.path.join(TEMP_DIR, "mnt")
    os.makedirs(MOUNT_POINT, exist_ok=True)

    # Calculating the amount of tests to be performed
    total_tests = calculate_total_tests(args.repeat, args.max_zstd, args.force.lower(), args.hdd)

    # Check archive size (HEAD or file://) before downloading
    archive_size_est = get_archive_size(args.archive_url)
    if archive_size_est:
        estimated_required_ram = archive_size_est * 3.0 * 1.1
        available_ram = psutil.virtual_memory().available
        vprint(f"Archive size (from HEAD): {archive_size_est} bytes")
        vprint(f"Estimated required RAM (with margin): {int(estimated_required_ram)} bytes")
        vprint(f"Available RAM: {available_ram} bytes")
        if available_ram < estimated_required_ram:
            answer = input("Warning: your available RAM may be insufficient for this benchmark. Continue anyway? (y/n): ")
            if answer.lower() != "y":
                print("Aborting.")
                sys.exit(1)
    else:
        answer = input("Warning: The archive size could not be determined. Continue anyway? (y/n): ")
        if answer.lower() != "y":
            print("Aborting.")
            sys.exit(1)

    # Download and (optionally) check archive integrity.
    download_archive(args.archive_url, archive_path)
    if not args.nointegritycheck:
        test_archive_integrity(archive_path)
    else:
        print("Skipping archive integrity check as per --nointegritycheck.")

    # Determine decompressed size and set up tmpfs.
    decompressed_size = get_decompressed_size_from_archive(archive_path)
    margin = 1.1
    tmpfs_size_bytes = int(decompressed_size * margin)
    tmpfs_size_str = bytes_to_human(tmpfs_size_bytes)
    vprint(f"Decompressed archive size: {decompressed_size} bytes")
    vprint(f"Mounting tmpfs with size: {tmpfs_size_str}")

    # Warn user about potential data volume written to the drive
    total_written_est = decompressed_size * total_tests
    human_total = bytes_to_human(total_written_est)
    print(f"\nWarning: This benchmark could write up to {total_written_est} bytes ({human_total}) on the target drive over {total_tests} tests.")
    answer = input("Do you want to continue? (y/n): ")
    if answer.lower() != "y":
        print("Aborting.")
        sys.exit(1)

    # Handle HDD option.
    if args.hdd is not None:
        percentage = args.hdd
        if percentage < 1 or percentage > 50:
            print("The provided hdd percentage must be between 1 and 50.")
            sys.exit(1)
        disk_total_bytes = get_disk_size_bytes(args.disk)
        partition_size_bytes = int(disk_total_bytes * (percentage/100.0))
        partition_size_str = bytes_to_human(partition_size_bytes)
        vprint(f"Disk size: {bytes_to_human(disk_total_bytes)}")
        status_update(f"Creating two partitions of size {partition_size_str} each")
        subprocess.run(["sgdisk", "--zap-all", args.disk], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL if not VERBOSE else None)
        subprocess.run(["sgdisk", f"-n1:2048:+{partition_size_str}", args.disk], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL if not VERBOSE else None)
        subprocess.run(["sgdisk", f"-n2:-{partition_size_str}:0", args.disk], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL if not VERBOSE else None)
        part1, part2 = get_partition_names(args.disk)
        vprint(f"Partitions created: {part1} (beginning) and {part2} (end)")
    else:
        part1 = args.disk

    # Mount dedicated tmpfs for extraction.
    TMPFS_EXTRACT = os.path.join(TEMP_DIR, "tmpfs_extract")
    os.makedirs(TMPFS_EXTRACT, exist_ok=True)
    subprocess.run(["mount", "-t", "tmpfs", "-o", f"size={tmpfs_size_str}", "tmpfs", TMPFS_EXTRACT], check=True)
    vprint(f"Mounted dedicated tmpfs on {TMPFS_EXTRACT}")

    # Extract archive into tmpfs.
    extract_archive(archive_path, TMPFS_EXTRACT)
    total_uncompressed = get_total_size(TMPFS_EXTRACT)
    vprint(f"Total extracted data size: {total_uncompressed} bytes")

    # Build list of tests.
    tests = []
    for lvl in range(args.max_zstd, 0, -1):
        tests.append(("zstd", f"zstd:{lvl}"))
    tests.append(("lzo", "lzo"))
    tests.append(("none", None))  # 'none' only standard is run

    results_by_combo = defaultdict(list)
    if args.hdd is not None:
        locations = [("begin", part1), ("end", part2)]
    else:
        locations = [("begin", part1)]

    test_count = 0
    for loc, disk_device in locations:
        for algo_key, base_target in tests:
            if base_target is None:
                force_variants = [("standard", False)]
            else:
                if args.force.lower() == "both":
                    force_variants = [("standard", False), ("force", True)]
                else:
                    flag = False
                    if args.force.lower() == "all":
                        flag = True
                    elif args.force.lower() == "zstd" and algo_key=="zstd":
                        flag = True
                    elif args.force.lower() == "lzo" and algo_key=="lzo":
                        flag = True
                    force_variants = [("force" if flag else "standard", flag)]
            for variant_label, force_flag in force_variants:
                key = (loc, base_target if base_target else "none", variant_label)
                for i in range(args.repeat):
                    test_count += 1
                    status_update(f"Test {test_count} out of {total_tests}: {base_target if base_target else 'none'} with {variant_label} at {loc} (repeat {i+1}/{args.repeat})...")
                    unmount(MOUNT_POINT)
                    format_disk(disk_device)
                    mount_btrfs(disk_device, MOUNT_POINT, base_target if base_target else "", force_flag)
                    elapsed, elapsed_sync, avg_cpu = copy_data(TMPFS_EXTRACT, MOUNT_POINT)
                    rate = total_uncompressed / elapsed
                    mib_rate = math.ceil(rate / (1024*1024))
                    rate_sync = total_uncompressed / elapsed_sync
                    mib_rate_sync = math.ceil(rate_sync / (1024*1024))
                    vprint(f"Elapsed: {elapsed:.2f} s, Rate: {mib_rate} MiB/s, Elapsed (to sync): {elapsed_sync:.2f} s, Rate (to sync): {mib_rate_sync} MiB/s, Avg CPU: {avg_cpu:.1f}%")
                    vprint("Synchronizing drives...")
                    disk_usage, ratio = run_compsize(MOUNT_POINT)
                    sec_per_reduction = float('inf') if ratio >= 1.0 else elapsed/(1 - ratio)
                    run_result = {
                        "Location": loc,
                        "Target": base_target if base_target else "none",
                        "Time (s)": elapsed_sync if args.sync else elapsed,
                        "Rate (MiB/s)": mib_rate_sync if args.sync else mib_rate,
                        "Compressed (B)": disk_usage,
                        "Ratio": ratio,
                        "Seconds/Reduction": sec_per_reduction,
                        "CPU (%)": avg_cpu,
                        "Force": variant_label
                    }
                    results_by_combo[key].append(run_result)
                    unmount(MOUNT_POINT)

    averaged_results = []
    for key, runs in results_by_combo.items():
        loc, target, force_variant = key
        count = len(runs)
        avg_time = sum(r["Time (s)"] for r in runs)/count
        avg_rate = sum(r["Rate (MiB/s)"] for r in runs)/count
        avg_compressed = sum(r["Compressed (B)"] for r in runs)/count
        avg_ratio = sum(r["Ratio"] for r in runs)/count
        noninf_runs = [r for r in runs if r["Seconds/Reduction"] != float('inf')]
        avg_sec_red = (sum(r["Seconds/Reduction"] for r in noninf_runs)/len(noninf_runs)
                       if noninf_runs else float('inf'))
        avg_cpu = sum(r["CPU (%)"] for r in runs)/count
        averaged_results.append({
            "Location": loc,
            "Target": target,
            "Time (s)": round(avg_time),
            "Rate (MiB/s)": round(avg_rate),
            "Compressed (B)": round(avg_compressed),
            "Ratio": round(avg_ratio,4),
            "Seconds/Reduction": round(avg_sec_red,3) if avg_sec_red != float('inf') else "infinite",
            "CPU (%)": round(avg_cpu,1),
            "Force": force_variant,
            "Repeat": count
        })

    if args.repeat > 1:
        print(f"\nAll results are averaged over {args.repeat} runs.")

    main_header = ["Location", "Target", "Time (s)", "Rate (MiB/s)", "Compressed (B)", "Ratio", "Seconds/Reduction", "CPU (%)", "Force", "Repeat"]
    col_widths = {h: max(len(h), max(len(str(r[h])) for r in averaged_results)) for h in main_header}
    print("\nBenchmark Results:")
    header_line = "   ".join(h.ljust(col_widths[h]) for h in main_header)
    print(header_line)
    print("-" * len(header_line))
    for row in averaged_results:
        line = "   ".join(str(row[h]).ljust(col_widths[h]) for h in main_header)
        print(line)

    interpolated = []
    if args.hdd is not None:
        grouped = defaultdict(list)
        for row in averaged_results:
            key = (row["Target"], row["Force"])
            grouped[key].append(row)
        for (target, force_variant), rows in grouped.items():
            if len(rows) == 2:
                interp = {
                    "Location": "Interpolated",
                    "Target": target,
                    "Time (s)": round((rows[0]["Time (s)"] + rows[1]["Time (s)"]) / 2),
                    "Rate (MiB/s)": round((rows[0]["Rate (MiB/s)"] + rows[1]["Rate (MiB/s)"]) / 2),
                    "Compressed (B)": round((rows[0]["Compressed (B)"] + rows[1]["Compressed (B)"]) / 2),
                    "Ratio": round((rows[0]["Ratio"] + rows[1]["Ratio"]) / 2, 4)
                }
                sr_vals = []
                for r in rows:
                    val = r["Seconds/Reduction"]
                    sr_vals.append(val if isinstance(val, (int, float)) else float('inf'))
                interp["Seconds/Reduction"] = "infinite" if all(v==float('inf') for v in sr_vals) else round(sum(sr_vals)/2,3)
                interp["CPU (%)"] = round((rows[0]["CPU (%)"] + rows[1]["CPU (%)"]) / 2, 1)
                interp["Force"] = force_variant
                interp["Repeat"] = "avg of 2"
                interpolated.append(interp)
        if interpolated:
            print("\nInterpolated (Average) Results across Disk:")
            interp_header = main_header
            col_widths2 = {h: max(len(h), max(len(str(r[h])) for r in interpolated)) for h in interp_header}
            header_line2 = "   ".join(h.ljust(col_widths2[h]) for h in interp_header)
            print(header_line2)
            print("-" * len(header_line2))
            for row in interpolated:
                line = "   ".join(str(row[h]).ljust(col_widths2[h]) for h in interp_header)
                print(line)

    print("\nBest Results:")
    best_summary = []
    all_locations = ["begin", "end"] if args.hdd is not None else ["begin"]
    for loc in all_locations:
        subset = [r for r in averaged_results if r["Location"] == loc]
        if subset:
            fastest = max(subset, key=lambda r: r["Rate (MiB/s)"])
            most_compression = min(subset, key=lambda r: r["Ratio"])
            most_balanced = min(subset, key=lambda r: float(r["Seconds/Reduction"]) if r["Seconds/Reduction"]!="infinite" else float('inf'))
            summary = {
                "Location": loc,
                "Fastest": f"{fastest['Target']} ({fastest['Rate (MiB/s)']} MiB/s, CPU {fastest['CPU (%)']}%, {fastest['Force']})",
                "Most Compression": f"{most_compression['Target']} (ratio {most_compression['Ratio']}, CPU {most_compression['CPU (%)']}%, {most_compression['Force']})",
                "Most Balanced": f"{most_balanced['Target']} (sec/reduction {most_balanced['Seconds/Reduction']}, CPU {most_balanced['CPU (%)']}%, {most_balanced['Force']})"
            }
            best_summary.append(summary)
            print(f"{loc.capitalize()} - Fastest: {summary['Fastest']}, Most Compression: {summary['Most Compression']}, Most Balanced: {summary['Most Balanced']}")
    if interpolated:
        fastest = max(interpolated, key=lambda r: r["Rate (MiB/s)"])
        most_compression = min(interpolated, key=lambda r: r["Ratio"])
        most_balanced = min(interpolated, key=lambda r: float(r["Seconds/Reduction"]) if r["Seconds/Reduction"]!="infinite" else float('inf'))
        summary = {
            "Location": "Interpolated",
            "Fastest": f"{fastest['Target']} ({fastest['Rate (MiB/s)']} MiB/s, CPU {fastest['CPU (%)']}%, {fastest['Force']})",
            "Most Compression": f"{most_compression['Target']} (ratio {most_compression['Ratio']}, CPU {most_compression['CPU (%)']}%, {most_compression['Force']})",
            "Most Balanced": f"{most_balanced['Target']} (sec/reduction {most_balanced['Seconds/Reduction']}, CPU {most_balanced['CPU (%)']}%, {most_balanced['Force']})"
        }
        best_summary.append(summary)
        print(f"Interpolated - Fastest: {summary['Fastest']}, Most Compression: {summary['Most Compression']}, Most Balanced: {summary['Most Balanced']}")

    archive_size = os.path.getsize(archive_path)
    compression_factor = decompressed_size / archive_size if archive_size else 0
    print(f"\nAverage dataset compression factor: {compression_factor:.2f} (decompressed/archive)")

    csv_main_header = main_header
    csv_rows = []
    for row in averaged_results:
        new_row = row.copy()
        csv_rows.append(new_row)
    for row in interpolated:
        new_row = row.copy()
        new_row["Location"] = "Interpolated"
        new_row["Repeat"] = "avg of 2"
        csv_rows.append(new_row)
    for summ in best_summary:
        new_row = {
            "Location": summ["Location"] + " (Best)",
            "Target": "",
            "Time (s)": "",
            "Rate (MiB/s)": summ["Fastest"],
            "Compressed (B)": "",
            "Ratio": summ["Most Compression"],
            "Seconds/Reduction": summ["Most Balanced"],
            "CPU (%)": "",
            "Force": summ.get("Force", ""),
            "Repeat": ""
        }
        csv_rows.append(new_row)
    comparison_table = []
    if args.force.lower() == "both":
        grouped = defaultdict(dict)
        for row in averaged_results:
            if row["Target"]=="none":
                continue
            key = (row["Location"], row["Target"])
            grouped[key][row["Force"]] = row
        for key, variants in grouped.items():
            if "standard" in variants and "force" in variants:
                std = variants["standard"]
                frc = variants["force"]
                def perc_diff(force_val, std_val):
                    return ((force_val - std_val)/std_val*100) if std_val != 0 else float('inf')
                comp_row = {
                    "Location": key[0],
                    "Target": key[1],
                    "Time diff (%)": round(perc_diff(frc["Time (s)"], std["Time (s)"]),1),
                    "Rate diff (%)": round(perc_diff(frc["Rate (MiB/s)"], std["Rate (MiB/s)"]),1),
                    "Compressed diff (%)": round(perc_diff(frc["Compressed (B)"], std["Compressed (B)"]),1),
                    "Ratio diff (%)": round(perc_diff(frc["Ratio"], std["Ratio"]),1),
                    "Sec/Red diff (%)": ("infinite" if std["Seconds/Reduction"]=="infinite" 
                                           else round(perc_diff(float(frc["Seconds/Reduction"]), float(std["Seconds/Reduction"])),1)),
                    "CPU diff (%)": round(perc_diff(frc["CPU (%)"], std["CPU (%)"]),1)
                }
                comparison_table.append(comp_row)
        if comparison_table:
            comp_header = ["Location", "Target", "Time diff (%)", "Rate diff (%)", "Compressed diff (%)", "Ratio diff (%)", "Sec/Red diff (%)", "CPU diff (%)"]
            col_widths_comp = {h: max(len(h), max(len(str(r[h])) for r in comparison_table)) for h in comp_header}
            header_line_comp = "   ".join(h.ljust(col_widths_comp[h]) for h in comp_header)
            print("\nComparison of 'force' vs 'standard' (percentage differences):")
            print(header_line_comp)
            print("-" * len(header_line_comp))
            for row in comparison_table:
                line = "   ".join(str(row[h]).ljust(col_widths_comp[h]) for h in comp_header)
                print(line)
    metadata = {
        "Archive URL": args.archive_url,
        "Decompressed Size (B)": decompressed_size,
        "Archive Size (B)": archive_size,
        "Compression Factor": f"{compression_factor:.2f}",
        "Test Repeats": args.repeat,
        "Max zstd level": args.max_zstd,
        "Force Option": args.force,
        "To Sync Option": args.sync
    }
    csv_header = csv_main_header
    if args.output:
        try:
            with open(args.output, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_header)
                writer.writeheader()
                for row in csv_rows:
                    writer.writerow(row)
                csvfile.write("\n# Metadata:\n")
                for k, v in metadata.items():
                    csvfile.write(f"# {k}: {v}\n")
            print(f"\nResults have been saved to {args.output}")
        except Exception as e:
            print(f"Error writing CSV file: {e}")

    cleanup_resources()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        cleanup_resources()
        sys.exit(1)
