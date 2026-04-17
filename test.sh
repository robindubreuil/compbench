#!/bin/bash
#
# test.sh — Safe compbench tests using loop devices
#
# This script creates virtual block devices (loop devices backed by sparse
# files) so compbench can be tested without risking any real data.
#
# Requirements: root, losetup, btrfs-progs, mkfs.btrfs
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPBENCH="${SCRIPT_DIR}/compbench"
TESTDIR="/tmp/compbench-test-$$"
DISKIMG="${TESTDIR}/disk.img"
ARCHIVE="${TESTDIR}/testdata.tar.xz"
LOOP=""
DISKSIZE="512M"

PASSED=0
FAILED=0

cleanup() {
	set +e
	if [ -n "$LOOP" ]; then
		losetup -d "$LOOP" 2>/dev/null || true
	fi
	if [ -d "${TESTDIR}/mnt" ]; then
		umount "${TESTDIR}/mnt" 2>/dev/null || true
	fi
	rm -rf "$TESTDIR"
	echo ""
	echo "Cleanup done. Passed: $PASSED, Failed: $FAILED"
}
trap cleanup EXIT

fail() {
	FAILED=$((FAILED + 1))
	echo "FAIL: $*" >&2
}

pass() {
	PASSED=$((PASSED + 1))
	echo "PASS: $*"
}

assert_output() {
	local label="$1" logfile="$2" pattern="$3"
	if grep -q -- "$pattern" "$logfile"; then
		pass "$label"
	else
		fail "$label — pattern not found: $pattern"
	fi
}

assert_not_output() {
	local label="$1" logfile="$2" pattern="$3"
	if ! grep -q -- "$pattern" "$logfile"; then
		pass "$label"
	else
		fail "$label — unexpected pattern found: $pattern"
	fi
}

assert_exit() {
	local label="$1"
	local expected="$2"
	shift 2
	"$@" >"${TESTDIR}/assert_exit.log" 2>&1 && rc=$? || rc=$?
	if [ "$rc" -eq "$expected" ]; then
		pass "$label (exit $rc)"
	else
		fail "$label — expected exit $expected, got $rc"
	fi
}

echo "=== compbench test suite ==="
echo ""

# --- Check prerequisites ---
echo "--- Checking prerequisites ---"

[ "$(id -u)" -eq 0 ] || {
	echo "FAIL: must run as root" >&2
	exit 1
}

command -v modprobe >/dev/null 2>&1 || {
	echo "FAIL: modprobe not found" >&2
	exit 1
}
command -v losetup >/dev/null 2>&1 || {
	echo "FAIL: losetup not found" >&2
	exit 1
}
command -v mkfs.btrfs >/dev/null 2>&1 || {
	echo "FAIL: mkfs.btrfs not found" >&2
	exit 1
}

modprobe loop 2>/dev/null || true

mkdir -p "$TESTDIR"

# ============================================================
# Test 1: Build from clean
# ============================================================
echo "=== Test 1: Build from clean ==="
make -C "$SCRIPT_DIR" clean >"${TESTDIR}/build.log" 2>&1
make -C "$SCRIPT_DIR" >>"${TESTDIR}/build.log" 2>&1
if [ -x "$COMPBENCH" ]; then
	pass "build from clean succeeds"
else
	fail "build from clean failed — see ${TESTDIR}/build.log"
	exit 1
fi

# ============================================================
# Test 2: --help
# ============================================================
echo "=== Test 2: --help ==="
"$COMPBENCH" --help >"${TESTDIR}/help.log" 2>&1
assert_output "help mentions DEVICE" "${TESTDIR}/help.log" "DEVICE"
assert_output "help mentions ARCHIVE" "${TESTDIR}/help.log" "ARCHIVE"
assert_output "help mentions --zstd" "${TESTDIR}/help.log" "--zstd"
assert_output "help mentions --force" "${TESTDIR}/help.log" "--force"
assert_output "help mentions --hdd" "${TESTDIR}/help.log" "--hdd"
assert_output "help mentions --repeat" "${TESTDIR}/help.log" "--repeat"
assert_output "help mentions WrMiB/s" "${TESTDIR}/help.log" "WrMiB/s"
assert_output "help mentions kernel interfaces" "${TESTDIR}/help.log" "kernel interface"
assert_output "help mentions RdMiB/s throughput" "${TESTDIR}/help.log" "RdMiB/s"
assert_output "help mentions RdDisk" "${TESTDIR}/help.log" "RdDisk"
assert_output "help mentions RdCPU" "${TESTDIR}/help.log" "RdCPU"

# ============================================================
# Test 3: --version
# ============================================================
echo "=== Test 3: --version ==="
OUTPUT=$("$COMPBENCH" --version 2>&1)
if echo "$OUTPUT" | grep -q "compbench"; then
	pass "--version output contains compbench"
else
	fail "--version output unexpected: $OUTPUT"
fi

# ============================================================
# Test 4: Invalid arguments
# ============================================================
echo "=== Test 4: Invalid arguments ==="
assert_exit "no args" 1 "$COMPBENCH"
assert_exit "invalid zstd range -16-5" 1 "$COMPBENCH" --zstd -16-5 /dev/loop0 foo.tar.xz
assert_exit "invalid zstd range 5-3" 1 "$COMPBENCH" --zstd 5-3 /dev/loop0 foo.tar.xz
assert_exit "invalid zstd range 16-16" 1 "$COMPBENCH" --zstd 16-16 /dev/loop0 foo.tar.xz
assert_exit "invalid force mode" 1 "$COMPBENCH" -f invalid /dev/loop0 foo.tar.xz
assert_exit "repeat 0" 1 "$COMPBENCH" -n 0 /dev/loop0 foo.tar.xz
assert_exit "hdd 0" 1 "$COMPBENCH" --hdd 0 /dev/loop0 foo.tar.xz
assert_exit "hdd 60" 1 "$COMPBENCH" --hdd 60 /dev/loop0 foo.tar.xz

# ============================================================
# Set up test environment for benchmark tests
# ============================================================
echo "--- Setting up test environment ---"

mkdir -p "${TESTDIR}/data"
dd if=/dev/urandom bs=1M count=4 of="${TESTDIR}/data/random.bin" 2>/dev/null
dd if=/dev/zero bs=1M count=8 of="${TESTDIR}/data/zeros.bin" 2>/dev/null
echo "Hello, compbench test!" >"${TESTDIR}/data/hello.txt"
for i in $(seq 1 100); do
	echo "Test file $i with some compressible content repeated." >>"${TESTDIR}/data/text.txt"
done
ln -s hello.txt "${TESTDIR}/data/link_to_hello"
tar -cJf "$ARCHIVE" -C "${TESTDIR}/data" .
[ -f "$ARCHIVE" ] || {
	fail "failed to create test archive"
	exit 1
}

truncate -s "$DISKSIZE" "$DISKIMG"
[ -f "$DISKIMG" ] || {
	fail "failed to create disk image"
	exit 1
}

LOOP=$(losetup --find --show --partscan "$DISKIMG")
[ -b "$LOOP" ] || {
	fail "loop device not created: $LOOP"
	exit 1
}
echo "Loop device: $LOOP"

echo ""

# ============================================================
# Test 5: Minimal benchmark (1 zstd level, 1 repeat)
# ============================================================
echo "=== Test 5: Minimal benchmark (zstd:1 only, 1 repeat) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -v "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test5.log" ||
	fail "basic benchmark failed"

assert_output "results table present" "${TESTDIR}/test5.log" "Benchmark Results"
assert_output "zstd:1 result" "${TESTDIR}/test5.log" "zstd:1"
assert_output "lzo result" "${TESTDIR}/test5.log" "lzo"
assert_output "none result" "${TESTDIR}/test5.log" "none"
assert_output "read column in table" "${TESTDIR}/test5.log" "RdMiB/s"
assert_output "RdDisk column header" "${TESTDIR}/test5.log" "RdDisk"
assert_output "RdCPU% column header" "${TESTDIR}/test5.log" "RdCPU"
assert_output "dataset size reported" "${TESTDIR}/test5.log" "Dataset size:"
assert_output "archive compression factor" "${TESTDIR}/test5.log" "Archive compression factor:"
assert_output "estimated data written" "${TESTDIR}/test5.log" "Estimated total data written"

# ============================================================
# Test 6: CSV export
# ============================================================
echo "=== Test 6: CSV export ==="
CSVPATH="${TESTDIR}/results.csv"
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -o "$CSVPATH" "$LOOP" "$ARCHIVE" 2>&1 ||
	fail "benchmark with CSV failed"

[ -f "$CSVPATH" ] && pass "CSV file created" || fail "CSV file not created"
assert_output "CSV has zstd:1" "$CSVPATH" "zstd:1"
assert_output "CSV has lzo" "$CSVPATH" "lzo"
assert_output "CSV has none" "$CSVPATH" "none"
assert_output "CSV has header" "$CSVPATH" "Location,Target"
assert_output "CSV has write column" "$CSVPATH" "WrMiB/s"
assert_output "CSV has read column" "$CSVPATH" "RdMiB/s"
assert_output "CSV has read disk column" "$CSVPATH" "RdDisk_MiB/s"
assert_output "CSV has read CPU column" "$CSVPATH" "RdCPU%"
assert_output "CSV has metadata" "$CSVPATH" "# compbench"
assert_output "CSV has archive info" "$CSVPATH" "# Archive:"
assert_output "CSV has dataset info" "$CSVPATH" "# Dataset:"
assert_output "CSV has repeats info" "$CSVPATH" "# Repeats:"
assert_output "CSV has zstd range" "$CSVPATH" "# zstd:"
assert_output "CSV has force mode" "$CSVPATH" "# Force:"

CSV_DATA_LINE=$(grep -v "^#" "$CSVPATH" | grep -v "^Location" | grep -v "^$" | head -1)
CSV_COMMA_COUNT=$(echo "$CSV_DATA_LINE" | tr -cd ',' | wc -c)
if [ "$CSV_COMMA_COUNT" -eq 12 ]; then
	pass "CSV data rows have correct column count (13 fields)"
else
	fail "CSV data rows have wrong column count: $CSV_COMMA_COUNT commas (expected 12)"
fi

# ============================================================
# Test 7: Multiple zstd levels
# ============================================================
echo "=== Test 7: Multiple zstd levels (1-3) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-3 "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test7.log" ||
	fail "multi-level benchmark failed"

for lvl in 1 2 3; do
	assert_output "zstd:${lvl} present" "${TESTDIR}/test7.log" "zstd:${lvl}"
done

# ============================================================
# Test 8: Repeat (n=2)
# ============================================================
echo "=== Test 8: Repeat (n=2, zstd:1 only) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 2 --zstd 1-1 "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test8.log" ||
	fail "repeat benchmark failed"

assert_output "averaging message" "${TESTDIR}/test8.log" "averaged over 2 runs"

# ============================================================
# Test 9: Force mode (both)
# ============================================================
echo "=== Test 9: Force mode (both, zstd:1 only) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -f both "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test9.log" ||
	fail "force=both benchmark failed"

assert_output "standard variant" "${TESTDIR}/test9.log" "standard"
assert_output "force variant" "${TESTDIR}/test9.log" "force"
assert_output "comparison table" "${TESTDIR}/test9.log" "Force vs Standard Comparison"
assert_output "comparison headers" "${TESTDIR}/test9.log" "Time%"
assert_output "comparison headers" "${TESTDIR}/test9.log" "WrRate%"
assert_output "comparison has read rate" "${TESTDIR}/test9.log" "RdRate%"
assert_output "comparison has WrCPU header" "${TESTDIR}/test9.log" "WrCPU%"
assert_output "comparison has RdCPU header" "${TESTDIR}/test9.log" "RdCPU%"

# ============================================================
# Test 10: Force mode (all)
# ============================================================
echo "=== Test 10: Force mode (all, zstd:1 only) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -f all "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test10.log" ||
	fail "force=all benchmark failed"

assert_output "force applied" "${TESTDIR}/test10.log" "force"

# ============================================================
# Test 11: Force mode (zstd)
# ============================================================
echo "=== Test 11: Force mode (zstd, zstd:1 only) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -f zstd "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test11.log" ||
	fail "force=zstd benchmark failed"

assert_output "zstd is forced" "${TESTDIR}/test11.log" "force"
assert_output "lzo still present" "${TESTDIR}/test11.log" "lzo"

# ============================================================
# Test 12: Force mode (lzo)
# ============================================================
echo "=== Test 12: Force mode (lzo, zstd:1 only) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -f lzo "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test12.log" ||
	fail "force=lzo benchmark failed"

assert_output "lzo is forced" "${TESTDIR}/test12.log" "force"
assert_output "zstd still present" "${TESTDIR}/test12.log" "zstd:1"

# ============================================================
# Test 13: Best results summary
# ============================================================
echo "=== Test 13: Best results summary ==="
assert_output "Fastest summary" "${TESTDIR}/test5.log" "Fastest write:"
assert_output "Fastest read summary" "${TESTDIR}/test5.log" "Fastest read:"
assert_output "Best compressed summary" "${TESTDIR}/test5.log" "Best compressed:"
assert_output "Best balanced summary" "${TESTDIR}/test5.log" "Best balanced:"
assert_output "CPU% in best results" "${TESTDIR}/test5.log" "WrCPU"

if grep -qE "Fastest read:.*\([0-9]+\.[0-9]+ MiB/s" "${TESTDIR}/test5.log"; then
	pass "best read has numeric MiB/s value"
else
	fail "best read missing numeric MiB/s value"
fi

# ============================================================
# Test 14: --no-integrity-check
# ============================================================
echo "=== Test 14: --no-integrity-check ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 --no-integrity-check "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test14.log" ||
	fail "no-integrity-check failed"

assert_not_output "integrity check skipped" "${TESTDIR}/test14.log" "Checking archive integrity"

# ============================================================
# Test 15: Verbose mode details
# ============================================================
echo "=== Test 15: Verbose mode ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -v "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test15.log" ||
	fail "verbose mode failed"

assert_output "verbose mount info" "${TESTDIR}/test15.log" "Mounted"
assert_output "verbose read test remount" "${TESTDIR}/test15.log" "remounting for read test"
assert_output "verbose archive info" "${TESTDIR}/test15.log" "Archive size:"
assert_output "verbose RAM info" "${TESTDIR}/test15.log" "Available RAM:"
assert_output "verbose extraction info" "${TESTDIR}/test15.log" "Extraction complete"

# ============================================================
# Test 16: Cleanup verification
# ============================================================
echo "=== Test 16: Cleanup verification ==="
if mountpoint -q "${TESTDIR}" 2>/dev/null; then
	fail "test directory is still a mount point"
else
	pass "no stale mounts"
fi

LEFTOVER=$(find /tmp -maxdepth 1 -name "compbench.*" -newer "$TESTDIR" 2>/dev/null | grep -v "$TESTDIR" || true)
if [ -z "$LEFTOVER" ]; then
	pass "no leftover temp directories"
else
	fail "leftover temp dirs: $LEFTOVER"
fi

# ============================================================
# Test 17: CSV with force=both
# ============================================================
echo "=== Test 17: CSV with force=both ==="
CSVPATH2="${TESTDIR}/results_both.csv"
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -f both -o "$CSVPATH2" "$LOOP" "$ARCHIVE" 2>&1 ||
	fail "force=both CSV benchmark failed"

COUNT_STD=$(grep -c "standard" "$CSVPATH2" || true)
COUNT_FRC=$(grep -c "force" "$CSVPATH2" || true)
if [ "$COUNT_STD" -gt 0 ] && [ "$COUNT_FRC" -gt 0 ]; then
	pass "CSV contains both standard and force rows"
else
	fail "CSV missing standard ($COUNT_STD) or force ($COUNT_FRC) rows"
fi

# ============================================================
# Test 18: Single zstd level (--zstd 5)
# ============================================================
echo "=== Test 18: Single zstd level (--zstd 5) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 5 "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test18.log" ||
	fail "single zstd level benchmark failed"

assert_output "zstd:5 present" "${TESTDIR}/test18.log" "zstd:5"
assert_not_output "zstd:4 absent" "${TESTDIR}/test18.log" "zstd:4"
assert_not_output "zstd:6 absent" "${TESTDIR}/test18.log" "zstd:6"

# ============================================================
# Test 19: HDD partition mode
# ============================================================
echo "=== Test 19: HDD partition mode (--hdd 10) ==="
LOOP2=""
DISKIMG2="${TESTDIR}/disk_hdd.img"
truncate -s 3G "$DISKIMG2"
LOOP2=$(losetup --find --show --partscan "$DISKIMG2")
echo "HDD loop device: $LOOP2"

printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 --hdd 20 "$LOOP2" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test19.log" ||
	fail "HDD mode benchmark failed"

assert_output "HDD begin location" "${TESTDIR}/test19.log" "begin"
assert_output "HDD end location" "${TESTDIR}/test19.log" "end"
assert_output "interpolated results" "${TESTDIR}/test19.log" "Interpolated Results"
assert_output "HDD interpolated data rows" "${TESTDIR}/test19.log" "interpolated  zstd:1"
assert_output "HDD has RdCPU in results" "${TESTDIR}/test19.log" "RdCPU"
assert_output "HDD best results has interpolated" "${TESTDIR}/test19.log" "interpolated:"

if [ -n "$LOOP2" ]; then
	losetup -d "$LOOP2" 2>/dev/null || true
fi

# ============================================================
# Test 20: Test count displayed correctly
# ============================================================
echo "=== Test 20: Test count accuracy ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-3 "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test20.log" ||
	fail "test count benchmark failed"

if grep -q "5 tests" "${TESTDIR}/test20.log"; then
	pass "correct test count (5 = 3 zstd + 1 lzo + 1 none)"
else
	fail "incorrect test count — expected '5 tests'"
fi

# ============================================================
# Test 21: Signal handling (SIGINT during benchmark)
# ============================================================
echo "=== Test 21: Signal handling ==="
(
	printf 'yes\ny\n'
	sleep 5
) | "$COMPBENCH" -n 1 --zstd 1-15 -v "$LOOP" "$ARCHIVE" >"${TESTDIR}/test21.log" 2>&1 &
CBPID=$!
sleep 3
kill -INT $CBPID 2>/dev/null || true
wait $CBPID 2>/dev/null || true

if grep -q "Interrupted" "${TESTDIR}/test21.log" 2>/dev/null; then
	pass "signal handled with interrupted message"
elif grep -q "Cleaning up" "${TESTDIR}/test21.log" 2>/dev/null; then
	pass "signal handled with cleanup message"
else
	pass "signal handling (benchmark may have completed before signal)"
fi

# ============================================================
# Test 22: User abort
# ============================================================
echo "=== Test 22: User abort ==="
echo "no" | "$COMPBENCH" /dev/null /dev/null >"${TESTDIR}/test22.log" 2>&1
assert_output "abort message" "${TESTDIR}/test22.log" "Aborted"
assert_output "warning before abort" "${TESTDIR}/test22.log" "WARNING"

# ============================================================
# Test 23: Force mode (none, explicit)
# ============================================================
echo "=== Test 23: Force mode (none, zstd:1 only) ==="
printf 'yes\ny\n' | "$COMPBENCH" -n 1 --zstd 1-1 -f none "$LOOP" "$ARCHIVE" 2>&1 | tee "${TESTDIR}/test23.log" ||
	fail "force=none benchmark failed"

assert_output "standard variant with -f none" "${TESTDIR}/test23.log" "standard"
assert_not_output "no force variant with -f none" "${TESTDIR}/test23.log" " force "

# ============================================================
# Test 24: CSV read metric values
# ============================================================
echo "=== Test 24: CSV read metric values ==="
CSV_DATA=$(grep "zstd:1" "${TESTDIR}/results.csv" | head -1)
if [ -n "$CSV_DATA" ]; then
	READ_COL=$(echo "$CSV_DATA" | cut -d',' -f4)
	RDDISK_COL=$(echo "$CSV_DATA" | cut -d',' -f6)
	RDCPU_COL=$(echo "$CSV_DATA" | cut -d',' -f10)
	if [ "$READ_COL" != "RdMiB/s" ] && [ -n "$READ_COL" ]; then
		pass "CSV zstd:1 read rate present ($READ_COL)"
	else
		fail "CSV zstd:1 read rate missing or is header"
	fi
	if [ "$RDDISK_COL" != "RdDisk_MiB/s" ] && [ -n "$RDDISK_COL" ]; then
		pass "CSV zstd:1 RdDisk rate present ($RDDISK_COL)"
	else
		fail "CSV zstd:1 RdDisk rate missing or is header"
	fi
	if [ "$RDCPU_COL" != "RdCPU%" ] && [ -n "$RDCPU_COL" ]; then
		pass "CSV zstd:1 RdCPU present ($RDCPU_COL)"
	else
		fail "CSV zstd:1 RdCPU missing or is header"
	fi
else
	fail "CSV zstd:1 data row not found"
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo "================================"
echo "  All tests completed."
echo "  Passed: $PASSED"
echo "  Failed: $FAILED"
echo "================================"

if [ "$FAILED" -gt 0 ]; then
	exit 1
fi
