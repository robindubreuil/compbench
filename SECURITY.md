# Security Policy

## Supported Versions

Only the latest release is supported.

## Reporting a Vulnerability

Use GitHub's **private vulnerability reporting**: go to the
[Security tab](https://github.com/robindubreuil/compbench/security) and click
**Report a vulnerability**.

Please include how to reproduce the issue and which environment it affects
(kernel version, filesystem type). You can expect an acknowledgement within a
few days. Please avoid opening a public issue for anything security-sensitive.

## Scope

Compbench runs as root, formats the target block device, and downloads the
test archive from a user-supplied URL. Reports in these areas are welcome:

- Archive download or extraction handling (e.g. path traversal, unvalidated
  input from the fetched archive).
- Device selection logic — anything that could write to a device other than
  the one confirmed by the user.
- Memory corruption in the C code (buffer overflows, unchecked parsing).

### Out of scope

- Destructive operations on the **target device** itself: compbench formats
  `DEVICE` by design, after printing the disk model/size and asking for
  confirmation (see the README warning).
- Anything requiring an attacker to already control the archive URL or the
  command line — the user already decides what to benchmark.
- Social engineering, DoS by exhausting disk space, and issues in
  dependencies of the build/CI toolchain; report those upstream.
