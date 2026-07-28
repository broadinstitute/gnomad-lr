#!/usr/bin/env python3
"""Offline, read-only attestation of a deployable gnomad-lr Linux worker."""

import argparse
import hashlib
import hmac
import json
import re
from pathlib import Path

FULL_HEX_REVISION = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
LINUX_WORKER_IDENTITY = re.compile(
    r"gnomad-lr/([0-9a-f]{40}(?:[0-9a-f]{24})?)/"
    r"x86_64-linux-release/features-[a-z0-9][a-z0-9+,_-]*\Z"
)


def verify(path: Path, revision: str, build_identity: str, expected_sha256: str) -> dict:
    """Verify artifact format, provenance, and digest against exact expected values."""
    if not FULL_HEX_REVISION.fullmatch(revision):
        raise ValueError("expected revision must be a lowercase full 40- or 64-hex Git object ID")
    identity_match = LINUX_WORKER_IDENTITY.fullmatch(build_identity)
    if not identity_match:
        raise ValueError("expected build identity is not a clean Linux worker release identity")
    if identity_match.group(1) != revision:
        raise ValueError("expected build identity is not bound to expected revision")
    if not SHA256.fullmatch(expected_sha256):
        raise ValueError("expected SHA-256 must be 64 lowercase hexadecimal characters")

    data = path.read_bytes()
    if len(data) < 20 or data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
        raise ValueError("worker is not a 64-bit little-endian ELF artifact")
    if int.from_bytes(data[18:20], "little") != 62:
        raise ValueError("worker ELF is not x86_64")
    if build_identity.encode("ascii") not in data:
        raise ValueError("worker does not embed the exact expected build identity")
    for forbidden in (b"/development-build", b"-dirty/"):
        if forbidden in data:
            raise ValueError(f"worker embeds forbidden provenance marker {forbidden.decode()}")

    actual_sha256 = hashlib.sha256(data).hexdigest()
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise ValueError(f"worker SHA-256 mismatch: got {actual_sha256}")

    return {
        "artifact": str(path),
        "bytes": len(data),
        "sha256": actual_sha256,
        "backend_revision": revision,
        "build_identity": build_identity,
        "elf_class": "ELF64",
        "architecture": "x86_64",
        "clean_revision_bound_identity": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--expected-build-identity", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(
        args.binary,
        args.expected_revision,
        args.expected_build_identity,
        args.expected_sha256,
    )
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
