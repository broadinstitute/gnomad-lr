#!/usr/bin/env python3

import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "verify_worker", Path(__file__).with_name("verify-worker-artifact.py")
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

REVISION = "0123456789abcdef0123456789abcdef01234567"
IDENTITY = f"gnomad-lr/{REVISION}/x86_64-linux-release/features-clickhouse"


def elf(marker: bytes = IDENTITY.encode()) -> bytes:
    value = bytearray(64)
    value[:6] = b"\x7fELF\x02\x01"
    value[18:20] = (62).to_bytes(2, "little")
    return bytes(value) + marker


class WorkerArtifactTest(unittest.TestCase):
    def check(
        self,
        payload: bytes,
        *,
        revision: str = REVISION,
        identity: str = IDENTITY,
        sha256: str | None = None,
    ) -> dict:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "worker"
            path.write_bytes(payload)
            return module.verify(
                path,
                revision,
                identity,
                sha256 or hashlib.sha256(payload).hexdigest(),
            )

    def test_accepts_exact_revision_identity_sha_and_x86_64_elf(self):
        report = self.check(elf())
        self.assertEqual(report["backend_revision"], REVISION)
        self.assertEqual(report["build_identity"], IDENTITY)
        self.assertEqual(report["sha256"], hashlib.sha256(elf()).hexdigest())
        self.assertTrue(report["clean_revision_bound_identity"])

    def test_rejects_non_elf_wrong_class_endianness_and_architecture(self):
        corruptions = ((0, ord("X")), (4, 1), (5, 2), (18, 183))
        for offset, value in corruptions:
            with self.subTest(offset=offset, value=value):
                payload = bytearray(elf())
                if offset == 18:
                    payload[18:20] = value.to_bytes(2, "little")
                else:
                    payload[offset] = value
                with self.assertRaises(ValueError):
                    self.check(bytes(payload))

    def test_rejects_revision_identity_and_sha_mismatches(self):
        other_revision = "f" * 40
        cases = (
            {"revision": other_revision},
            {
                "identity": (
                    f"gnomad-lr/{REVISION}/x86_64-linux-release/features-clickhouse,zstd"
                )
            },
            {"sha256": "0" * 64},
        )
        for arguments in cases:
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                self.check(elf(), **arguments)

    def test_rejects_malformed_or_unbound_expectations(self):
        with self.assertRaises(ValueError):
            self.check(elf(), revision=REVISION.upper())
        with self.assertRaises(ValueError):
            self.check(elf(), identity=f"gnomad-lr/{'f' * 40}/x86_64-linux-release/features-clickhouse")
        with self.assertRaises(ValueError):
            self.check(elf(), sha256="abc")

    def test_rejects_dirty_and_development_provenance(self):
        for marker in (
            f"gnomad-lr/{REVISION}-dirty/x86_64-linux-release/features-clickhouse",
            f"gnomad-lr/{REVISION}/development-build",
        ):
            with self.subTest(marker=marker), self.assertRaises(ValueError):
                self.check(elf((IDENTITY + "\0" + marker).encode()))


if __name__ == "__main__":
    unittest.main()
