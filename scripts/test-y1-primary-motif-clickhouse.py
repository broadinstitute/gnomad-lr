#!/usr/bin/env python3
"""Disposable ClickHouse lifecycle test for synthetic reviewed primary-motif rows.

The test never reads or mutates production data. It launches a loopback-only
ClickHouse 26.3.9.8 container, initializes the optional schema, stages two
synthetic reviewed AoU runs, proves corruption fails closed, and exercises
produced -> independently_verified -> accepted_frozen for the clean run.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
IMAGE = (
    "clickhouse/clickhouse-server:26.3.9.8@"
    "sha256:537014a67ce8bf1f5c79c2e2b26fb30b8285a86ffff03875bb14ed17ea35db62"
)


def request(endpoint: str, query: str, database: str | None = None, *, raw: bool = False):
    url = endpoint
    if database:
        url += "?" + urllib.parse.urlencode({"database": database})
    req = urllib.request.Request(url, data=query.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            body = response.read()
            return body if raw else body.decode()
    except urllib.error.HTTPError as error:
        raise RuntimeError(error.read().decode(errors="replace")) from error


def wait_ready(endpoint: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if request(endpoint, "SELECT 1").strip() == "1":
                return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("ClickHouse did not become ready")


def run(binary: Path, *args: str, succeeds: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run([str(binary), *args], cwd=ROOT, text=True, capture_output=True)
    if (result.returncode == 0) != succeeds:
        raise AssertionError(
            f"expected succeeds={succeeds}, status={result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def target_args(endpoint: str, database: str) -> list[str]:
    return [
        "--endpoint", endpoint, "--database", database,
        "--target-kind", "scratch", "--auth-source", "none",
    ]


def canonical_sha(domain: bytes, value) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(domain + body).hexdigest()


def physical_snapshot(endpoint: str, database: str, run_id: str) -> dict:
    specs = [
        ("lr_y1_primary_motif_loci", "chrom, source_position, source_variant_id"),
        ("lr_y1_primary_motif_allele_bins", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), exact_units"),
        ("lr_y1_primary_motif_genotype_pairs", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), shorter_exact_units, longer_exact_units, shorter_allele_index, longer_allele_index"),
        ("lr_y1_primary_motif_genotype_margins", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), allele_index"),
    ]
    counts, digests = [], []
    for table, order in specs:
        counts.append(int(request(endpoint, f"SELECT count() FROM {table} WHERE product_run_id = '{run_id}'", database)))
        data = request(
            endpoint,
            f"SELECT * FROM {table} WHERE product_run_id = '{run_id}' ORDER BY {order} FORMAT RowBinary",
            database,
            raw=True,
        )
        domain = f"Y1_PRIMARY_MOTIF_ROWBINARY_V1\0{table}\0{run_id}".encode()
        hasher = hashlib.sha256()
        hasher.update(b"gnomad-lr-y1-canonical-content-v1\0")
        hasher.update(domain)
        hasher.update(b"\0")
        hasher.update(data)
        digests.append(hasher.hexdigest())
    return {
        "product_run_id": run_id,
        "locus_rows": counts[0], "bin_rows": counts[1],
        "genotype_pair_rows": counts[2], "genotype_margin_rows": counts[3],
        "locus_content_sha256": digests[0], "bin_content_sha256": digests[1],
        "genotype_pair_content_sha256": digests[2],
        "genotype_margin_content_sha256": digests[3],
    }


def insert_json(endpoint: str, database: str, table: str, rows: list[dict]) -> None:
    body = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    request(endpoint, f"INSERT INTO {table} FORMAT JSONEachRow\n{body}", database)


def reviewed_registry(path: Path) -> tuple[dict, str]:
    value = json.loads((ROOT / "sources/y1/primary-repeat-registry.json").read_text())
    value["approval_state"] = "REVIEWED"
    for entry in value["entries"]:
        entry["approval_state"] = "REVIEWED"
        entry["reviewer"] = "synthetic-clickhouse-fixture"
        entry["approval_receipt"] = "synthetic-reviewed-fixture-only"
        entry["catalog_digest"] = "a" * 64
    value.pop("content_sha256")
    digest = hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    value["content_sha256"] = digest
    path.write_text(json.dumps(value, indent=2) + "\n")
    return value, digest


def stage_synthetic_produced(
    endpoint: str, database: str, run_id: str, registry: dict, registry_digest: str
) -> dict:
    entry = next(item for item in registry["entries"] if item["chrom"] == "chr6")
    now = int(time.time())
    empty = hashlib.sha256(b"").hexdigest()
    base_run = {
        "product_run_id": run_id, "release": "y1", "cohort": "aou",
        "reference_genome": "GRCh38", "chrom": "chr6",
        "primary_database": database, "primary_run_id": "synthetic-primary",
        "registry_digest": registry_digest, "registry_approval_state": "REVIEWED",
        "metric": "WHOLE_RECORD_EXACT_PRIMARY_MOTIF_UNITS_V1",
        "algorithm_version": "SYNTHETIC_REVIEWED_FIXTURE_V1", "algorithm_sha256": "1" * 64,
        "executable_revision": "synthetic", "executable_sha256": "2" * 64,
        "anchor_rule": "TRID_ENVELOPE_LEFT_PADDING_BASE_V1",
        "source_inventory_sha256": registry["source_inventory_sha256"],
        "bounds_status": "planned", "locus_content_sha256": empty,
        "bin_content_sha256": empty, "receipt_sha256": empty,
        "created_at": now, "updated_at": now,
        "operator_identity": "synthetic-test", "message": "planned synthetic reviewed fixture",
    }
    planned = dict(base_run, revision=1, state="planned")
    producing = dict(base_run, revision=2, state="producing", bounds_status="planned")
    insert_json(endpoint, database, "lr_y1_primary_motif_runs", [planned, producing])
    insert_json(endpoint, database, "lr_y1_primary_motif_loci", [{
        "product_run_id": run_id, "release": "y1", "cohort": "aou",
        "reference_genome": "GRCh38", "chrom": "chr6", "primary_run_id": "synthetic-primary",
        "source_variant_id": entry["source_variant_id"], "canonical_locus_id": entry["canonical_locus_id"],
        "source_position": entry["source_position"], "registry_digest": registry_digest,
        "source_record_sha256": "3" * 64, "component_digest": "4" * 64,
        "registry_approval_state": "REVIEWED", "metric": "WHOLE_RECORD_EXACT_PRIMARY_MOTIF_UNITS_V1",
        "algorithm_sha256": "1" * 64, "allele_receipt_sha256": "5" * 64,
        "genotype_status": "UNAVAILABLE", "genotype_reason_code": "AGGREGATE_ONLY_SOURCE_NO_GT_PAIRING",
        "bounds_status": "complete_no_truncation", "status": "complete",
    }])
    insert_json(endpoint, database, "lr_y1_primary_motif_allele_bins", [{
        "product_run_id": run_id, "release": "y1", "cohort": "aou",
        "reference_genome": "GRCh38", "chrom": "chr6", "primary_run_id": "synthetic-primary",
        "source_variant_id": entry["source_variant_id"], "canonical_locus_id": entry["canonical_locus_id"],
        "registry_digest": registry_digest, "metric": "WHOLE_RECORD_EXACT_PRIMARY_MOTIF_UNITS_V1",
        "division": "all", "exact_units": 30, "allele_copies": 2,
        "reference_copies": 2, "alternate_copies": 0, "stratum_an": 2,
        "stratum_alt_ac": 0, "stratum_ref_copies": 2, "stratum_receipt_sha256": "6" * 64,
    }])
    physical = physical_snapshot(endpoint, database, run_id)
    produced = dict(
        base_run, revision=3, state="produced", bounds_status="complete_no_truncation",
        locus_rows=physical["locus_rows"], bin_rows=physical["bin_rows"],
        genotype_pair_rows=0, genotype_margin_rows=0,
        locus_content_sha256=physical["locus_content_sha256"],
        bin_content_sha256=physical["bin_content_sha256"],
        genotype_pair_content_sha256=None, genotype_margin_content_sha256=None,
        message="synthetic aggregate production complete",
    )
    insert_json(endpoint, database, "lr_y1_primary_motif_runs", [produced])
    return physical


def write_receipt(path: Path, run_id: str, registry: dict, digest: str, physical: dict) -> None:
    receipt = {
        "contract": "Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1",
        "product_run_id": run_id, "primary_run_id": "synthetic-primary", "release": "y1",
        "cohort": "aou", "reference_genome": "GRCh38", "chrom": "chr6",
        "source_inventory_sha256": registry["source_inventory_sha256"],
        "source_manifest_sha256": "7" * 64,
        "source_uri": "gs://synthetic-fixture/source.vcf.gz", "source_generation": "1",
        "source_size_bytes": 1, "source_md5_base64": base64.b64encode(bytes(16)).decode(),
        "source_index_uri": "gs://synthetic-fixture/source.vcf.gz.tbi", "source_index_generation": "2",
        "source_index_size_bytes": 1, "source_index_md5_base64": base64.b64encode(bytes(16)).decode(),
        "registry_digest": digest, "registry_approval_state": "REVIEWED",
        "registered_locus_ids": ["y1-grch38-atxn1-tgc-v1"],
        "complete_strata": True, "no_truncation": True,
        "exact_ac_an_and_genotype_margins": True,
        "metadata_run_id": None, "metadata_receipt_sha256": None,
        "metadata_manifest_sha256": None, "physical": physical,
    }
    receipt["receipt_sha256"] = canonical_sha(
        b"Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1\0", receipt
    )
    path.write_text(json.dumps(receipt, indent=2) + "\n")


def main() -> int:
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError("docker is required")
    name = f"gnomad-lr-primary-motif-{os.getpid()}-{secrets.token_hex(4)}"
    subprocess.run([
        docker, "run", "--detach", "--rm", "--name", name,
        "--publish", "127.0.0.1::8123", "--env", "CLICKHOUSE_SKIP_USER_SETUP=1", IMAGE,
    ], check=True, stdout=subprocess.DEVNULL)
    try:
        mapping = subprocess.check_output([docker, "port", name, "8123/tcp"], text=True).strip()
        endpoint = f"http://127.0.0.1:{mapping.rsplit(':', 1)[1]}/"
        wait_ready(endpoint)
        subprocess.run(["cargo", "build", "--locked"], cwd=ROOT, check=True)
        binary = ROOT / "target/debug/gnomad-lr"
        database = f"gnomad_lr_y1_scratch_v5_primary_motif_{os.getpid()}"
        request(endpoint, f"CREATE DATABASE {database}")
        run(binary, "init-y1-primary-motif", *target_args(endpoint, database))
        run(binary, "init-y1-primary-motif", *target_args(endpoint, database))
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            registry, digest = reviewed_registry(temp / "reviewed-registry.json")

            corrupt_run = "synthetic-corrupt"
            corrupt_physical = stage_synthetic_produced(endpoint, database, corrupt_run, registry, digest)
            write_receipt(temp / "corrupt-receipt.json", corrupt_run, registry, digest, corrupt_physical)
            request(endpoint, "CREATE USER motif_corrupt IDENTIFIED WITH no_password SETTINGS async_insert = 0")
            request(endpoint, f"GRANT SELECT, INSERT ON {database}.* TO motif_corrupt")
            # Same row count is not enough: an extra physical bin changes count and RowBinary evidence.
            request(endpoint, f"INSERT INTO lr_y1_primary_motif_allele_bins SELECT * REPLACE(31 AS exact_units) FROM lr_y1_primary_motif_allele_bins WHERE product_run_id = '{corrupt_run}'", database)
            failure = run(
                binary, "verify-y1-primary-motif", *target_args(endpoint, database),
                "--worker-principal", "motif_corrupt", "--worker-auth-source", "passwordless-user",
                "--cohort", "aou", "--product-run-id", corrupt_run, "--primary-run-id", "synthetic-primary",
                "--registry", str(temp / "reviewed-registry.json"),
                "--independent-receipt", str(temp / "corrupt-receipt.json"),
                "--operator-identity", "synthetic-test", "--message", "must fail closed",
                "--report", str(temp / "corrupt-report.json"), succeeds=False,
            )
            assert "latest product ledger revision differs" in failure.stderr, failure.stderr
            assert request(endpoint, f"SELECT state FROM lr_y1_primary_motif_runs WHERE product_run_id = '{corrupt_run}' ORDER BY revision DESC LIMIT 1", database).strip() == "produced"

            clean_run = "synthetic-clean"
            clean_physical = stage_synthetic_produced(endpoint, database, clean_run, registry, digest)
            receipt = temp / "clean-receipt.json"
            write_receipt(receipt, clean_run, registry, digest, clean_physical)
            request(endpoint, "CREATE USER motif_clean IDENTIFIED WITH no_password SETTINGS async_insert = 0")
            request(endpoint, f"GRANT SELECT, INSERT ON {database}.* TO motif_clean")
            common = [
                *target_args(endpoint, database), "--worker-principal", "motif_clean",
                "--worker-auth-source", "passwordless-user", "--cohort", "aou",
                "--product-run-id", clean_run, "--primary-run-id", "synthetic-primary",
                "--registry", str(temp / "reviewed-registry.json"),
                "--independent-receipt", str(receipt), "--operator-identity", "synthetic-test",
            ]
            run(binary, "verify-y1-primary-motif", *common, "--message", "independently verified", "--report", str(temp / "verify.json"))
            run(binary, "finalize-y1-primary-motif", *common, "--message", "accepted synthetic fixture", "--report", str(temp / "finalize.json"))
            state = request(endpoint, f"SELECT state FROM lr_y1_primary_motif_runs WHERE product_run_id = '{clean_run}' ORDER BY revision DESC LIMIT 1", database).strip()
            assert state == "accepted_frozen", state
        request(endpoint, f"DROP DATABASE {database} SYNC")
        print("Primary-motif disposable ClickHouse lifecycle passed")
        return 0
    finally:
        subprocess.run([docker, "rm", "--force", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    raise SystemExit(main())
