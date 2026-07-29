#!/usr/bin/env python3
"""Real ClickHouse 26.3.9.8 matrix for conservative Y1 schema initialization.

By default this launches an ephemeral loopback-only Docker container with no
volume and removes it in a finally block. --endpoint accepts only an explicitly
supplied loopback local instance; every uniquely named test database is dropped.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
IMAGE = (
    "clickhouse/clickhouse-server:26.3.9.8@"
    "sha256:537014a67ce8bf1f5c79c2e2b26fb30b8285a86ffff03875bb14ed17ea35db62"
)
VERSION = "26.3.9.8"


def request(endpoint: str, query: str, database: str | None = None) -> str:
    url = endpoint
    if database is not None:
        url += "?" + urllib.parse.urlencode({"database": database})
    req = urllib.request.Request(url, data=query.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.read().decode()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"ClickHouse query failed: {query}\n{detail}") from error


def run_init(binary: Path, endpoint: str, database: str, *, succeeds: bool) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [
            str(binary),
            "init-y1",
            "--endpoint",
            endpoint,
            "--database",
            database,
            "--target-kind",
            "scratch",
            "--auth-source",
            "none",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if (result.returncode == 0) != succeeds:
        raise AssertionError(
            f"init-y1 expected succeeds={succeeds}, status={result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def wait_ready(endpoint: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if request(endpoint, "SELECT 1").strip() == "1":
                return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("ephemeral ClickHouse did not become ready")


def launch_container() -> tuple[str, str]:
    docker = shutil.which("docker")
    if docker is None:
        raise RuntimeError("docker is unavailable; pass --endpoint for a loopback 26.3.9.8 instance")
    name = f"gnomad-lr-y1-schema-{os.getpid()}-{secrets.token_hex(4)}"
    subprocess.run(
        [
            docker,
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            "127.0.0.1::8123",
            "--env",
            "CLICKHOUSE_SKIP_USER_SETUP=1",
            IMAGE,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    try:
        mapping = subprocess.check_output(
            [docker, "port", name, "8123/tcp"], text=True
        ).strip()
        host, port = mapping.rsplit(":", 1)
        if host not in {"127.0.0.1", "localhost"}:
            raise RuntimeError(f"container HTTP port is not loopback-bound: {mapping}")
        return name, f"http://127.0.0.1:{port}/"
    except Exception:
        subprocess.run(
            [docker, "rm", "--force", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", help="loopback local ClickHouse HTTP endpoint")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/gnomad-lr")
    args = parser.parse_args()

    container: str | None = None
    endpoint = args.endpoint
    owned_databases: list[str] = []
    try:
        if endpoint is None:
            container, endpoint = launch_container()
        parsed = urllib.parse.urlparse(endpoint)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError("schema integration endpoint must be loopback HTTP")
        endpoint = endpoint.rstrip("/") + "/"
        wait_ready(endpoint)
        actual_version = request(endpoint, "SELECT version()").strip()
        if actual_version != VERSION:
            raise RuntimeError(f"expected ClickHouse {VERSION}, got {actual_version}")

        subprocess.run(["cargo", "build", "--locked"], cwd=ROOT, check=True)
        binary = args.binary.resolve()
        if not binary.exists():
            raise RuntimeError(f"requested binary does not exist after build: {binary}")
        nonce = f"{os.getpid()}_{secrets.token_hex(4)}"

        def new_database(case: str) -> str:
            database = f"gnomad_lr_y1_scratch_v4_semantic_{nonce}_{case}"
            request(endpoint, f"CREATE DATABASE {database}")
            owned_databases.append(database)
            return database

        # Fresh catalog, scoped non-authorization receipt, and no-op retry.
        fresh = new_database("fresh")
        run_init(binary, endpoint, fresh, succeeds=True)
        receipt = request(
            endpoint,
            "SELECT schema_scope, schema_version, state, contract FROM lr_y1_schema_versions FINAL FORMAT TabSeparated",
            fresh,
        ).strip()
        assert receipt == (
            "y1_full\t4\tapplied\t"
            "y1_full_v4_semantic_schema_attestation_not_load_authorization"
        ), receipt
        rendered = request(
            endpoint,
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() "
            "AND name = 'lr_y1_methylation_availability' FORMAT TabSeparatedRaw",
            fresh,
        )
        assert "allow_nullable_key = 1" in rendered
        assert "index_granularity = 8192" in rendered
        column_catalog = request(
            endpoint,
            "SELECT default_kind, default_expression, compression_codec FROM system.columns "
            "WHERE database = currentDatabase() AND table = 'lr_y1_methylation' "
            "AND name = 'coverage' FORMAT TabSeparated",
            fresh,
        ).rstrip("\n")
        assert column_catalog == "\t\t", repr(column_catalog)
        run_init(binary, endpoint, fresh, succeeds=True)
        assert request(endpoint, "SELECT count() FROM lr_y1_schema_versions", fresh).strip() == "1"

        # Partial DDL and retries are fail-closed and do not mutate the object.
        partial = new_database("partial")
        request(endpoint, "CREATE TABLE lr_y1_methylation (sentinel UInt8) ENGINE = MergeTree ORDER BY tuple()", partial)
        partial_before = request(endpoint, "SHOW CREATE TABLE lr_y1_methylation", partial)
        run_init(binary, endpoint, partial, succeeds=False)
        run_init(binary, endpoint, partial, succeeds=False)
        assert request(endpoint, "SHOW CREATE TABLE lr_y1_methylation", partial) == partial_before
        assert request(endpoint, "EXISTS TABLE lr_y1_schema_versions", partial).strip() == "0"

        # Empty and populated v3-shaped tables are both rejected unchanged.
        v3_ddl = (
            "CREATE TABLE lr_y1_methylation (ancillary_run_id String, coverage UInt16) "
            "ENGINE = MergeTree ORDER BY ancillary_run_id"
        )
        for case, populated in (("v3_empty", False), ("v3_populated", True)):
            database = new_database(case)
            request(endpoint, v3_ddl, database)
            if populated:
                request(endpoint, "INSERT INTO lr_y1_methylation VALUES ('historical', 7)", database)
            before = request(endpoint, "SHOW CREATE TABLE lr_y1_methylation", database)
            rows_before = request(endpoint, "SELECT count() FROM lr_y1_methylation", database)
            run_init(binary, endpoint, database, succeeds=False)
            assert request(endpoint, "SHOW CREATE TABLE lr_y1_methylation", database) == before
            assert request(endpoint, "SELECT count() FROM lr_y1_methylation", database) == rows_before
            assert request(endpoint, "EXISTS TABLE lr_y1_schema_versions", database).strip() == "0"

        # Exact tables without the scoped receipt cannot be adopted or repaired.
        no_receipt = new_database("no_receipt")
        run_init(binary, endpoint, no_receipt, succeeds=True)
        request(endpoint, "TRUNCATE TABLE lr_y1_schema_versions", no_receipt)
        before = request(endpoint, "SHOW CREATE TABLE lr_y1_methylation", no_receipt)
        run_init(binary, endpoint, no_receipt, succeeds=False)
        assert request(endpoint, "SHOW CREATE TABLE lr_y1_methylation", no_receipt) == before
        assert request(endpoint, "SELECT count() FROM lr_y1_schema_versions", no_receipt).strip() == "0"

        # Same names/types/keys with a changed default must not bypass receipt validation.
        altered_default = new_database("altered_default")
        run_init(binary, endpoint, altered_default, succeeds=True)
        request(
            endpoint,
            "ALTER TABLE lr_y1_methylation MODIFY COLUMN coverage UInt32 DEFAULT 7",
            altered_default,
        )
        failure = run_init(binary, endpoint, altered_default, succeeds=False)
        assert "SHOW CREATE" in failure.stderr

        # An altered effective table setting likewise invalidates live attestation.
        altered_setting = new_database("altered_setting")
        run_init(binary, endpoint, altered_setting, succeeds=True)
        altered_create = request(
            endpoint,
            "SHOW CREATE TABLE lr_y1_methylation_availability FORMAT TabSeparatedRaw",
            altered_setting,
        ).replace("index_granularity = 8192", "index_granularity = 4096")
        request(
            endpoint,
            "RENAME TABLE lr_y1_methylation_availability TO lr_y1_methylation_availability_original",
            altered_setting,
        )
        request(endpoint, altered_create, altered_setting)
        request(
            endpoint,
            "DROP TABLE lr_y1_methylation_availability_original SYNC",
            altered_setting,
        )
        failure = run_init(binary, endpoint, altered_setting, succeeds=False)
        assert "SHOW CREATE" in failure.stderr

        # The full receipt also covers non-methylation tables initialized by init-y1.
        altered_primary = new_database("altered_primary")
        run_init(binary, endpoint, altered_primary, succeeds=True)
        request(
            endpoint,
            "ALTER TABLE lr_y1_load_runs MODIFY COLUMN expected_tasks UInt32 DEFAULT 7",
            altered_primary,
        )
        failure = run_init(binary, endpoint, altered_primary, succeeds=False)
        assert "SHOW CREATE" in failure.stderr

        print("ClickHouse 26.3.9.8 Y1 schema semantic matrix passed")
        return 0
    finally:
        if endpoint is not None:
            for database in reversed(owned_databases):
                try:
                    request(endpoint, f"DROP DATABASE IF EXISTS {database} SYNC")
                except Exception as error:
                    print(f"warning: failed to drop owned database {database}: {error}", file=sys.stderr)
        if container is not None:
            docker = shutil.which("docker")
            if docker is not None:
                subprocess.run(
                    [docker, "rm", "--force", container],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )


if __name__ == "__main__":
    raise SystemExit(main())
