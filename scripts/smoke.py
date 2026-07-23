#!/usr/bin/env python3
"""Run every loader against bounded real-data subsets in an isolated database."""

from __future__ import annotations

import argparse
import ipaddress
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "development" / "smoke.toml"
SAFE_DATABASE = re.compile(r"^gnomad_lr_smoke(?:_[A-Za-z0-9_]+)?$")
ALL_DATASETS = {"vcf", "coverage", "histograms", "methylation", "metadata"}
TABLES = {
    "vcf": ("lr_variants", "lr_haplotypes"),
    "coverage": ("lr_coverage",),
    "histograms": ("lr_str_histograms",),
    "methylation": ("lr_methylation", "lr_methylation_summary_mv"),
    "metadata": ("lr_sample_metadata",),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--clickhouse-url",
        default=os.environ.get("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
        help="base ClickHouse HTTP URL (default: local port 8123)",
    )
    parser.add_argument("--database", help="override the smoke database from the config")
    parser.add_argument(
        "--only",
        help="comma-separated subset: vcf,coverage,histograms,methylation,metadata",
    )
    parser.add_argument("--binary", type=Path, help="existing gnomad-lr binary")
    parser.add_argument("--no-build", action="store_true", help="do not build the debug binary")
    parser.add_argument("--no-reset", action="store_true", help="append to the existing smoke DB")
    parser.add_argument("--cleanup", action="store_true", help="drop the smoke DB after success")
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="required before targeting a non-loopback ClickHouse host",
    )
    parser.add_argument("--dry-run", action="store_true", help="print the plan without connecting")
    return parser.parse_args()


def load_config(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def selected_datasets(value: str | None) -> set[str]:
    if not value:
        return set(ALL_DATASETS)
    selected = {item.strip() for item in value.split(",") if item.strip()}
    unknown = selected - ALL_DATASETS
    if unknown:
        raise ValueError(f"unknown datasets: {', '.join(sorted(unknown))}")
    if not selected:
        raise ValueError("--only must select at least one dataset")
    return selected


def is_loopback(url: str) -> bool:
    host = urlsplit(url).hostname
    if host in {"localhost", "localhost.localdomain"}:
        return True
    if not host:
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def with_params(url: str, *, database: str | None = None, query: str | None = None) -> str:
    parts = urlsplit(url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params.pop("query", None)
    params.pop("database", None)
    if database is not None:
        params["database"] = database
    if query is not None:
        params["query"] = query
    return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", urlencode(params), ""))


def query(clickhouse_url: str, sql: str, *, database: str | None = None) -> str:
    request = Request(with_params(clickhouse_url, database=database, query=sql), data=b"", method="POST")
    try:
        with urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8").strip()
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse returned HTTP {error.code}: {body[:1000]}") from error
    except URLError as error:
        raise RuntimeError(f"cannot reach ClickHouse at {redact_url(clickhouse_url)}: {error}") from error


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port:
        host = f"{host}:{parts.port}"
    params = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        params.append((key, "***" if key.lower() in {"password", "token"} else value))
    return urlunsplit((parts.scheme, host, parts.path, urlencode(params), ""))


def run(command: list[str], *, dry_run: bool) -> None:
    displayed = [redact_url(arg) if arg.startswith(("http://", "https://")) else arg for arg in command]
    print("+", " ".join(displayed))
    if not dry_run:
        subprocess.run(command, cwd=ROOT, check=True)


def parse_region(region: str) -> tuple[str, str, str]:
    try:
        chrom, interval = region.replace(",", "").split(":", 1)
        start, stop = interval.split("-", 1)
        int(start)
        int(stop)
    except ValueError as error:
        raise ValueError(f"invalid smoke region {region!r}; expected chr:start-stop") from error
    if not chrom.startswith("chr"):
        chrom = f"chr{chrom}"
    return chrom, start, stop


def loader_commands(binary: Path, config: dict, target_url: str, datasets: set[str]) -> list[list[str]]:
    smoke = config["smoke"]
    inputs = config["inputs"]
    region = str(smoke["region"])
    chrom, start, stop = parse_region(region)
    record_limit = str(smoke["vcf_record_limit"])
    row_limit = str(smoke["row_limit"])
    common_target = ["--clickhouse-url", target_url]
    commands: list[list[str]] = []

    if "vcf" in datasets:
        commands.append(
            [
                str(binary),
                "load",
                "all",
                "--region",
                region,
                "--vcf-path",
                str(inputs["vcf"]),
                "--limit",
                record_limit,
                *common_target,
            ]
        )
    if "coverage" in datasets:
        # Coverage is sequential rather than tabix-indexed. A row cap stops the
        # decompressor early instead of scanning 18 GB to reach chr22.
        commands.append(
            [
                str(binary),
                "load",
                "coverage",
                "--gcs-path",
                str(inputs["coverage"]),
                "--limit",
                row_limit,
                *common_target,
            ]
        )
    if "histograms" in datasets:
        commands.append(
            [
                str(binary),
                "load",
                "histograms",
                "--gcs-path",
                str(inputs["histograms"]),
                "--limit",
                row_limit,
                *common_target,
            ]
        )
    if "methylation" in datasets:
        commands.append(
            [
                str(binary),
                "load",
                "methylation",
                "--bed-path",
                str(inputs["methylation_bed"]),
                "--sample-id",
                str(inputs["methylation_sample"]),
                "--chrom",
                chrom,
                "--start",
                start,
                "--stop",
                stop,
                "--limit",
                row_limit,
                *common_target,
            ]
        )
    if "metadata" in datasets:
        commands.append(
            [
                str(binary),
                "load",
                "metadata",
                "--csv-url",
                str(inputs["metadata"]),
                "--limit",
                row_limit,
                *common_target,
            ]
        )
    return commands


def main() -> int:
    args = parse_args()
    config = load_config(args.config)
    datasets = selected_datasets(args.only)
    database = args.database or str(config["smoke"]["database"])

    if not SAFE_DATABASE.fullmatch(database):
        raise ValueError(
            "smoke database must match gnomad_lr_smoke[_suffix]; refusing a potentially destructive target"
        )
    if not is_loopback(args.clickhouse_url) and not args.allow_remote:
        raise ValueError(
            "remote ClickHouse target refused; use --allow-remote with an isolated smoke database"
        )

    binary = args.binary or Path(os.environ.get("GNOMAD_LR_BIN", ROOT / "target/debug/gnomad-lr"))
    if not binary.is_absolute():
        binary = (ROOT / binary).resolve()
    target_url = with_params(args.clickhouse_url, database=database)

    print("gnomad-lr source-backed smoke plan")
    print(f"  ClickHouse: {redact_url(args.clickhouse_url)}")
    print(f"  Database:   {database}")
    print(f"  Datasets:   {', '.join(sorted(datasets))}")
    print(f"  Config:     {args.config}")

    if not args.no_build:
        run(
            ["cargo", "build", "--locked", "--features", "clickhouse"],
            dry_run=args.dry_run,
        )
    elif not args.dry_run and not binary.is_file():
        raise FileNotFoundError(f"binary not found: {binary}")

    commands = loader_commands(binary, config, target_url, datasets)
    if args.dry_run:
        print(f"+ DROP/CREATE DATABASE `{database}`" if not args.no_reset else f"+ CREATE DATABASE `{database}`")
        run([str(binary), "init", "--clickhouse-url", target_url], dry_run=True)
        for command in commands:
            run(command, dry_run=True)
        return 0

    query(args.clickhouse_url, "SELECT 1")
    if not args.no_reset:
        query(args.clickhouse_url, f"DROP DATABASE IF EXISTS `{database}`")
    query(args.clickhouse_url, f"CREATE DATABASE IF NOT EXISTS `{database}`")

    run([str(binary), "init", "--clickhouse-url", target_url], dry_run=False)
    for command in commands:
        run(command, dry_run=False)

    expected_tables = sorted({table for dataset in datasets for table in TABLES[dataset]})
    counts: dict[str, int] = {}
    for table in expected_tables:
        value = query(args.clickhouse_url, f"SELECT count() FROM `{table}`", database=database)
        counts[table] = int(value)

    empty = [table for table, count in counts.items() if count == 0]
    print("\nSmoke row counts:")
    for table, count in counts.items():
        print(f"  {table:32} {count:>10,}")
    if empty:
        raise RuntimeError(f"smoke load produced empty tables: {', '.join(empty)}")

    print(f"\nSmoke passed. Inspect with CLICKHOUSE_URL='{redact_url(target_url)}'.")
    if args.cleanup:
        query(args.clickhouse_url, f"DROP DATABASE `{database}`")
        print(f"Dropped {database}.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, ValueError, RuntimeError, FileNotFoundError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
