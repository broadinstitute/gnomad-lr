#!/usr/bin/env python3
"""Fail-closed r1/r2 Y1 chr22 content-signature comparison.

The signature definition intentionally matches the accepted r1 query: each row is
hashed with cityHash64(toJSONString(tuple(...))) and combined with groupBitXor,
so physical ClickHouse row order does not affect the result.
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable

Key = tuple[str, str]
Value = tuple[int, int, str | None]

# Accepted r1 evidence from full-22-eb0cb343/execution/content-signatures.tsv.
# AoU carriers are an explicit eighth invariant: no row and therefore XOR 0.
EXPECTED: dict[Key, tuple[int, int]] = {
    ("hgsvc_hprc", "summaries"): (808853, 14634967967081205611),
    ("hgsvc_hprc", "alleles"): (1046072, 14614298358322652621),
    ("hgsvc_hprc", "frequencies"): (21967512, 3800520885522330351),
    ("hgsvc_hprc", "carriers"): (38285467, 5740761881423515696),
    ("aou", "summaries"): (1166762, 17948364209855283030),
    ("aou", "alleles"): (3152223, 6909096278152444077),
    ("aou", "frequencies"): (18913338, 10838463094380439429),
    ("aou", "carriers"): (0, 0),
}
ORDER = tuple(EXPECTED)
UINT64_MAX = 2**64 - 1
SAFE_DATABASE = re.compile(
    r"^(?:gnomad_lr_y1_pilot|gnomad_lr_y1_(?:scratch|serving)_[A-Za-z0-9_]+)$"
)

COLUMNS = {
    "summaries": "chrom,position,source_variant_id,ref_allele,alts,ac,an,af",
    "alleles": "chrom,position,source_variant_id,alt_index,ref_allele,alt,ac,an,af,rsids,cadd_phred,phylop,major_consequence,short_read_match_id,short_read_match_type,short_read_match_source",
    "frequencies": "chrom,position,source_variant_id,alt_index,division,ac,an,af,values_available",
    "carriers": "chrom,position,source_variant_id,alt_index,sample_id,genotype_position,gt_alleles,gt_phased",
}


def fail(message: str) -> "None":
    raise ValueError(message)


def parse_uint64(value: str, label: str) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        fail(f"{label} must be an unsigned decimal integer")
    number = int(value)
    if number > UINT64_MAX:
        fail(f"{label} exceeds UInt64")
    return number


def parse_tsv(text: str, source: str) -> dict[Key, Value]:
    """Parse legacy 4-column evidence or provenance-bearing 5-column evidence."""
    rows: dict[Key, Value] = {}
    width: int | None = None
    if not text:
        fail(f"{source}: empty TSV")
    for line_number, raw in enumerate(text.splitlines(), 1):
        if not raw or raw.startswith("#"):
            fail(f"{source}:{line_number}: blank lines and comments are not allowed")
        fields = raw.split("\t")
        if len(fields) not in {4, 5}:
            fail(f"{source}:{line_number}: expected exactly 4 or 5 tab-separated fields")
        if width is None:
            width = len(fields)
        elif len(fields) != width:
            fail(f"{source}:{line_number}: mixed legacy and provenance-bearing rows")
        cohort, table, count_text, signature_text = fields[:4]
        run_id = fields[4] if len(fields) == 5 else None
        if run_id == "":
            fail(f"{source}:{line_number}: run ID must not be empty")
        key = (cohort, table)
        if key in rows:
            fail(f"{source}:{line_number}: duplicate or mixed run association {cohort}/{table}")
        rows[key] = (
            parse_uint64(count_text, f"{source}:{line_number}: count"),
            parse_uint64(signature_text, f"{source}:{line_number}: signature"),
            run_id,
        )
    return rows


def compare(actual: dict[Key, Value], expected_runs: dict[str, str] | None = None) -> None:
    missing = set(EXPECTED) - set(actual)
    extra = set(actual) - set(EXPECTED)
    problems: list[str] = []
    if missing:
        problems.append("missing: " + ", ".join(f"{c}/{t}" for c, t in sorted(missing)))
    if extra:
        problems.append("extra: " + ", ".join(f"{c}/{t}" for c, t in sorted(extra)))
    provenance = {value[2] for value in actual.values()}
    if None in provenance and len(provenance) > 1:
        problems.append("mixed legacy and provenance-bearing rows")
    if None not in provenance:
        cohort_runs: dict[str, set[str]] = {}
        for (cohort, _), (_, _, run_id) in actual.items():
            assert run_id is not None
            cohort_runs.setdefault(cohort, set()).add(run_id)
        for cohort, run_ids in sorted(cohort_runs.items()):
            if len(run_ids) != 1:
                problems.append(f"mixed run IDs for cohort {cohort}: {', '.join(sorted(run_ids))}")
        if len({next(iter(ids)) for ids in cohort_runs.values() if len(ids) == 1}) != len(cohort_runs):
            problems.append("cohort run IDs must be distinct")
        if expected_runs:
            for key, (_, _, run_id) in actual.items():
                expected_run = expected_runs.get(key[0])
                if expected_run is not None and run_id != expected_run:
                    problems.append(
                        f"unexpected run/cohort association {run_id}/{key[0]}; expected {expected_run}/{key[0]}"
                    )
    elif expected_runs:
        problems.append("query result is missing run provenance")
    for key in ORDER:
        if key in actual and actual[key][:2] != EXPECTED[key]:
            problems.append(
                f"mismatch {key[0]}/{key[1]}: expected count/signature "
                f"{EXPECTED[key][0]}/{EXPECTED[key][1]}, got {actual[key][0]}/{actual[key][1]}"
            )
    if problems:
        fail("signature acceptance failed; " + "; ".join(problems))


def signature_sql() -> str:
    selects = []
    expected_pairs = (
        ("hgsvc_hprc", "hgsvc_run_id"),
        ("aou", "aou_run_id"),
    )
    for table, columns in COLUMNS.items():
        signature = f"groupBitXor(cityHash64(toJSONString(tuple({columns}))))"
        # Each declared run is measured only for its declared cohort. Aggregate
        # queries without GROUP BY emit the required zero row for empty tables.
        for cohort, parameter in expected_pairs:
            selects.append(
                f"SELECT '{cohort}','{table}',count(),{signature},{{{parameter}:String}} "
                f"FROM lr_y1_{table} WHERE run_id = {{{parameter}:String}} AND cohort = '{cohort}'"
            )
        # Also surface every association involving either declared run that is
        # not one of the two expected pairs. Such a row becomes extra or a
        # duplicate and is rejected before an artifact can be written.
        selects.append(
            f"SELECT cohort,'{table}',count(),{signature},run_id FROM lr_y1_{table} WHERE "
            "(run_id = {hgsvc_run_id:String} AND cohort != 'hgsvc_hprc') OR "
            "(run_id = {aou_run_id:String} AND cohort != 'aou') GROUP BY run_id,cohort"
        )
    return " UNION ALL ".join(selects) + " FORMAT TabSeparated"


def checked_endpoint(endpoint: str, allow_remote: bool) -> str:
    parsed = urllib.parse.urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        fail("endpoint must be an HTTP(S) URL with a host and no credentials")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        fail("endpoint must not contain a path, query, or fragment")
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} and not allow_remote:
        fail("remote endpoint requires --allow-remote")
    return endpoint.rstrip("/") + "/"


def query(args: argparse.Namespace) -> str:
    endpoint = checked_endpoint(args.endpoint, args.allow_remote)
    if not SAFE_DATABASE.fullmatch(args.database) or args.database == "default":
        fail(
            "database must be gnomad_lr_y1_pilot or an explicit "
            "gnomad_lr_y1_scratch_* or gnomad_lr_y1_serving_* database"
        )
    if args.hgsvc_run_id == args.aou_run_id:
        fail("cohort run IDs must be distinct")
    params = urllib.parse.urlencode({
        "database": args.database,
        "param_hgsvc_run_id": args.hgsvc_run_id,
        "param_aou_run_id": args.aou_run_id,
    })
    request = urllib.request.Request(
        endpoint + "?" + params,
        data=signature_sql().encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain; charset=utf-8"},
    )
    if args.username_env or args.password_env:
        if not args.username_env or not args.password_env:
            fail("--username-env and --password-env must be supplied together")
        username = os.environ.get(args.username_env)
        password = os.environ.get(args.password_env)
        if username is None or password is None:
            fail("configured ClickHouse credential environment variable is missing")
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        request.add_header("Authorization", "Basic " + token)
    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            body = response.read(65537)
    except (urllib.error.URLError, TimeoutError) as error:
        fail(f"ClickHouse query failed: {error}")
    if len(body) > 65536:
        fail("ClickHouse response exceeds 64 KiB")
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        fail("ClickHouse response is not UTF-8")


def render(rows: dict[Key, Value]) -> str:
    if any(rows[key][2] is None for key in ORDER):
        fail("acceptance artifact requires explicit cohort-to-run provenance")
    return "".join(
        f"{c}\t{t}\t{rows[(c, t)][0]}\t{rows[(c, t)][1]}\t{rows[(c, t)][2]}\n"
        for c, t in ORDER
    )


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    source = result.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--input",
        type=Path,
        help="candidate legacy 4-column or provenance-bearing 5-column TSV ('-' reads stdin)",
    )
    source.add_argument("--endpoint", help="ClickHouse endpoint; enables query mode")
    result.add_argument("--database")
    result.add_argument("--hgsvc-run-id")
    result.add_argument("--aou-run-id")
    result.add_argument("--allow-remote", action="store_true")
    result.add_argument("--username-env", help="name of username environment variable")
    result.add_argument("--password-env", help="name of password environment variable")
    result.add_argument("--timeout", type=float, default=3600.0)
    result.add_argument(
        "--artifact",
        type=Path,
        help="write canonical credential-free 5-column acceptance TSV after success (requires run provenance)",
    )
    return result


def main(argv: Iterable[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.artifact:
        args.artifact.unlink(missing_ok=True)  # Never leave a stale artifact after a failed invocation.
    try:
        if args.input is not None:
            if any((args.database, args.hgsvc_run_id, args.aou_run_id, args.username_env, args.password_env)):
                fail("query-only options cannot be used with --input")
            text = sys.stdin.read() if str(args.input) == "-" else args.input.read_text(encoding="utf-8")
            source = "stdin" if str(args.input) == "-" else str(args.input)
        else:
            if not args.database or not args.hgsvc_run_id or not args.aou_run_id:
                fail("query mode requires --database, --hgsvc-run-id, and --aou-run-id")
            text, source = query(args), "ClickHouse"
        rows = parse_tsv(text, source)
        expected_runs = None
        if args.input is None:
            expected_runs = {"hgsvc_hprc": args.hgsvc_run_id, "aou": args.aou_run_id}
        compare(rows, expected_runs)
        if args.artifact:
            atomic_write(args.artifact, render(rows))
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print("Y1 full-chr22 signatures match accepted r1 baseline (7 signatures; AoU carriers=0).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
