#!/usr/bin/env python3
"""Offline command-surface and default-safety tests for fixed Y1 wrappers."""

import os
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOAD = ROOT / "scripts/load-y1-fixed-chr22.sh"
DROP = ROOT / "scripts/drop-y1-fixed.sh"
DB = "gnomad_lr_y1_scratch_v5_current"


def run(script: Path, *args: str, env: dict[str, str]):
    return subprocess.run(
        [str(script), *args],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10,
    )


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp) / "fake-bin"
        fake.mkdir()
        marker = Path(tmp) / "called"
        # A dry-run must return before any command capable of local/cloud mutation.
        for name in ("curl", "gcloud", "git", "make", "python3", "shasum"):
            path = fake / name
            path.write_text(f"#!/bin/sh\necho {name} >>'{marker}'\nexit 97\n")
            path.chmod(0o755)
        env = os.environ.copy()
        env["PATH"] = f"{fake}:{env['PATH']}"

        result = run(LOAD, env=env)
        assert result.returncode == 0, result.stdout
        assert "mode:            dry-run" in result.stdout
        assert "0 -> 1 gate -> 8 -> 0" in result.stdout
        assert "51/51 accepted, 0 failed attempts, 0 rejects" in result.stdout
        assert "no build, network request, cloud command, or local write" in result.stdout
        assert not marker.exists(), marker.read_text() if marker.exists() else ""

        result = run(LOAD, "--max-workers", "3", env=env)
        assert result.returncode == 0 and "0 -> 1 gate -> 3 -> 0" in result.stdout
        assert not marker.exists()

        for value in ("0", "9", "many"):
            result = run(LOAD, "--max-workers", value, env=env)
            assert result.returncode == 2 and "integer from 1 through 8" in result.stdout
        result = run(LOAD, "--execute", env=env)
        assert result.returncode == 2 and f"--confirm-empty-fixed-database {DB}" in result.stdout
        assert not marker.exists()

        result = run(LOAD, "--help", env=env)
        assert result.returncode == 0 and "Dry-run is the default" in result.stdout
        assert "maximum: 8" in result.stdout

        result = run(DROP, env=env)
        assert result.returncode == 0, result.stdout
        assert "mode:       dry-run" in result.stdout
        assert "exact v5 receipt" in result.stdout
        assert "no network request, cloud command, or local write" in result.stdout
        assert not marker.exists()

        result = run(DROP, "--execute", "--confirm-drop", "wrong_database", env=env)
        assert result.returncode == 2 and f"--confirm-drop {DB}" in result.stdout
        assert not marker.exists()

        result = run(DROP, "--help", env=env)
        assert result.returncode == 0 and "writer grants outside" in result.stdout

        load_text = LOAD.read_text()
        for required in (
            'MAX_WORKERS=8',
            'MAX_SCALE_ATTEMPTS=5',
            'scale_workers_with_retry 1',
            'scale_workers_with_retry "$MAX_WORKERS"',
            'scale retry reset workers=0',
            'poll_receipts gate',
            'accepted=51/51 failed_attempts=0 rejects=0',
            '"source_records":808853',
            '"source_records":1166762',
            'roster_rows") != 292',
            'pool destroy',
            'DROP USER IF EXISTS',
        ):
            assert required in load_text, required
        drop_text = DROP.read_text()
        assert 'DROP DATABASE $DB SYNC' in drop_text
        assert 'SHOW GRANTS FOR $PRINCIPAL' in drop_text
        assert '--confirm-drop $DB' in drop_text

    print("fixed Y1 command safety tests passed")


if __name__ == "__main__":
    main()
