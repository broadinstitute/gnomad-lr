#!/usr/bin/env python3
import base64
import copy
import gzip
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


generic = load("generic_reconcile", "reconcile-y1-contig-source.py")
legacy = load("legacy_reconcile", "reconcile-y1-chr22-source.py")


def md5_base64(data):
    return base64.b64encode(hashlib.md5(data).digest()).decode("ascii")


def fixture_output(manifest, text, cohort, contig, run_id, evidence_uri, producer,
                   primary_load_mode=None):
    # Bounded fixtures are NOT the frozen production object. Give them their own
    # honest byte identity rather than attaching production provenance to a slice.
    manifest = copy.deepcopy(manifest)
    payload = gzip.compress(text.encode(), mtime=0)
    for obj in manifest["objects"]:
        if obj["cohort"] == cohort and obj["name"].endswith(".vcf.gz"):
            obj.update(size=len(payload), md5_base64=md5_base64(payload), mirror_generation="42")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "fixture.vcf.gz"
        path.write_bytes(payload)
        with generic.open_vcf(str(path)) as stream:
            return generic.build_output(manifest, stream, cohort, contig, run_id,
                                        evidence_uri, producer, primary_load_mode)


class PerContigReconciliationTests(unittest.TestCase):
    def fixture_text(self, name):
        return (ROOT / "tests" / "fixtures" / "y1" / name).read_text()

    def manifest(self, contig):
        return json.loads((ROOT / "sources" / "y1" / f"primary-source-{contig}.json").read_text())

    def sex_chromosome_text(self, contig):
        base = self.fixture_text("hgsvc_hprc_trv_13_alt.vcf")
        header = "\n".join(line for line in base.splitlines() if line.startswith("#"))
        header = header.replace(
            "ID=chr22,length=50818468",
            f"ID={contig},length={generic.GRCH38_CONTIG_LENGTHS[contig]}",
            1,
        )
        records = (ROOT / "tests" / "fixtures" / "y1" /
                   "hgsvc_hprc_sex_chromosome_bounded_records.vcf.records").read_text()
        selected = [line for line in records.splitlines() if line.startswith(contig + "\t")]
        return header + "\n" + "\n".join(selected) + "\n"

    def test_chr22_reconciliation_is_backward_compatible(self):
        text = self.fixture_text("hgsvc_hprc_trv_13_alt.vcf")
        generic_facts = generic.reconcile(io.StringIO(text), "hgsvc_hprc", "chr22")
        self.assertEqual(generic_facts, legacy.reconcile(io.StringIO(text), "hgsvc_hprc"))
        output = fixture_output(
            json.loads((ROOT / "sources" / "y1" / "primary-source-manifest.json").read_text()),
            text, "hgsvc_hprc", "chr22", "run", "gs://evidence", "test",
        )
        self.assertEqual(output["contract_version"], 1)
        self.assertEqual(output["chrom"], "chr22")
        self.assertEqual(output["facts"], generic_facts)
        self.assertEqual(output["counts"], {
            "source_records": 1, "summaries": 1, "alleles": 13,
            "frequencies": 273, "carriers": 214, "rejects": 0,
        })

    def test_aggregate_only_replays_exact_sex_records_without_reading_genotypes(self):
        expected = {
            "chrX": (3, 10),
            "chrY": (2, 2),
        }
        for contig, (source_records, alt_alleles) in expected.items():
            text = self.sex_chromosome_text(contig)
            facts = generic.reconcile(
                io.StringIO(text), "hgsvc_hprc", contig,
                generic.AGGREGATE_ONLY_MODE,
            )
            self.assertEqual(facts["source_records"], source_records)
            self.assertEqual(facts["alt_alleles"], alt_alleles)
            self.assertEqual(facts["genotype_calls"], 0)
            self.assertEqual(facts["called_alleles"], 0)
            self.assertEqual(facts["carrier_alt_copies"], 0)

            # Corrupt every post-INFO byte. Aggregate-only reconciliation must not
            # parse FORMAT, GT, or ALLR, while the ordinary path remains strict.
            lines = []
            for line in text.splitlines():
                if line.startswith("#"):
                    lines.append(line)
                else:
                    lines.append("\t".join(line.split("\t")[:8] + ["NOT_FORMAT_OR_GT"]))
            malformed_gt = "\n".join(lines) + "\n"
            bypassed = generic.reconcile(
                io.StringIO(malformed_gt), "hgsvc_hprc", contig,
                generic.AGGREGATE_ONLY_MODE,
            )
            self.assertEqual(bypassed["source_records"], source_records)
            with self.assertRaises(ValueError):
                generic.reconcile(io.StringIO(malformed_gt), "hgsvc_hprc", contig)

            output = fixture_output(
                self.manifest(contig), text, "hgsvc_hprc", contig,
                f"fresh-{contig}", "file://bounded", "test",
                generic.AGGREGATE_ONLY_MODE,
            )
            self.assertEqual(output["primary_load_mode"], generic.AGGREGATE_ONLY_MODE)
            self.assertEqual(output["carrier_loading_status"], "unavailable_not_loaded")
            self.assertEqual(output["counts"]["carriers"], 0)

    def test_aggregate_only_reconciliation_rejects_every_other_scope(self):
        for cohort, contig in (("aou", "chrX"), ("hgsvc_hprc", "chr22")):
            with self.assertRaisesRegex(ValueError, "restricted"):
                generic.reconcile(
                    io.StringIO(""), cohort, contig, generic.AGGREGATE_ONLY_MODE
                )

    def test_other_grch38_contig_uses_exact_declared_length(self):
        text = self.fixture_text("aou_summary_only_ins.vcf")
        text = text.replace("ID=chr22,length=50818468", "ID=chr1,length=248956422", 1)
        text = text.replace("\nchr22\t", "\nchr1\t")
        facts = generic.reconcile(io.StringIO(text), "aou", "chr1")
        self.assertEqual(facts["source_records"], 1)
        self.assertEqual(facts["carrier_alt_copies"], 0)
        self.assertEqual(facts["called_alleles"], 0)

        wrong_length = text.replace("ID=chr1,length=248956422", "ID=chr1,length=248956421", 1)
        with self.assertRaisesRegex(ValueError, "exactly GRCh38 chr1 length"):
            generic.reconcile(io.StringIO(wrong_length), "aou", "chr1")

    def test_cross_contig_manifest_and_vcf_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "must describe Y1 chr2"):
            generic.checked_source(self.manifest("chr1"), "aou", "chr2")

        text = self.fixture_text("aou_summary_only_ins.vcf")
        with self.assertRaisesRegex(ValueError, "exactly GRCh38 chr1 length"):
            generic.reconcile(io.StringIO(text), "aou", "chr1")

    def test_source_name_mismatch_is_rejected(self):
        manifest = self.manifest("chr1")
        manifest["objects"][0]["name"] = "gnomAD_LR_Y1.hgsvc_hprc.chr2.vcf.gz"
        with self.assertRaisesRegex(ValueError, "canonical chr1 VCF/TBI pair"):
            generic.checked_source(manifest, "hgsvc_hprc", "chr1")

    def test_all_primary_contigs_have_checked_grch38_identity(self):
        self.assertEqual(len(generic.GRCH38_CONTIG_LENGTHS), 24)
        for contig in generic.GRCH38_CONTIG_LENGTHS:
            for cohort in generic.COHORTS:
                source = generic.checked_source(self.manifest(contig), cohort, contig)
                self.assertIn(f".{contig}.vcf.gz", source["uri"])
                self.assertGreater(source["size"], 0)


class FakeProcess:
    def __init__(self, stdout, status=0):
        self.stdout = stdout
        self.status = status
        self.returncode = None
        self.terminated = self.killed = False
        self.waits = []

    def wait(self, timeout=None):
        self.waits.append(timeout)
        self.returncode = self.status
        return self.returncode

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True


class ImmutableInputTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.manifest_path = self.directory / "manifest.json"
        self.output_path = self.directory / "output.json"
        self.local_path = self.directory / "local.vcf.gz"
        self.text = ("##fileformat=VCFv4.2\n"
                     "##contig=<ID=chr22,length=50818468>\n"
                     "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                     "chr22\t100\tv1\tA\tC\t.\tPASS\tAC=1;AN=2\n")
        self.payload = gzip.compress(self.text.encode(), mtime=0)
        self.manifest = {
            "schema_version": 2, "contract_type": "y1_per_contig_immutable_source",
            "reference_genome": "GRCh38", "release": "Y1", "chromosome": "chr22",
            "mirror_prefix": generic.MIRROR_PREFIX,
            "objects": [
                {"cohort": "aou", "name": "gnomAD_LR_Y1.aou.chr22.vcf.gz",
                 "mirror_generation": "42", "size": len(self.payload),
                 "md5_base64": md5_base64(self.payload)},
                {"cohort": "aou", "name": "gnomAD_LR_Y1.aou.chr22.vcf.gz.tbi",
                 "mirror_generation": "43", "size": 5, "md5_base64": md5_base64(b"index")},
            ],
        }
        self.uri = generic.MIRROR_PREFIX + "/aou/vcfs/gnomAD_LR_Y1.aou.chr22.vcf.gz"

    def run_cli(self, process=None, override=None, entrypoint=generic, popen_factory=None):
        self.manifest_path.write_text(json.dumps(self.manifest))
        argv = ["reconcile", "--source-manifest", str(self.manifest_path),
                "--cohort", "aou", "--run-id", "test-run", "--evidence-uri", "file://test",
                "--producer", "offline-test", "--output", str(self.output_path)]
        if entrypoint is generic:
            argv += ["--contig", "chr22"]
        if override is not None:
            argv += ["--vcf", str(override)]
        process = process or FakeProcess(io.BytesIO(self.payload))
        with mock.patch.object(generic.subprocess, "Popen", return_value=process,
                               side_effect=popen_factory) as popen:
            with mock.patch.object(sys, "argv", argv), mock.patch("sys.stdout", new_callable=io.StringIO):
                entrypoint.main()
        return json.loads(self.output_path.read_text()), popen, process

    def assert_cli_fails(self, exception, message, **kwargs):
        with self.assertRaisesRegex(exception, message):
            self.run_cli(**kwargs)
        self.assertFalse(self.output_path.exists(), "failure must not emit a receipt")

    def test_cloud_read_names_exact_generation_and_emits_valid_artifact(self):
        output, popen, process = self.run_cli()
        self.assertEqual(popen.call_args.args[0], ["gcloud", "storage", "cat", self.uri + "#42"])
        self.assertNotEqual(popen.call_args.kwargs["stderr"], subprocess.PIPE)
        self.assertTrue(process.stdout.closed)
        self.assertEqual(process.waits, [None])
        self.assertFalse(process.terminated)
        self.assertEqual(output["contract_version"], 1)
        self.assertEqual(output["source_generation"], "42")
        self.assertEqual(output["source_checksum"], md5_base64(self.payload))
        self.assertEqual(output["counts"], {"source_records": 1, "summaries": 1,
                                          "alleles": 1, "frequencies": 1,
                                          "carriers": 0, "rejects": 0})
        self.assertEqual(output["facts"], generic.reconcile(io.StringIO(self.text), "aou", "chr22"))

    def test_refresh_read_is_generation_pinned_and_checksum_verified(self):
        prefix = "gs://gnomad-lr-data/y1/refreshes/20260923-012345abcdef/sources"
        self.manifest["mirror_prefix"] = prefix
        output, popen, _ = self.run_cli()
        uri = self.uri.replace(generic.MIRROR_PREFIX, prefix)
        self.assertEqual(popen.call_args.args[0], ["gcloud", "storage", "cat", uri + "#42"])
        self.assertEqual(generic.checked_source(self.manifest, "aou", "chr22")["uri"], uri)
        self.assertEqual(output["source_generation"], "42")
        self.assertEqual(output["source_checksum"], md5_base64(self.payload))

    def test_refresh_deceptive_prefixes_fail_before_source_open(self):
        prefix = "gs://gnomad-lr-data/y1/refreshes/20260923-012345abcdef/sources"
        for bad in (prefix + "/..", prefix + "?generation=42", prefix + "#42", prefix + "\n",
                    prefix.replace("012345abcdef", "../old"),
                    prefix.replace("gnomad-lr-data", "other")):
            self.manifest["mirror_prefix"] = bad
            with self.subTest(prefix=bad), mock.patch.object(generic.subprocess, "Popen") as popen:
                with self.assertRaises(ValueError):
                    generic.checked_source(self.manifest, "aou", "chr22")
                popen.assert_not_called()

    def test_changed_compressed_bytes_with_identical_decoded_text_fail(self):
        changed = bytearray(self.payload)
        changed[4] = 1  # gzip mtime changes only the compressed object's identity
        self.assertEqual(gzip.decompress(changed), self.text.encode())
        self.assert_cli_fails(ValueError, "MD5 checksum", process=FakeProcess(io.BytesIO(changed)))

    def test_wrong_compressed_size_fails_in_both_directions(self):
        for delta in (-1, 1):
            with self.subTest(delta=delta):
                self.manifest["objects"][0]["size"] = len(self.payload) + delta
                self.assert_cli_fails(ValueError, "byte size")

    def test_wrong_well_formed_checksum_fails(self):
        self.manifest["objects"][0]["md5_base64"] = md5_base64(b"other object")
        self.assert_cli_fails(ValueError, "MD5 checksum")

    def test_decoded_byte_checksum_is_not_an_object_checksum(self):
        self.manifest["objects"][0]["md5_base64"] = md5_base64(self.text.encode())
        self.assert_cli_fails(ValueError, "MD5 checksum")

    def test_decoded_byte_size_is_not_an_object_size(self):
        self.manifest["objects"][0]["size"] = len(self.text.encode())
        self.assert_cli_fails(ValueError, "byte size")

    def test_local_override_must_match_exact_compressed_contract(self):
        self.local_path.write_bytes(self.payload)
        output, popen, _ = self.run_cli(override=self.local_path)
        popen.assert_not_called()
        self.assertEqual(output["source_generation"], "42")

    def test_changed_local_override_cannot_claim_frozen_identity(self):
        changed = bytearray(self.payload)
        changed[4] = 1
        self.local_path.write_bytes(changed)
        self.assert_cli_fails(ValueError, "MD5 checksum", override=self.local_path)

    def test_missing_local_override_fails_without_receipt(self):
        self.assert_cli_fails(FileNotFoundError, "local.vcf.gz", override=self.local_path)

    def test_cloud_override_cannot_substitute_another_generation_or_object(self):
        for uri in (self.uri + "#41", "gs://other/object.gz", self.uri + "?generation=42"):
            with self.subTest(uri=uri):
                self.assert_cli_fails(ValueError, "exact contract", override=uri)

    def test_exact_generation_cloud_override_is_not_double_qualified(self):
        _, popen, _ = self.run_cli(override=self.uri + "#42")
        self.assertEqual(popen.call_args.args[0][-1], self.uri + "#42")

    def test_missing_or_malformed_identity_fails_before_any_source_open(self):
        invalid = {
            "mirror_generation": [None, "", "0", 0, -1, True, 1.5, "01", "-2", "42#7",
                                  "42?generation=7", " 42", "٤٢", str(2**64), [], {}],
            "size": [None, 0, -1, True, "123", 1.5, 2**64],
            "md5_base64": [None, "", "not-base64", "YWJj", "A" * 32,
                           md5_base64(self.payload) + "\n", [], {}],
        }
        original = copy.deepcopy(self.manifest)
        for index in (0, 1):
            for field, values in invalid.items():
                for value in values:
                    with self.subTest(index=index, field=field, value=value):
                        self.manifest = copy.deepcopy(original)
                        if value is None:
                            self.manifest["objects"][index].pop(field)
                        else:
                            self.manifest["objects"][index][field] = value
                        with mock.patch.object(generic, "_open_compressed") as opener:
                            self.assert_cli_fails(ValueError, "immutable")
                            opener.assert_not_called()

    def test_nonzero_process_status_even_after_valid_bytes_fails(self):
        process = FakeProcess(io.BytesIO(self.payload), status=9)
        self.assert_cli_fails(RuntimeError, r"gcloud storage cat failed \(9\)", process=process)
        self.assertTrue(process.stdout.closed)

    def test_process_launch_error_fails(self):
        self.assert_cli_fails(OSError, "gcloud unavailable",
                              popen_factory=OSError("gcloud unavailable"))

    def test_large_subprocess_stderr_cannot_deadlock_stdout_reader(self):
        real_popen = subprocess.Popen

        def local_producer(command, **kwargs):
            self.assertEqual(command[-1], self.uri + "#42")
            return real_popen([sys.executable, "-c",
                               "import sys; sys.stderr.buffer.write(b'E' * 262144); "
                               f"sys.stdout.buffer.write({self.payload!r})"], **kwargs)

        output, _, _ = self.run_cli(popen_factory=local_producer)
        self.assertEqual(output["counts"]["source_records"], 1)

    def test_failed_subprocess_diagnostics_are_bounded_and_retained(self):
        real_popen = subprocess.Popen

        def local_producer(command, **kwargs):
            return real_popen([sys.executable, "-c",
                               "import sys; sys.stderr.buffer.write(b'E' * 262144 + b'FINAL ERROR'); "
                               f"sys.stdout.buffer.write({self.payload!r}); sys.exit(7)"], **kwargs)

        with self.assertRaisesRegex(RuntimeError, r"failed \(7\).*FINAL ERROR") as caught:
            self.run_cli(popen_factory=local_producer)
        self.assertLess(len(str(caught.exception)), 16500)
        self.assertFalse(self.output_path.exists())

    def test_truncated_gzip_and_pipe_read_errors_fail_and_reap_process(self):
        class BrokenPipe(io.BytesIO):
            def read(self, size=-1):
                raise OSError("injected pipe read failure")

        for stream, error, message in (
            (io.BytesIO(self.payload[:-8]), EOFError, "end-of-stream"),
            (BrokenPipe(self.payload), OSError, "pipe read failure"),
            (io.BytesIO(b"not gzip"), gzip.BadGzipFile, "Not a gzipped file"),
        ):
            with self.subTest(message=message):
                process = FakeProcess(stream)
                self.assert_cli_fails(error, message, process=process)
                self.assertTrue(stream.closed)
                self.assertTrue(process.terminated)
                self.assertEqual(process.waits, [10])

    def test_parser_error_stops_producer_without_draining_it(self):
        payload = gzip.compress(b"not a valid VCF\n", mtime=0)
        self.manifest["objects"][0].update(size=len(payload), md5_base64=md5_base64(payload))
        process = FakeProcess(io.BytesIO(payload))
        self.assert_cli_fails(ValueError, "before #CHROM", process=process)
        self.assertTrue(process.terminated)
        self.assertTrue(process.stdout.closed)

    def test_unresponsive_producer_is_killed_after_bounded_cleanup_wait(self):
        process = FakeProcess(io.BytesIO(b"not gzip"))
        wait = process.wait

        def timeout_once(timeout=None):
            if timeout is not None:
                raise subprocess.TimeoutExpired("fake gcloud", timeout)
            return wait(timeout)

        process.wait = timeout_once
        self.assert_cli_fails(gzip.BadGzipFile, "Not a gzipped file", process=process)
        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)
        self.assertTrue(process.stdout.closed)

    def test_concatenated_gzip_members_and_empty_bgzf_style_tail_are_hashed(self):
        split = self.text.index("chr22\t100")
        payload = (gzip.compress(self.text[:split].encode(), mtime=0)
                   + gzip.compress(self.text[split:].encode(), mtime=0)
                   + gzip.compress(b"", mtime=0))
        self.manifest["objects"][0].update(size=len(payload), md5_base64=md5_base64(payload))
        output, _, _ = self.run_cli(process=FakeProcess(io.BytesIO(payload)))
        self.assertEqual(output["counts"]["source_records"], 1)
        self.assertEqual(output["source_checksum"], md5_base64(payload))

    def test_raw_reads_are_bounded_even_with_short_pipe_reads(self):
        class ShortReads(io.BytesIO):
            def read(self, size=-1):
                if not 0 < size <= 64 * 1024:
                    raise AssertionError(f"unbounded compressed read: {size}")
                return super().read(min(size, 3))

        output, _, _ = self.run_cli(process=FakeProcess(ShortReads(self.payload)))
        self.assertEqual(output["counts"]["source_records"], 1)

    def test_incomplete_parser_consumption_cannot_emit_receipt(self):
        with mock.patch.object(generic, "reconcile", return_value={}):
            self.assert_cli_fails(ValueError, "complete input")

    def test_gzip_crc_and_utf8_errors_cannot_emit_receipt(self):
        corrupt_crc = bytearray(self.payload)
        corrupt_crc[-8] ^= 1
        for payload, error in ((bytes(corrupt_crc), gzip.BadGzipFile),
                               (gzip.compress(b"\xff", mtime=0), UnicodeDecodeError)):
            with self.subTest(error=error):
                self.manifest["objects"][0].update(size=len(payload), md5_base64=md5_base64(payload))
                self.assert_cli_fails(error, ".", process=FakeProcess(io.BytesIO(payload)))

    def test_unverified_text_cannot_be_attached_to_frozen_provenance(self):
        with self.assertRaisesRegex(ValueError, "verified compressed source identity"):
            generic.build_output(self.manifest, io.StringIO(self.text), "aou", "chr22",
                                 "run", "file://test", "test")

    def test_legacy_chr22_cli_also_uses_verified_generation_without_wrapper_changes(self):
        self.manifest.pop("schema_version")
        output, popen, _ = self.run_cli(entrypoint=legacy)
        self.assertEqual(popen.call_args.args[0][-1], self.uri + "#42")
        self.assertEqual(output["chrom"], "chr22")
        self.assertEqual(output["counts"]["source_records"], 1)

    def test_legacy_chr22_cli_rejects_changed_local_override(self):
        self.local_path.write_bytes(self.payload + b"\0")
        self.assert_cli_fails(ValueError, "byte size", override=self.local_path, entrypoint=legacy)


if __name__ == "__main__":
    unittest.main()
