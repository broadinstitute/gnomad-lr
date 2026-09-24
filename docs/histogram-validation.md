# Read-only histogram validation

`gnomad-lr validate-histograms` calls the **same private header and row parser**
as `load histograms`, without calling that load path. It has no ClickHouse target,
client, inserter, insert callback, region filter, or skip-invalid mode. It only
reads source bytes and prints one JSON report to stdout. Errors exit nonzero;
parser error values are redacted so reports do not persist private cell contents.
Identity strings start as JSON `null` and are populated only after validation:
base64 MD5 must decode to 16 bytes, GCS source/generation must form a canonical
immutable identity, and local source paths must pass regular-file opening checks.
Rejected raw source/generation/MD5 strings and raw errors are never reported.

## Local fixture example

```sh
source=src/loader/histograms/fixtures/aou.tsv
size=$(wc -c < "$source" | tr -d ' ')
md5=$(openssl dgst -md5 -binary "$source" | openssl base64 -A)
target/debug/gnomad-lr validate-histograms \
  --source "$source" --source-size-bytes "$size" --source-md5-base64 "$md5"
```

Computing the expected digest from the same file is useful for a fixture smoke,
not independent provenance. For real local inputs, supply an independently frozen
expected size and base64 MD5. No decompression is performed. Local inputs must be
regular files; symlinks, FIFOs, devices and directories are rejected before open.
On Linux (x86-64/AArch64) and macOS, opening is also nonblocking and refuses
symlinks; descriptor-level type/device/inode checks reject a path swapped after
the initial check. Other platforms fail closed rather than use a potentially
blocking open.
`--source-generation` is forbidden locally because a matching local body cannot
independently attest its original GCS generation.

## Frozen GCS source (schedule separately)

Prerequisite: install the Google Cloud CLI and put its `gcloud` executable on
`PATH` using your installation's instructions; configure authorized credentials
separately. No author-specific SDK path is required.

```sh
target/debug/gnomad-lr validate-histograms \
  --source 'gs://BUCKET/OBJECT.tsv' \
  --source-generation GENERATION \
  --source-size-bytes COMPLETE_SIZE \
  --source-md5-base64 'COMPLETE_BASE64_MD5'
```

Use an independently frozen identity, not a newly resolved mutable path. The
existing `ImmutableGcsReader` verifies generation/size/metadata MD5 on open and
generation/size/range identity on every read. Validation additionally computes
MD5 over delivered bytes and checks the full byte count at EOF. Authentication
uses the existing immutable backend. This command does not mutate GCS.

**This implementation has only been exercised on local fixtures and an in-memory
fake GCS backend, not on the full production object.** Scheduling full-source
reads, scientific contract approval, and any future load remain separate gates.

## Coverage and limits

- `complete_success`: parser reached EOF; every nonblank data row passed; complete
  size and independently recomputed MD5 match. `complete_body_identity_verified`
  is true. Validated zero-call rows count as `empty_rows`, not stored zeroes.
- `stopped_first_error`: nonzero exit, no skip or retry. `diagnostic` is a fixed
  stage/code, not source values. Counters describe only work done before failure.
  No complete-body claim is made even if a buffer already fetched the whole file.
- `bounded_validation`: exit zero means the examined prefix passed, **not** a
  complete-file validation. `complete_body_identity_verified` and `eof_observed`
  remain false. This also applies when the configured limit happens to equal the
  file's row/byte count; validation does not probe beyond the requested limit.

`--max-rows N` bounds nonblank data rows examined, including empty rows (unlike the
load path's submitted-row limit). `--max-bytes N` bounds delivered bytes and never
parses a partial final line. With conforming GCS HTTP responses, range prefetch may
fetch up to 8 MiB beyond the logical byte/row boundary. No region filtering is
supported or implied.

`bytes_read` includes buffer readahead; `bytes_in_examined_lines` counts complete
lines examined. `data_rows_examined = accepted_rows + empty_rows + rejected_rows`
for parsed UTF-8 rows; malformed UTF-8/read/resource errors are separate diagnostics.
`gcs_metadata_verified` attests only metadata, never complete body identity by itself.
`local_origin_attested` is always false. No per-locus or per-sample data is reported.

Parser memory is independent of total file length: one buffered line and one
parsed row. Including the immutable reader's 8 MiB range, this bounded-memory
claim **assumes conforming GCS HTTP responses**. The shared HTTP backend is **not
hard capped**: it buffers range bodies before checking their length, and metadata
JSON responses also have no hard body cap. Malformed/oversized HTTP responses can
therefore exceed the normal memory bound even with a tiny `--max-bytes` budget.
Hard response caps and HTTP-level overflow tests are tracked separately in inbox
ticket `20260923-hard-cap-immutable-gcs-http-metadata-and-range-res.md`; this patch
does not change the shared immutable reader.

A line exceeding 16 MiB fails closed with `line_resource_limit_exceeded`; this
resource guard does not relax parser semantics.
Populated VC/hemi values remain rejected by the production parser until separately
approved semantic changes are made. A complete report is not authorization to load.
