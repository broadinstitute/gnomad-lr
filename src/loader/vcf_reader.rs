//! VCF streaming reader using genohype-core's IO layer + noodles BGZF.
//!
//! Supports two modes:
//! - `open()`: streams all records from byte 0 (header + data)
//! - `open_region()`: uses a tabix index to seek to a specific genomic region,
//!   reading only the relevant BGZF blocks via HTTP range requests on GCS.
//!
//! Both modes yield raw tab-delimited lines one at a time without buffering.

use crate::loader::immutable_gcs::{
    validate_source_index_pair, ImmutableGcsBackend, ImmutableGcsObject, ImmutableGcsReader,
};
use anyhow::Context;
use genohype_core::io::get_reader;
use noodles::bgzf;
use noodles::csi::BinningIndex;
use noodles::tabix;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Seek};
use std::sync::{mpsc, Arc};
use tracing::info;

/// A lightweight VCF line reader that streams from GCS via genohype-core.
/// Records are yielded one at a time without buffering the entire file.
pub struct VcfStream {
    pub sample_names: Vec<String>,
    lines: Box<dyn Iterator<Item = anyhow::Result<String>> + Send>,
}

impl VcfStream {
    /// Open a bgzipped VCF and stream all records.
    pub fn open(vcf_path: &str) -> anyhow::Result<Self> {
        let reader = get_reader(vcf_path)?;
        let bgzf_reader = bgzf::Reader::new(reader);
        let buf_reader = BufReader::new(bgzf_reader);

        let mut lines_iter = buf_reader.lines();

        // Read header
        let sample_names = loop {
            match lines_iter.next() {
                Some(Ok(line)) => {
                    if line.starts_with("##") {
                        continue;
                    }
                    if line.starts_with("#CHROM") {
                        let parts: Vec<&str> = line.split('\t').collect();
                        if parts.len() < 8 {
                            anyhow::bail!("invalid #CHROM header: expected at least 8 columns");
                        }
                        let names = if parts.len() > 9 {
                            parts[9..].iter().map(|s| s.to_string()).collect()
                        } else {
                            Vec::new()
                        };
                        info!("Found {} samples in VCF header", names.len());
                        break names;
                    }
                }
                Some(Err(e)) => return Err(e.into()),
                None => anyhow::bail!("VCF header not found"),
            }
        };

        let streaming_iter = lines_iter.map(|result| result.map_err(anyhow::Error::from));

        Ok(VcfStream {
            sample_names,
            lines: Box::new(streaming_iter),
        })
    }

    /// Open a bgzipped VCF and stream only records in the given region.
    /// Legacy callers may fall back to a full scan when the adjacent TBI is absent.
    pub fn open_region(vcf_path: &str, chrom: &str, start: u32, stop: u32) -> anyhow::Result<Self> {
        Self::open_region_with_index_policy(vcf_path, chrom, start, stop, false)
    }

    /// Open a bounded region and require the adjacent TBI.
    /// Y1 paths use this contract so a missing/corrupt index cannot become a full scan.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open_region_required_index(
        vcf_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
    ) -> anyhow::Result<Self> {
        Self::open_region_with_index_policy(vcf_path, chrom, start, stop, true)
    }

    fn open_region_with_index_policy(
        vcf_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
        require_index: bool,
    ) -> anyhow::Result<Self> {
        let tbi_path = format!("{vcf_path}.tbi");
        let index = match load_tabix_index(&tbi_path) {
            Ok(index) => index,
            Err(error) if !require_index => {
                info!(
                    "No usable tabix index at {}, falling back to full scan: {}",
                    tbi_path, error
                );
                return Self::open(vcf_path);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("required tabix index is unavailable at {tbi_path}"));
            }
        };
        let header_reader = get_reader(vcf_path)?;
        let data_reader = get_reader(vcf_path)?;
        open_indexed_region(index, header_reader, data_reader, chrom, start, stop)
    }

    /// Open an exact immutable GCS VCF/TBI pair. Metadata, index, header, and
    /// data byte ranges are all generation-qualified and identity-checked.
    pub fn open_immutable_region(
        backend: Arc<dyn ImmutableGcsBackend>,
        source: &ImmutableGcsObject,
        index: &ImmutableGcsObject,
        chrom: &str,
        start: u32,
        stop: u32,
    ) -> anyhow::Result<Self> {
        validate_source_index_pair(source, index)?;
        let index_reader = ImmutableGcsReader::open(backend.clone(), index)
            .context("failed to open immutable VCF index")?;
        let index = load_tabix_index_from_reader(index_reader)?;
        let header_reader = ImmutableGcsReader::open(backend.clone(), source)
            .context("failed to open immutable VCF header")?;
        let data_reader = ImmutableGcsReader::open(backend, source)
            .context("failed to open immutable VCF data")?;
        open_indexed_region(index, header_reader, data_reader, chrom, start, stop)
    }

    /// Iterate over data lines. I/O and BGZF decoding errors are never discarded.
    pub fn records(self) -> impl Iterator<Item = anyhow::Result<String>> + Send {
        self.lines
    }
}

fn stream_indexed_records<R: BufRead>(
    query: &mut R,
    tx: &mpsc::SyncSender<anyhow::Result<String>>,
    expected_chrom: &str,
    start: u32,
    stop: u32,
) -> anyhow::Result<()> {
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        let count = query.read_until(b'\n', &mut bytes)?;
        if count == 0 {
            return Ok(());
        }
        if bytes.ends_with(b"\n") {
            bytes.pop();
        }
        send_indexed_record(&mut bytes, tx, expected_chrom, start, stop)?;
    }
}

fn send_indexed_record(
    bytes: &mut Vec<u8>,
    tx: &mpsc::SyncSender<anyhow::Result<String>>,
    expected_chrom: &str,
    start: u32,
    stop: u32,
) -> anyhow::Result<()> {
    if bytes.ends_with(b"\r") {
        bytes.pop();
    }
    let line = std::str::from_utf8(bytes).context("indexed VCF record is not valid UTF-8")?;
    let pos_end = line
        .find('\t')
        .ok_or_else(|| anyhow::anyhow!("indexed VCF record has no CHROM/POS separator"))?;
    let line_chrom = &line[..pos_end];
    if line_chrom != expected_chrom {
        return Ok(());
    }
    let next_tab = line[pos_end + 1..]
        .find('\t')
        .ok_or_else(|| anyhow::anyhow!("indexed VCF record has no POS/ID separator"))?;
    let pos_str = &line[pos_end + 1..pos_end + 1 + next_tab];
    let pos = pos_str
        .parse::<u32>()
        .map_err(|error| anyhow::anyhow!("invalid indexed VCF position {pos_str:?}: {error}"))?;
    if pos < start || pos > stop {
        return Ok(());
    }

    tx.send(Ok(line.to_string()))
        .map_err(|_| anyhow::anyhow!("indexed VCF receiver dropped before completion"))
}

fn open_indexed_region<H, D>(
    index: tabix::Index,
    header_reader: H,
    data_reader: D,
    chrom: &str,
    start: u32,
    stop: u32,
) -> anyhow::Result<VcfStream>
where
    H: Read,
    D: Read + Seek + Send + 'static,
{
    info!("Using tabix index for {}:{}-{}", chrom, start, stop);
    let bgzf_header = bgzf::Reader::new(header_reader);
    let mut vcf_header_reader = noodles::vcf::io::Reader::new(bgzf_header);
    let header = vcf_header_reader.read_header()?;
    let sample_names: Vec<String> = header
        .sample_names()
        .iter()
        .map(|sample| sample.to_string())
        .collect();
    info!("Found {} samples in VCF header", sample_names.len());

    let index_header = index
        .header()
        .ok_or_else(|| anyhow::anyhow!("tabix index has no header"))?;
    let ref_seq_id = index_header
        .reference_sequence_names()
        .iter()
        .position(|name| {
            let bytes: &[u8] = name.as_ref();
            bytes == chrom.as_bytes()
        })
        .ok_or_else(|| anyhow::anyhow!("chrom {chrom} not found in tabix index"))?;
    let interval_start = noodles::core::Position::try_from(start.max(1) as usize)?;
    let interval_end = noodles::core::Position::try_from(stop as usize)?;
    let interval = noodles::core::region::Interval::from(interval_start..=interval_end);
    let chunks = index.query(ref_seq_id, interval)?;
    if chunks.is_empty() {
        info!("No indexed chunks for region {}:{}-{}", chrom, start, stop);
        return Ok(VcfStream {
            sample_names,
            lines: Box::new(std::iter::empty()),
        });
    }

    info!("Found {} chunks for region", chunks.len());
    let chrom_owned = chrom.to_string();
    let (tx, rx) = mpsc::sync_channel::<anyhow::Result<String>>(1024);
    std::thread::spawn(move || {
        let mut bgzf_data = bgzf::Reader::new(data_reader);
        let mut query = noodles::csi::io::Query::new(&mut bgzf_data, chunks);
        if let Err(error) = stream_indexed_records(&mut query, &tx, &chrom_owned, start, stop) {
            let _ = tx.send(Err(error));
        }
    });
    Ok(VcfStream {
        sample_names,
        lines: Box::new(rx.into_iter()),
    })
}

/// Read and preserve the raw VCF header required by the strict Y1 contract.
#[allow(dead_code)]
pub fn read_header_text(vcf_path: &str) -> anyhow::Result<String> {
    read_header_text_from_reader(get_reader(vcf_path)?, vcf_path)
}

pub fn read_immutable_header_text(
    backend: Arc<dyn ImmutableGcsBackend>,
    source: &ImmutableGcsObject,
) -> anyhow::Result<String> {
    let reader =
        ImmutableGcsReader::open(backend, source).context("failed to open immutable VCF header")?;
    read_header_text_from_reader(reader, &source.immutable_read_uri)
}

fn read_header_text_from_reader<R: Read>(reader: R, label: &str) -> anyhow::Result<String> {
    let bgzf_reader = bgzf::Reader::new(reader);
    let buf_reader = BufReader::new(bgzf_reader);
    let mut lines = Vec::new();
    for line in buf_reader.lines() {
        let line = line?;
        if !line.starts_with('#') {
            break;
        }
        let is_columns = line.starts_with("#CHROM");
        lines.push(line);
        if is_columns {
            return Ok(lines.join("\n"));
        }
    }
    anyhow::bail!("VCF header not found in {label}")
}

fn load_tabix_index(tbi_path: &str) -> anyhow::Result<tabix::Index> {
    load_tabix_index_from_reader(get_reader(tbi_path)?)
}

fn load_tabix_index_from_reader<R: Read>(reader: R) -> anyhow::Result<tabix::Index> {
    let mut tbi_reader = tabix::io::Reader::new(reader);
    Ok(tbi_reader.read_index()?)
}

/// Parse a VCF INFO field string into key-value pairs (zero-allocation, borrows input).
/// Flag fields (no `=`) get value `None`.
pub fn parse_info_field_shallow(info_str: &str) -> HashMap<&str, Option<&str>> {
    let mut info = HashMap::new();
    for entry in info_str.split(';') {
        if let Some(eq_pos) = entry.find('=') {
            let key = &entry[..eq_pos];
            let val = &entry[eq_pos + 1..];
            info.insert(key, Some(val));
        } else {
            info.insert(entry, None);
        }
    }
    info
}

/// Extract a float from the info map.
pub fn info_float(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<f32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| if *v == "." { None } else { v.parse().ok() })
}

/// Extract an int from the info map.
pub fn info_int(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<i32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| if *v == "." { None } else { v.parse().ok() })
}

/// Extract a u32 from the info map.
pub fn info_u32(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<u32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| if *v == "." { None } else { v.parse().ok() })
}

/// Extract the first comma-separated value as a string.
pub fn info_first(info: &HashMap<&str, Option<&str>>, key: &str) -> String {
    info.get(key)
        .and_then(|v| v.as_ref())
        .map(|v| {
            if *v == "." {
                return String::new();
            }
            v.split(',').next().unwrap_or("").to_string()
        })
        .unwrap_or_default()
}

/// Check if an INFO flag is present.
pub fn info_flag(info: &HashMap<&str, Option<&str>>, key: &str) -> bool {
    info.contains_key(key)
}

/// Extract the first element of a Number=A field as f32.
pub fn info_first_float(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<f32> {
    info.get(key).and_then(|v| v.as_ref()).and_then(|v| {
        if *v == "." {
            return None;
        }
        v.split(',').next().and_then(|s| s.parse().ok())
    })
}

/// Extract the first element of a Number=A field as u32.
pub fn info_first_u32(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<u32> {
    info.get(key).and_then(|v| v.as_ref()).and_then(|v| {
        if *v == "." {
            return None;
        }
        v.split(',').next().and_then(|s| s.parse().ok())
    })
}

#[cfg(test)]
mod tests {
    use super::{read_immutable_header_text, stream_indexed_records, VcfStream};
    use crate::loader::immutable_gcs::{
        GcsObjectMetadata, GcsObjectRequest, GcsRangeResponse, ImmutableGcsBackend,
        ImmutableGcsObject,
    };
    use noodles::bgzf;
    use noodles::core::Position;
    use noodles::csi::binning_index::index::reference_sequence::bin::Chunk;
    use noodles::tabix;
    use std::collections::HashMap;
    use std::io::Write;
    use std::ops::Range;
    use std::path::PathBuf;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn indexed_fixture(label: &str, records: &[(u32, &str)]) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "gnomad-lr-vcf-{label}-{}-{nonce}.vcf.gz",
            std::process::id()
        ));
        let index_path = PathBuf::from(format!("{}.tbi", path.display()));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = bgzf::Writer::new(file);
        writeln!(writer, "##fileformat=VCFv4.3").unwrap();
        writeln!(writer, "##contig=<ID=chr1,length=100000>").unwrap();
        writeln!(writer, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO").unwrap();
        writer.flush().unwrap();

        let mut indexer = tabix::index::Indexer::default();
        indexer.set_header(noodles::csi::binning_index::index::header::Builder::vcf().build());
        for (position, line) in records {
            let chunk_start = writer.virtual_position();
            writeln!(writer, "{line}").unwrap();
            let chunk_end = writer.virtual_position();
            let position = Position::try_from(*position as usize).unwrap();
            indexer
                .add_record(
                    "chr1",
                    position,
                    position,
                    Chunk::new(chunk_start, chunk_end),
                )
                .unwrap();
        }
        writer.finish().unwrap();

        let file = std::fs::File::create(&index_path).unwrap();
        let mut index_writer = tabix::io::Writer::new(file);
        index_writer.write_index(&indexer.build()).unwrap();
        (path, index_path)
    }

    #[derive(Clone)]
    struct FakeObject {
        bytes: Vec<u8>,
        generation: String,
        md5: String,
    }

    #[derive(Default)]
    struct FakeGcsBackend {
        objects: HashMap<String, FakeObject>,
        requests: Mutex<Vec<(String, String, Range<u64>)>>,
    }

    impl ImmutableGcsBackend for FakeGcsBackend {
        fn metadata(&self, request: &GcsObjectRequest) -> anyhow::Result<GcsObjectMetadata> {
            self.requests.lock().unwrap().push((
                request.object.clone(),
                request.generation.clone(),
                0..0,
            ));
            let object = self
                .objects
                .get(&request.object)
                .ok_or_else(|| anyhow::anyhow!("fixture object not found"))?;
            Ok(GcsObjectMetadata {
                generation: object.generation.clone(),
                byte_size: object.bytes.len() as u64,
                md5_base64: object.md5.clone(),
            })
        }

        fn read_range(
            &self,
            request: &GcsObjectRequest,
            range: Range<u64>,
        ) -> anyhow::Result<GcsRangeResponse> {
            self.requests.lock().unwrap().push((
                request.object.clone(),
                request.generation.clone(),
                range.clone(),
            ));
            let object = self
                .objects
                .get(&request.object)
                .ok_or_else(|| anyhow::anyhow!("fixture object not found"))?;
            Ok(GcsRangeResponse {
                generation: object.generation.clone(),
                total_size: object.bytes.len() as u64,
                range_start: range.start,
                data: object.bytes[range.start as usize..range.end as usize].to_vec(),
            })
        }
    }

    const FIXTURE_MD5: &str = "AAAAAAAAAAAAAAAAAAAAAA==";

    fn immutable_object(uri: &str, generation: &str, bytes: &[u8]) -> ImmutableGcsObject {
        ImmutableGcsObject {
            uri: uri.into(),
            generation: generation.into(),
            byte_size: bytes.len() as u64,
            checksum_algorithm: "md5_base64".into(),
            checksum: FIXTURE_MD5.into(),
            immutable_read_uri: format!("{uri}?generation={generation}"),
        }
    }

    fn immutable_fixture() -> (
        Arc<FakeGcsBackend>,
        ImmutableGcsObject,
        ImmutableGcsObject,
        PathBuf,
        PathBuf,
    ) {
        let (path, index_path) =
            indexed_fixture("immutable", &[(10_000, "chr1\t10000\t.\tA\tC\t.\tPASS\t.")]);
        let source_bytes = std::fs::read(&path).unwrap();
        let index_bytes = std::fs::read(&index_path).unwrap();
        let source = immutable_object("gs://bucket/source.vcf.gz", "42", &source_bytes);
        let index = immutable_object("gs://bucket/source.vcf.gz.tbi", "43", &index_bytes);
        let objects = HashMap::from([
            (
                "source.vcf.gz".into(),
                FakeObject {
                    bytes: source_bytes,
                    generation: "42".into(),
                    md5: FIXTURE_MD5.into(),
                },
            ),
            (
                "source.vcf.gz.tbi".into(),
                FakeObject {
                    bytes: index_bytes,
                    generation: "43".into(),
                    md5: FIXTURE_MD5.into(),
                },
            ),
        ]);
        (
            Arc::new(FakeGcsBackend {
                objects,
                requests: Mutex::new(Vec::new()),
            }),
            source,
            index,
            path,
            index_path,
        )
    }

    fn explicit_chunk_fixture(label: &str, records: &[Vec<u8>]) -> (PathBuf, Vec<Chunk>) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "gnomad-lr-vcf-explicit-{label}-{}-{nonce}.vcf.gz",
            std::process::id()
        ));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = bgzf::Writer::new(file);
        writeln!(writer, "##fileformat=VCFv4.3").unwrap();
        writeln!(writer, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO").unwrap();
        writer.flush().unwrap();

        let chunks = records
            .iter()
            .map(|record| {
                let start = writer.virtual_position();
                writer.write_all(record).unwrap();
                let end = writer.virtual_position();
                Chunk::new(start, end)
            })
            .collect();
        writer.finish().unwrap();
        (path, chunks)
    }

    fn collect_explicit_chunks(
        path: &PathBuf,
        chunks: Vec<Chunk>,
        start: u32,
        stop: u32,
    ) -> anyhow::Result<Vec<String>> {
        let file = std::fs::File::open(path).unwrap();
        let mut bgzf_reader = bgzf::Reader::new(file);
        let mut query = noodles::csi::io::Query::new(&mut bgzf_reader, chunks);
        let (sender, receiver) = mpsc::sync_channel(16);
        stream_indexed_records(&mut query, &sender, "chr1", start, stop)?;
        drop(sender);
        receiver.into_iter().collect()
    }

    fn remove_fixture(path: &PathBuf, index_path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(index_path);
    }

    #[test]
    fn immutable_primary_reader_uses_declared_generation_for_index_header_and_data() {
        let (backend, source, index, path, index_path) = immutable_fixture();
        let header = read_immutable_header_text(backend.clone(), &source).unwrap();
        assert!(header.contains("#CHROM"));
        let rows: anyhow::Result<Vec<_>> = VcfStream::open_immutable_region(
            backend.clone(),
            &source,
            &index,
            "chr1",
            10_000,
            10_000,
        )
        .unwrap()
        .records()
        .collect();
        remove_fixture(&path, &index_path);
        assert_eq!(rows.unwrap().len(), 1);
        let requests = backend.requests.lock().unwrap();
        assert!(requests.iter().any(|(name, generation, range)| {
            name == "source.vcf.gz.tbi" && generation == "43" && !range.is_empty()
        }));
        assert!(requests.iter().any(|(name, generation, range)| {
            name == "source.vcf.gz" && generation == "42" && !range.is_empty()
        }));
    }

    #[test]
    fn stale_or_substituted_primary_index_is_rejected() {
        let (backend, source, mut index, path, index_path) = immutable_fixture();
        index.generation = "44".into();
        index.immutable_read_uri = format!("{}?generation=44", index.uri);
        assert!(VcfStream::open_immutable_region(
            backend.clone(),
            &source,
            &index,
            "chr1",
            1,
            10_000
        )
        .is_err());

        let mut substituted = index;
        substituted.generation = "43".into();
        substituted.uri = "gs://bucket/other.vcf.gz.tbi".into();
        substituted.immutable_read_uri =
            format!("{}?generation={}", substituted.uri, substituted.generation);
        assert!(VcfStream::open_immutable_region(
            backend,
            &source,
            &substituted,
            "chr1",
            1,
            10_000
        )
        .is_err());
        remove_fixture(&path, &index_path);
    }

    #[test]
    fn changed_primary_vcf_generation_is_rejected_before_header_read() {
        let (backend, mut source, _, path, index_path) = immutable_fixture();
        source.generation = "41".into();
        source.immutable_read_uri = format!("{}?generation=41", source.uri);
        assert!(read_immutable_header_text(backend, &source).is_err());
        remove_fixture(&path, &index_path);
    }

    #[test]
    fn record_iteration_exposes_background_errors() {
        let stream = VcfStream {
            sample_names: Vec::new(),
            lines: Box::new(vec![Err(anyhow::anyhow!("fixture I/O failure"))].into_iter()),
        };
        let error = stream.records().next().unwrap().unwrap_err();
        assert_eq!(error.to_string(), "fixture I/O failure");
    }

    #[test]
    fn indexed_chunk_tail_is_completed_without_losing_or_duplicating_records() {
        let prefix = "chr1\t10000\t.\tA\tC\t.\tPASS\tPAD=";
        let first = format!("{prefix}{}", "x".repeat(8187 - prefix.len()));
        assert_eq!(first.len(), 8187);
        let second = "chr1\t20000\t.\tG\tT\t.\tPASS\t.";
        let third = "chr1\t20001\t.\tC\tA\t.\tPASS\t.";
        let (path, index_path) = indexed_fixture(
            "chunk-tail",
            &[(10_000, &first), (20_000, second), (20_001, third)],
        );

        let first_rows: anyhow::Result<Vec<_>> =
            VcfStream::open_region_required_index(path.to_str().unwrap(), "chr1", 10_000, 10_000)
                .unwrap()
                .records()
                .collect();
        let adjacent_rows: anyhow::Result<Vec<_>> =
            VcfStream::open_region_required_index(path.to_str().unwrap(), "chr1", 20_000, 20_001)
                .unwrap()
                .records()
                .collect();
        remove_fixture(&path, &index_path);

        assert_eq!(first_rows.unwrap(), [first]);
        assert_eq!(
            adjacent_rows.unwrap(),
            [second.to_string(), third.to_string()]
        );
    }

    #[test]
    fn noncontiguous_chunks_do_not_merge_a_gap_prefix_with_the_next_record() {
        let prefix = "chr1\t10000\t.\tA\tC\t.\tPASS\tPAD=";
        let first = format!("{prefix}{}", "x".repeat(8187 - prefix.len()));
        let second = "chr1\t20000\t.\tG\tT\t.\tPASS\t.";
        let records = vec![
            format!("{first}\n").into_bytes(),
            b"chr1\t15000\t.\tA\tG\t.\tPASS\t.\n".to_vec(),
            format!("{second}\n").into_bytes(),
        ];
        let (path, chunks) = explicit_chunk_fixture("merge", &records);
        assert!(chunks[0].end() < chunks[2].start());

        let rows = collect_explicit_chunks(&path, vec![chunks[0], chunks[2]], 10_000, 20_000);
        let _ = std::fs::remove_file(path);
        assert_eq!(rows.unwrap(), [first, second.to_string()]);
    }

    #[test]
    fn noncontiguous_chunks_do_not_duplicate_a_record_after_seeking() {
        let first = "chr1\t10000\t.\tA\tC\t.\tPASS\t.";
        let second = "chr1\t20000\t.\tG\tT\t.\tPASS\t.";
        let records = vec![
            format!("{first}\n").into_bytes(),
            b"chr1\t15000\t.\tA\tG\t.\tPASS\t.\n".to_vec(),
            format!("{second}\n").into_bytes(),
        ];
        let (path, chunks) = explicit_chunk_fixture("duplicate", &records);
        assert!(chunks[0].end() < chunks[2].start());

        let rows = collect_explicit_chunks(&path, vec![chunks[0], chunks[2]], 10_000, 20_000);
        let _ = std::fs::remove_file(path);
        assert_eq!(rows.unwrap(), [first.to_string(), second.to_string()]);
    }

    #[test]
    fn chr1_multi_chunk_query_shape_is_exact_once_across_all_229_transitions() {
        // The identity-checked chr1 TBI has 107 multi-chunk 1 Mb queries with
        // this chunk-count distribution (336 chunks, 229 transitions).
        let distribution = [
            (2usize, 47usize),
            (3, 27),
            (4, 21),
            (5, 4),
            (6, 4),
            (7, 1),
            (8, 2),
            (10, 1),
        ];
        let mut records = Vec::new();
        let mut expected = Vec::new();
        for i in 0..10u32 {
            let selected = format!("chr1\t{}\t.\tA\tC\t.\tPASS\t.", 10_000 + 2 * i);
            expected.push(selected.clone());
            records.push(format!("{selected}\n").into_bytes());
            if i < 9 {
                records
                    .push(format!("chr1\t{}\t.\tG\tT\t.\tPASS\t.\n", 10_001 + 2 * i).into_bytes());
            }
        }
        let (path, all_chunks) = explicit_chunk_fixture("chr1-query-shape", &records);
        let selected_chunks: Vec<_> = all_chunks.iter().step_by(2).copied().collect();

        let mut query_count = 0;
        let mut transition_count = 0;
        for (chunk_count, repetitions) in distribution {
            for _ in 0..repetitions {
                let rows = collect_explicit_chunks(
                    &path,
                    selected_chunks[..chunk_count].to_vec(),
                    10_000,
                    10_018,
                )
                .unwrap();
                assert_eq!(rows, expected[..chunk_count]);
                query_count += 1;
                transition_count += chunk_count - 1;
            }
        }
        let _ = std::fs::remove_file(path);
        assert_eq!(query_count, 107);
        assert_eq!(transition_count, 229);
    }

    #[test]
    fn indexed_crlf_and_unterminated_source_eof_match_line_semantics() {
        let crlf = "chr1\t10000\t.\tA\tC\t.\tPASS\t.";
        let eof = "chr1\t20000\t.\tG\tT\t.\tPASS\t.";
        let records = vec![format!("{crlf}\r\n").into_bytes(), eof.as_bytes().to_vec()];
        let (path, chunks) = explicit_chunk_fixture("crlf-eof", &records);
        let contiguous = Chunk::new(chunks[0].start(), chunks[1].end());

        let rows = collect_explicit_chunks(&path, vec![contiguous], 10_000, 20_000);
        let _ = std::fs::remove_file(path);
        assert_eq!(rows.unwrap(), [crlf.to_string(), eof.to_string()]);
    }

    #[test]
    fn malformed_complete_indexed_record_is_not_suppressed() {
        let (path, index_path) = indexed_fixture("malformed", &[(10_000, "chr1")]);
        let result: anyhow::Result<Vec<_>> =
            VcfStream::open_region_required_index(path.to_str().unwrap(), "chr1", 10_000, 10_000)
                .unwrap()
                .records()
                .collect();
        remove_fixture(&path, &index_path);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no CHROM/POS separator"));
    }

    #[test]
    fn bounded_strict_reads_require_a_tabix_index() {
        let path = std::env::temp_dir().join(format!(
            "gnomad-lr-missing-index-{}-{}.vcf.gz",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let error =
            match VcfStream::open_region_required_index(path.to_str().unwrap(), "chr22", 1, 10) {
                Ok(_) => panic!("strict indexed read unexpectedly succeeded"),
                Err(error) => error,
            };
        assert!(error.to_string().contains("required tabix index"));
    }
}
