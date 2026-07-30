//! Generation-qualified, fail-closed GCS object reads.
//!
//! Metadata and every byte-range request name the declared object generation.
//! Responses are accepted only when generation, total size, and complete MD5
//! still match the repository manifest, so there is no mutable check/read gap.

use anyhow::{bail, Context};
use base64::Engine;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_RANGE, RANGE};
use serde::Deserialize;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

const READ_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableGcsObject {
    pub uri: String,
    pub generation: String,
    pub byte_size: u64,
    pub checksum_algorithm: String,
    pub checksum: String,
    pub immutable_read_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsObjectRequest {
    pub bucket: String,
    pub object: String,
    pub generation: String,
    pub byte_size: u64,
    pub md5_base64: String,
}

impl ImmutableGcsObject {
    pub fn request(&self) -> anyhow::Result<GcsObjectRequest> {
        if self.checksum_algorithm != "md5_base64" {
            bail!("immutable GCS objects require checksum algorithm md5_base64");
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&self.checksum)
            .context("immutable GCS MD5 is not valid base64")?;
        if decoded.len() != 16 {
            bail!("immutable GCS MD5 must decode to exactly 16 bytes");
        }
        if self.byte_size == 0 {
            bail!("immutable GCS object byte size must be positive");
        }
        let generation = self
            .generation
            .parse::<u64>()
            .context("immutable GCS generation must be decimal UInt64")?;
        if generation == 0 || generation.to_string() != self.generation {
            bail!("immutable GCS generation must be canonical positive decimal UInt64");
        }

        let (bucket, object) = parse_base_uri(&self.uri)?;
        if self.immutable_read_uri != format!("{}?generation={}", self.uri, self.generation) {
            bail!("immutable GCS read URI must be the exact declared URI plus generation query");
        }
        let read_uri = reqwest::Url::parse(&self.immutable_read_uri)
            .context("invalid immutable GCS read URI")?;
        if read_uri.scheme() != "gs"
            || read_uri.host_str() != Some(bucket.as_str())
            || read_uri.path().trim_start_matches('/') != object
            || read_uri.fragment().is_some()
            || !read_uri.username().is_empty()
            || read_uri.password().is_some()
            || read_uri.port().is_some()
        {
            bail!("immutable GCS read URI substitutes the declared bucket or object");
        }
        let query: Vec<_> = read_uri.query_pairs().collect();
        if query.len() != 1 || query[0].0 != "generation" || query[0].1 != self.generation {
            bail!("immutable GCS read URI must contain only the declared generation query");
        }

        Ok(GcsObjectRequest {
            bucket,
            object,
            generation: self.generation.clone(),
            byte_size: self.byte_size,
            md5_base64: self.checksum.clone(),
        })
    }
}

fn parse_base_uri(uri: &str) -> anyhow::Result<(String, String)> {
    let parsed = reqwest::Url::parse(uri).context("invalid declared GCS URI")?;
    let bucket = parsed
        .host_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("declared GCS URI lacks bucket"))?;
    let object = parsed.path().trim_start_matches('/');
    if parsed.scheme() != "gs"
        || object.is_empty()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
    {
        bail!("declared GCS URI must be a mutable-query-free gs://bucket/object identity");
    }
    if uri != format!("gs://{bucket}/{object}") {
        bail!("declared GCS URI must use canonical gs://bucket/object spelling");
    }
    Ok((bucket.to_string(), object.to_string()))
}

pub fn validate_source_index_pair(
    source: &ImmutableGcsObject,
    index: &ImmutableGcsObject,
) -> anyhow::Result<(GcsObjectRequest, GcsObjectRequest)> {
    let source = source.request().context("invalid immutable BED identity")?;
    let index = index.request().context("invalid immutable TBI identity")?;
    if source.bucket != index.bucket || index.object != format!("{}.tbi", source.object) {
        bail!("immutable BED/TBI identities must be an adjacent same-bucket source/index pair");
    }
    Ok((source, index))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsObjectMetadata {
    pub generation: String,
    pub byte_size: u64,
    pub md5_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsRangeResponse {
    pub generation: String,
    pub total_size: u64,
    pub range_start: u64,
    pub data: Vec<u8>,
}

pub trait ImmutableGcsBackend: Send + Sync {
    fn metadata(&self, request: &GcsObjectRequest) -> anyhow::Result<GcsObjectMetadata>;
    fn read_range(
        &self,
        request: &GcsObjectRequest,
        range: Range<u64>,
    ) -> anyhow::Result<GcsRangeResponse>;
}

pub struct ImmutableGcsReader {
    backend: Arc<dyn ImmutableGcsBackend>,
    request: GcsObjectRequest,
    position: u64,
    buffer: Vec<u8>,
    buffer_start: u64,
    chunk_size: u64,
}

impl ImmutableGcsReader {
    pub fn open(
        backend: Arc<dyn ImmutableGcsBackend>,
        object: &ImmutableGcsObject,
    ) -> anyhow::Result<Self> {
        let request = object.request()?;
        let metadata = backend.metadata(&request).with_context(|| {
            format!(
                "failed to revalidate gs://{}/{}",
                request.bucket, request.object
            )
        })?;
        validate_metadata(&request, &metadata)?;
        Ok(Self {
            backend,
            request,
            position: 0,
            buffer: Vec::new(),
            buffer_start: 0,
            chunk_size: READ_CHUNK_SIZE,
        })
    }

    #[cfg(test)]
    fn with_chunk_size(mut self, chunk_size: u64) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    fn fill_buffer(&mut self) -> std::io::Result<()> {
        if self.position >= self.request.byte_size {
            self.buffer.clear();
            self.buffer_start = self.position;
            return Ok(());
        }
        let range = self.position
            ..self
                .position
                .saturating_add(self.chunk_size)
                .min(self.request.byte_size);
        let response = self
            .backend
            .read_range(&self.request, range.clone())
            .map_err(io_other)?;
        validate_range(&self.request, &range, &response).map_err(io_other)?;
        self.buffer = response.data;
        self.buffer_start = range.start;
        Ok(())
    }
}

fn validate_metadata(request: &GcsObjectRequest, actual: &GcsObjectMetadata) -> anyhow::Result<()> {
    if actual.generation != request.generation {
        bail!(
            "GCS generation mismatch: declared {}, got {}",
            request.generation,
            actual.generation
        );
    }
    if actual.byte_size != request.byte_size {
        bail!(
            "GCS byte-size mismatch: declared {}, got {}",
            request.byte_size,
            actual.byte_size
        );
    }
    if actual.md5_base64 != request.md5_base64 {
        bail!("GCS complete MD5 mismatch");
    }
    Ok(())
}

fn validate_range(
    request: &GcsObjectRequest,
    requested: &Range<u64>,
    actual: &GcsRangeResponse,
) -> anyhow::Result<()> {
    if actual.generation != request.generation {
        bail!("generation-bound GCS read returned a substituted generation");
    }
    if actual.total_size != request.byte_size {
        bail!("generation-bound GCS read returned a substituted total size");
    }
    let expected_len = requested.end - requested.start;
    if actual.range_start != requested.start || actual.data.len() as u64 != expected_len {
        bail!("generation-bound GCS read returned the wrong byte range");
    }
    Ok(())
}

fn io_other(error: anyhow::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, format!("{error:#}"))
}

impl Read for ImmutableGcsReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() || self.position >= self.request.byte_size {
            return Ok(0);
        }
        let buffer_end = self.buffer_start + self.buffer.len() as u64;
        if self.buffer.is_empty()
            || self.position < self.buffer_start
            || self.position >= buffer_end
        {
            self.fill_buffer()?;
        }
        let offset = (self.position - self.buffer_start) as usize;
        let count = output.len().min(self.buffer.len() - offset);
        output[..count].copy_from_slice(&self.buffer[offset..offset + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl Seek for ImmutableGcsReader {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let next = match position {
            SeekFrom::Start(value) => value,
            SeekFrom::End(delta) => checked_offset(self.request.byte_size, delta)?,
            SeekFrom::Current(delta) => checked_offset(self.position, delta)?,
        };
        if next > self.request.byte_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek exceeds immutable GCS object size",
            ));
        }
        self.position = next;
        Ok(next)
    }
}

fn checked_offset(base: u64, delta: i64) -> std::io::Result<u64> {
    if delta >= 0 {
        base.checked_add(delta as u64)
    } else {
        base.checked_sub(delta.unsigned_abs())
    }
    .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "seek offset overflow"))
}

#[derive(Clone)]
pub struct HttpGcsBackend {
    client: Client,
    bearer_token: String,
    api_base: reqwest::Url,
    download_base: reqwest::Url,
}

impl HttpGcsBackend {
    pub fn new() -> anyhow::Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()?;
        let bearer_token = resolve_access_token(&client)?;
        Ok(Self {
            client,
            bearer_token,
            api_base: reqwest::Url::parse("https://storage.googleapis.com/storage/v1/")?,
            download_base: reqwest::Url::parse(
                "https://storage.googleapis.com/download/storage/v1/",
            )?,
        })
    }

    fn object_url(
        &self,
        base: &reqwest::Url,
        request: &GcsObjectRequest,
    ) -> anyhow::Result<reqwest::Url> {
        // GCS JSON API treats the complete object name (including `/`) as one
        // path segment. Encoding each slash is required; generic URL path
        // segment helpers can preserve it and silently address a different URL.
        let bucket = utf8_percent_encode(&request.bucket, NON_ALPHANUMERIC);
        let object = utf8_percent_encode(&request.object, NON_ALPHANUMERIC);
        let mut url = reqwest::Url::parse(&format!("{}b/{bucket}/o/{object}", base.as_str()))?;
        url.query_pairs_mut()
            .append_pair("generation", &request.generation);
        Ok(url)
    }

    fn authorized(
        &self,
        request: reqwest::blocking::RequestBuilder,
    ) -> reqwest::blocking::RequestBuilder {
        request.header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetadataDocument {
    generation: String,
    size: String,
    md5_hash: String,
}

impl ImmutableGcsBackend for HttpGcsBackend {
    fn metadata(&self, request: &GcsObjectRequest) -> anyhow::Result<GcsObjectMetadata> {
        let url = self.object_url(&self.api_base, request)?;
        let response = self.authorized(self.client.get(url)).send()?;
        let response = require_success(response, "generation-qualified GCS metadata GET")?;
        let document: MetadataDocument = response.json()?;
        Ok(GcsObjectMetadata {
            generation: document.generation,
            byte_size: document
                .size
                .parse()
                .context("GCS metadata size is not UInt64")?,
            md5_base64: document.md5_hash,
        })
    }

    fn read_range(
        &self,
        request: &GcsObjectRequest,
        range: Range<u64>,
    ) -> anyhow::Result<GcsRangeResponse> {
        if range.start >= range.end || range.end > request.byte_size {
            bail!("invalid generation-qualified GCS range request");
        }
        let mut url = self.object_url(&self.download_base, request)?;
        url.query_pairs_mut().append_pair("alt", "media");
        let response = self
            .authorized(
                self.client
                    .get(url)
                    .header(RANGE, format!("bytes={}-{}", range.start, range.end - 1)),
            )
            .send()?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            bail!(
                "generation-qualified GCS range GET returned HTTP {} instead of 206",
                response.status()
            );
        }
        parse_range_response(response)
    }
}

fn require_success(response: Response, operation: &str) -> anyhow::Result<Response> {
    if !response.status().is_success() {
        bail!("{operation} returned HTTP {}", response.status());
    }
    Ok(response)
}

fn parse_range_response(response: Response) -> anyhow::Result<GcsRangeResponse> {
    let headers = response.headers();
    let generation = required_header(headers, "x-goog-generation")?.to_string();
    let content_range = required_header(headers, CONTENT_RANGE.as_str())?;
    let (range_start, range_end, total_size) = parse_content_range(content_range)?;
    let data = response.bytes()?.to_vec();
    if data.len() as u64 != range_end - range_start + 1 {
        bail!("GCS Content-Range length differs from response body");
    }
    Ok(GcsRangeResponse {
        generation,
        total_size,
        range_start,
        data,
    })
}

fn required_header<'a>(
    headers: &'a reqwest::header::HeaderMap,
    name: &str,
) -> anyhow::Result<&'a str> {
    headers
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("generation-qualified GCS response lacks {name}"))?
        .to_str()
        .with_context(|| format!("generation-qualified GCS response has invalid {name}"))
}

fn parse_content_range(value: &str) -> anyhow::Result<(u64, u64, u64)> {
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(|| anyhow::anyhow!("invalid GCS Content-Range unit"))?;
    let (range, total) = value
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid GCS Content-Range"))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("invalid GCS Content-Range bounds"))?;
    let start = start.parse::<u64>()?;
    let end = end.parse::<u64>()?;
    let total = total.parse::<u64>()?;
    if start > end || end >= total {
        bail!("invalid GCS Content-Range extent");
    }
    Ok((start, end, total))
}

fn resolve_access_token(client: &Client) -> anyhow::Result<String> {
    for name in ["GNOMAD_LR_GCS_BEARER_TOKEN", "GOOGLE_OAUTH_ACCESS_TOKEN"] {
        if let Ok(value) = std::env::var(name) {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        }
    }

    let metadata = client
        .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
        .header("Metadata-Flavor", "Google")
        .timeout(Duration::from_secs(2))
        .send();
    if let Ok(response) = metadata {
        if response.status().is_success() {
            let value: serde_json::Value = response.json()?;
            if let Some(token) = value
                .get("access_token")
                .and_then(serde_json::Value::as_str)
            {
                if !token.is_empty() {
                    return Ok(token.to_string());
                }
            }
        }
    }

    let output = Command::new("gcloud")
        .args(["auth", "print-access-token", "--quiet"])
        .output()
        .context("no GCS bearer token environment/metadata credential and gcloud is unavailable")?;
    if !output.status.success() {
        bail!("no usable read credential for generation-qualified GCS requests");
    }
    let token = String::from_utf8(output.stdout)?.trim().to_string();
    if token.is_empty() {
        bail!("gcloud returned an empty GCS access token");
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const MD5: &str = "Mhw89IbtUJFk7eweGYH+yA==";

    #[derive(Default)]
    struct FakeBackend {
        bytes: Vec<u8>,
        generation: String,
        size: u64,
        md5: String,
        requests: Mutex<Vec<(String, String, Range<u64>)>>,
        substitute_range_generation: bool,
    }

    impl FakeBackend {
        fn valid(bytes: &[u8]) -> Self {
            Self {
                bytes: bytes.to_vec(),
                generation: "42".into(),
                size: bytes.len() as u64,
                md5: MD5.into(),
                ..Self::default()
            }
        }
    }

    impl ImmutableGcsBackend for FakeBackend {
        fn metadata(&self, request: &GcsObjectRequest) -> anyhow::Result<GcsObjectMetadata> {
            self.requests.lock().unwrap().push((
                request.object.clone(),
                request.generation.clone(),
                0..0,
            ));
            Ok(GcsObjectMetadata {
                generation: self.generation.clone(),
                byte_size: self.size,
                md5_base64: self.md5.clone(),
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
            Ok(GcsRangeResponse {
                generation: if self.substitute_range_generation {
                    "43"
                } else {
                    &self.generation
                }
                .into(),
                total_size: self.size,
                range_start: range.start,
                data: self.bytes[range.start as usize..range.end as usize].to_vec(),
            })
        }
    }

    fn object(uri: &str, generation: &str, size: u64) -> ImmutableGcsObject {
        ImmutableGcsObject {
            uri: uri.into(),
            generation: generation.into(),
            byte_size: size,
            checksum_algorithm: "md5_base64".into(),
            checksum: MD5.into(),
            immutable_read_uri: format!("{uri}?generation={generation}"),
        }
    }

    #[test]
    fn malformed_mutable_missing_and_substituted_generation_uris_fail_closed() {
        let mut values = [
            object("https://example.test/a", "42", 6),
            object("gs://bucket/object?generation=42", "42", 6),
            object("gs://bucket/object", "42", 6),
            object("gs://bucket/object", "42", 6),
        ];
        values[2].immutable_read_uri = "gs://bucket/object".into();
        values[3].immutable_read_uri = "gs://bucket/object?generation=43".into();
        for value in values {
            assert!(value.request().is_err());
        }
    }

    #[test]
    fn stale_generation_size_and_checksum_metadata_are_rejected() {
        let expected = object("gs://bucket/object", "42", 6);
        let mut stale = FakeBackend::valid(b"abcdef");
        stale.generation = "41".into();
        assert!(ImmutableGcsReader::open(Arc::new(stale), &expected).is_err());
        let mut wrong_size = FakeBackend::valid(b"abcdef");
        wrong_size.size = 7;
        assert!(ImmutableGcsReader::open(Arc::new(wrong_size), &expected).is_err());
        let mut wrong_md5 = FakeBackend::valid(b"abcdef");
        wrong_md5.md5 = "AAAAAAAAAAAAAAAAAAAAAA==".into();
        assert!(ImmutableGcsReader::open(Arc::new(wrong_md5), &expected).is_err());
    }

    #[test]
    fn source_index_cross_identity_is_rejected() {
        let source = object("gs://bucket/HG00097.hap1.bed.gz", "42", 6);
        let wrong_sample = object("gs://bucket/HG00099.hap1.bed.gz.tbi", "43", 6);
        let wrong_bucket = object("gs://other/HG00097.hap1.bed.gz.tbi", "43", 6);
        assert!(validate_source_index_pair(&source, &wrong_sample).is_err());
        assert!(validate_source_index_pair(&source, &wrong_bucket).is_err());
    }

    #[test]
    fn generation_bound_reader_supports_ranges_and_seeks_without_query_stripping() {
        let backend = Arc::new(FakeBackend::valid(b"abcdef"));
        let expected = object("gs://bucket/object", "42", 6);
        let mut reader = ImmutableGcsReader::open(backend.clone(), &expected)
            .unwrap()
            .with_chunk_size(4);
        let mut first = [0; 2];
        reader.read_exact(&mut first).unwrap();
        assert_eq!(&first, b"ab");
        reader.seek(SeekFrom::Start(4)).unwrap();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).unwrap();
        assert_eq!(tail, b"ef");
        let requests = backend.requests.lock().unwrap();
        assert!(requests.iter().all(|(_, generation, _)| generation == "42"));
        assert!(requests.iter().any(|(_, _, range)| range == &(0..4)));
        assert!(requests.iter().any(|(_, _, range)| range == &(4..6)));
        // A backend that strips/substitutes generation is rejected on the read response.
        drop(requests);
        let mut substituted = FakeBackend::valid(b"abcdef");
        substituted.substitute_range_generation = true;
        let mut reader = ImmutableGcsReader::open(Arc::new(substituted), &expected).unwrap();
        assert!(reader.read(&mut first).is_err());
    }

    /// Explicit read-only acceptance probe. It performs one metadata GET and
    /// one generation-qualified range GET against the frozen HG00097 TBI.
    #[test]
    #[ignore = "requires a read credential and performs exact-generation GCS GETs"]
    fn live_frozen_tbi_generation_bound_read_only_probe() {
        let uri = "gs://fc-fd42e80c-b41e-4e60-a9cf-b7c0ade168c4/submissions/a1e9b9ce-0d67-4d42-b107-b9821f44dd60/MethylationProfiling/4944aeb1-faf3-49a0-995e-93454b836d2f/call-CpgPileup/HG00097.combined.bed.gz.tbi";
        let object = ImmutableGcsObject {
            uri: uri.into(),
            generation: "1777748004803103".into(),
            byte_size: 1_684_181,
            checksum_algorithm: "md5_base64".into(),
            checksum: "JAj0OCPxIIXYXbn/MfkEIg==".into(),
            immutable_read_uri: format!("{uri}?generation=1777748004803103"),
        };
        let backend = Arc::new(HttpGcsBackend::new().unwrap());
        let mut reader = ImmutableGcsReader::open(backend, &object).unwrap();
        let mut prefix = [0u8; 32];
        reader.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix[..2], &[0x1f, 0x8b]);
    }

    #[test]
    fn content_range_parser_is_strict() {
        assert_eq!(parse_content_range("bytes 2-4/6").unwrap(), (2, 4, 6));
        for value in ["2-4/6", "bytes 4-2/6", "bytes 2-6/6", "bytes */6"] {
            assert!(parse_content_range(value).is_err());
        }
    }
}
