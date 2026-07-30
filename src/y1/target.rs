use anyhow::{bail, Context};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::Url;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::net::IpAddr;
use std::thread;
use std::time::{Duration, Instant};

const CLICKHOUSE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const WORKER_INSERT_DRAIN_TIMEOUT: Duration = Duration::from_secs(120);
const WORKER_INSERT_DRAIN_POLL: Duration = Duration::from_millis(100);

pub const Y1_WORKER_USERNAME_ENV: &str = "Y1_CLICKHOUSE_WORKER_USER";
pub const Y1_WORKER_PASSWORD_ENV: &str = "Y1_CLICKHOUSE_WORKER_PASSWORD";

const SCRATCH_DATABASE_PREFIX: &str = "gnomad_lr_y1_scratch_";
const FULL_PROTOTYPE_SCRATCH_DATABASE_PREFIX: &str = "gnomad_lr_y1_full_prototype_scratch_";
const SERVING_DATABASE: &str = "gnomad_lr_y1_pilot";
const SERVING_DATABASE_PREFIX: &str = "gnomad_lr_y1_serving_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    Scratch,
    Serving,
}

impl TargetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scratch => "scratch",
            Self::Serving => "serving",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthSource {
    None,
    /// Unauthenticated ClickHouse access confined to a private RFC1918 endpoint
    /// and a separately managed VPC firewall, matching the established pool setup.
    PrivateNetwork,
    Environment {
        username_variable: String,
        password_variable: String,
    },
}

/// A fail-closed Y1 ClickHouse destination.
///
/// The endpoint, database, and credential source are deliberately separate.
/// Secrets are resolved only while a request is built and are never stored in
/// this value or interpolated into a URL.
#[derive(Debug, Clone)]
pub struct ClickHouseTarget {
    endpoint: Url,
    database: String,
    kind: TargetKind,
    auth: AuthSource,
}

impl ClickHouseTarget {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: &str,
        database: &str,
        kind: TargetKind,
        auth: AuthSource,
        allow_remote: bool,
        allow_serving: bool,
    ) -> anyhow::Result<Self> {
        let endpoint = Url::parse(endpoint).context("invalid ClickHouse endpoint URL")?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!("ClickHouse endpoint scheme must be http or https");
        }
        if endpoint.host_str().is_none() {
            bail!("ClickHouse endpoint must include a host");
        }
        if !endpoint.username().is_empty() || endpoint.password().is_some() {
            bail!("Y1 ClickHouse credentials must come from the declared auth source, not the endpoint URL");
        }
        if endpoint.query().is_some() {
            bail!("Y1 ClickHouse endpoint must not contain query parameters; pass the database separately");
        }
        if endpoint.fragment().is_some() {
            bail!("Y1 ClickHouse endpoint must not contain a fragment");
        }
        if !matches!(endpoint.path(), "" | "/") {
            bail!("Y1 ClickHouse endpoint path must be empty or '/'");
        }

        validate_identifier(database, "database")?;
        if database == "default" {
            bail!("the default ClickHouse database is forbidden for Y1");
        }
        match kind {
            TargetKind::Scratch
                if !database.starts_with(SCRATCH_DATABASE_PREFIX)
                    && !database.starts_with(FULL_PROTOTYPE_SCRATCH_DATABASE_PREFIX) =>
            {
                bail!(
                    "Y1 scratch database must start with {SCRATCH_DATABASE_PREFIX} or {FULL_PROTOTYPE_SCRATCH_DATABASE_PREFIX}"
                )
            }
            TargetKind::Serving
                if database != SERVING_DATABASE
                    && !database.starts_with(SERVING_DATABASE_PREFIX) =>
            {
                bail!(
                    "Y1 serving database must be {SERVING_DATABASE} or start with {SERVING_DATABASE_PREFIX}"
                )
            }
            _ => {}
        }
        if database == SCRATCH_DATABASE_PREFIX
            || database == FULL_PROTOTYPE_SCRATCH_DATABASE_PREFIX
            || database == SERVING_DATABASE_PREFIX
        {
            bail!("Y1 database prefix must have a non-empty suffix");
        }
        if kind == TargetKind::Serving && !allow_serving {
            bail!("serving Y1 targets require the explicit --allow-serving acknowledgement");
        }

        let loopback = endpoint_is_loopback(&endpoint);
        if !loopback && !allow_remote {
            bail!("remote Y1 ClickHouse endpoints require the explicit --allow-remote acknowledgement");
        }
        if !loopback && matches!(auth, AuthSource::None) {
            bail!("remote Y1 ClickHouse endpoints require an authenticated or explicit private-network source");
        }
        if matches!(auth, AuthSource::PrivateNetwork) && !endpoint_is_private(&endpoint) {
            bail!("private-network ClickHouse access requires a literal RFC1918 endpoint");
        }
        if let AuthSource::Environment {
            username_variable,
            password_variable,
        } = &auth
        {
            validate_environment_variable(username_variable)?;
            validate_environment_variable(password_variable)?;
            if username_variable == password_variable {
                bail!("username and password must use different environment variables");
            }
        }

        Ok(Self {
            endpoint,
            database: database.to_string(),
            kind,
            auth,
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn kind(&self) -> TargetKind {
        self.kind
    }

    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    pub fn display_name(&self) -> String {
        format!(
            "{} database={} kind={}",
            self.endpoint,
            self.database,
            self.kind.as_str()
        )
    }

    pub(crate) fn same_destination(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
            && self.database == other.database
            && self.kind == other.kind
    }

    pub(crate) fn uses_environment_auth(&self) -> bool {
        matches!(self.auth, AuthSource::Environment { .. })
    }

    pub fn attest_current_user(&self, expected: &str) -> anyhow::Result<String> {
        validate_identifier(expected, "ClickHouse principal")?;
        let current = self.query_text("SELECT currentUser() FORMAT TabSeparated", &[])?;
        let current = current.trim();
        if current != expected {
            bail!(
                "configured ClickHouse principal {:?} does not match authenticated principal {:?}",
                expected,
                current
            );
        }
        Ok(current.to_string())
    }

    pub fn attest_synchronous_inserts(&self) -> anyhow::Result<()> {
        let value = self.query_text(
            "SELECT value FROM system.settings WHERE name = 'async_insert' FORMAT TabSeparated",
            &[],
        )?;
        if value.trim() != "0" {
            bail!(
                "Y1 workers require async_insert = 0 so the database fence can drain every insert"
            );
        }
        Ok(())
    }

    pub fn execute(&self, query: &str) -> anyhow::Result<()> {
        self.execute_with_params(query, &[])
    }

    pub fn execute_with_params(
        &self,
        query: &str,
        parameters: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        let response = self
            .authorized(clickhouse_client()?.post(self.request_url(parameters)?))?
            .header("Content-Type", "text/plain")
            .body(query.to_string())
            .send()
            .context("failed to send ClickHouse request")?;
        check_response(response, "query").map(|_| ())
    }

    pub fn query_text(&self, query: &str, parameters: &[(&str, &str)]) -> anyhow::Result<String> {
        let response = self
            .authorized(clickhouse_client()?.post(self.request_url(parameters)?))?
            .header("Content-Type", "text/plain")
            .body(query.to_string())
            .send()
            .context("failed to send ClickHouse query")?;
        check_response(response, "query")
    }

    /// Hash a deterministically ordered ClickHouse response without buffering a
    /// full-genome table in process memory. `domain` binds empty and nonempty
    /// streams to the table/task/attempt identity selected by the caller.
    pub fn query_sha256(
        &self,
        query: &str,
        parameters: &[(&str, &str)],
        domain: &[u8],
    ) -> anyhow::Result<String> {
        let mut response = self
            .authorized(clickhouse_client()?.post(self.request_url(parameters)?))?
            .header("Content-Type", "text/plain")
            .body(query.to_string())
            .send()
            .context("failed to send ClickHouse digest query")?;
        if !response.status().is_success() {
            return check_response(response, "digest query").map(|_| unreachable!());
        }
        let mut digest = canonical_content_hasher(domain);
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = response
                .read(&mut buffer)
                .context("failed while streaming ClickHouse digest response")?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(format!("{:x}", digest.finalize()))
    }

    pub fn insert_json_each_row<T: Serialize>(
        &self,
        table: &str,
        rows: &[T],
    ) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        validate_identifier(table, "table")?;
        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let mut body = String::new();
        for row in rows {
            body.push_str(&serde_json::to_string(row)?);
            body.push('\n');
        }

        let mut url = self.request_url(&[])?;
        url.query_pairs_mut()
            .append_pair("query", &query)
            .append_pair("async_insert", "0")
            .append_pair("wait_for_async_insert", "1");
        let response = self
            .authorized(clickhouse_client()?.post(url))?
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .context("failed to send ClickHouse insert")?;
        check_response(response, "insert").map(|_| ())
    }

    fn request_url(&self, parameters: &[(&str, &str)]) -> anyhow::Result<Url> {
        let mut url = self.endpoint.clone();
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("database", &self.database);
            for (name, value) in parameters {
                validate_parameter_name(name)?;
                pairs.append_pair(&format!("param_{name}"), value);
            }
        }
        Ok(url)
    }

    fn authorized(&self, request: RequestBuilder) -> anyhow::Result<RequestBuilder> {
        match &self.auth {
            AuthSource::None | AuthSource::PrivateNetwork => Ok(request),
            AuthSource::Environment {
                username_variable,
                password_variable,
            } => {
                let username = std::env::var(username_variable).with_context(|| {
                    format!("ClickHouse username environment variable {username_variable} is unset")
                })?;
                let password = std::env::var(password_variable).with_context(|| {
                    format!("ClickHouse password environment variable {password_variable} is unset")
                })?;
                if username.is_empty() || password.is_empty() {
                    bail!("ClickHouse credential environment variables must be non-empty");
                }
                Ok(request.basic_auth(username, Some(password)))
            }
        }
    }
}

/// Database-enforced fence for the one dedicated principal allowed to perform
/// Y1 worker inserts. The administrator target remains separate so freezing a
/// worker cannot freeze the finalizer itself.
#[derive(Debug, Clone)]
pub struct WorkerWriteFence {
    worker: ClickHouseTarget,
    principal: String,
}

impl WorkerWriteFence {
    pub fn new(
        administrator: &ClickHouseTarget,
        worker: ClickHouseTarget,
        principal: &str,
    ) -> anyhow::Result<Self> {
        if administrator.kind() != TargetKind::Scratch || !administrator.same_destination(&worker) {
            bail!("worker fence requires distinct credentials for the same scratch destination");
        }
        if !worker.uses_environment_auth() {
            bail!(
                "worker fence requires a dedicated environment-authenticated ClickHouse principal"
            );
        }
        validate_identifier(principal, "worker principal")?;
        Ok(Self {
            worker,
            principal: principal.to_string(),
        })
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    pub fn attest_identity(&self) -> anyhow::Result<()> {
        self.worker.attest_current_user(&self.principal).map(|_| ())
    }

    pub fn apply_and_drain(&self, administrator: &ClickHouseTarget) -> anyhow::Result<()> {
        self.validate_administrator(administrator)?;
        self.attest_identity()?;
        self.worker.attest_synchronous_inserts()?;
        let found = administrator.query_text(
            "SELECT count() FROM system.users WHERE name = {principal:String} FORMAT TabSeparated",
            &[("principal", self.principal())],
        )?;
        if found.trim() != "1" {
            bail!("dedicated ClickHouse worker principal is absent or ambiguous");
        }
        administrator.execute(&format!(
            "ALTER USER {} SETTINGS readonly = 1, async_insert = 0",
            self.principal
        ))?;
        self.attest_fenced_and_drained(administrator)
    }

    pub fn attest_fenced_and_drained(
        &self,
        administrator: &ClickHouseTarget,
    ) -> anyhow::Result<()> {
        self.validate_administrator(administrator)?;
        self.attest_identity()?;
        let settings = self.worker.query_text(
            "SELECT name, value FROM system.settings WHERE name IN ('readonly', 'async_insert') ORDER BY name FORMAT TabSeparated",
            &[],
        )?;
        if settings.trim() != "async_insert\t0\nreadonly\t1" {
            bail!("dedicated ClickHouse worker principal is not durably read-only with synchronous inserts");
        }

        let started = Instant::now();
        loop {
            let active = administrator.query_text(
                "SELECT count() FROM system.processes WHERE user = {principal:String} AND positionCaseInsensitive(query, 'INSERT') = 1 FORMAT TabSeparated",
                &[("principal", self.principal())],
            )?;
            if active.trim() == "0" {
                return Ok(());
            }
            if started.elapsed() >= WORKER_INSERT_DRAIN_TIMEOUT {
                bail!("timed out draining active ClickHouse worker inserts after writer fence");
            }
            thread::sleep(WORKER_INSERT_DRAIN_POLL);
        }
    }

    fn validate_administrator(&self, administrator: &ClickHouseTarget) -> anyhow::Result<()> {
        if !administrator.same_destination(&self.worker) {
            bail!("worker fence administrator and worker targets differ");
        }
        let administrator_principal =
            administrator.query_text("SELECT currentUser() FORMAT TabSeparated", &[])?;
        if administrator_principal.trim() == self.principal {
            bail!("finalizer administrator must be distinct from the dedicated worker principal");
        }
        Ok(())
    }
}

fn canonical_content_hasher(domain: &[u8]) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"gnomad-lr-y1-canonical-content-v1\0");
    digest.update(domain);
    digest.update([0]);
    digest
}

fn clickhouse_client() -> anyhow::Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(CLICKHOUSE_REQUEST_TIMEOUT)
        .build()
        .context("failed to build ClickHouse HTTP client")
}

fn endpoint_is_private(endpoint: &Url) -> bool {
    endpoint
        .host_str()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .map(|address| match address {
            IpAddr::V4(address) => address.is_private(),
            IpAddr::V6(address) => address.is_unique_local(),
        })
        .unwrap_or(false)
}

fn endpoint_is_loopback(endpoint: &Url) -> bool {
    let Some(host) = endpoint.host_str() else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false)
}

fn validate_identifier(value: &str, label: &str) -> anyhow::Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        bail!("{label} must not be empty");
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        bail!("{label} must be an unquoted ClickHouse identifier");
    }
    Ok(())
}

fn validate_environment_variable(value: &str) -> anyhow::Result<()> {
    validate_identifier(value, "credential environment variable")
}

fn validate_parameter_name(value: &str) -> anyhow::Result<()> {
    validate_identifier(value, "ClickHouse parameter name")
}

fn check_response(
    response: reqwest::blocking::Response,
    operation: &str,
) -> anyhow::Result<String> {
    let status = response.status();
    let body = response.text().unwrap_or_default();
    if !status.is_success() {
        bail!(
            "ClickHouse {operation} failed ({status}): {}",
            &body[..body.len().min(500)]
        );
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(endpoint: &str, database: &str) -> anyhow::Result<ClickHouseTarget> {
        ClickHouseTarget::new(
            endpoint,
            database,
            TargetKind::Scratch,
            AuthSource::None,
            false,
            false,
        )
    }

    #[test]
    fn same_count_content_mutation_changes_canonical_sha256() {
        let mut first = canonical_content_hasher(b"summaries\0task\0attempt\x002");
        first.update(b"row-a\nrow-b\n");
        let mut changed = canonical_content_hasher(b"summaries\0task\0attempt\x002");
        changed.update(b"row-a\nrow-c\n");
        assert_ne!(
            format!("{:x}", first.finalize()),
            format!("{:x}", changed.finalize())
        );
    }

    #[test]
    fn writer_fence_requires_a_separate_environment_authenticated_principal() {
        let administrator =
            scratch("http://127.0.0.1:8123", "gnomad_lr_y1_scratch_v5_unit").unwrap();
        let unauthenticated_worker = administrator.clone();
        assert!(WorkerWriteFence::new(
            &administrator,
            unauthenticated_worker,
            "gnomad_lr_y1_worker"
        )
        .is_err());

        let other_database = ClickHouseTarget::new(
            "http://127.0.0.1:8123",
            "gnomad_lr_y1_scratch_v5_other",
            TargetKind::Scratch,
            AuthSource::Environment {
                username_variable: Y1_WORKER_USERNAME_ENV.into(),
                password_variable: Y1_WORKER_PASSWORD_ENV.into(),
            },
            false,
            false,
        )
        .unwrap();
        assert!(
            WorkerWriteFence::new(&administrator, other_database, "gnomad_lr_y1_worker").is_err()
        );
    }

    #[test]
    fn accepts_explicit_loopback_scratch_target() {
        let target = scratch("http://127.0.0.1:8123", "gnomad_lr_y1_scratch_unit").unwrap();
        assert_eq!(target.database(), "gnomad_lr_y1_scratch_unit");
        assert_eq!(target.kind(), TargetKind::Scratch);

        let full_prototype = scratch(
            "http://127.0.0.1:8123",
            "gnomad_lr_y1_full_prototype_scratch_v1",
        )
        .unwrap();
        assert_eq!(
            full_prototype.database(),
            "gnomad_lr_y1_full_prototype_scratch_v1"
        );
        assert!(scratch(
            "http://127.0.0.1:8123",
            "gnomad_lr_y1_full_prototype_scratch_"
        )
        .is_err());
        assert!(scratch(
            "http://127.0.0.1:8123",
            "prefix_gnomad_lr_y1_full_prototype_scratch_v1"
        )
        .is_err());
    }

    #[test]
    fn rejects_default_embedded_database_and_credentials() {
        assert!(scratch("http://127.0.0.1:8123", "default").is_err());
        assert!(scratch(
            "http://127.0.0.1:8123/?database=gnomad_lr_y1_scratch_unit",
            "gnomad_lr_y1_scratch_unit"
        )
        .is_err());
        assert!(scratch(
            "http://user:secret@127.0.0.1:8123",
            "gnomad_lr_y1_scratch_unit"
        )
        .is_err());
    }

    #[test]
    fn rejects_remote_or_serving_targets_without_explicit_acknowledgements() {
        assert!(ClickHouseTarget::new(
            "http://192.0.2.1:8123",
            "gnomad_lr_y1_scratch_unit",
            TargetKind::Scratch,
            AuthSource::None,
            false,
            false,
        )
        .is_err());
        assert!(ClickHouseTarget::new(
            "http://127.0.0.1:8123",
            "gnomad_lr_y1_pilot",
            TargetKind::Serving,
            AuthSource::None,
            false,
            false,
        )
        .is_err());
    }

    #[test]
    fn private_network_auth_accepts_only_literal_private_addresses() {
        let target = ClickHouseTarget::new(
            "http://192.168.0.15:8123",
            "gnomad_lr_y1_scratch_unit",
            TargetKind::Scratch,
            AuthSource::PrivateNetwork,
            true,
            false,
        )
        .unwrap();
        assert_eq!(target.endpoint().host_str(), Some("192.168.0.15"));
        assert!(ClickHouseTarget::new(
            "https://clickhouse.example.org:8443",
            "gnomad_lr_y1_scratch_unit",
            TargetKind::Scratch,
            AuthSource::PrivateNetwork,
            true,
            false,
        )
        .is_err());
    }

    #[test]
    fn remote_target_requires_an_auth_source_even_when_acknowledged() {
        assert!(ClickHouseTarget::new(
            "https://clickhouse.example.org:8443",
            "gnomad_lr_y1_scratch_unit",
            TargetKind::Scratch,
            AuthSource::None,
            true,
            false,
        )
        .is_err());

        let target = ClickHouseTarget::new(
            "https://clickhouse.example.org:8443",
            "gnomad_lr_y1_scratch_unit",
            TargetKind::Scratch,
            AuthSource::Environment {
                username_variable: "Y1_CLICKHOUSE_USER".to_string(),
                password_variable: "Y1_CLICKHOUSE_PASSWORD".to_string(),
            },
            true,
            false,
        )
        .unwrap();
        assert_eq!(target.endpoint().host_str(), Some("clickhouse.example.org"));
    }
}
