use anyhow::{bail, Context};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::Url;
use serde::Serialize;
use std::net::IpAddr;
use std::time::Duration;

const CLICKHOUSE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);

const SCRATCH_DATABASE_PREFIX: &str = "gnomad_lr_y1_scratch_";
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
            TargetKind::Scratch if !database.starts_with(SCRATCH_DATABASE_PREFIX) => {
                bail!("Y1 scratch database must start with {SCRATCH_DATABASE_PREFIX}")
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
        if database == SCRATCH_DATABASE_PREFIX || database == SERVING_DATABASE_PREFIX {
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
        url.query_pairs_mut().append_pair("query", &query);
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
    fn accepts_explicit_loopback_scratch_target() {
        let target = scratch("http://127.0.0.1:8123", "gnomad_lr_y1_scratch_unit").unwrap();
        assert_eq!(target.database(), "gnomad_lr_y1_scratch_unit");
        assert_eq!(target.kind(), TargetKind::Scratch);
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
