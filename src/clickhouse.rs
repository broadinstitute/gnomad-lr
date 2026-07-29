//! Batching ClickHouse HTTP inserter.
//!
//! Buffers rows as NDJSON and POSTs them to ClickHouse's HTTP interface.

use serde::Serialize;
use tracing::info;

/// Execute a single DDL statement against ClickHouse.
pub fn execute_ddl(ch_url: &str, query: &str) -> anyhow::Result<()> {
    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(ch_url)
        .header("Content-Type", "text/plain")
        .body(query.to_string())
        .send()?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        anyhow::bail!(
            "ClickHouse DDL failed ({}): {}",
            status,
            &body[..body.len().min(500)]
        );
    }
    Ok(())
}

/// Initialize the current legacy-contract tables by executing embedded SQL files.
/// The primary DDL is not the cohort-aware Y1 v4 schema.
pub fn init_tables(ch_url: &str) -> anyhow::Result<()> {
    let schemas: &[(&str, &str)] = &[
        ("lr_variants", include_str!("../sql/lr_variants.sql")),
        ("lr_haplotypes", include_str!("../sql/lr_haplotypes.sql")),
        ("lr_coverage", include_str!("../sql/lr_coverage.sql")),
        (
            "lr_sample_metadata",
            include_str!("../sql/lr_sample_metadata.sql"),
        ),
        (
            "lr_str_histograms",
            include_str!("../sql/lr_str_histograms.sql"),
        ),
        ("lr_methylation", include_str!("../sql/lr_methylation.sql")),
        (
            "lr_methylation_summary_mv",
            include_str!("../sql/lr_methylation_summary_mv.sql"),
        ),
    ];

    for (name, ddl) in schemas {
        info!("Initializing table: {}", name);
        execute_ddl(ch_url, ddl)?;
    }

    info!("All tables initialized");
    Ok(())
}

pub struct ClickHouseInserter {
    url: String,
    table: String,
    batch_size: usize,
    buffer: Vec<String>,
    total_rows: usize,
    client: reqwest::blocking::Client,
    /// Accumulated time spent in HTTP POSTs to ClickHouse (ms).
    pub insert_time_ms: u64,
    /// Number of flush() calls that sent data.
    pub flush_count: usize,
}

impl ClickHouseInserter {
    pub fn new(url: &str, table: &str, batch_size: usize) -> Self {
        Self {
            url: url.to_string(),
            table: table.to_string(),
            batch_size,
            buffer: Vec::with_capacity(batch_size),
            total_rows: 0,
            client: reqwest::blocking::Client::new(),
            insert_time_ms: 0,
            flush_count: 0,
        }
    }

    /// Buffer a row. Auto-flushes when batch_size is reached.
    pub fn insert<T: Serialize>(&mut self, row: &T) -> anyhow::Result<()> {
        let json = serde_json::to_string(row)?;
        self.buffer.push(json);
        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Insert a pre-serialized JSON string. Auto-flushes when batch_size is reached.
    pub fn insert_raw(&mut self, json: String) -> anyhow::Result<()> {
        self.buffer.push(json);
        if self.buffer.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush buffered rows to ClickHouse.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let body = self.buffer.join("\n") + "\n";
        let rows_in_batch = self.buffer.len();

        let query = format!("INSERT INTO {} FORMAT JSONEachRow", self.table);

        let post_start = std::time::Instant::now();
        let resp = self
            .client
            .post(&self.url)
            .query(&[("query", query.as_str())])
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()?;
        let post_elapsed = post_start.elapsed();

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().unwrap_or_default();
            anyhow::bail!(
                "ClickHouse insert failed ({}): {}",
                status,
                &body[..body.len().min(500)]
            );
        }

        self.insert_time_ms += post_elapsed.as_millis() as u64;
        self.flush_count += 1;
        self.total_rows += rows_in_batch;
        self.buffer.clear();

        info!(
            "Inserted {} rows ({} total)",
            rows_in_batch, self.total_rows
        );
        Ok(())
    }

    /// Flush remaining rows and print final stats.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.flush()?;
        info!(
            "Finished inserting into {}: {} total rows",
            self.table, self.total_rows
        );
        Ok(())
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn request_query_preserves_selected_database() {
        let request = reqwest::blocking::Client::new()
            .post("http://127.0.0.1:8123/?database=gnomad_lr_smoke")
            .query(&[("query", "SELECT 1")])
            .build()
            .unwrap();
        let params: std::collections::HashMap<_, _> = request.url().query_pairs().collect();

        assert_eq!(params.get("database").unwrap(), "gnomad_lr_smoke");
        assert_eq!(params.get("query").unwrap(), "SELECT 1");
    }
}
