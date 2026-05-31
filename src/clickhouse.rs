//! Batching ClickHouse HTTP inserter.
//!
//! Buffers rows as NDJSON and POSTs them to ClickHouse's HTTP interface.

use serde::Serialize;
use tracing::info;

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
        let url = format!("{}/?query={}", self.url, urlencoding::encode(&query));

        let post_start = std::time::Instant::now();
        let resp = self
            .client
            .post(&url)
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

        info!("Inserted {} rows ({} total)", rows_in_batch, self.total_rows);
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
