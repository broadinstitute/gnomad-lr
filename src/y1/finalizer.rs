use super::{
    contig::grch38_contig_length, record_load_run, storage::delete_attempt_rows, ClickHouseTarget,
    LoadRunLedgerRow, LoadScope, PoolY1TaskSpec, ReferenceGenome, Release, StagedCounts,
    TargetKind, Y1_SCHEMA_VERSION,
};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentExpectedCounts {
    pub contract_version: u8,
    pub run_id: String,
    pub cohort: String,
    pub chrom: String,
    pub evidence_uri: String,
    pub producer: String,
    pub source_generation: String,
    pub source_checksum: String,
    pub counts: StagedCounts,
    pub facts: IndependentReconciliationFacts,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReconciliationFacts {
    pub source_records: u64,
    pub alt_alleles: u64,
    pub frequency_rows: u64,
    pub genotype_calls: u64,
    pub called_alleles: u64,
    pub carrier_alt_copies: u64,
    pub fully_missing_genotypes: u64,
    pub partially_called_genotypes: u64,
    pub annotated_alt_alleles: u64,
    pub source_content_sha256: String,
    pub genotype_content_sha256: String,
    pub annotation_content_sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AttemptView {
    task_id: String,
    attempt_id: String,
    state: String,
    chrom: String,
    interval_start: u32,
    interval_end: u32,
    source_records: u64,
    summary_rows: u64,
    allele_rows: u64,
    frequency_rows: u64,
    carrier_rows: u64,
    rejected_records: u64,
    report_json: String,
}

#[derive(Debug, Clone, Deserialize)]
struct PhysicalAttemptView {
    table: String,
    task_id: String,
    attempt_id: String,
    rows: u64,
    unique_keys: u64,
    identity_violations: u64,
    min_position: u32,
    max_position: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalContentDigest {
    pub table: String,
    pub task_id: String,
    pub attempt_id: String,
    pub rows: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalTableCount {
    pub table: String,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadAcceptanceReceipt {
    pub contract_version: u8,
    pub schema_version: u16,
    pub run_id: String,
    pub cohort: String,
    pub chrom: String,
    pub manifest_sha256: String,
    pub independent_counts_sha256: String,
    pub source_uri: String,
    pub source_generation: String,
    pub source_checksum: String,
    pub source_index_uri: String,
    pub source_index_generation: String,
    pub source_index_checksum: String,
    pub accepted_attempts: BTreeMap<String, String>,
    pub failed_attempts: BTreeMap<String, Vec<String>>,
    pub expected_counts: StagedCounts,
    pub source_content_sha256: String,
    pub genotype_content_sha256: String,
    pub annotation_content_sha256: String,
    pub canonical_counts: Vec<CanonicalTableCount>,
    pub canonical_digests: Vec<CanonicalContentDigest>,
}

#[derive(Debug, Serialize)]
pub struct FinalizationReport {
    pub run_id: String,
    pub cohort: String,
    pub chrom: String,
    pub operator_identity: String,
    pub manifest_tasks: usize,
    pub manifest_sha256: String,
    pub accepted_attempts: BTreeMap<String, String>,
    pub failed_attempts: BTreeMap<String, Vec<String>>,
    pub expected_counts: StagedCounts,
    pub independent_evidence_uri: String,
    pub independent_counts_sha256: String,
    pub acceptance_receipt_sha256: String,
    pub acceptance: LoadAcceptanceReceipt,
    pub accepted: bool,
    pub frozen: bool,
    pub published: bool,
}

struct LedgerCoverage {
    accepted: BTreeMap<String, String>,
    failed: BTreeMap<String, Vec<String>>,
    nonaccepted: Vec<(String, String)>,
    counts: Vec<CanonicalTableCount>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PhysicalPhase {
    BeforeCleanup,
    Frozen,
}

#[derive(Clone, Copy)]
struct TableSpec {
    label: &'static str,
    table: &'static str,
    unique_key: &'static str,
    columns: &'static str,
    order_by: &'static str,
    has_primary_identity: bool,
}

const TABLES: [TableSpec; 5] = [
    TableSpec {
        label: "summaries",
        table: "lr_y1_summaries",
        unique_key: "tuple(position, source_variant_id)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,source_variant_id,ref_allele,alts,allele_type,qual,filters,ac,an,af,allele_lengths,length_provenance,source_allele_length,source_svlen,source_svlen_present,frequencies_json,source_info_json",
        order_by: "chrom,position,source_variant_id",
        has_primary_identity: true,
    },
    TableSpec {
        label: "alleles",
        table: "lr_y1_alleles",
        unique_key: "tuple(position, source_variant_id, alt_index)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,reference_end,xpos,source_variant_id,alt_index,ref_allele,alt,allele_type,qual,filters,ac,an,af,allele_length,length_provenance,rsids,cadd_phred,phylop,major_consequence,short_read_match_id,short_read_match_type,short_read_match_source",
        order_by: "chrom,position,source_variant_id,alt_index",
        has_primary_identity: true,
    },
    TableSpec {
        label: "frequencies",
        table: "lr_y1_frequencies",
        unique_key: "tuple(position, source_variant_id, alt_index, division)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,source_variant_id,alt_index,division,ac,an,af,values_available",
        order_by: "chrom,position,source_variant_id,alt_index,division",
        has_primary_identity: true,
    },
    TableSpec {
        label: "carriers",
        table: "lr_y1_carriers",
        unique_key: "tuple(position, source_variant_id, alt_index, sample_id, genotype_position)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,source_variant_id,alt_index,alt,sample_id,genotype_position,gt_alleles,gt_phased,genotype_fields_json,position_fields_json",
        order_by: "chrom,position,source_variant_id,alt_index,sample_id,genotype_position",
        has_primary_identity: true,
    },
    TableSpec {
        label: "rejects",
        table: "lr_y1_rejects_staging",
        unique_key: "tuple(record_number, source_variant_id, reject_code, message)",
        columns: "run_id,task_id,attempt_id,record_number,source_variant_id,reject_code,message",
        order_by: "reject_code,record_number,source_variant_id,message",
        has_primary_identity: false,
    },
];

/// Backward-compatible chr22-only entry point with its original signature.
pub fn finalize_chr22_run(
    target: &ClickHouseTarget,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<FinalizationReport> {
    finalize_contig_run_inner(
        target,
        manifest_path,
        expected_path,
        operator_identity,
        Some("chr22"),
    )
}

/// Freeze and accept exactly one complete canonical GRCh38 contig in place.
pub fn finalize_contig_run(
    target: &ClickHouseTarget,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<FinalizationReport> {
    finalize_contig_run_inner(
        target,
        manifest_path,
        expected_path,
        operator_identity,
        None,
    )
}

fn finalize_contig_run_inner(
    target: &ClickHouseTarget,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
    required_chrom: Option<&str>,
) -> anyhow::Result<FinalizationReport> {
    if target.kind() != TargetKind::Scratch {
        bail!("contig candidate finalization is restricted to an isolated scratch target");
    }
    if operator_identity.trim().is_empty() {
        bail!("finalization requires a non-empty operator identity");
    }

    let manifest_bytes = std::fs::read(manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let tasks: Vec<PoolY1TaskSpec> = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("invalid task manifest {}", manifest_path.display()))?;
    let run = validate_manifest(&tasks)?;
    if required_chrom.is_some_and(|chrom| run.chrom != chrom) {
        bail!("the legacy chr22 finalizer accepts only chr22 manifests");
    }
    let manifest_sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));

    let expected_bytes = std::fs::read(expected_path)
        .with_context(|| format!("failed to read {}", expected_path.display()))?;
    let expected: IndependentExpectedCounts = serde_json::from_slice(&expected_bytes)
        .with_context(|| format!("invalid independent counts {}", expected_path.display()))?;
    validate_independent_reconciliation(&expected, &run)?;
    let independent_counts_sha256 = format!("{:x}", Sha256::digest(&expected_bytes));

    ensure_freeze_transition(target, &run.run_id)?;
    record_state(
        target,
        &run,
        &expected,
        tasks.len(),
        revision_now()?,
        "freezing",
        operator_identity,
    )?;

    let result = (|| -> anyhow::Result<FinalizationReport> {
        // The freezing row is the application-level writer fence. Every primary
        // batch checks it before insertion. All attempt claims must now be terminal.
        let before = validate_ledger_coverage(target, &run, &tasks, PhysicalPhase::BeforeCleanup)?;
        validate_expected_counts(&before.counts, expected.counts)?;

        // Fresh-instance retries are resolved in place. Failed attempt rows are
        // removed synchronously; no accepted rows are copied to another table.
        for (task_id, attempt_id) in &before.nonaccepted {
            delete_attempt_rows(target, &run.run_id, task_id, attempt_id)?;
        }

        let frozen = validate_ledger_coverage(target, &run, &tasks, PhysicalPhase::Frozen)?;
        validate_expected_counts(&frozen.counts, expected.counts)?;
        let digests = read_canonical_digests(target, &run, &frozen.accepted, &frozen.counts)?;
        let acceptance = build_acceptance_receipt(
            &run,
            &expected,
            &frozen,
            manifest_sha256.clone(),
            independent_counts_sha256.clone(),
            digests,
        );
        let acceptance_json = serde_json::to_string(&acceptance)?;
        let acceptance_receipt_sha256 = format!("{:x}", Sha256::digest(acceptance_json.as_bytes()));

        record_state(
            target,
            &run,
            &expected,
            tasks.len(),
            revision_now()?,
            "frozen",
            &acceptance_json,
        )?;
        validate_persisted_receipt(target, &run.run_id, "frozen", &acceptance_json)?;

        // Reread the exact same canonical rows after the durable frozen marker.
        // Any late ledger contribution, row, deletion, or same-count mutation
        // changes this snapshot and fails before acceptance.
        let reread = validate_ledger_coverage(target, &run, &tasks, PhysicalPhase::Frozen)?;
        validate_expected_counts(&reread.counts, expected.counts)?;
        let reread_digests =
            read_canonical_digests(target, &run, &reread.accepted, &reread.counts)?;
        let reread_receipt = build_acceptance_receipt(
            &run,
            &expected,
            &reread,
            manifest_sha256.clone(),
            independent_counts_sha256.clone(),
            reread_digests,
        );
        if reread_receipt != acceptance {
            bail!("canonical rows or attempt ledger changed after the run was frozen");
        }

        record_state(
            target,
            &run,
            &expected,
            tasks.len(),
            revision_now()?,
            "accepted_frozen",
            &acceptance_json,
        )?;
        validate_persisted_receipt(target, &run.run_id, "accepted_frozen", &acceptance_json)?;

        Ok(FinalizationReport {
            run_id: run.run_id.clone(),
            cohort: run.cohort.clone(),
            chrom: run.chrom.clone(),
            operator_identity: operator_identity.to_string(),
            manifest_tasks: tasks.len(),
            manifest_sha256,
            accepted_attempts: frozen.accepted,
            failed_attempts: frozen.failed,
            expected_counts: expected.counts,
            independent_evidence_uri: expected.evidence_uri.clone(),
            independent_counts_sha256,
            acceptance_receipt_sha256,
            acceptance,
            accepted: true,
            frozen: true,
            published: false,
        })
    })();

    if let Err(error) = &result {
        let _ = record_state(
            target,
            &run,
            &expected,
            tasks.len(),
            revision_now()?,
            "finalization_failed",
            &format!("{operator_identity}: {error:#}"),
        );
    }
    result
}

fn ensure_freeze_transition(target: &ClickHouseTarget, run_id: &str) -> anyhow::Result<()> {
    let body = target.query_text(
        "SELECT state FROM lr_y1_load_runs WHERE run_id = {run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT TabSeparated",
        &[("run_id", run_id)],
    )?;
    match body.trim() {
        "accepted_frozen" | "frozen" => {
            bail!("run is already frozen; immutable acceptance cannot be repeated in place")
        }
        _ => Ok(()),
    }
}

fn validate_persisted_receipt(
    target: &ClickHouseTarget,
    run_id: &str,
    state: &str,
    receipt: &str,
) -> anyhow::Result<()> {
    let body = target.query_text(
        "SELECT message FROM lr_y1_load_runs WHERE run_id = {run_id:String} AND state = {state:String} ORDER BY revision DESC LIMIT 1 FORMAT JSONEachRow",
        &[("run_id", run_id), ("state", state)],
    )?;
    #[derive(Deserialize)]
    struct Row {
        message: String,
    }
    let row: Row = serde_json::from_str(body.trim())
        .context("durable freeze/acceptance receipt is missing or malformed")?;
    if row.message != receipt {
        bail!("durable freeze/acceptance receipt differs from the verified receipt");
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_independent_reconciliation(
    expected: &IndependentExpectedCounts,
    run: &PoolY1TaskSpec,
) -> anyhow::Result<()> {
    if expected.contract_version != 1
        || expected.run_id != run.run_id
        || expected.cohort != run.cohort
        || expected.chrom != run.chrom
        || expected.source_generation != run.source_generation
        || expected.source_checksum != run.source_checksum
    {
        bail!("independent reconciliation identity does not match the manifest run/source");
    }
    let facts = &expected.facts;
    if expected.evidence_uri.trim().is_empty()
        || expected.producer.trim().is_empty()
        || expected.counts.source_records == 0
        || facts.source_records != expected.counts.source_records
        || facts.alt_alleles != expected.counts.alleles
        || facts.frequency_rows != expected.counts.frequencies
        || facts.carrier_alt_copies != expected.counts.carriers
        || facts.fully_missing_genotypes > facts.genotype_calls
        || facts.partially_called_genotypes > facts.genotype_calls
        || facts
            .fully_missing_genotypes
            .checked_add(facts.partially_called_genotypes)
            .is_none_or(|value| value > facts.genotype_calls)
        || facts.annotated_alt_alleles > facts.alt_alleles
        || facts.carrier_alt_copies > facts.called_alleles
        || (run.cohort == "hgsvc_hprc" && (facts.genotype_calls == 0 || facts.called_alleles == 0))
        || !valid_sha256(&facts.source_content_sha256)
        || !valid_sha256(&facts.genotype_content_sha256)
        || !valid_sha256(&facts.annotation_content_sha256)
    {
        bail!(
            "independent reconciliation facts are incomplete or inconsistent with expected counts"
        );
    }
    if expected.counts.rejects != 0 || expected.counts.summaries != expected.counts.source_records {
        bail!("independent counts require zero rejects and one summary per source record");
    }
    if run.cohort == "aou"
        && (expected.counts.carriers != 0
            || facts.genotype_calls != 0
            || facts.called_alleles != 0
            || facts.carrier_alt_copies != 0
            || facts.fully_missing_genotypes != 0
            || facts.partially_called_genotypes != 0)
    {
        bail!("AoU independent reconciliation must contain no genotype or carrier observations");
    }
    Ok(())
}

fn validate_expected_counts(
    counts: &[CanonicalTableCount],
    expected: StagedCounts,
) -> anyhow::Result<()> {
    let rows = |table: &str| {
        counts
            .iter()
            .find(|snapshot| snapshot.table == table)
            .map(|snapshot| snapshot.rows)
            .with_context(|| format!("canonical snapshot is missing {table}"))
    };
    let observed = StagedCounts {
        source_records: rows("summaries")?,
        summaries: rows("summaries")?,
        alleles: rows("alleles")?,
        frequencies: rows("frequencies")?,
        carriers: rows("carriers")?,
        rejects: rows("rejects")?,
    };
    if observed != expected {
        bail!("frozen canonical counts {observed:?} do not equal independent expected counts {expected:?}");
    }
    Ok(())
}

fn build_acceptance_receipt(
    run: &PoolY1TaskSpec,
    expected: &IndependentExpectedCounts,
    ledger: &LedgerCoverage,
    manifest_sha256: String,
    independent_counts_sha256: String,
    canonical_digests: Vec<CanonicalContentDigest>,
) -> LoadAcceptanceReceipt {
    LoadAcceptanceReceipt {
        contract_version: 2,
        schema_version: Y1_SCHEMA_VERSION,
        run_id: run.run_id.clone(),
        cohort: run.cohort.clone(),
        chrom: run.chrom.clone(),
        manifest_sha256,
        independent_counts_sha256,
        source_uri: run.source_uri.clone(),
        source_generation: run.source_generation.clone(),
        source_checksum: run.source_checksum.clone(),
        source_index_uri: run.source_index_uri.clone(),
        source_index_generation: run.source_index_generation.clone(),
        source_index_checksum: run.source_index_checksum.clone(),
        accepted_attempts: ledger.accepted.clone(),
        failed_attempts: ledger.failed.clone(),
        expected_counts: expected.counts,
        source_content_sha256: expected.facts.source_content_sha256.clone(),
        genotype_content_sha256: expected.facts.genotype_content_sha256.clone(),
        annotation_content_sha256: expected.facts.annotation_content_sha256.clone(),
        canonical_counts: ledger.counts.clone(),
        canonical_digests,
    }
}

fn validate_manifest(tasks: &[PoolY1TaskSpec]) -> anyhow::Result<PoolY1TaskSpec> {
    let Some(first) = tasks.first() else {
        bail!("task manifest must not be empty")
    };
    let contig_length = grch38_contig_length(&first.chrom)?;
    let mut previous_stop = 0;
    let mut task_ids = BTreeSet::new();
    for (index, task) in tasks.iter().enumerate() {
        task.validate(&format!("custom_{index}"))?;
        if task.run_id != first.run_id
            || task.cohort != first.cohort
            || task.chrom != first.chrom
            || task.source_uri != first.source_uri
            || task.source_generation != first.source_generation
            || task.source_checksum != first.source_checksum
            || task.source_size_bytes != first.source_size_bytes
            || task.source_index_uri != first.source_index_uri
            || task.source_index_generation != first.source_index_generation
            || task.source_index_checksum != first.source_index_checksum
        {
            bail!("task {index} changes a run or immutable source identity");
        }
        if task.start != previous_stop + 1 {
            bail!("task {index} does not begin immediately after the previous task");
        }
        if !task_ids.insert(task.task_id.clone()) {
            bail!("task manifest contains duplicate task_id {}", task.task_id);
        }
        previous_stop = task.stop;
    }
    if previous_stop != contig_length {
        bail!(
            "task manifest must cover {}:1-{contig_length} exactly",
            first.chrom
        );
    }
    Ok(first.clone())
}

fn validate_worker_provenance(report: &serde_json::Value, attempt_id: &str) -> anyhow::Result<()> {
    let field = |name: &str| {
        report
            .get(name)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("attempt {attempt_id} has no {name}"))
    };
    let worker_identity = field("worker_identity")?;
    let build_identity = field("worker_build_version")?;
    let backend_revision = field("backend_revision")?;
    if matches!(worker_identity, "unknown" | "unknown-worker")
        || matches!(
            build_identity,
            "unknown" | "unknown-build" | "unversioned-development-build"
        )
    {
        bail!("attempt {attempt_id} has placeholder worker/build identity");
    }
    if !matches!(backend_revision.len(), 40 | 64)
        || !backend_revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || !build_identity.contains(backend_revision)
    {
        bail!("attempt {attempt_id} worker build is not bound to a full backend revision");
    }
    Ok(())
}

fn validate_ledger_coverage(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
    tasks: &[PoolY1TaskSpec],
    phase: PhysicalPhase,
) -> anyhow::Result<LedgerCoverage> {
    let body = target.query_text(
        r#"
SELECT task_id, attempt_id, argMax(state, revision) AS state,
       argMax(chrom, revision) AS chrom,
       argMax(interval_start, revision) AS interval_start,
       argMax(interval_end, revision) AS interval_end,
       argMax(source_records, revision) AS source_records,
       argMax(summary_rows, revision) AS summary_rows,
       argMax(allele_rows, revision) AS allele_rows,
       argMax(frequency_rows, revision) AS frequency_rows,
       argMax(carrier_rows, revision) AS carrier_rows,
       argMax(rejected_records, revision) AS rejected_records,
       argMax(report_json, revision) AS report_json
FROM lr_y1_task_attempts
WHERE run_id = {run_id:String}
GROUP BY task_id, attempt_id
FORMAT JSONEachRow
"#,
        &[("run_id", &run.run_id)],
    )?;
    let physical = read_physical_attempts(target, run)?;
    let expected: BTreeMap<&str, (u32, u32)> = tasks
        .iter()
        .map(|task| (task.task_id.as_str(), (task.start, task.stop)))
        .collect();
    let mut accepted = BTreeMap::new();
    let mut failed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut terminal_counts = BTreeMap::new();
    let mut terminal_states = BTreeMap::new();

    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let row: AttemptView = serde_json::from_str(line).context("invalid attempt ledger JSON")?;
        if row.attempt_id.trim().is_empty() {
            bail!("attempt ledger contains an empty attempt ID");
        }
        let bounds = expected.get(row.task_id.as_str()).with_context(|| {
            format!(
                "run ledger contains task {} absent from manifest",
                row.task_id
            )
        })?;
        if row.chrom != run.chrom || (row.interval_start, row.interval_end) != *bounds {
            bail!(
                "attempt {} has bounds inconsistent with its manifest task",
                row.attempt_id
            );
        }
        let report: serde_json::Value = serde_json::from_str(&row.report_json)
            .context("invalid durable attempt report JSON")?;
        validate_worker_provenance(&report, &row.attempt_id)?;
        let manifest_task = tasks
            .iter()
            .find(|task| task.task_id == row.task_id)
            .expect("ledger task was checked above");
        for (field, expected_value) in [
            ("run_id", run.run_id.as_str()),
            ("task_id", row.task_id.as_str()),
            ("attempt_id", row.attempt_id.as_str()),
            ("cohort", manifest_task.cohort.as_str()),
            ("chrom", run.chrom.as_str()),
        ] {
            if report.get(field).and_then(|value| value.as_str()) != Some(expected_value) {
                bail!("attempt {} report has inconsistent {field}", row.attempt_id);
            }
        }
        let counts = StagedCounts {
            source_records: row.source_records,
            summaries: row.summary_rows,
            alleles: row.allele_rows,
            frequencies: row.frequency_rows,
            carriers: row.carrier_rows,
            rejects: row.rejected_records,
        };
        let key = (row.task_id.clone(), row.attempt_id.clone());
        if terminal_counts.insert(key.clone(), counts).is_some()
            || terminal_states.insert(key, row.state.clone()).is_some()
        {
            bail!("attempt ledger query returned a duplicate terminal attempt");
        }
        let report_counts: StagedCounts = serde_json::from_value(
            report
                .get("counts")
                .cloned()
                .context("attempt report has no counts")?,
        )?;
        let inserted_rows = report
            .pointer("/inserted/rows")
            .and_then(|value| value.as_u64());
        let inserted_bytes = report
            .pointer("/inserted/bytes")
            .and_then(|value| value.as_u64());
        if report.get("source_uri").and_then(|value| value.as_str())
            != Some(manifest_task.source_uri.as_str())
            || report
                .get("source_generation")
                .and_then(|value| value.as_str())
                != Some(manifest_task.source_generation.as_str())
            || report
                .get("source_size_bytes")
                .and_then(|value| value.as_u64())
                != Some(manifest_task.source_size_bytes)
            || report.get("start").and_then(|value| value.as_u64())
                != Some(u64::from(manifest_task.start))
            || report.get("stop").and_then(|value| value.as_u64())
                != Some(u64::from(manifest_task.stop))
            || report.get("published").and_then(|value| value.as_bool()) != Some(false)
            || report_counts != counts
            || inserted_rows.is_none()
            || inserted_bytes.is_none()
            || report
                .get("started_at_ms")
                .and_then(|value| value.as_u64())
                .is_none()
            || report
                .get("finished_at_ms")
                .and_then(|value| value.as_u64())
                .is_none()
            || report
                .get("elapsed_ms")
                .and_then(|value| value.as_u64())
                .is_none()
            || report.get("linux_peak_rss_bytes").is_none()
        {
            bail!(
                "attempt {} report is incomplete or inconsistent with its declared source identity",
                row.attempt_id
            );
        }
        match row.state.as_str() {
            "accepted" => {
                let expected_inserted = [
                    counts.summaries,
                    counts.alleles,
                    counts.frequencies,
                    counts.carriers,
                    counts.rejects,
                ]
                .into_iter()
                .try_fold(0u64, |total, value| total.checked_add(value))
                .context("attempt row total exceeds UInt64")?;
                if report.get("state").and_then(|value| value.as_str()) != Some("accepted")
                    || !report
                        .get("failure")
                        .is_some_and(serde_json::Value::is_null)
                    || inserted_rows != Some(expected_inserted)
                    || (expected_inserted > 0 && inserted_bytes == Some(0))
                    || accepted
                        .insert(row.task_id.clone(), row.attempt_id.clone())
                        .is_some()
                {
                    bail!(
                        "task {} lacks exactly one complete accepted attempt",
                        row.task_id
                    );
                }
            }
            "failed" => {
                if report.get("state").and_then(|value| value.as_str()) != Some("failed")
                    || !report
                        .get("failure")
                        .is_some_and(serde_json::Value::is_object)
                {
                    bail!(
                        "failed attempt {} has no structured failure result",
                        row.attempt_id
                    );
                }
                failed.entry(row.task_id).or_default().push(row.attempt_id);
            }
            state => bail!("attempt ledger has unsupported nonterminal state {state:?}"),
        }
    }
    for attempts in failed.values_mut() {
        attempts.sort();
    }
    if accepted.len() != tasks.len()
        || tasks
            .iter()
            .any(|task| !accepted.contains_key(&task.task_id))
    {
        bail!("run does not have exactly one accepted terminal attempt for every manifest task");
    }
    for task in tasks
        .iter()
        .filter(|task| task.controlled_fail_once.is_some())
    {
        let failures = failed
            .get(&task.task_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let initial_matches = failures.len() == 1
            && (failures[0] == task.attempt_id
                || crate::pool::durable_attempt_matches_prefix(&failures[0], &task.attempt_id));
        let retry_matches = task.retry_attempt_id.as_ref().is_some_and(|prefix| {
            accepted.get(&task.task_id).is_some_and(|attempt| {
                attempt == prefix || crate::pool::durable_attempt_matches_prefix(attempt, prefix)
            })
        });
        if !initial_matches || !retry_matches {
            bail!(
                "controlled fail-once task {} lacks its failed initial and accepted retry evidence",
                task.task_id
            );
        }
    }

    let counts = validate_physical_attempts(
        tasks,
        &accepted,
        &terminal_counts,
        &terminal_states,
        &physical,
        phase,
    )?;
    let mut nonaccepted = terminal_states
        .iter()
        .filter(|(_, state)| state.as_str() != "accepted")
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    nonaccepted.sort();
    Ok(LedgerCoverage {
        accepted,
        failed,
        nonaccepted,
        counts,
    })
}

fn read_physical_attempts(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
) -> anyhow::Result<Vec<PhysicalAttemptView>> {
    let mut result = Vec::new();
    for spec in TABLES {
        let identity = if spec.has_primary_identity {
            "countIf(release != 'y1' OR cohort != {cohort:String} OR reference_genome != 'GRCh38' OR chrom != {chrom:String})"
        } else {
            "toUInt64(0)"
        };
        let positions = if spec.has_primary_identity {
            "min(position), max(position)"
        } else {
            "toUInt32(0), toUInt32(0)"
        };
        let query = format!(
            "SELECT '{}' AS table, task_id, attempt_id, count() AS rows, uniqExact({}) AS unique_keys, {} AS identity_violations, {} AS min_position, {} AS max_position FROM {} WHERE run_id = {{run_id:String}} GROUP BY task_id, attempt_id FORMAT JSONEachRow",
            spec.label,
            spec.unique_key,
            identity,
            positions.split(',').next().expect("position expression"),
            positions.split(',').nth(1).expect("position expression"),
            spec.table,
        );
        let body = target.query_text(
            &query,
            &[
                ("run_id", &run.run_id),
                ("cohort", &run.cohort),
                ("chrom", &run.chrom),
            ],
        )?;
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            result.push(serde_json::from_str(line).context("invalid canonical aggregate JSON")?);
        }
    }
    Ok(result)
}

fn validate_physical_attempts(
    tasks: &[PoolY1TaskSpec],
    accepted: &BTreeMap<String, String>,
    terminal_counts: &BTreeMap<(String, String), StagedCounts>,
    terminal_states: &BTreeMap<(String, String), String>,
    physical: &[PhysicalAttemptView],
    phase: PhysicalPhase,
) -> anyhow::Result<Vec<CanonicalTableCount>> {
    let task_bounds: BTreeMap<&str, (u32, u32)> = tasks
        .iter()
        .map(|task| (task.task_id.as_str(), (task.start, task.stop)))
        .collect();
    let accepted_terminal_count = terminal_states
        .values()
        .filter(|state| state.as_str() == "accepted")
        .count();
    if accepted.len() != tasks.len()
        || accepted_terminal_count != tasks.len()
        || tasks.iter().any(|task| {
            accepted
                .get(&task.task_id)
                .and_then(|attempt| terminal_states.get(&(task.task_id.clone(), attempt.clone())))
                .map(String::as_str)
                != Some("accepted")
        })
    {
        bail!("physical acceptance lacks exactly one accepted terminal attempt per task");
    }
    let mut aggregates = BTreeMap::new();
    for row in physical {
        if !TABLES.iter().any(|spec| spec.label == row.table) {
            bail!("canonical snapshot contains an unknown table");
        }
        let attempt_key = (row.task_id.clone(), row.attempt_id.clone());
        if !terminal_counts.contains_key(&attempt_key) {
            bail!(
                "stale or orphan canonical contribution for task {}/attempt {}",
                row.task_id,
                row.attempt_id
            );
        }
        let bounds = task_bounds
            .get(row.task_id.as_str())
            .context("canonical row names a task absent from the manifest")?;
        if row.identity_violations != 0 {
            bail!(
                "canonical {} contains cross-run/cohort/contig identity violations",
                row.table
            );
        }
        if row.table != "rejects"
            && row.rows != 0
            && (row.min_position < bounds.0 || row.max_position > bounds.1)
        {
            bail!(
                "canonical {} escapes task {} bounds",
                row.table,
                row.task_id
            );
        }
        if row.unique_keys != row.rows {
            bail!(
                "canonical {} contains duplicate keys for task {}/attempt {}",
                row.table,
                row.task_id,
                row.attempt_id
            );
        }
        let key = (
            row.table.clone(),
            row.task_id.clone(),
            row.attempt_id.clone(),
        );
        if aggregates.insert(key, row.rows).is_some() {
            bail!("canonical snapshot contains a duplicate attempt aggregate");
        }
    }

    let mut accepted_rows = BTreeMap::new();
    for (attempt_key, counts) in terminal_counts {
        if counts.summaries.checked_add(counts.rejects) != Some(counts.source_records) {
            bail!(
                "terminal attempt counts do not reconcile source records to summaries plus rejects"
            );
        }
        let expected_rows = [
            counts.summaries,
            counts.alleles,
            counts.frequencies,
            counts.carriers,
            counts.rejects,
        ];
        let is_accepted = accepted.get(&attempt_key.0) == Some(&attempt_key.1)
            && terminal_states.get(attempt_key).map(String::as_str) == Some("accepted");
        for (spec, expected) in TABLES.into_iter().zip(expected_rows) {
            let rows = aggregates
                .get(&(
                    spec.label.to_string(),
                    attempt_key.0.clone(),
                    attempt_key.1.clone(),
                ))
                .copied()
                .unwrap_or(0);
            if is_accepted {
                if rows != expected {
                    bail!(
                        "canonical {} rows disagree with accepted ledger for task {}/attempt {}",
                        spec.label,
                        attempt_key.0,
                        attempt_key.1
                    );
                }
                let total = accepted_rows.entry(spec.label).or_insert(0u64);
                *total = total
                    .checked_add(rows)
                    .context("canonical row total exceeds UInt64")?;
            } else {
                let valid = match phase {
                    PhysicalPhase::BeforeCleanup => rows == 0 || rows == expected,
                    PhysicalPhase::Frozen => rows == 0,
                };
                if !valid {
                    bail!(
                        "nonaccepted canonical {} rows are partial or survived the freeze cleanup",
                        spec.label
                    );
                }
            }
        }
        if is_accepted && counts.rejects != 0 {
            bail!("accepted terminal attempt contains rejected records");
        }
    }
    Ok(TABLES
        .into_iter()
        .map(|spec| CanonicalTableCount {
            table: spec.label.to_string(),
            rows: accepted_rows.get(spec.label).copied().unwrap_or(0),
        })
        .collect())
}

fn read_canonical_digests(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
    accepted: &BTreeMap<String, String>,
    counts: &[CanonicalTableCount],
) -> anyhow::Result<Vec<CanonicalContentDigest>> {
    let total = |label: &str| {
        counts
            .iter()
            .find(|count| count.table == label)
            .map(|count| count.rows)
            .unwrap_or(0)
    };
    let mut result = Vec::with_capacity(accepted.len() * TABLES.len());
    let mut digest_totals: BTreeMap<&str, u64> = BTreeMap::new();
    for (task_id, attempt_id) in accepted {
        for spec in TABLES {
            let count_query = format!(
                "SELECT count() FROM {} WHERE run_id = {{run_id:String}} AND task_id = {{task_id:String}} AND attempt_id = {{attempt_id:String}} FORMAT TabSeparated",
                spec.table
            );
            let parameters = [
                ("run_id", run.run_id.as_str()),
                ("task_id", task_id.as_str()),
                ("attempt_id", attempt_id.as_str()),
            ];
            let rows = target
                .query_text(&count_query, &parameters)?
                .trim()
                .parse::<u64>()
                .context("invalid canonical digest row count")?;
            let query = format!(
                "SELECT {} FROM {} WHERE run_id = {{run_id:String}} AND task_id = {{task_id:String}} AND attempt_id = {{attempt_id:String}} ORDER BY {} FORMAT RowBinary",
                spec.columns, spec.table, spec.order_by
            );
            let domain = format!("{}\0{}\0{}\0{}", spec.label, task_id, attempt_id, rows);
            let sha256 = target.query_sha256(&query, &parameters, domain.as_bytes())?;
            *digest_totals.entry(spec.label).or_default() += rows;
            result.push(CanonicalContentDigest {
                table: spec.label.to_string(),
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
                rows,
                sha256,
            });
        }
    }
    for spec in TABLES {
        if digest_totals.get(spec.label).copied().unwrap_or(0) != total(spec.label) {
            bail!(
                "per-attempt {} digest counts do not equal the frozen canonical count",
                spec.label
            );
        }
    }
    Ok(result)
}

fn record_state(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
    expected: &IndependentExpectedCounts,
    task_count: usize,
    revision: u64,
    state: &str,
    message: &str,
) -> anyhow::Result<()> {
    record_load_run(
        target,
        &LoadRunLedgerRow {
            run_id: run.run_id.clone(),
            revision,
            state: state.to_string(),
            load_scope: LoadScope::FullChromosome.as_str().to_string(),
            release: Release::Y1.as_str().to_string(),
            cohort: run.cohort.clone(),
            reference_genome: ReferenceGenome::Grch38.as_str().to_string(),
            chrom: run.chrom.clone(),
            interval_start: 1,
            interval_end: grch38_contig_length(&run.chrom)?,
            source_uri: run.source_uri.clone(),
            source_generation: run.source_generation.clone(),
            source_checksum_algorithm: run.source_checksum_algorithm.clone(),
            source_checksum: run.source_checksum.clone(),
            source_index_uri: run.source_index_uri.clone(),
            source_index_generation: run.source_index_generation.clone(),
            source_index_checksum: run.source_index_checksum.clone(),
            schema_version: Y1_SCHEMA_VERSION,
            loader_version: env!("CARGO_PKG_VERSION").to_string(),
            expected_tasks: u32::try_from(task_count)?,
            expected_source_records: expected.counts.source_records,
            summary_rows: expected.counts.summaries,
            allele_rows: expected.counts.alleles,
            frequency_rows: expected.counts.frequencies,
            carrier_rows: expected.counts.carriers,
            rejected_records: expected.counts.rejects,
            created_at_ms: revision / 1_000_000,
            updated_at_ms: revision / 1_000_000,
            message: message.to_string(),
        },
    )
}

fn revision_now() -> anyhow::Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_for(chrom: &str, index: usize, start: u32, stop: u32) -> PoolY1TaskSpec {
        PoolY1TaskSpec {
            coordinator_task_id: format!("custom_{index}"),
            label: "test".into(),
            run_id: "run".into(),
            task_id: format!("task-{index}"),
            attempt_id: format!("attempt-{index}"),
            release: "y1".into(),
            cohort: "aou".into(),
            reference_genome: "GRCh38".into(),
            chrom: chrom.into(),
            start,
            stop,
            source_uri: format!(
                "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.{chrom}.vcf.gz"
            ),
            source_generation: "1".into(),
            source_checksum_algorithm: "md5_base64".into(),
            source_checksum: "x".into(),
            source_size_bytes: 1,
            source_index_uri: format!(
                "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.{chrom}.vcf.gz.tbi"
            ),
            source_index_generation: "2".into(),
            source_index_checksum_algorithm: "md5_base64".into(),
            source_index_checksum: "y".into(),
            retry_attempt_id: None,
            controlled_fail_once: None,
        }
    }

    fn independent_for(task: &PoolY1TaskSpec) -> IndependentExpectedCounts {
        IndependentExpectedCounts {
            contract_version: 1,
            run_id: task.run_id.clone(),
            cohort: task.cohort.clone(),
            chrom: task.chrom.clone(),
            evidence_uri: "file://independent.json".into(),
            producer: "independent-test".into(),
            source_generation: task.source_generation.clone(),
            source_checksum: task.source_checksum.clone(),
            counts: StagedCounts {
                source_records: 1,
                summaries: 1,
                alleles: 2,
                frequencies: 3,
                carriers: 0,
                rejects: 0,
            },
            facts: IndependentReconciliationFacts {
                source_records: 1,
                alt_alleles: 2,
                frequency_rows: 3,
                genotype_calls: 0,
                called_alleles: 0,
                carrier_alt_copies: 0,
                fully_missing_genotypes: 0,
                partially_called_genotypes: 0,
                annotated_alt_alleles: 2,
                source_content_sha256: "a".repeat(64),
                genotype_content_sha256: "b".repeat(64),
                annotation_content_sha256: "c".repeat(64),
            },
        }
    }

    type TestSnapshot = (
        Vec<PoolY1TaskSpec>,
        BTreeMap<String, String>,
        BTreeMap<(String, String), StagedCounts>,
        BTreeMap<(String, String), String>,
        Vec<PhysicalAttemptView>,
    );

    fn snapshot() -> TestSnapshot {
        let task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        let counts = StagedCounts {
            source_records: 1,
            summaries: 1,
            alleles: 2,
            frequencies: 3,
            carriers: 0,
            rejects: 0,
        };
        let accepted = BTreeMap::from([(task.task_id.clone(), "accepted".into())]);
        let terminal_counts = BTreeMap::from([
            ((task.task_id.clone(), "accepted".into()), counts),
            ((task.task_id.clone(), "failed".into()), counts),
        ]);
        let terminal_states = BTreeMap::from([
            ((task.task_id.clone(), "accepted".into()), "accepted".into()),
            ((task.task_id.clone(), "failed".into()), "failed".into()),
        ]);
        let mut physical = Vec::new();
        for attempt in ["accepted", "failed"] {
            for (table, rows) in [("summaries", 1), ("alleles", 2), ("frequencies", 3)] {
                physical.push(PhysicalAttemptView {
                    table: table.into(),
                    task_id: task.task_id.clone(),
                    attempt_id: attempt.into(),
                    rows,
                    unique_keys: rows,
                    identity_violations: 0,
                    min_position: 1,
                    max_position: 1,
                });
            }
        }
        (
            vec![task],
            accepted,
            terminal_counts,
            terminal_states,
            physical,
        )
    }

    #[test]
    fn manifest_requires_exact_adjacency_full_coverage_and_one_identity() {
        for chrom in (1..=22)
            .map(|n| format!("chr{n}"))
            .chain(["chrX".into(), "chrY".into()])
        {
            let length = grch38_contig_length(&chrom).unwrap();
            assert!(validate_manifest(&[task_for(&chrom, 0, 1, length)]).is_ok());
            assert!(validate_manifest(&[task_for(&chrom, 0, 1, length - 1)]).is_err());
            assert!(validate_manifest(&[
                task_for(&chrom, 0, 1, 10),
                task_for(&chrom, 1, 12, length)
            ])
            .is_err());
        }
        assert!(validate_manifest(&[task_for("chrM", 0, 1, 16_569)]).is_err());
    }

    #[test]
    fn independent_reconciliation_rejects_malformed_hash_source_drop_and_cross_identity() {
        let task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        assert!(validate_independent_reconciliation(&independent_for(&task), &task).is_ok());
        let mut malformed = independent_for(&task);
        malformed.facts.source_content_sha256 = "bad".into();
        assert!(validate_independent_reconciliation(&malformed, &task).is_err());
        let mut dropped = independent_for(&task);
        dropped.facts.source_records = 2;
        assert!(validate_independent_reconciliation(&dropped, &task).is_err());
        let mut cross = independent_for(&task);
        cross.source_generation = "other".into();
        assert!(validate_independent_reconciliation(&cross, &task).is_err());
    }

    #[test]
    fn retry_rows_are_verified_then_must_be_removed_before_freeze() {
        let (tasks, accepted, counts, states, physical) = snapshot();
        let before = validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &physical,
            PhysicalPhase::BeforeCleanup,
        )
        .unwrap();
        assert_eq!(
            before
                .iter()
                .find(|row| row.table == "summaries")
                .unwrap()
                .rows,
            1
        );
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &physical,
            PhysicalPhase::Frozen
        )
        .is_err());
        let accepted_only = physical
            .into_iter()
            .filter(|row| row.attempt_id == "accepted")
            .collect::<Vec<_>>();
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &accepted_only,
            PhysicalPhase::Frozen
        )
        .is_ok());
    }

    #[test]
    fn physical_acceptance_rejects_missing_duplicate_partial_stale_and_cross_identity_rows() {
        let (tasks, accepted, counts, states, physical) = snapshot();
        assert!(validate_physical_attempts(
            &tasks,
            &BTreeMap::new(),
            &counts,
            &states,
            &physical,
            PhysicalPhase::BeforeCleanup
        )
        .is_err());
        let mut stale = physical.clone();
        let mut orphan = stale[0].clone();
        orphan.attempt_id = "orphan".into();
        stale.push(orphan);
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &stale,
            PhysicalPhase::BeforeCleanup
        )
        .is_err());
        let mut cross = physical.clone();
        cross[0].identity_violations = 1;
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &cross,
            PhysicalPhase::BeforeCleanup
        )
        .is_err());
        let mut partial = physical;
        partial
            .iter_mut()
            .find(|row| row.attempt_id == "failed" && row.table == "alleles")
            .unwrap()
            .rows = 1;
        partial
            .iter_mut()
            .find(|row| row.attempt_id == "failed" && row.table == "alleles")
            .unwrap()
            .unique_keys = 1;
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &partial,
            PhysicalPhase::BeforeCleanup
        )
        .is_err());
    }

    #[test]
    fn freeze_state_rejects_late_primary_writes() {
        for state in [
            "freezing",
            "frozen",
            "accepted_frozen",
            "finalization_failed",
        ] {
            assert!(super::super::storage::validate_primary_write_state(Some(state)).is_err());
        }
        assert!(super::super::storage::validate_primary_write_state(None).is_ok());
        assert!(super::super::storage::validate_primary_write_state(Some("validated")).is_ok());
    }

    #[test]
    fn future_finalization_requires_revision_bound_worker_provenance() {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let valid = serde_json::json!({
            "worker_identity": "worker-7",
            "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
            "backend_revision": revision,
        });
        assert!(validate_worker_provenance(&valid, "attempt-7").is_ok());
        let invalid = serde_json::json!({
            "worker_identity": "unknown-worker",
            "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
            "backend_revision": revision,
        });
        assert!(validate_worker_provenance(&invalid, "attempt-7").is_err());
    }

    fn record_terminal_fixture_attempt(
        target: &ClickHouseTarget,
        task: &PoolY1TaskSpec,
        context: &super::super::AttemptContext,
        counts: StagedCounts,
        transformation: &super::super::TransformationReport,
        accepted: bool,
    ) {
        let revision = revision_now().unwrap();
        let mut ledger = super::super::TaskAttemptLedgerRow::new(
            context,
            revision,
            if accepted {
                super::super::AttemptState::Accepted
            } else {
                super::super::AttemptState::Failed
            },
            counts,
            transformation,
            if accepted { "" } else { "fixture retry" },
        )
        .unwrap();
        let inserted_rows = counts.summaries
            + counts.alleles
            + counts.frequencies
            + counts.carriers
            + counts.rejects;
        ledger.report_json = serde_json::json!({
            "run_id": task.run_id,
            "task_id": task.task_id,
            "attempt_id": context.attempt_id,
            "cohort": task.cohort,
            "chrom": task.chrom,
            "start": task.start,
            "stop": task.stop,
            "source_uri": task.source_uri,
            "source_generation": task.source_generation,
            "source_size_bytes": task.source_size_bytes,
            "counts": counts,
            "inserted": { "rows": inserted_rows, "bytes": 1, "requests": 1 },
            "started_at_ms": 1,
            "finished_at_ms": 2,
            "elapsed_ms": 1,
            "parse_transform_insert_ms": 1,
            "linux_peak_rss_bytes": null,
            "worker_identity": "integration-worker",
            "worker_build_version": "gnomad-lr/0123456789abcdef0123456789abcdef01234567/x86_64-linux-release",
            "backend_revision": "0123456789abcdef0123456789abcdef01234567",
            "state": if accepted { "accepted" } else { "failed" },
            "failure": if accepted { serde_json::Value::Null } else { serde_json::json!({"code":"fixture_retry"}) },
            "published": false
        })
        .to_string();
        super::super::record_task_attempt(target, &ledger).unwrap();
    }

    #[test]
    fn local_clickhouse_freezes_canonical_rows_in_place_and_cleans_retry() {
        let Ok(endpoint) = std::env::var("GNOMAD_LR_Y1_TEST_ENDPOINT") else {
            return;
        };
        let database = std::env::var("GNOMAD_LR_Y1_TEST_DATABASE")
            .unwrap_or_else(|_| "gnomad_lr_y1_scratch_v5_ci".to_string());
        let target = ClickHouseTarget::new(
            &endpoint,
            &database,
            TargetKind::Scratch,
            super::super::AuthSource::None,
            false,
            false,
        )
        .unwrap();
        super::super::init_schema(&target).unwrap();

        let fixture = include_str!("../../tests/fixtures/y1/aou_summary_only_ins.vcf");
        let header = super::super::Y1Header::parse(fixture, super::super::Cohort::Aou).unwrap();
        let batch = super::super::transform_records(
            &header,
            fixture
                .lines()
                .filter(|line| !line.is_empty() && !line.starts_with('#')),
        );
        let mut task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        task.run_id = format!("freeze-integration-{}", revision_now().unwrap());
        let base = super::super::AttemptContext {
            run_id: task.run_id.clone(),
            task_id: task.task_id.clone(),
            attempt_id: "failed".into(),
            cohort: super::super::Cohort::Aou,
            chrom: task.chrom.clone(),
            interval_start: task.start,
            interval_end: task.stop,
        };
        let failed_counts = super::super::stage_attempt(&target, &base, &batch).unwrap();
        record_terminal_fixture_attempt(&target, &task, &base, failed_counts, &batch.report, false);
        let accepted_context = super::super::AttemptContext {
            attempt_id: "accepted".into(),
            ..base.clone()
        };
        let accepted_counts =
            super::super::stage_attempt(&target, &accepted_context, &batch).unwrap();
        record_terminal_fixture_attempt(
            &target,
            &task,
            &accepted_context,
            accepted_counts,
            &batch.report,
            true,
        );

        let mut expected = independent_for(&task);
        expected.counts = accepted_counts;
        expected.facts.source_records = accepted_counts.source_records;
        expected.facts.alt_alleles = accepted_counts.alleles;
        expected.facts.frequency_rows = accepted_counts.frequencies;
        expected.facts.annotated_alt_alleles = accepted_counts.alleles;
        let nonce = format!("{}-{}", std::process::id(), revision_now().unwrap());
        let manifest_path = std::env::temp_dir().join(format!("gnomad-lr-manifest-{nonce}.json"));
        let expected_path = std::env::temp_dir().join(format!("gnomad-lr-expected-{nonce}.json"));
        std::fs::write(
            &manifest_path,
            serde_json::to_vec(&vec![task.clone()]).unwrap(),
        )
        .unwrap();
        std::fs::write(&expected_path, serde_json::to_vec(&expected).unwrap()).unwrap();
        let report =
            finalize_contig_run(&target, &manifest_path, &expected_path, "integration-test")
                .unwrap();
        let _ = std::fs::remove_file(manifest_path);
        let _ = std::fs::remove_file(expected_path);
        assert!(report.accepted && report.frozen && !report.published);
        assert!(report
            .acceptance
            .canonical_digests
            .iter()
            .all(|digest| valid_sha256(&digest.sha256)));

        let rows = target.query_text(
            "SELECT countIf(attempt_id = 'failed'), countIf(attempt_id = 'accepted') FROM lr_y1_summaries WHERE run_id = {run_id:String} FORMAT TabSeparated",
            &[("run_id", &task.run_id)],
        ).unwrap();
        assert_eq!(rows.trim(), "0\t1");
        let late_context = super::super::AttemptContext {
            attempt_id: "late".into(),
            ..accepted_context
        };
        assert!(super::super::stage_attempt(&target, &late_context, &batch).is_err());
    }
}
