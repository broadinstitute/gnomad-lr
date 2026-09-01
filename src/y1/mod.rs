mod contig;
mod direct_methylation;
pub mod finalizer;
mod interval;
pub mod metadata;
pub mod methylation;
mod model;
mod parser;
mod phased_pool;
pub mod primary_motif;
pub mod primary_motif_product;
mod storage;
mod target;

pub use direct_methylation::{
    load_direct_methylation_task, DirectMethylationTaskReceipt, DirectMethylationTaskSpec,
    DIRECT_METHYLATION_TABLE,
};
pub use interval::{
    run_pool_interval_attempt, PoolY1AttemptReport, PoolY1JobSpec, PoolY1TargetSpec, PoolY1TaskSpec,
};
pub use methylation::{
    open_prepared_methylation_records, parse_methylation_record, plan_methylation_finalization,
    plan_methylation_finalization_from_snapshot, prepare_methylation_attempt,
    run_phased_methylation_evaluation, run_phased_methylation_smoke,
    AuthoritativeMethylationLedgerSnapshot, ImmutableObjectIdentity, MethylationDataLayer,
    MethylationFinalizationPlan, MethylationLeaseOwnership, MethylationLedgerState,
    MethylationRecord, MethylationResolvedAttempt, MethylationSourceType,
    MethylationTaskOwnerIdentity, PreparedMethylationAttempt, SourceHaplotype,
    Y1MethylationFinalizationSpec, Y1MethylationTaskSpec, PHASED_METHYLATION_EVALUATION_DATABASE,
};
pub use model::*;
pub use parser::{
    transform_record, transform_record_with_mode, transform_records, transform_records_with_mode,
    FieldDefinition, Y1Header,
};
pub use phased_pool::{
    run_phased_mirror_task, validate_task_against_ledger, PhasedMirrorJobSpec,
    PhasedMirrorTaskReceipt, PhasedMirrorTaskSpec, MIRROR_CONTRACT_ID,
    MIRROR_LEDGER_CONTENT_SHA256, MIRROR_LEDGER_RAW_SHA256, MIRROR_RUN_ID, MIRROR_WORKER_PRINCIPAL,
};
pub use storage::{
    attest_exact_y1_schema, attest_fresh_y1_schema, init_schema, record_load_run, stage_attempt,
    stage_attempt_tracked, AttemptContext, InsertStats, LoadRunLedgerRow, LoadScope, StagedCounts,
    Y1_SCHEMA_VERSION,
};
pub use target::{
    AuthSource, ClickHouseTarget, TargetKind, WorkerWriteFence, Y1_WORKER_PASSWORD_ENV,
    Y1_WORKER_USERNAME_ENV,
};

#[cfg(test)]
mod tests;
