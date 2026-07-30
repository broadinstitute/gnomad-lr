mod contig;
pub mod finalizer;
mod interval;
pub mod metadata;
pub mod methylation;
mod model;
mod parser;
mod storage;
mod target;

pub use interval::{
    run_pool_interval_attempt, PoolY1AttemptReport, PoolY1JobSpec, PoolY1TargetSpec, PoolY1TaskSpec,
};
pub use methylation::{
    open_prepared_methylation_records, parse_methylation_record, plan_methylation_finalization,
    plan_methylation_finalization_from_snapshot, prepare_methylation_attempt,
    AuthoritativeMethylationLedgerSnapshot, ImmutableObjectIdentity, MethylationDataLayer,
    MethylationFinalizationPlan, MethylationLeaseOwnership, MethylationLedgerState,
    MethylationRecord, MethylationResolvedAttempt, MethylationSourceType,
    MethylationTaskOwnerIdentity, PreparedMethylationAttempt, SourceHaplotype,
    Y1MethylationFinalizationSpec, Y1MethylationTaskSpec,
};
pub use model::*;
pub use parser::{transform_record, transform_records, FieldDefinition, Y1Header};
pub use storage::{
    init_schema, record_load_run, stage_attempt, stage_attempt_tracked, AttemptContext,
    InsertStats, LoadRunLedgerRow, LoadScope, StagedCounts, Y1_SCHEMA_VERSION,
};
pub use target::{
    AuthSource, ClickHouseTarget, TargetKind, WorkerWriteFence, Y1_WORKER_PASSWORD_ENV,
    Y1_WORKER_USERNAME_ENV,
};

#[cfg(test)]
mod tests;
