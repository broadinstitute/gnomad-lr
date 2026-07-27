pub mod finalizer;
mod interval;
pub mod metadata;
mod model;
mod parser;
mod storage;
mod target;

pub use interval::{
    run_pool_interval_attempt, PoolY1AttemptReport, PoolY1JobSpec, PoolY1TargetSpec, PoolY1TaskSpec,
};
pub use model::*;
pub use parser::{transform_record, transform_records, FieldDefinition, Y1Header};
pub use storage::{
    activate_published_run, init_schema, publish_staged_run, record_load_run, record_task_attempt,
    stage_attempt, stage_attempt_tracked, AttemptContext, AttemptState, InsertStats,
    LoadRunLedgerRow, LoadScope, PublicationRequest, StagedCounts, TaskAttemptLedgerRow,
    Y1_SCHEMA_VERSION,
};
pub use target::{AuthSource, ClickHouseTarget, TargetKind};

#[cfg(test)]
mod tests;
