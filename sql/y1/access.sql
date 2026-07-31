-- Candidate-scoped passwordless writer for private-network Y1 pool workers.
-- This file is intentionally pinned to the current chr22 rehearsal candidate.
-- The ClickHouse endpoint must remain private; never grant this user on serving data.

CREATE USER IF NOT EXISTS gnomad_lr_y1_pool_writer
    IDENTIFIED WITH no_password
    SETTINGS async_insert = 0;

GRANT SELECT, INSERT ON gnomad_lr_y1_scratch_v5_chr22_pool_r3.*
    TO gnomad_lr_y1_pool_writer;
