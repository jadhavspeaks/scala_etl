-- DDL Design for Iceberg Audit Log Table
-- To be created in a specific namespace/database managed by your Iceberg catalog.
-- Example catalog.db.table: etl_catalog.audit_db.etl_job_audit_logs

/*
CREATE TABLE ${catalogName}.${auditDbName}.${auditTableName} (
    AUDIT_LOG_ID STRING, -- Primary Key / Unique ID for this log entry, can be same as RUN_ID
    JOB_ID STRING,
    RUN_ID STRING NOT NULL, -- Unique ID for each specific job execution
    JOB_RUN_START_TS TIMESTAMP,
    JOB_RUN_END_TS TIMESTAMP,
    SOURCE_FILE_PATH STRING,
    TARGET_ICEBERG_NAMESPACE STRING,
    TARGET_ICEBERG_TABLE_NAME STRING,
    STATUS STRING, -- e.g., RUNNING, SUCCESS, FAILED
    INPUT_ROW_COUNT BIGINT,
    OUTPUT_ROW_COUNT BIGINT,
    ERROR_MESSAGE STRING -- For storing error messages
)
USING iceberg
-- Example partitioning strategy, adjust as needed for query patterns
PARTITIONED BY (days(JOB_RUN_START_TS), JOB_ID);
*/

-- Note: The actual table creation is handled by the user/ops or via a separate script.
-- The Scala code (AuditRecord and AuditLogger) will conform to this structure.
