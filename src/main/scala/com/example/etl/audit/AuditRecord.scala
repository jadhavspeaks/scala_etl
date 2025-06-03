package com.example.etl.audit

import java.sql.Timestamp

case class AuditRecord(
    AUDIT_LOG_ID: String,
    JOB_ID: String,
    RUN_ID: String,
    JOB_RUN_START_TS: Option[Timestamp] = None,
    JOB_RUN_END_TS: Option[Timestamp] = None,
    SOURCE_FILE_PATH: Option[String] = None,
    TARGET_ICEBERG_NAMESPACE: Option[String] = None,
    TARGET_ICEBERG_TABLE_NAME: Option[String] = None,
    STATUS: String,
    INPUT_ROW_COUNT: Option[Long] = None,
    OUTPUT_ROW_COUNT: Option[Long] = None,
    DQ_FAILURE_COUNT: Option[Long] = None,
    ERROR_MESSAGE: Option[String] = None
)
