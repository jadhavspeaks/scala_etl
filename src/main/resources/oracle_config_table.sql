CREATE TABLE ETL_JOB_CONFIG (
    JOB_ID VARCHAR2(100) PRIMARY KEY,
    JOB_DESCRIPTION VARCHAR2(255),
    SOURCE_FILE_PATH VARCHAR2(1000) NOT NULL,
    SOURCE_FILE_TYPE VARCHAR2(10) NOT NULL, -- CSV, TXT, XLSX, XLS, DAT, DATA
    SOURCE_FILE_DELIMITER VARCHAR2(5),      -- For CSV/TXT
    SOURCE_HAS_HEADER CHAR(1) DEFAULT 'Y' NOT NULL, -- Y/N
    TARGET_ICEBERG_NAMESPACE VARCHAR2(100) NOT NULL,
    TARGET_ICEBERG_TABLE_NAME VARCHAR2(100) NOT NULL,
    TARGET_ICEBERG_PARTITION_COLUMNS VARCHAR2(255), -- Comma-separated
    BUSINESS_KEY_COLUMNS VARCHAR2(255),          -- Comma-separated, for SCD2
    SCD2_START_DATE_COLUMN_NAME VARCHAR2(100) DEFAULT 'eff_start_ts',
    SCD2_END_DATE_COLUMN_NAME VARCHAR2(100) DEFAULT 'eff_end_ts',
    SCD2_CURRENT_FLAG_COLUMN_NAME VARCHAR2(100) DEFAULT 'current_flag',
    AS_IS_LOAD CHAR(1) DEFAULT 'N' NOT NULL,         -- Y/N
    SCHEMA_MAPPING_JSON CLOB,                        -- JSON string for explicit schema. Example:
    --                                                 -- [{ "sourceName": "src_col1", "targetName": "tgt_col1", "targetType": "StringType", "nullable": true, "maxLength": 100, "format": "yyyy-MM-dd" }, ...]
    --                                                 -- targetType can be Spark SQL types like: StringType, IntegerType, TimestampType, DateType, DecimalType(p,s), etc.
    --                                                 -- 'format' is optional, for date/timestamp parsing. 'maxLength' for StringType.
    BUSINESS_TRANSFORMATION_RULES_JSON CLOB,         -- JSON or structured string
    DATA_QUALITY_RULES_JSON CLOB,                    -- JSON string for DQ rules. Example:
    --                                                 -- [{ "columnName": "email", "dqType": "NOT_NULL" },
    --                                                 --  { "columnName": "email", "dqType": "REGEX", "pattern": "\\S+@\\S+\\.\\S+" },
    --                                                 --  { "columnName": "age", "dqType": "TYPE_CHECK", "expectedSparkType": "IntegerType" },
    --                                                 --  { "columnName": "status_code", "dqType": "VALUE_SET", "allowedValues": ["active", "inactive", "pending"] },
    --                                                 --  { "columnName": "product_code", "dqType": "LENGTH_CHECK", "maxLength": 10, "minLength": 5 },
    --                                                 --  { "columnName": "description", "dqType": "MAX_LENGTH", "maxLength": 255 }]
    IS_ACTIVE CHAR(1) DEFAULT 'Y' NOT NULL,          -- Y/N
    CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT CHK_SOURCE_HAS_HEADER CHECK (SOURCE_HAS_HEADER IN ('Y', 'N')),
    CONSTRAINT CHK_AS_IS_LOAD CHECK (AS_IS_LOAD IN ('Y', 'N')),
    CONSTRAINT CHK_IS_ACTIVE CHECK (IS_ACTIVE IN ('Y', 'N')),
    CONSTRAINT CHK_FILE_TYPE CHECK (SOURCE_FILE_TYPE IN ('CSV', 'TXT', 'XLSX', 'XLS', 'DAT', 'DATA', 'JSON', 'PARQUET', 'ORC')) -- Added more common types
);

COMMENT ON COLUMN ETL_JOB_CONFIG.JOB_ID IS 'Unique identifier for the ETL job.';
COMMENT ON COLUMN ETL_JOB_CONFIG.JOB_DESCRIPTION IS 'Brief description of the ETL job.';
COMMENT ON COLUMN ETL_JOB_CONFIG.SOURCE_FILE_PATH IS 'Path to the source file or directory. Can include wildcards.';
COMMENT ON COLUMN ETL_JOB_CONFIG.SOURCE_FILE_TYPE IS 'Type of the source file (e.g., CSV, TXT, XLSX).';
COMMENT ON COLUMN ETL_JOB_CONFIG.SOURCE_FILE_DELIMITER IS 'Delimiter for text-based files (e.g., ",", "|", "	").';
COMMENT ON COLUMN ETL_JOB_CONFIG.SOURCE_HAS_HEADER IS 'Flag indicating if the source file has a header row (Y/N).';
COMMENT ON COLUMN ETL_JOB_CONFIG.TARGET_ICEBERG_NAMESPACE IS 'Namespace (database name) for the target Iceberg table.';
COMMENT ON COLUMN ETL_JOB_CONFIG.TARGET_ICEBERG_TABLE_NAME IS 'Name of the target Iceberg table.';
COMMENT ON COLUMN ETL_JOB_CONFIG.TARGET_ICEBERG_PARTITION_COLUMNS IS 'Comma-separated list of columns for partitioning the Iceberg table.';
COMMENT ON COLUMN ETL_JOB_CONFIG.BUSINESS_KEY_COLUMNS IS 'Comma-separated list of business key columns for SCD Type 2 processing.';
COMMENT ON COLUMN ETL_JOB_CONFIG.SCD2_START_DATE_COLUMN_NAME IS 'Name of the column to store the effective start date for SCD2 records.';
COMMENT ON COLUMN ETL_JOB_CONFIG.SCD2_END_DATE_COLUMN_NAME IS 'Name of the column to store the effective end date for SCD2 records.';
COMMENT ON COLUMN ETL_JOB_CONFIG.SCD2_CURRENT_FLAG_COLUMN_NAME IS 'Name of the column to flag the current active SCD2 record.';
COMMENT ON COLUMN ETL_JOB_CONFIG.AS_IS_LOAD IS 'Flag to bypass transformations, SCD2, and DQ for a direct load (Y/N).';
COMMENT ON COLUMN ETL_JOB_CONFIG.SCHEMA_MAPPING_JSON IS 'JSON string defining explicit schema mapping from source to target.';
COMMENT ON COLUMN ETL_JOB_CONFIG.BUSINESS_TRANSFORMATION_RULES_JSON IS 'JSON string or other structured format defining business transformation logic.';
COMMENT ON COLUMN ETL_JOB_CONFIG.DATA_QUALITY_RULES_JSON IS 'JSON string defining data quality checks to be performed.';
COMMENT ON COLUMN ETL_JOB_CONFIG.IS_ACTIVE IS 'Flag to enable or disable the job (Y/N).';
COMMENT ON COLUMN ETL_JOB_CONFIG.CREATED_TS IS 'Timestamp when the configuration record was created.';
COMMENT ON COLUMN ETL_JOB_CONFIG.UPDATED_TS IS 'Timestamp when the configuration record was last updated.';
