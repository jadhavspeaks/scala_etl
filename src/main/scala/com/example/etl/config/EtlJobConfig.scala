package com.example.etl.config

import java.sql.Timestamp
// import com.example.etl.config.ApiSourceConfig // Not strictly needed if classes are in same package and compiled together

// EtlJobConfig case class (updated)
case class EtlJobConfig(
    jobId: String,
    jobDescription: Option[String],

    sourceType: String, // New: "FILE" or "API"

    // File-specific (relevant if sourceType == "FILE")
    sourceFilePath: Option[String], // Made Option for API sources
    sourceFileType: String, // For FILE: type of file; For API: payload type from API (e.g. JSON)
    sourceFileDelimiter: Option[String],
    sourceHasHeader: Boolean,

    // API-specific (relevant if sourceType == "API")
    apiSourceConfig: Option[ApiSourceConfig], // New

    targetIcebergNamespace: String,
    targetIcebergTableName: String,
    targetIcebergPartitionColumns: List[String],
    businessKeyColumns: List[String],
    scd2StartDateColumnName: String,
    scd2EndDateColumnName: String,
    scd2CurrentFlagColumnName: String,
    asIsLoad: Boolean,
    schemaMappings: Option[List[SchemaMappingRule]],
    dataQualityRules: Option[List[DataQualityRule]],
    businessTransformationRulesJson: Option[String],
    isActive: Boolean,
    createdTs: Timestamp,
    updatedTs: Timestamp
)
