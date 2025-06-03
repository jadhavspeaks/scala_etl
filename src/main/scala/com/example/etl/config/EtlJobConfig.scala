package com.example.etl.config

import java.sql.Timestamp

// EtlJobConfig case class (updated)
case class EtlJobConfig(
    jobId: String,
    jobDescription: Option[String],
    sourceFilePath: String,
    sourceFileType: String,
    sourceFileDelimiter: Option[String],
    sourceHasHeader: Boolean,
    targetIcebergNamespace: String,
    targetIcebergTableName: String,
    targetIcebergPartitionColumns: List[String],
    businessKeyColumns: List[String],
    scd2StartDateColumnName: String,
    scd2EndDateColumnName: String,
    scd2CurrentFlagColumnName: String,
    asIsLoad: Boolean,
    // Updated fields:
    schemaMappings: Option[List[SchemaMappingRule]],
    dataQualityRules: Option[List[DataQualityRule]],
    // Kept raw JSON for business rules for now
    businessTransformationRulesJson: Option[String],
    isActive: Boolean,
    createdTs: Timestamp,
    updatedTs: Timestamp
)
