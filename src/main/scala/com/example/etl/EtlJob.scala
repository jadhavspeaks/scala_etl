package com.example.etl

import com.example.etl.config.{ConfigLoader, EtlJobConfig}
import com.example.etl.ingestion.FileIngestor
import com.example.etl.writer.IcebergWriter
import com.example.etl.audit.{AuditLogger, AuditRecord}
import com.example.etl.validation.DataQualityValidator
import com.example.etl.processing.SCD2Processor // Import SCD2Processor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, array, col}
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}
import java.util.UUID
import java.sql.Timestamp

object EtlJob {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      logger.error("Usage: EtlJob <jobId> <dbUrl> <dbUser> <dbPass> <icebergCatalogName> <auditTableIdentifier> [<sparkMaster>]")
      System.exit(1)
    }

    val jobId = args(0)
    val dbUrl = args(1)
    val dbUser = args(2)
    val dbPass = args(3)
    val icebergCatalogName = args(4)
    val auditTableIdentifier = args(5)
    val sparkMaster = if (args.length > 6) args(6) else "local[*]"

    val runId = UUID.randomUUID().toString()
    val jobStartTime = new Timestamp(System.currentTimeMillis())

    var auditRecord = AuditRecord(
      AUDIT_LOG_ID = runId,
      JOB_ID = jobId,
      RUN_ID = runId,
      JOB_RUN_START_TS = Some(jobStartTime),
      STATUS = "INITIALIZING"
    )
    logger.info(s"Starting ETL job for Job ID: $jobId, Run ID: $runId")
    logger.info(s"Using Iceberg Catalog: $icebergCatalogName, Audit Table: $auditTableIdentifier")

    val sparkBuilder = SparkSession.builder()
      .appName(s"ETL Job: $jobId Run: $runId")
      .master(sparkMaster)

    if (icebergCatalogName == "test_local_catalog" && sparkMaster.startsWith("local")) {
       val testWarehousePath = "target/prod-like-warehouse"
       logger.info(s"Configuring local test catalog '$icebergCatalogName' with warehouse: $testWarehousePath")
       sparkBuilder
          .config(s"spark.sql.catalog.$icebergCatalogName", "org.apache.iceberg.spark.SparkCatalog")
          .config(s"spark.sql.catalog.$icebergCatalogName.type", "hadoop")
          .config(s"spark.sql.catalog.$icebergCatalogName.warehouse", new java.io.File(testWarehousePath).getAbsolutePath)
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    } else {
      sparkBuilder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      logger.info(s"Ensure Spark session is configured with Iceberg extensions and catalog '$icebergCatalogName' details.")
    }

    val spark = sparkBuilder.getOrCreate()
    var pipelineSuccessfullyCompleted = false

    try {
      auditRecord = auditRecord.copy(STATUS = "CONFIG_LOADING")
      ConfigLoader.loadConfig(jobId, dbUrl, dbUser, dbPass) match {
        case Success(config) =>
          auditRecord = auditRecord.copy(
            STATUS = "CONFIG_LOADED",
            TARGET_ICEBERG_NAMESPACE = Some(config.targetIcebergNamespace),
            TARGET_ICEBERG_TABLE_NAME = Some(config.targetIcebergTableName),
            SOURCE_FILE_PATH = Some(config.sourceFilePath)
          )
          logger.info(s"Configuration loaded successfully for job: ${config.jobId}")

          val (etlStepSuccess, updatedAuditRecordAfterPipeline) = runEtlPipeline(spark, config, icebergCatalogName, auditRecord)
          auditRecord = updatedAuditRecordAfterPipeline

          if (etlStepSuccess) {
            pipelineSuccessfullyCompleted = true
          } else {
            pipelineSuccessfullyCompleted = false
          }

        case Failure(exception) =>
          logger.error(s"Failed to load configuration for Job ID: $jobId, Run ID: $runId", exception)
          auditRecord = auditRecord.copy(STATUS = "FAILED", ERROR_MESSAGE = Some(s"Config loading failed: ${exception.getMessage}"))
          pipelineSuccessfullyCompleted = false
      }
    } catch {
      case e: Exception =>
        logger.error(s"An unexpected error occurred during ETL job execution for Job ID: $jobId, Run ID: $runId", e)
        auditRecord = auditRecord.copy(STATUS = "FAILED", ERROR_MESSAGE = Some(s"Outer scope error: ${e.getMessage}"))
        pipelineSuccessfullyCompleted = false
    } finally {
      val jobEndTime = new Timestamp(System.currentTimeMillis())
      auditRecord = auditRecord.copy(JOB_RUN_END_TS = Some(jobEndTime))

      if (pipelineSuccessfullyCompleted && auditRecord.STATUS == "PROCESSING_SUCCESS") {
         auditRecord = auditRecord.copy(STATUS = "SUCCESS")
      } else if (auditRecord.STATUS != "FAILED") {
         auditRecord = auditRecord.copy(STATUS = "FAILED", ERROR_MESSAGE = auditRecord.ERROR_MESSAGE.orElse(Some("Job did not complete with an overall SUCCESS status.")))
      }

      logger.info(s"Attempting to log final audit record for RUN_ID: $runId. Final status: ${auditRecord.STATUS}")
      AuditLogger.log(spark, auditRecord, auditTableIdentifier) match {
        case Success(_) => logger.info(s"Final audit record logged successfully for RUN_ID: $runId.")
        case Failure(auditEx) => logger.error(s"CRITICAL: Failed to log final audit record for RUN_ID: $runId. Audit Error: ${auditEx.getMessage}", auditEx)
      }

      logger.info(s"Stopping Spark session for Job ID: $jobId, Run ID: $runId")
      spark.stop()
      logger.info(s"ETL job for Job ID: $jobId, Run ID: $runId finished with status: ${auditRecord.STATUS}.")
    }
  }

  private def runEtlPipeline(spark: SparkSession, config: EtlJobConfig, icebergCatalogName: String, initialAuditRecord: AuditRecord): (Boolean, AuditRecord) = {
    var currentAuditRecord = initialAuditRecord
    var inputRowCount: Long = 0L

    try {
        currentAuditRecord = currentAuditRecord.copy(STATUS = "INGESTING_FILE")
        logger.info(s"Starting file ingestion for job: ${config.jobId}, file: ${config.sourceFilePath}")
        FileIngestor.ingestFile(spark, config) match {
        case Success(sourceDf) =>
            inputRowCount = sourceDf.count()
            currentAuditRecord = currentAuditRecord.copy(INPUT_ROW_COUNT = Some(inputRowCount), STATUS = "FILE_INGESTED_SCHEMA_APPLIED")
            logger.info(s"File ingested and schema mapped successfully for job: ${config.jobId}. Row count: $inputRowCount")

            currentAuditRecord = currentAuditRecord.copy(STATUS = "APPLYING_DQ_RULES")
            val dfWithDQ = config.dataQualityRules match {
                case Some(rules) if rules.nonEmpty =>
                    logger.info("Applying data quality rules...")
                    DataQualityValidator.applyDQRules(sourceDf, rules)
                case _ =>
                    logger.info("No data quality rules to apply or rules list is empty.")
                    sourceDf.withColumn("dq_passed", lit(true)).withColumn("dq_failure_reasons", array().cast("array<string>"))
            }
            currentAuditRecord = currentAuditRecord.copy(STATUS = "DQ_RULES_APPLIED")

            val dqFailedCount = dfWithDQ.filter(col("dq_passed") === false).count()
            currentAuditRecord = currentAuditRecord.copy(DQ_FAILURE_COUNT = Some(dqFailedCount))
            logger.info(s"DQ processing complete. Number of rows failing DQ checks: $dqFailedCount")

            val dfForProcessing = dfWithDQ // Process all rows regardless of DQ status for now. Filter if needed: dfWithDQ.filter(col("dq_passed") === true)

            if (config.asIsLoad) {
                logger.info(s"Executing 'as-is' load for job: ${config.jobId}. Skipping SCD2. Writing with DQ columns.")
                currentAuditRecord = currentAuditRecord.copy(STATUS = "WRITING_AS_IS_LOAD_TO_ICEBERG")
                IcebergWriter.writeToIceberg(spark, dfForProcessing, config, icebergCatalogName) match {
                    case Success(_) =>
                        currentAuditRecord = currentAuditRecord.copy(OUTPUT_ROW_COUNT = Some(dfForProcessing.count()), STATUS = "PROCESSING_SUCCESS")
                        logger.info(s"As-is data written successfully to Iceberg for job: ${config.jobId}.")
                        (true, currentAuditRecord)
                    case Failure(ex) => throw ex
                }
            } else if (config.businessKeyColumns.nonEmpty) {
                logger.info(s"SCD2 processing is active for job: ${config.jobId}.")
                currentAuditRecord = currentAuditRecord.copy(STATUS = "APPLYING_SCD2_LOGIC")
                val targetTableIdForSCD2 = s"${config.targetIcebergNamespace}.${config.targetIcebergTableName}"
                SCD2Processor.processSCD2(spark, dfForProcessing, targetTableIdForSCD2, config, icebergCatalogName) match {
                    case Success(affectedRows) =>
                        logger.info(s"SCD2 processing completed successfully. Affected rows: $affectedRows")
                        currentAuditRecord = currentAuditRecord.copy(OUTPUT_ROW_COUNT = Some(affectedRows), STATUS = "PROCESSING_SUCCESS")
                        (true, currentAuditRecord)
                    case Failure(ex) => throw ex
                }
            } else {
                logger.info(s"Standard load (non-SCD2) for job: ${config.jobId}. Writing with DQ columns.")
                currentAuditRecord = currentAuditRecord.copy(STATUS = "WRITING_STANDARD_LOAD_TO_ICEBERG")
                IcebergWriter.writeToIceberg(spark, dfForProcessing, config, icebergCatalogName) match {
                    case Success(_) =>
                        currentAuditRecord = currentAuditRecord.copy(OUTPUT_ROW_COUNT = Some(dfForProcessing.count()), STATUS = "PROCESSING_SUCCESS")
                        logger.info(s"Standard data written successfully to Iceberg for job: ${config.jobId}.")
                        (true, currentAuditRecord)
                    case Failure(ex) => throw ex
                }
            }
        case Failure(exception) =>
            logger.error(s"Failed to ingest file for job: ${config.jobId}", exception)
            throw exception
        }
    } catch {
        case ex: Exception =>
            logger.error(s"Error in ETL pipeline for job ${config.jobId}: ${ex.getMessage}", ex)
            (false, currentAuditRecord.copy(STATUS="FAILED", ERROR_MESSAGE=Some(s"Pipeline step failed: ${ex.getMessage}")))
    }
  }
}
