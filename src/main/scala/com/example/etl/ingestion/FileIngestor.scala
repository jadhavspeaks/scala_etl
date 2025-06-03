package com.example.etl.ingestion

import com.example.etl.config.{EtlJobConfig, SchemaMappingRule}
import com.example.etl.utils.SparkSchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_timestamp, to_date}
import org.apache.spark.sql.types.{DateType, TimestampType} // Explicit import for .isInstanceOf checks
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory

object FileIngestor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def ingestFile(spark: SparkSession, config: EtlJobConfig): Try[DataFrame] = {
    Try {
      val fileType = config.sourceFileType.toUpperCase()
      val filePath = config.sourceFilePath
      logger.info(s"Starting ingestion for file type: $fileType from path: $filePath")

      val reader = spark.read.option("inferSchema", "true") // Initial schema inference

      val rawDf = fileType match {
        case "CSV" =>
          reader
            .option("header", config.sourceHasHeader.toString)
            .option("delimiter", config.sourceFileDelimiter.getOrElse(","))
            .csv(filePath)
        case "TXT" =>
          logger.warn("TXT file type is currently treated as CSV. Ensure delimiter is set correctly in config if not comma.")
          reader
            .option("header", config.sourceHasHeader.toString)
            .option("delimiter", config.sourceFileDelimiter.getOrElse(","))
            .csv(filePath)
        case _ =>
          logger.error(s"Unsupported file type: $fileType")
          throw new IllegalArgumentException(s"Unsupported file type: $fileType for path: $filePath")
      }
      logger.info(s"Successfully inferred schema and ingested data from: $filePath. Raw schema: ${rawDf.schema.simpleString}")

      // Apply schema mapping if defined
      config.schemaMappings match {
        case Some(mappings) if mappings.nonEmpty =>
          logger.info("Applying explicit schema mappings...")
          val selectExprs = mappings.map { rule =>
            var columnExpr = col(rule.sourceName)
            val targetSparkType = SparkSchemaUtils.stringToSparkType(rule.targetType)

            // Handle casting with optional format string for date/timestamp
            if (targetSparkType.isInstanceOf[TimestampType] && rule.format.isDefined) {
              columnExpr = to_timestamp(columnExpr, rule.format.get)
            } else if (targetSparkType.isInstanceOf[DateType] && rule.format.isDefined) {
              columnExpr = to_date(columnExpr, rule.format.get)
            } else {
              columnExpr = columnExpr.cast(targetSparkType)
            }

            columnExpr.alias(rule.targetName)
          }
          val mappedDf = rawDf.select(selectExprs: _*)
          logger.info(s"Applied schema mappings. New schema: ${mappedDf.schema.simpleString}")
          mappedDf
        case _ =>
          logger.info("No schema mappings defined or mappings list is empty. Using inferred schema.")
          rawDf
      }
    } match {
      case Success(df) => Success(df)
      case Failure(e) =>
        logger.error(s"Failed to ingest and map file from path: ${config.sourceFilePath}", e)
        Failure(e)
    }
  }
}
