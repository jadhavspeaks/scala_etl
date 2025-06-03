package com.example.etl.ingestion

import com.example.etl.config.{EtlJobConfig, SchemaMappingRule}
import com.example.etl.utils.SparkSchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_timestamp, to_date}
import org.apache.spark.sql.types.{DateType, TimestampType} // Explicit imports
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory

object FileIngestor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def ingestFile(spark: SparkSession, config: EtlJobConfig): Try[DataFrame] = {
    Try {
      val fileType = config.sourceFileType.toUpperCase()
      val filePath = config.sourceFilePath
      logger.info(s"Starting ingestion for file type: $fileType from path: $filePath")

      // Common reader options
      // For Excel, options are typically passed differently.
      var reader = spark.read
      if (fileType != "XLSX" && fileType != "XLS" && fileType != "PARQUET" && fileType != "ORC") { // Options for non-Excel/Parquet/ORC text-based or standard formats
         reader = reader.option("inferSchema", "true") // Initial schema inference for most types
      }


      val rawDf = fileType match {
        case "CSV" =>
          reader
            .option("header", config.sourceHasHeader.toString)
            .option("delimiter", config.sourceFileDelimiter.getOrElse(","))
            .csv(filePath)
        case "TXT" | "DAT" | "DATA" => // Treat TXT, DAT, DATA as generic delimited files
          val delimiter = config.sourceFileDelimiter.getOrElse {
              logger.warn(s"No delimiter specified for $fileType file type, defaulting to comma (,).")
              ","
          }
          logger.info(s"Treating $fileType as a delimited file with delimiter: '$delimiter' and header: ${config.sourceHasHeader}")
          reader
            .option("header", config.sourceHasHeader.toString)
            .option("delimiter", delimiter)
            .csv(filePath) // Using CSV reader for generic delimited files
        case "XLSX" | "XLS" =>
          // spark-excel specific options
          // inferSchema is often true by default or handled by dataAddress option
          logger.info(s"Reading Excel file. Header: ${config.sourceHasHeader}. Other options may apply via config if extended.")
          spark.read
            .format("com.crealytics.spark.excel")
            .option("header", config.sourceHasHeader.toString)
            // .option("dataAddress", "'Sheet1'!A1") // Example: more specific sheet/range, could be from config
            .option("inferSchema", "true") // Let spark-excel infer schema
            // .option("treatEmptyValuesAsNulls", "true") // Default is true
            // .option("setErrorCellsToFallbackValues", "true") // Default is false
            // .option("usePlainNumberFormat", "false") // Default is false, attempt to parse numbers
            .load(filePath)
        case "JSON" =>
          logger.info("Reading JSON file(s).")
          // For JSON, typically expect one JSON object per line (multiline=false) or an array of objects (multiline=true)
          // Add multiline option to config if needed, default to false (JSON lines)
          reader.option("multiline", "false").json(filePath)
        case "PARQUET" =>
          logger.info("Reading Parquet file(s).")
          spark.read.parquet(filePath) // Parquet reader doesn't use common reader options like inferSchema/header/delimiter
        case "ORC" =>
          logger.info("Reading ORC file(s).")
          spark.read.orc(filePath) // ORC reader similar to Parquet
        case _ =>
          logger.error(s"Unsupported file type: $fileType for ingestion.")
          throw new IllegalArgumentException(s"Unsupported file type: $fileType for path: $filePath")
      }
      logger.info(s"Successfully inferred schema and ingested data from: $filePath for file type: $fileType.")
      if(rawDf.columns.isEmpty && rawDf.count() == 0) {
        logger.warn(s"Ingested DataFrame from $filePath is empty (no columns and no rows). This might be due to incorrect file path, empty file, or misconfiguration for $fileType.")
      } else {
        logger.debug(s"Raw schema: ${rawDf.schema.simpleString}")
      }

      // Apply schema mapping if defined
      config.schemaMappings match {
        case Some(mappings) if mappings.nonEmpty =>
          logger.info("Applying explicit schema mappings...")
          val selectExprs = mappings.map { rule =>
            var columnExpr = col(rule.sourceName) // Assume sourceName exists from inferred schema or header
            val targetSparkType = SparkSchemaUtils.stringToSparkType(rule.targetType)

            if (targetSparkType.isInstanceOf[TimestampType] && rule.format.isDefined) {
              columnExpr = to_timestamp(columnExpr, rule.format.get)
            } else if (targetSparkType.isInstanceOf[DateType] && rule.format.isDefined) {
              columnExpr = to_date(columnExpr, rule.format.get)
            } else {
              columnExpr = columnExpr.cast(targetSparkType)
            }

            columnExpr.alias(rule.targetName)
          }
          // Check if all source columns in mappings exist in rawDf
          val rawDfColumns = rawDf.columns.toSet
          mappings.foreach{ rule =>
            if (!rawDfColumns.contains(rule.sourceName)) {
              logger.warn(s"Schema mapping rule refers to source column '${rule.sourceName}' which is not found in the ingested file's columns: [${rawDfColumns.mkString(", ")}]. This mapping will likely fail or produce nulls.")
            }
          }
          val mappedDf = rawDf.select(selectExprs: _*)
          logger.info(s"Applied schema mappings. New schema: ${mappedDf.schema.simpleString}")
          mappedDf
        case _ =>
          logger.info("No schema mappings defined or mappings list is empty. Using schema from file.")
          rawDf
      }
    } match {
      case Success(df) => Success(df)
      case Failure(e) =>
        logger.error(s"Failed to ingest and map file from path: ${config.sourceFilePath}. Error: ${e.getMessage}", e)
        Failure(e)
    }
  }
}
