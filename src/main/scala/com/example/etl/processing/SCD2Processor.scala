package com.example.etl.processing

import com.example.etl.config.EtlJobConfig
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import scala.util.{Try, Failure} // Added Failure for direct use
import java.time.LocalDate

object SCD2Processor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val HIGH_DATE = java.sql.Date.valueOf(LocalDate.of(9999, 12, 31)) // Use java.sql.Date

  def processSCD2(
      spark: SparkSession,
      sourceDf: DataFrame,
      targetTableIdentifier: String,
      config: EtlJobConfig,
      icebergCatalogName: String // Added for full table identifier construction
  ): Try[Long] = {
    val fullTargetTableName = s"$icebergCatalogName.${config.targetIcebergNamespace}.${config.targetIcebergTableName}"
    Try {
      if (config.businessKeyColumns.isEmpty) {
        logger.info(s"No business keys defined for SCD2 processing on $fullTargetTableName. Skipping SCD2 logic.")
        return Try(0L)
      }

      val businessKeys = config.businessKeyColumns
      val scdStartCol = config.scd2StartDateColumnName
      val scdEndCol = config.scd2EndDateColumnName
      val currentFlagCol = config.scd2CurrentFlagColumnName

      logger.info(s"Starting SCD2 processing for target table: $fullTargetTableName")
      logger.info(s"Business keys: ${businessKeys.mkString(", ")}")

      val sourcePayloadCols = sourceDf.columns.filterNot(c => Set(scdStartCol, scdEndCol, currentFlagCol).contains(c))
      val joinCondition = businessKeys.map(key => col(s"s.$key") === col(s"t.$key")).reduce(_ && _)

      val nonKeyPayloadCols = sourcePayloadCols.filterNot(businessKeys.contains)
      if (nonKeyPayloadCols.isEmpty) {
        val msg = s"SCD2 requires at least one non-key column. Found none for $fullTargetTableName."
        logger.error(msg)
        throw new IllegalArgumentException(msg)
      }

      // More robust change condition using struct and null-safe comparison
      val sourceNonKeyStruct = struct(nonKeyPayloadCols.map(c => col(s"s.$c")): _*)
      val targetNonKeyStruct = struct(nonKeyPayloadCols.map(c => col(s"t.$c")): _*)
      val changeConditionExpr = not(sourceNonKeyStruct <=> targetNonKeyStruct) // Null-safe inequality

      val enrichedSourceDf = sourceDf.alias("s") // Alias here
        .withColumn(scdStartCol, current_timestamp())
        .withColumn(scdEndCol, lit(HIGH_DATE))
        .withColumn(currentFlagCol, lit(true))

      if (!spark.catalog.tableExists(fullTargetTableName)) {
        logger.info(s"Target table $fullTargetTableName does not exist. Writing all source records as new SCD2 records.")
        enrichedSourceDf.write
          .format("iceberg")
          .mode(SaveMode.Append) // Append since table doesn't exist, will create it
          .save(fullTargetTableName)
        return Try(enrichedSourceDf.count())
      }

      val targetDfActive = spark.read.table(fullTargetTableName).as("t").where(col(s"t.$currentFlagCol") === true)

      // 1. New rows: source rows not in active target
      val newRowsDf = enrichedSourceDf
        .join(targetDfActive, joinCondition, "left_anti")
      logger.info(s"Identified ${newRowsDf.count()} new rows for direct insertion.")

      // 2. Rows in source that match an active record in target
      val joinedDf = enrichedSourceDf
        .join(targetDfActive, joinCondition, "inner")
        .withColumn("has_changed", changeConditionExpr)

      // 3. New versions of changed records (from source perspective)
      val newVersionsOfChangedDf = joinedDf
        .filter(col("has_changed") === true)
        .select("s.*")
      logger.info(s"Identified ${newVersionsOfChangedDf.count()} rows that changed and will be inserted as new versions.")

      // 4. DataFrame of records to expire in the target table
      val rowsToExpireInTargetDf = joinedDf
        .filter(col("has_changed") === true)
        .select(col("t.*")) // Select all columns from the target alias 't'
        .withColumn(scdEndCol, date_sub(current_timestamp(), 1))
        .withColumn(currentFlagCol, lit(false))
      logger.info(s"Identified ${rowsToExpireInTargetDf.count()} existing rows in target to be expired.")

      // 5. Active records from target that did not change (and are not in source, or matched but unchanged)
      // Records in target that are current and had no corresponding record in source based on keys
      val targetNoSourceMatchDf = targetDfActive
        .join(enrichedSourceDf, joinCondition, "left_anti")

      // Records in target that are current, matched source, but did not change
      val targetMatchedNoChangeDf = joinedDf
        .filter(col("has_changed") === false)
        .select("t.*")

      val unchangedActiveTargetDf = targetNoSourceMatchDf.unionByName(targetMatchedNoChangeDf)
      logger.info(s"Identified ${unchangedActiveTargetDf.count()} active rows in target that remain unchanged.")

      // 6. Combine all DFs for the new state of the table
      // Ensure all DFs in union have the same schema structure as the target table.
      // enrichedSourceDf (used for newRowsDf, newVersionsOfChangedDf) must align with target table schema if target exists.
      // This implies sourceDf should already conform to target table's non-SCD columns.

      val finalDfToWrite = newRowsDf
        .unionByName(newVersionsOfChangedDf)
        .unionByName(rowsToExpireInTargetDf)
        .unionByName(unchangedActiveTargetDf)

      logger.info(s"Combined DataFrame for overwrite has ${finalDfToWrite.count()} rows. Overwriting target table $fullTargetTableName.")

      finalDfToWrite.write
        .format("iceberg")
        .mode(SaveMode.Overwrite)
        .option("overwriteSchema", "true")
        .save(fullTargetTableName)

      val affectedCount = newRowsDf.count() + newVersionsOfChangedDf.count()
      logger.info(s"SCD2 processing complete for $fullTargetTableName. Affected (new/updated versions) logical records: $affectedCount")
      affectedCount
    } recoverWith { // Add specific recovery for better error messages if needed
        case e: Exception =>
            logger.error(s"Error during SCD2 processing for $fullTargetTableName: ${e.getMessage}", e)
            Failure(e)
    }
  }
}
