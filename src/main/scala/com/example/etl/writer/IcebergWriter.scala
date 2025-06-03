package com.example.etl.writer

import com.example.etl.config.EtlJobConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.util.Try
import org.slf4j.LoggerFactory

object IcebergWriter {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def writeToIceberg(spark: SparkSession, df: DataFrame, config: EtlJobConfig, catalogName: String): Try[Unit] = {
    Try {
      val fullTableName = s"${catalogName}.${config.targetIcebergNamespace}.${config.targetIcebergTableName}"
      logger.info(s"Starting write operation to Iceberg table: $fullTableName")

      var writer = df.write.format("iceberg").mode(SaveMode.Append)

      if (config.targetIcebergPartitionColumns.nonEmpty) {
        logger.info(s"Applying partitioning by columns: ${config.targetIcebergPartitionColumns.mkString(", ")}")
        writer = writer.partitionBy(config.targetIcebergPartitionColumns: _*)
      }

      // Ensure the DataFrame schema does not contain characters disallowed by Parquet/Iceberg (e.g. spaces, semicolons, etc.)
      // This is a common place for failures. A more robust solution would be to sanitize column names.
      val sanitizedDf = df.select(df.columns.map(c => df(c).as(c.replaceAll("[ ,;{}()\n\t=]", "_"))): _*)


      if (sanitizedDf.schema.fields.isEmpty) {
          logger.warn(s"DataFrame for table $fullTableName is empty or has no schema. Skipping write.")
      } else {
          logger.info(s"Writing DataFrame with schema: ${sanitizedDf.schema.treeString} to $fullTableName")
          writer.save(fullTableName)
          logger.info(s"Successfully wrote data to Iceberg table: $fullTableName")
      }
    }
  }
}
