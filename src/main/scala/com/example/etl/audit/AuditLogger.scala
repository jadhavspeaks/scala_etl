package com.example.etl.audit

import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.util.Try
import org.slf4j.LoggerFactory

object AuditLogger {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def log(spark: SparkSession, record: AuditRecord, auditTableFullIdentifier: String): Try[Unit] = {
    Try {
      logger.info(s"Logging audit record for JOB_ID: ${record.JOB_ID}, RUN_ID: ${record.RUN_ID} to $auditTableFullIdentifier")

      import spark.implicits._
      val auditDf = spark.sparkContext.parallelize(Seq(record)).toDF()

      if (auditDf.schema.isEmpty) {
          val msg = s"Audit DataFrame for table $auditTableFullIdentifier has no schema. Skipping write."
          logger.error(msg)
          throw new IllegalStateException(msg)
      }

      logger.debug(s"Attempting to write audit record with schema: ${auditDf.schema.treeString}")

      auditDf.write
        .format("iceberg")
        .mode(SaveMode.Append)
        .save(auditTableFullIdentifier)
      logger.info(s"Successfully wrote audit record for RUN_ID: ${record.RUN_ID} to $auditTableFullIdentifier")
    } recoverWith { case ex: Exception =>
      logger.error(s"Failed to write audit log for RUN_ID: ${record.RUN_ID} to $auditTableFullIdentifier. Error: ${ex.getMessage}", ex)
      Try(throw ex)
    }
  }
}
