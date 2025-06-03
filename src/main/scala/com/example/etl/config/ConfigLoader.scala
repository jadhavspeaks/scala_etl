package com.example.etl.config

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.util.{Try, Success, Failure, Using}
import org.slf4j.LoggerFactory // Added for logging
import spray.json._ // Spray JSON import
import com.example.etl.config.JsonConfigProtocol._ // Your custom protocol

object ConfigLoader {
  private val logger = LoggerFactory.getLogger(this.getClass) // Logger instance

  private def csvToList(csv: Option[String]): List[String] = {
    csv.map(_.split(',').map(_.trim).filter(_.nonEmpty).toList).getOrElse(List.empty)
  }

  // Generic JSON string parser
  private def parseJsonString[T](jsonString: Option[String], parser: String => T, fieldName: String): Option[T] = {
    jsonString.filter(_.trim.nonEmpty).flatMap { str =>
      Try(parser(str)) match {
        case Success(parsedObject) => Some(parsedObject)
        case Failure(ex) =>
          logger.warn(s"Failed to parse JSON for $fieldName: ${ex.getMessage}. JSON string was: $str")
          None
      }
    }
  }

  def loadConfig(jobId: String, dbUrl: String, dbUser: String, dbPass: String): Try[EtlJobConfig] = {
    Try(Class.forName("oracle.jdbc.driver.OracleDriver")).recoverWith {
        case ex: ClassNotFoundException =>
          logger.error("Failed to load Oracle JDBC driver.", ex)
          Failure(new RuntimeException("Failed to load Oracle JDBC driver", ex))
    }

    val properties = new Properties()
    properties.setProperty("user", dbUser)
    properties.setProperty("password", dbPass)

    val query = "SELECT * FROM ETL_JOB_CONFIG WHERE JOB_ID = ? AND IS_ACTIVE = 'Y'"

    Using(DriverManager.getConnection(dbUrl, properties)) { conn =>
      Using(conn.prepareStatement(query)) { stmt =>
        stmt.setString(1, jobId)
        Using(stmt.executeQuery()) { rs =>
          if (rs.next()) {
            Success(mapRowToEtlJobConfig(rs))
          } else {
            Failure(new RuntimeException(s"No active configuration found for JOB_ID: $jobId"))
          }
        }
      }
    }.flatten
  }

  private def mapRowToEtlJobConfig(rs: ResultSet): EtlJobConfig = {
    val schemaMappingsStr = Option(rs.getString("SCHEMA_MAPPING_JSON"))
    val dqRulesStr = Option(rs.getString("DATA_QUALITY_RULES_JSON"))

    val schemaMappingsList = parseJsonString[List[SchemaMappingRule]](
        schemaMappingsStr,
        _.parseJson.convertTo[List[SchemaMappingRule]],
        "SchemaMapping"
    )

    val dataQualityRulesList = parseJsonString[List[DataQualityRule]](
        dqRulesStr,
        _.parseJson.convertTo[List[DataQualityRule]],
        "DataQualityRules"
    )

    EtlJobConfig(
      jobId = rs.getString("JOB_ID"),
      jobDescription = Option(rs.getString("JOB_DESCRIPTION")),
      sourceFilePath = rs.getString("SOURCE_FILE_PATH"),
      sourceFileType = rs.getString("SOURCE_FILE_TYPE"),
      sourceFileDelimiter = Option(rs.getString("SOURCE_FILE_DELIMITER")),
      sourceHasHeader = rs.getString("SOURCE_HAS_HEADER") == "Y",
      targetIcebergNamespace = rs.getString("TARGET_ICEBERG_NAMESPACE"),
      targetIcebergTableName = rs.getString("TARGET_ICEBERG_TABLE_NAME"),
      targetIcebergPartitionColumns = csvToList(Option(rs.getString("TARGET_ICEBERG_PARTITION_COLUMNS"))),
      businessKeyColumns = csvToList(Option(rs.getString("BUSINESS_KEY_COLUMNS"))),
      scd2StartDateColumnName = rs.getString("SCD2_START_DATE_COLUMN_NAME"),
      scd2EndDateColumnName = rs.getString("SCD2_END_DATE_COLUMN_NAME"),
      scd2CurrentFlagColumnName = rs.getString("SCD2_CURRENT_FLAG_COLUMN_NAME"),
      asIsLoad = rs.getString("AS_IS_LOAD") == "Y",
      // Updated fields:
      schemaMappings = schemaMappingsList,
      dataQualityRules = dataQualityRulesList,
      businessTransformationRulesJson = Option(rs.getString("BUSINESS_TRANSFORMATION_RULES_JSON")),
      isActive = rs.getString("IS_ACTIVE") == "Y",
      createdTs = rs.getTimestamp("CREATED_TS"),
      updatedTs = rs.getTimestamp("UPDATED_TS")
    )
  }
}
