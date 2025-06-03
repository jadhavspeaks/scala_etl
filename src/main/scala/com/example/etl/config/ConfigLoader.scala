package com.example.etl.config

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.util.{Try, Success, Failure, Using}
import org.slf4j.LoggerFactory
import spray.json._
import com.example.etl.config.JsonConfigProtocol._

object ConfigLoader {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private def csvToList(csv: Option[String]): List[String] = {
    csv.map(_.split(',').map(_.trim).filter(_.nonEmpty).toList).getOrElse(List.empty)
  }

  private def parseJsonString[T](jsonString: Option[String], parser: String => T, fieldName: String): Option[T] = {
    jsonString.filter(s => s != null && s.trim.nonEmpty).flatMap { str =>
      Try(parser(str)) match {
        case Success(parsedObject) => Some(parsedObject)
        case Failure(ex) =>
          logger.warn(s"Failed to parse JSON for $fieldName: ${ex.getMessage}. JSON string was: $str", ex) // Log exception
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
    val apiConfigSourceStr = Option(rs.getString("API_SOURCE_CONFIG_JSON"))
    val sourceType = rs.getString("SOURCE_TYPE")

    val schemaMappingsList = parseJsonString[List[SchemaMappingRule]](
        schemaMappingsStr, _.parseJson.convertTo[List[SchemaMappingRule]], "SchemaMapping"
    )
    val dataQualityRulesList = parseJsonString[List[DataQualityRule]](
        dqRulesStr, _.parseJson.convertTo[List[DataQualityRule]], "DataQualityRules"
    )
    val apiSourceConfigOpt = if (sourceType == "API") {
        parseJsonString[ApiSourceConfig](
            apiConfigSourceStr, _.parseJson.convertTo[ApiSourceConfig], "ApiSourceConfig"
        )
    } else {
        None
    }

    if (sourceType == "API" && apiSourceConfigOpt.isEmpty && apiConfigSourceStr.exists(_.trim.nonEmpty)) {
        logger.warn(s"SOURCE_TYPE is API for JOB_ID: ${rs.getString("JOB_ID")} but API_SOURCE_CONFIG_JSON was either missing, empty, or could not be parsed. API configuration will be effectively None.")
        // Consider if this should be a fatal error depending on requirements
        // For now, it logs a warning and proceeds with apiSourceConfigOpt as None.
    }


    EtlJobConfig(
      jobId = rs.getString("JOB_ID"),
      jobDescription = Option(rs.getString("JOB_DESCRIPTION")),

      sourceType = sourceType,

      sourceFilePath = Option(rs.getString("SOURCE_FILE_PATH")),
      sourceFileType = rs.getString("SOURCE_FILE_TYPE"),
      sourceFileDelimiter = Option(rs.getString("SOURCE_FILE_DELIMITER")),
      // Default sourceHasHeader to false if not explicitly set or if it's an API source (where it might not be relevant in the same way)
      sourceHasHeader = Option(rs.getString("SOURCE_HAS_HEADER")).map(_ == "Y").getOrElse(false),


      apiSourceConfig = apiSourceConfigOpt,

      targetIcebergNamespace = rs.getString("TARGET_ICEBERG_NAMESPACE"),
      targetIcebergTableName = rs.getString("TARGET_ICEBERG_TABLE_NAME"),
      targetIcebergPartitionColumns = csvToList(Option(rs.getString("TARGET_ICEBERG_PARTITION_COLUMNS"))),
      businessKeyColumns = csvToList(Option(rs.getString("BUSINESS_KEY_COLUMNS"))),
      scd2StartDateColumnName = rs.getString("SCD2_START_DATE_COLUMN_NAME"),
      scd2EndDateColumnName = rs.getString("SCD2_END_DATE_COLUMN_NAME"),
      scd2CurrentFlagColumnName = rs.getString("SCD2_CURRENT_FLAG_COLUMN_NAME"),
      asIsLoad = rs.getString("AS_IS_LOAD") == "Y",
      schemaMappings = schemaMappingsList,
      dataQualityRules = dataQualityRulesList,
      businessTransformationRulesJson = Option(rs.getString("BUSINESS_TRANSFORMATION_RULES_JSON")),
      isActive = rs.getString("IS_ACTIVE") == "Y",
      createdTs = rs.getTimestamp("CREATED_TS"),
      updatedTs = rs.getTimestamp("UPDATED_TS")
    )
  }
}
