package com.example.etl.utils

import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object SparkSchemaUtils {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def stringToSparkType(typeString: String): DataType = {
    val cleanedTypeString = typeString.trim.toLowerCase
    cleanedTypeString match {
      case "string" | "stringtype" => StringType
      case "int" | "integer" | "integertype" => IntegerType
      case "long" | "longtype" => LongType
      case "double" | "doubletype" => DoubleType
      case "float" | "floattype" => FloatType
      case "boolean" | "booleantype" => BooleanType
      case "date" | "datetype" => DateType
      case "timestamp" | "timestamptype" => TimestampType
      case "byte" | "bytetype" => ByteType
      case "short" | "shorttype" => ShortType
      case "binary" | "binarytype" => BinaryType
      case dec if dec.startsWith("decimal") || dec.startsWith("decimaltype") =>
        val pattern = "decimal\\((\\d+),(\\d+)\\)".r
        dec match {
          case pattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
          case _ =>
            logger.warn(s"Could not parse precision/scale for DecimalType: '$typeString'. Defaulting to DecimalType(10,0).")
            DecimalType(10, 0) // Default decimal
        }
      case _ =>
        logger.warn(s"Unsupported or unknown data type string: '$typeString'. Defaulting to StringType.")
        StringType // Default to StringType if unknown
    }
  }
}
