package com.example.etl.config

// Schema Mapping
case class SchemaMappingRule(
    sourceName: String,
    targetName: String,
    targetType: String, // Spark SQL type string e.g. "StringType", "IntegerType", "DecimalType(10,2)"
    nullable: Option[Boolean] = Some(true),
    maxLength: Option[Int] = None, // For StringType
    format: Option[String] = None // For date/timestamp types
)

// Data Quality Rules
sealed trait DataQualityRule {
    def columnName: String
    def dqType: String
}

case class NotNullRule(columnName: String, dqType: String = "NOT_NULL") extends DataQualityRule
case class RegexRule(columnName: String, pattern: String, dqType: String = "REGEX") extends DataQualityRule
case class TypeCheckRule(columnName: String, expectedSparkType: String, dqType: String = "TYPE_CHECK") extends DataQualityRule
case class ValueSetRule(columnName: String, allowedValues: List[String], dqType: String = "VALUE_SET") extends DataQualityRule
case class LengthCheckRule(columnName: String, minLength: Option[Int] = None, maxLength: Option[Int] = None, dqType: String = "LENGTH_CHECK") extends DataQualityRule {
    require(minLength.isDefined || maxLength.isDefined, "Either minLength or maxLength must be defined for LengthCheckRule")
}
// Simple MaxLength only rule for convenience
case class MaxLengthRule(columnName: String, maxLength: Int, dqType: String = "MAX_LENGTH") extends DataQualityRule


// API Configuration Models
case class ApiHttpHeader(name: String, value: String)

sealed trait ApiPaginationStrategy
object ApiPaginationStrategy {
    case object NextPageUrl extends ApiPaginationStrategy { override def toString = "NEXT_PAGE_URL" }
    case object PageNumberLimit extends ApiPaginationStrategy { override def toString = "PAGE_NUMBER_LIMIT" }
    case object OffsetLimit extends ApiPaginationStrategy { override def toString = "OFFSET_LIMIT" }

    def fromString(s: String): ApiPaginationStrategy = s.toUpperCase match {
        case "NEXT_PAGE_URL" => NextPageUrl
        case "PAGE_NUMBER_LIMIT" => PageNumberLimit
        case "OFFSET_LIMIT" => OffsetLimit
        case _ => throw new IllegalArgumentException(s"Unknown API pagination strategy: $s")
    }
}

case class ApiPaginationConfig(
    strategy: String,
    nextPageUrlPath: Option[String] = None,
    pageNumberParam: Option[String] = None,
    pageSizeParam: Option[String] = None,
    pageSize: Option[Int] = None,
    maxPages: Option[Int] = None,
    offsetParam: Option[String] = None,
    limitParam: Option[String] = None,
    limitValue: Option[Int] = None
)

case class ApiSourceConfig(
    url: String,
    method: String,
    headers: Option[List[ApiHttpHeader]] = None,
    requestBody: Option[String] = None,
    apiPayloadType: String,
    dataPath: Option[String] = None,
    pagination: Option[ApiPaginationConfig] = None
)


// JSON Protocol for Spray JSON
import spray.json._
object JsonConfigProtocol extends DefaultJsonProtocol {
  implicit val schemaMappingRuleFormat = jsonFormat6(SchemaMappingRule)

  implicit object DataQualityRuleFormat extends RootJsonFormat[DataQualityRule] {
    def write(obj: DataQualityRule): JsValue = obj match {
      case r: NotNullRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("NOT_NULL"))
      case r: RegexRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("REGEX"), "pattern" -> JsString(r.pattern))
      case r: TypeCheckRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("TYPE_CHECK"), "expectedSparkType" -> JsString(r.expectedSparkType))
      case r: ValueSetRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("VALUE_SET"), "allowedValues" -> r.allowedValues.toJson)
      case r: LengthCheckRule =>
        val fields = scala.collection.mutable.ListBuffer[(String, JsValue)]()
        fields += "columnName" -> JsString(r.columnName)
        fields += "dqType" -> JsString("LENGTH_CHECK")
        r.minLength.foreach(ml => fields += "minLength" -> JsNumber(ml))
        r.maxLength.foreach(ml => fields += "maxLength" -> JsNumber(ml))
        JsObject(fields.toMap)
      case r: MaxLengthRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("MAX_LENGTH"), "maxLength" -> JsNumber(r.maxLength))
    }

    def read(json: JsValue): DataQualityRule = {
      val jsObject = json.asJsObject
      val commonColumnName = jsObject.getFields("columnName") match {
          case Seq(JsString(cn)) => cn
          case _ => throw DeserializationException("DataQualityRule expected field: columnName as string")
      }
      jsObject.getFields("dqType") match {
        case Seq(JsString("NOT_NULL")) => NotNullRule(commonColumnName)
        case Seq(JsString("REGEX")) => RegexRule(commonColumnName, jsObject.fields("pattern").convertTo[String])
        case Seq(JsString("TYPE_CHECK")) => TypeCheckRule(commonColumnName, jsObject.fields("expectedSparkType").convertTo[String])
        case Seq(JsString("VALUE_SET")) => ValueSetRule(commonColumnName, jsObject.fields("allowedValues").convertTo[List[String]])
        case Seq(JsString("LENGTH_CHECK")) =>
            LengthCheckRule(commonColumnName,
                            jsObject.fields.get("minLength").map(_.convertTo[Int]),
                            jsObject.fields.get("maxLength").map(_.convertTo[Int]))
        case Seq(JsString("MAX_LENGTH")) => MaxLengthRule(commonColumnName, jsObject.fields("maxLength").convertTo[Int])
        case Seq(JsString(unknown)) => throw DeserializationException(s"Unknown DataQualityRule dqType: $unknown")
        case _ => throw DeserializationException("DataQualityRule expected field: dqType as string")
      }
    }
  }

  // Add formats for new API config models
  implicit val apiHttpHeaderFormat: RootJsonFormat[ApiHttpHeader] = jsonFormat2(ApiHttpHeader)
  implicit val apiPaginationConfigFormat: RootJsonFormat[ApiPaginationConfig] = jsonFormat8(ApiPaginationConfig)
  implicit val apiSourceConfigFormat: RootJsonFormat[ApiSourceConfig] = jsonFormat7(ApiSourceConfig)
}
